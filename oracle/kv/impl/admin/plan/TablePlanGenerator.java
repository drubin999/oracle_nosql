/*-
 *
 *  This file is part of Oracle NoSQL Database
 *  Copyright (C) 2011, 2014 Oracle and/or its affiliates.  All rights reserved.
 *
 *  Oracle NoSQL Database is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU Affero General Public License
 *  as published by the Free Software Foundation, version 3.
 *
 *  Oracle NoSQL Database is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public
 *  License in the LICENSE file along with Oracle NoSQL Database.  If not,
 *  see <http://www.gnu.org/licenses/>.
 *
 *  An active Oracle commercial licensing agreement for this product
 *  supercedes this license.
 *
 *  For more information please contact:
 *
 *  Vice President Legal, Development
 *  Oracle America, Inc.
 *  5OP-10
 *  500 Oracle Parkway
 *  Redwood Shores, CA 94065
 *
 *  or
 *
 *  berkeleydb-info_us@oracle.com
 *
 *  [This line intentionally left blank.]
 *  [This line intentionally left blank.]
 *  [This line intentionally left blank.]
 *  [This line intentionally left blank.]
 *  [This line intentionally left blank.]
 *  [This line intentionally left blank.]
 *  EOF
 *
 */

package oracle.kv.impl.admin.plan;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.plan.task.AddTable;
import oracle.kv.impl.admin.plan.task.CompleteAddIndex;
import oracle.kv.impl.admin.plan.task.EvolveTable;
import oracle.kv.impl.admin.plan.task.ParallelBundle;
import oracle.kv.impl.admin.plan.task.RemoveIndex;
import oracle.kv.impl.admin.plan.task.RemoveTable;
import oracle.kv.impl.admin.plan.task.StartAddIndex;
import oracle.kv.impl.admin.plan.task.WaitForAddIndex;
import oracle.kv.impl.admin.plan.task.WaitForRemoveTableData;
import oracle.kv.impl.api.table.FieldMap;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.Topology;

/**
 * Static utility class for generating plans for secondary indexes.
 *
 * Exception handling note.  This code runs in the context of the admin service
 * and the rule in the admin is that non-fatal runtime exceptions are thrown as
 * IllegalCommandException.  For that reason these methods catch and rethrow
 * exceptions from called methods.
 */
public class TablePlanGenerator {

    /* Prevent construction */
    private TablePlanGenerator() {}

    /**
     * Creates a plan to add a table.
     */
    static DeployTableMetadataPlan
        createAddTablePlan(AtomicInteger idGen,
                           String planName,
                           Planner planner,
                           String tableName,
                           String parentName,
                           FieldMap fieldMap,
                           List<String> primaryKey,
                           List<String> majorKey,
                           boolean r2compat,
                           int schemaId,
                           String description) {

        if (tableName == null) {
            throw new IllegalCommandException("Table name cannot be null");
        }
        if (fieldMap == null || fieldMap.isEmpty()) {
            throw new IllegalCommandException("Table has no defined fields");
        }

        final DeployTableMetadataPlan plan =
            new DeployTableMetadataPlan(idGen,
                                        makeName(planName, tableName, null),
                                        planner);

        tableName = plan.getRealTableName(tableName);
        try {
            plan.addTask(new AddTable(plan,
                                      tableName,
                                      parentName,
                                      fieldMap,
                                      primaryKey,
                                      majorKey,
                                      r2compat,
                                      schemaId,
                                      description));
        } catch (IllegalArgumentException iae) {
            throw new IllegalCommandException
                ("Failed to add table: " + iae.getMessage(), iae);
        }
        return plan;
    }

    /**
     * Creates a plan to evolve a table.
     *
     * The table version is the version of the table used as a basis for this
     * evolution.  It is used to verify that only the latest version of a table
     * is evolved.
     */
    static DeployTableMetadataPlan
        createEvolveTablePlan(AtomicInteger idGen,
                              String planName,
                              Planner planner,
                              String tableName,
                              int tableVersion,
                              FieldMap fieldMap) {
        checkTable(tableName);
        if (fieldMap == null || fieldMap.isEmpty()) {
            throw new IllegalCommandException("Fields cannot be null or empty");
        }

        final DeployTableMetadataPlan plan =
            new DeployTableMetadataPlan(idGen,
                                        makeName(planName, tableName, null),
                                        planner);

        tableName = plan.getRealTableName(tableName);
        try {
            plan.addTask(new EvolveTable(plan,
                                         tableName,
                                         tableVersion,
                                         fieldMap));
        } catch (IllegalArgumentException iae) {
            throw new IllegalCommandException
                ("Failed to evolve table: " + iae.getMessage(), iae);
        }

        return plan;
    }

    /**
     * Creates a plan to remove a table.
     */
    static DeployTableMetadataPlan createRemoveTablePlan(AtomicInteger idGen,
                                                         String planName,
                                                         Planner planner,
                                                         Topology topology,
                                                         String tableName,
                                                         boolean removeData) {
        checkTable(tableName);

        final DeployTableMetadataPlan plan =
            new DeployTableMetadataPlan(idGen,
                                        makeName(planName, tableName, null),
                                        planner);
        tableName = plan.getRealTableName(tableName);

        /*
         * If we need to remove data, we first mark the table for deletion and
         * broadcast that change. This will trigger the RNs to remove the
         * table data from it's respective shard. The plan will wait for all
         * RNs to finish. Once the data is deleted, the table object can be
         * removed.
         */
        try {
            addRemoveIndexTasks(plan, tableName);
            if (removeData) {
                plan.addTask(new RemoveTable(plan, tableName, true));

                final ParallelBundle bundle = new ParallelBundle();
                for (RepGroupId id : topology.getRepGroupIds()) {
                    bundle.addTask(new WaitForRemoveTableData(plan,
                                                              id,
                                                              tableName));
                }
                if (!bundle.isEmpty()) {
                    plan.addTask(bundle);
                }
            }
            plan.addTask(new RemoveTable(plan, tableName, false));
        } catch (IllegalArgumentException iae) {
            throw new IllegalCommandException
                ("Failed to remove table: " + iae.getMessage(), iae);
        }

        return plan;
    }

    /**
     * Add a task to remove each index defined on the table.  Do this before
     * removing data as indexes are affected and performance would be quite bad
     * otherwise.
     */
    static private void addRemoveIndexTasks(final DeployTableMetadataPlan plan,
                                            final String tableName) {
        final TableMetadata md = plan.getMetadata();
        if (md != null) {
            TableImpl table = md.checkForRemove(tableName, true);
            for (String indexName : table.getIndexes().keySet()) {
                try {
                    plan.addTask(new RemoveIndex(plan, indexName, tableName));
                } catch (IllegalArgumentException iae) {
                    throw new IllegalCommandException
                        ("Failed to remove index: " + iae.getMessage(), iae);
                }
            }
        }
    }

    /**
     * Creates a plan to add an index.
     * This operates in 3 parts
     * 1.  Update metadata to include the new index, which is in state
     *     "Populating". In that state it will be populated and used on
     *      RepNodes but will not appear to users in metadata.
     * 2.  Ask all RepNode masters to populate the index in a parallel bundle
     * 3.  Update the metadata again with the state "Ready" on the index to make
     *     it visible to users.
     */
    static DeployTableMetadataPlan createAddIndexPlan(AtomicInteger idGen,
                                                      String planName,
                                                      Planner planner,
                                                      Topology topology,
                                                      String indexName,
                                                      String tableName,
                                                      String[] indexedFields,
                                                      String description) {
        checkTable(tableName);
        checkIndex(indexName);
        if (indexedFields == null) {    // TODO - check for empty?
            throw new IllegalCommandException("Indexed fields cannot be null");
        }

        final DeployTableMetadataPlan plan =
            new DeployTableMetadataPlan(idGen,
                                        makeName(planName, tableName, indexName),
                                        planner);
        tableName = plan.getRealTableName(tableName);

        /*
         * Create the index, not-yet-visible
         */
        try {
            plan.addTask(new StartAddIndex(plan,
                                           indexName,
                                           tableName,
                                           indexedFields,
                                           description));

            /*
             * Wait for the added index to be populated. This may take a while.
             */
            final ParallelBundle bundle = new ParallelBundle();
            for (RepGroupId id : topology.getRepGroupIds()) {
                bundle.addTask(new WaitForAddIndex(plan,
                                                   id,
                                                   indexName,
                                                   tableName));
            }
            if (!bundle.isEmpty()) {
                plan.addTask(bundle);
            }

            /*
             * Complete the job, make the index visible
             */
            plan.addTask(new CompleteAddIndex(plan,
                                              indexName,
                                              tableName));
        } catch (IllegalArgumentException iae) {
            throw new IllegalCommandException
                ("Failed to add index: " + iae.getMessage(), iae);
        }

        return plan;
    }

    /**
     * Creates a plan to remove an index.
     */
    @SuppressWarnings("unused")
	static DeployTableMetadataPlan
        createRemoveIndexPlan(AtomicInteger idGen,
                              String planName,
                              BasicPlannerImpl planner,
                              Topology topology,
                              String indexName,
                              String tableName) {
        checkTable(tableName);
        checkIndex(indexName);

        final DeployTableMetadataPlan plan =
            new DeployTableMetadataPlan(idGen,
                                        makeName(planName, tableName, indexName),
                                        planner);

        tableName = plan.getRealTableName(tableName);
        try {
            plan.addTask(new RemoveIndex(plan, indexName, tableName));
        } catch (IllegalArgumentException iae) {
            throw new IllegalCommandException
                ("Failed to remove index: " + iae.getMessage(), iae);
        }

        return plan;
    }

    private static void checkTable(String tableName) {
        if (tableName == null) {
            throw new IllegalCommandException("Table path cannot be null");
        }
    }

    private static void checkIndex(String indexName) {
        if (indexName == null) {
            throw new IllegalCommandException("Index name cannot be null");
        }
    }

    /**
     * Create a plan name that puts more information in the log stream.
     */
    public static String makeName(String name, String tableName,
                                  String indexName) {
        StringBuilder sb = new StringBuilder();
        sb.append(name);
        sb.append(":");
        sb.append(tableName);
        if (indexName != null) {
            sb.append(":");
            sb.append(indexName);
        }
        return sb.toString();
    }
}
