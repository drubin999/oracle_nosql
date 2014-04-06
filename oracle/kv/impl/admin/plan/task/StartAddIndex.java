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

package oracle.kv.impl.admin.plan.task;

import java.util.ArrayList;
import java.util.Arrays;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.plan.MetadataPlan;
import oracle.kv.impl.admin.plan.TablePlanGenerator;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;

import com.sleepycat.persist.model.Persistent;

/**
 * Creates a new index.  The index is not visible until it is populated.
 */
@Persistent
public class StartAddIndex extends UpdateMetadata<TableMetadata> {
    private static final long serialVersionUID = 1L;

    private /*final*/ String indexName;
    private /*final*/ String tableName;
    private /*final*/ String[] indexedFields;
    private /*final*/ String description;

    /**
     */
    @SuppressWarnings("unused")
    public StartAddIndex(MetadataPlan<TableMetadata> plan,
                         String indexName,
                         String tableName,
                         String[] indexedFields,
                         String description) {
        super(plan);
        this.indexName = indexName;
        this.tableName = tableName;
        this.indexedFields = indexedFields;
        this.description = description;

        /*
         * Make sure that the table metadata exists and validate that (1) the
         * table exists, 2) the index does not exist and (2) the state passed
         * in can be used for the index.  The simplest way to do the latter is
         * to create a transient copy of the index.
         */
        final TableMetadata md = plan.getMetadata();
        if (md == null) {
            throw new IllegalStateException("Table metadata not found");
        }
        final TableImpl table = md.getTable(tableName);

        if (table == null) {
            throw new IllegalCommandException
                ("AddIndex: table does not exist: " + tableName);
        }
        if (table.getIndex(indexName) != null) {
            throw new IllegalCommandException
                    ("AddIndex: index: " + indexName +
                     " already exists in table: " + tableName);
        }
        new IndexImpl(indexName, table,
                      new ArrayList<String>(Arrays.asList(indexedFields)),
                      description);
    }

    /*
     * No-arg ctor for use by DPL.
     */
    @SuppressWarnings("unused")
    private StartAddIndex() {
    }

    @Override
    protected TableMetadata updateMetadata() {
        final TableMetadata md = plan.getMetadata();
        if (md == null) {
            throw new IllegalStateException("Table metadata not found");
        }
        final TableImpl table = md.getTable(tableName);
	if (table == null) {
            throw new IllegalStateException("Table " + tableName +
                                            " is missing from " + md);
	}
        if (table.getIndex(indexName) == null) {
            md.addIndex(indexName,
                        tableName,
                        new ArrayList<String>(Arrays.asList(indexedFields)),
                        description);
            plan.getAdmin().saveMetadata(md, plan);
        }
        return md;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    public String toString() {
        return TablePlanGenerator.makeName("StartAddIndex", tableName,
                                           indexName);
    }
}
