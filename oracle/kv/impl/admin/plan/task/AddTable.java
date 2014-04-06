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

import java.util.List;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.plan.MetadataPlan;
import oracle.kv.impl.admin.plan.TablePlanGenerator;
import oracle.kv.impl.api.table.FieldMap;
import oracle.kv.impl.api.table.TableMetadata;

import com.sleepycat.persist.model.Persistent;

/**
 * Adds a table
 */
@Persistent
public class AddTable extends UpdateMetadata<TableMetadata> {
    private static final long serialVersionUID = 1L;

    private /*final*/ String tableName;
    private /*final*/ String parentName;
    private /*final*/ List<String> primaryKey;
    private /*final*/ List<String> majorKey;
    private /*final*/ FieldMap fieldMap;
    private /*final*/ boolean r2compat;
    private /*final*/ int schemaId;
    private /*final*/ String description;

    /**
     */
    public AddTable(MetadataPlan<TableMetadata> plan,
                    String tableName,
                    String parentName,
                    FieldMap fieldMap,
                    List<String> primaryKey,
                    List<String> majorKey,
                    boolean r2compat,
                    int schemaId,
                    String description) {
        super(plan);

        /*
         * Caller verifies parameters
         */

        this.tableName = tableName;
        this.parentName = parentName;
        this.primaryKey = primaryKey;
        this.majorKey = majorKey;
        this.fieldMap = fieldMap;
        this.r2compat = r2compat;
        this.schemaId = schemaId;
        this.description = description;

        final TableMetadata md = plan.getMetadata();
        if ((md != null) && md.tableExists(tableName, parentName)) {
            throw new IllegalCommandException
            ("Table already exists: " +
             TableMetadata.makeQualifiedName(tableName, parentName));
        }
    }

    /*
     * No-arg ctor for use by DPL.
     */
    @SuppressWarnings("unused")
    private AddTable() {
    }

    @Override
    protected TableMetadata updateMetadata() {
        TableMetadata md = plan.getMetadata();
        if (md == null) {
            md = new TableMetadata(true);
        }

        /*
         * If exist, then we are done. Return the metadata so that it is
         * broadcast, just in case this is a re-execute.
         */
        if (!md.tableExists(tableName, parentName)) {
            md.addTable(tableName,    // TODO the add table method does not check for dup
                        parentName,
                        primaryKey,
                        majorKey,
                        fieldMap,
                        r2compat,
                        schemaId,
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
        String name = TableMetadata.makeQualifiedName(tableName, parentName);
        return TablePlanGenerator.makeName("AddTable", name, null);
    }
}
