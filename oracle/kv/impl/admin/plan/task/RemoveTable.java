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

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.plan.MetadataPlan;
import oracle.kv.impl.admin.plan.TablePlanGenerator;
import oracle.kv.impl.api.table.TableMetadata;

import com.sleepycat.persist.model.Persistent;
import oracle.kv.impl.api.table.TableImpl;

/**
 * Remove/drop a table
 */
@Persistent
public class RemoveTable extends UpdateMetadata<TableMetadata> {
    private static final long serialVersionUID = 1L;

    private /*final*/ String tableName;
    private /*final*/ boolean markForDelete;

    /**
     */
    public RemoveTable(MetadataPlan<TableMetadata> plan,
                       String tableName,
                       boolean markForDelete) {
        super(plan);

        /*
         * Caller verifies parameters
         */
        this.tableName = tableName;
        this.markForDelete = markForDelete;

        final TableMetadata md = plan.getMetadata();

        if (md != null) {
            md.checkForRemove(tableName, true);
            return;
        }
        throw new IllegalCommandException
            ("Table does not exist: " + tableName);
    }

    /*
     * No-arg ctor for use by DPL.
     */
    @SuppressWarnings("unused")
    private RemoveTable() {
    }

    @Override
    protected TableMetadata updateMetadata() {
        final TableMetadata md = plan.getMetadata();
        if (md == null) {
            throw new IllegalStateException("Table metadata not found");
        }

        /*
         * See if the table is still present.  This will not throw if the
         * table is absent. Return the metadata so that it is broadcast, just
         * in case this is a re-execute.
         */
        final TableImpl table = md.getTable(tableName);
        if (table != null) {
            md.dropTable(tableName, markForDelete);
            plan.getAdmin().saveMetadata(md, plan);
        }
        return md;
    }

    @Override
    public String toString() {
        return TablePlanGenerator.makeName("RemoveTable", tableName, null);
    }
}
