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

import java.util.concurrent.atomic.AtomicInteger;

import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;

import com.sleepycat.persist.model.Persistent;

/**
 * A general purpose plan for executing changes to table metadata. By default
 * this plan will lock out other table metadata plans and elasticity plans.
 */
@Persistent
public class DeployTableMetadataPlan extends MetadataPlan<TableMetadata> {
    private static final long serialVersionUID = 1L;

    DeployTableMetadataPlan(AtomicInteger idGen,
                            String planName,
                            Planner planner) {
        super(idGen, planName, planner);
    }

    /*
     * No-arg ctor for use by DPL.
     */
    @SuppressWarnings("unused")
    private DeployTableMetadataPlan() {
    }

    @Override
    protected Class<TableMetadata> getMetadataClass() {
        return TableMetadata.class;
    }

    @Override
    public MetadataType getMetadataType() {
        return MetadataType.TABLE;
    }

    @Override
    public void preExecutionSave() {
        /* Nothing to save before execution. */
    }

    @Override
    public String getDefaultName() {
        return "Deploy Table Metadata";
    }

    @Override
    public boolean isExclusive() {
      return false;
    }

    @Override
    public void getCatalogLocks() {
        /*
         * Several table metadata plans can't take place at the same time as
         * a partition migration.
         */
        planner.lockElasticity(getId(), getName());
        getPerTaskLocks();
    }

    @Override
    void stripForDisplay() {

    }

    /**
     * Returns the real table name for the table name, making it insensitive to
     * case.  If the table does not exist, return the argument and allow the
     * caller to continue.  This is called for plans where the table may or may
     * not exist.
     */
    String getRealTableName(String tableName) {
        final TableMetadata md = getMetadata();
        if (md != null) {
            TableImpl table = md.getTable(tableName, false);
            if (table != null) {
                return table.getFullName();
            }
        }
        return tableName;
    }
}
