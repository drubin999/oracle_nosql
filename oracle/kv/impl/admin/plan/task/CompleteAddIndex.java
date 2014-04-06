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

import oracle.kv.impl.admin.plan.MetadataPlan;
import oracle.kv.impl.admin.plan.TablePlanGenerator;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.TableMetadata;

import com.sleepycat.persist.model.Persistent;

/**
 * Completes index addition, making the index publicly visible.
 */
@Persistent
public class CompleteAddIndex extends UpdateMetadata<TableMetadata> {
    private static final long serialVersionUID = 1L;

    private /*final*/ String indexName;
    private /*final*/ String tableName;

    /**
     */
    public CompleteAddIndex(MetadataPlan<TableMetadata> plan,
                            String indexName,
                            String tableName) {
        super(plan);
        this.indexName = indexName;
        this.tableName = tableName;
    }

    /*
     * No-arg ctor for use by DPL.
     */
    @SuppressWarnings("unused")
    private CompleteAddIndex() {
    }

    @Override
    protected TableMetadata updateMetadata() {
        final TableMetadata md = plan.getMetadata();

        if (md == null) {
            throw new IllegalStateException("Table metadata not found");
        }
        if (!md.updateIndexStatus(indexName,
                                  tableName,
                                  IndexImpl.IndexStatus.READY)) {
            throw new IllegalStateException
                ("CompleteAddIndex: index does not exists: " +
                 indexName + " in table: " + tableName);
        }
        plan.getAdmin().saveMetadata(md, plan);
        return md;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    public String toString() {
        return TablePlanGenerator.makeName("CompleteAddIndex", tableName,
                                           indexName);
    }
}
