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

import java.util.Set;

import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;

import com.sleepycat.persist.model.Persistent;

/**
 * Task for creating and starting one or more RepNodes on a particular Storage
 * Node.
 * @Deprecated at of R2
 *
 * version 0: original
 * version 1: Changed inheritance chain.
 */
@Persistent(version=1)
public abstract class DeployRepNode extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    protected AbstractPlan plan;
    protected StorageNodeId snId;
    protected String snDescriptor;

    /**
     * Creates a task for creating and starting a new RepNode.
     * @Deprecated
     */
    @Deprecated
    public DeployRepNode(AbstractPlan plan,
                         StorageNodeId snId) {
        super();
        this.plan = plan;
        this.snId = snId;

        /* A more descriptive label used for error messages, etc. */
        StorageNodeParams snp = plan.getAdmin().getStorageNodeParams(snId);
        snDescriptor = snp.displaySNIdAndHost();
    }

    abstract Set<RepNodeId> getTargets();

    /*
     * No-arg ctor for use by DPL.
     */
    DeployRepNode() {
    }

    @Override
    public State doWork()
        throws Exception {
        throw new IllegalStateException("Deprecated as of R2");
    }

    @Override
    public boolean continuePastError() {
        return false;
    }
}
