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

import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import com.sleepycat.persist.model.Persistent;

/**
 * Start a repNode. Assumes the node has already been created.
 *
 * version 0: original.
 * version 1: Changed inheritance chain.
 */
@Persistent(version=1)
public class StartRepNode extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private AbstractPlan plan;
    private StorageNodeId snId;
    private RepNodeId repNodeId;
    private boolean continuePastError;

    public StartRepNode(AbstractPlan plan,
                        StorageNodeId storageNodeId,
                        RepNodeId repNodeId,
                        boolean continuePastError) {
        super();
        this.plan = plan;
        this.snId = storageNodeId;
        this.repNodeId = repNodeId;
        this.continuePastError = continuePastError;
    }

    /* DPL */
    protected StartRepNode() {
    }

    @Override
    public State doWork()
        throws Exception {

        Utils.startRN(plan, snId, repNodeId);
        return State.SUCCEEDED;
    }


    @Override
    public boolean continuePastError() {
        return continuePastError;
    }

    @Override
    public void lockTopoComponents(Planner planner) {
        planner.lockRN(plan.getId(), plan.getName(), repNodeId);
    }
}
