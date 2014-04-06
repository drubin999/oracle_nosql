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


import com.sleepycat.persist.model.Persistent;
import oracle.kv.impl.admin.PlannerAdmin;
import oracle.kv.impl.admin.plan.DeployTopoPlan;


/**
 * Broadcast the current topology to all RNs. This is typically done after a
 * series of tasks that modify the topology.
 */
@Persistent
public class BroadcastTopo extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    protected DeployTopoPlan plan;

    /**
     * Constructor.
     * @param plan the plan
     */
    public BroadcastTopo(DeployTopoPlan plan) {
        this.plan = plan;
    }

    /*
     * No-arg ctor for use by DPL.
     */
    BroadcastTopo() {
    }

    @Override
    public State doWork() throws Exception {
        final PlannerAdmin pa = plan.getAdmin();
        if (!Utils.broadcastTopoChangesToRNs(plan.getLogger(),
                                             pa.getCurrentTopology(),
                                             toString(),
                                             pa.getParams().getAdminParams(),
                                             plan)) {
            return State.INTERRUPTED;
        }
        return State.SUCCEEDED;
    }

    /**
     * Stop the plan if this task fails. Although there are other mechanisms
     * that will let the topology trickle down to the node, the barrier for
     * broadcast success is low (only a small percent of the RNs need to
     * acknowledge the topology), and plan execution will be easier to
     * understand without failures.
     */
    @Override
    public boolean continuePastError() {
        return false;
    }
}
