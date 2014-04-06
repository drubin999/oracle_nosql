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

import oracle.kv.impl.admin.plan.TopologyPlan;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;

import com.sleepycat.persist.model.Persistent;

/**
 * Update parameters to disable all the services running on the target storage
 * node. This is purely a change to the admin database.We assume that the
 * target node is already down, and that there is no need to issue remote
 * requests to stop those services.
 *
 * Suppose we are migrating the services on SN1 -> SN20, which causes the
 * topology to change from version 5 -> 6, and suppose that succeeds. The basic
 * steps that occurred were:
 *
 *  1. create a new topo and params
 *  2. broadcast the topo changes
 *  3. ask the new SN to create the desired services.
 *
 * If we repeat this plan, the second plan execution will find that there are
 * no topology changes between what is desired and what is currently stored in
 * the admin db. We placidly accept this and continue nevertheless to do steps
 * 2 and 3, because we do not know whether the previous attempt was interrupted
 * between steps 1 and 2, or whether it succeeded. Because of that, if there
 * are no changes to the topology found, we wil broadcast the entire topology
 * instead of just the delta.
 */
@Persistent
public class RemoveSN extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private StorageNodeId target;
    private TopologyPlan plan;

    public RemoveSN(TopologyPlan plan,
                    StorageNodeId target) {

        super();
        this.plan = plan;
        this.target = target;
    }

    /*
     * No-arg ctor for use by DPL.
     */
    @SuppressWarnings("unused")
    private RemoveSN() {
    }

    @Override
    public State doWork()
        throws Exception {

        /*
         * By the time we get here, we have verified that there are no services
         * hosted on this SN, and that it has been stopped and does not respond
         * to a ping. Remove this SN from any owning storage node pools, the
         * Admin params, and the topology.
         */

        /* Remove it from the storage node pools */
        Topology currentTopo = plan.getTopology();
        currentTopo.remove(target);

        /*
         * Save topo and params to the administrative db to preserve a
         * consistent view of the change. Note that if this plan has been
         * retried it's possible that the topology created by this plan
         * has already been saved.
         */
        if (plan.isFirstExecutionAttempt()) {
            plan.getAdmin().saveTopoAndRemoveSN(currentTopo,
                                                plan.getDeployedInfo(),
                                                target, plan);
        }

        /* Send topology changes to all nodes.*/
        if (!Utils.broadcastTopoChangesToRNs(plan.getLogger(),
                                             currentTopo,
                                             "remove SN " + target,
                                             plan.getAdmin().getParams().
                                                            getAdminParams(),
                                             plan)) {
            return Task.State.INTERRUPTED;
        }
        return Task.State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }
}
