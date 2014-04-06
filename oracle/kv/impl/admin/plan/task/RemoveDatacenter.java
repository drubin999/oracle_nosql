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
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.Topology;

import com.sleepycat.persist.model.Persistent;

/**
 * Update the topology to remove the desired datacenter. This is purely a
 * change to the admin database. It is assumed that the target datacenter is
 * already empty, and that there is no need to issue remote requests to stop
 * storage nodes belonging to that datacenter.
 */
@Persistent
public class RemoveDatacenter extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private DatacenterId targetId;
    private TopologyPlan plan;

    public RemoveDatacenter(TopologyPlan plan, DatacenterId targetId) {

        super();
        this.plan = plan;
        this.targetId = targetId;
    }

    /*
     * No-arg ctor for use by DPL.
     */
    @SuppressWarnings("unused")
    private RemoveDatacenter() {
    }

    @Override
    public State doWork()
        throws Exception {

        /*
         * At this point, it has been determined that the datacenter is
         * empty. So the target datacenter can be removed from the topology.
         */

        final Topology currentTopo = plan.getTopology();
        currentTopo.remove(targetId);

        /*
         * Save the modified topology to the administrative db to preserve a
         * consistent view of the change. Note that if this plan has been
         * retried it's possible that the topology created by this task has
         * already been saved.
         */
        if (plan.isFirstExecutionAttempt()) {
            plan.getAdmin().saveTopoAndRemoveDatacenter(currentTopo,
                                                        plan.getDeployedInfo(),
                                                        targetId,
                                                        plan);
        }

        /* Send topology changes to all nodes.*/
        if (!Utils.broadcastTopoChangesToRNs
            (plan.getLogger(),
             currentTopo,
             "remove zone [id=" + targetId + "]",
             plan.getAdmin().getParams().getAdminParams(),
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
