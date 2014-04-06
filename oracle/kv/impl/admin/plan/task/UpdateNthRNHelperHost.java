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

import oracle.kv.impl.admin.PlannerAdmin;
import oracle.kv.impl.admin.plan.DeployTopoPlan;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;

import com.sleepycat.persist.model.Persistent;

/**
 * A task for asking a RepNode to update its helper hosts to include all its
 * peers.Because the topology is not written until task execution time, this
 * flavor of UpdateNthRNHelperHost must wait until task run time to know the
 * actual RepGroupId and RepNodeId to use.
 */
@Persistent
public class UpdateNthRNHelperHost extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private DeployTopoPlan plan;

    private int planShardIdx;

    private int nthRN;

    public UpdateNthRNHelperHost(DeployTopoPlan plan, 
                                 int planShardIdx,
                                 int nthRN) {

        super();
        this.plan = plan;
        this.planShardIdx = planShardIdx;
        this.nthRN = nthRN;
    }

    /*
     * No-arg ctor for use by DPL.
     */
    @SuppressWarnings("unused")
    private UpdateNthRNHelperHost() {
    }

    /**
     */
    @Override
    public State doWork()
        throws Exception {

        PlannerAdmin admin = plan.getAdmin();
        Topology topo = admin.getCurrentTopology();
        RepGroupId rgId = plan.getShardId(planShardIdx);
        List<RepNodeId> rnList = topo.getSortedRepNodeIds(rgId);
        RepNodeId rnId = rnList.get(nthRN);

        Utils.updateHelperHost(admin, topo, rgId, rnId, plan.getLogger());
        return State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }
}
