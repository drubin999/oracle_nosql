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
import java.util.logging.Logger;

import oracle.kv.impl.admin.PlannerAdmin;
import oracle.kv.impl.admin.plan.DeployTopoPlan;
import oracle.kv.impl.topo.Partition;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.Topology;

import com.sleepycat.persist.model.Persistent;

/**
 * Add new partitions to the topology, only done for an initial
 * deployment. This is purely an update to the topology stored in the
 * administrative db.
 */
@Persistent
public class AddPartitions extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    protected DeployTopoPlan plan;
    private List<Integer> partitionCounts;
    private int totalPartitions;

    /**
     * @param plan the owning plan
     * @param partitionCounts A list of the number of partitions per shard,
     * listed in ordinal order.
     * @param totalPartitions a count of the total number of partitions that
     * should be created. It's equals to the sum of all the values in the
     * partitionCounts list.
     */
    public AddPartitions(DeployTopoPlan plan,
                         List<Integer> partitionCounts,
                         int totalPartitions) {
        this.plan = plan;
        this.partitionCounts = partitionCounts;
        this.totalPartitions = totalPartitions;
    }

    /*
     * No-arg ctor for use by DPL.
     */
    AddPartitions() {
    }

    @Override
    public State doWork()
        throws Exception {

        Logger logger = plan.getLogger();

        PlannerAdmin plannerAdmin = plan.getAdmin();
        Topology current = plannerAdmin.getCurrentTopology();

        /*
         * If this plan is being repeated, this task may have executed
         * successfully before.
         */
        int numExistingPartitions = current.getPartitionMap().size();
        if (numExistingPartitions == totalPartitions) {
            logger.info("Partitions already created.");
            return State.SUCCEEDED;
        }

        /*
         * At this point, we expect the current topology to have no partitions.
         * Having a non-zero number of partitions that is not the expected
         * value is unexpected, and means that a previous plan execution was
         * not correctly cleaned up.
         */
        if ((numExistingPartitions != totalPartitions) &&
            (numExistingPartitions != 0)) {
            throw new IllegalStateException
                ("Trying to create " + totalPartitions +
                 " but this topology unexpectedly already has " +
                 numExistingPartitions + ". Store must be reinitialized");
        }

        /*
         * We expect the partition placements to mimic those from the
         * candidate precisely.
         */
        for (int whichShard = 0; whichShard < partitionCounts.size();
             whichShard++) {
            int numPartitionsForShard = partitionCounts.get(whichShard);
            RepGroupId rgId = plan.getShardId(whichShard);

            for (int i = 0; i < numPartitionsForShard; i++) {
                current.add(new Partition(current.get(rgId)));
            }
        }

        plannerAdmin.saveTopo(current, plan.getDeployedInfo(), plan);
        if (!Utils.broadcastTopoChangesToRNs(logger, current,
                                            "Initializing new partitions",
                                             plannerAdmin.getParams().
                                                        getAdminParams(),
                                             plan)) {
            return State.INTERRUPTED;
        }
        return State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    public String toString() {
        return super.toString() +  " totalPartitions = " + totalPartitions;
    }
}
