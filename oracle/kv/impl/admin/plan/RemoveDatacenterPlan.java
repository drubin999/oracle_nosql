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

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.plan.task.ConfirmDatacenterStatus;
import oracle.kv.impl.admin.plan.task.RemoveDatacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.Topology;

import com.sleepycat.persist.model.Persistent;

/**
 * Remove a datacenter. Only permitted for datacenters that are empty.
 */
@Persistent
public class RemoveDatacenterPlan extends TopologyPlan {

    private static final long serialVersionUID = 1L;

    /* The original inputs. */
    private DatacenterId targetId;

    public RemoveDatacenterPlan(AtomicInteger idGen,
                                String planName,
                                Planner planner,
                                Topology topology,
                                DatacenterId targetId) {

        super(idGen, planName, planner, topology);

        if (targetId == null) {
            throw new IllegalArgumentException("null targetId");
        }

        this.targetId = targetId;

        /* Confirm that the target datacenter exists. */
        validate();

        /* Confirm that the datacenter is empty. */
        final String infoMsg = "Cannot remove non-empty zone " +
            "[id=" + targetId +
            " name=" + topology.get(targetId).getName() + "].";

        addTask(new ConfirmDatacenterStatus(this, targetId, infoMsg));

        /* Remove the specified datacenter. */
        addTask(new RemoveDatacenter(this, targetId));
    }

    /*
     * No-arg ctor for use by DPL.
     */
    @SuppressWarnings("unused")
    private RemoveDatacenterPlan() {
    }

    @Override
    public String getDefaultName() {
        return "Remove zone";
    }

    private void validate() {

        /* Confirm that the target datacenter exists in the topology. */
        final Topology topo = getTopology();

        if (topo.get(targetId) == null) {
            throw new IllegalCommandException
                ("Zone [id=" + targetId + "] does not exist in the " +
                 "topology and so cannot be removed");
        }
    }

    @Override
    void preExecutionSave() {

        /* Nothing to do, topology is saved after the datacenter is removed. */
    }

    public DatacenterId getTarget() {
        return targetId;
    }
}
