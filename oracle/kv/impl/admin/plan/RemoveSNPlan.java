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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.plan.task.ConfirmSNStatus;
import oracle.kv.impl.admin.plan.task.RemoveSN;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;

import com.sleepycat.persist.model.Persistent;

/**
 * Remove a storage node permanently. Only permitted for storage nodes that
 * are not hosting any RNs or Admins.
 */
@Persistent
public class RemoveSNPlan extends TopologyPlan {

    private static final long serialVersionUID = 1L;

    /* The original inputs. */
    private StorageNodeId target;


    public RemoveSNPlan(AtomicInteger idGen,
                        String planName,
                        Planner planner,
                        Topology topology,
                        StorageNodeId target) {

        super(idGen, planName, planner, topology);
        this.target = target;

        /*
         * Check the target exists and is not hosting any components.
         */
        validate();

        /* Confirm that the old node is dead. */
        addTask(new ConfirmSNStatus(this,
                                    target,
                                    false /* shouldBeRunning*/,
                                    "Please ensure that " + target +
                                    " is stopped before attempting to remove " +
                                    "it."));
        addTask(new RemoveSN(this, target));
    }

    /*
     * No-arg ctor for use by DPL.
     */
    @SuppressWarnings("unused")
    private RemoveSNPlan() {
    }


    @Override
    public String getDefaultName() {
        return "Remove Storage Node";
    }
    private void validate() {

        Topology topology = getTopology();
        Parameters parameters = planner.getAdmin().getCurrentParameters();

        /* Confirm that the target exists in the params and topology. */
        if (topology.get(target) == null) {
            throw new IllegalCommandException
                (target + " does not exist in the topology and cannot " +
                 "be removed");
        }

        if (parameters.get(target) == null) {
            throw new IllegalCommandException
                (target + " does not exist in the parameters and cannot " +
                 "be migrated");
        }

        /*
         * This target should not host any services. Check the topology and
         * parameters for the presence of a RN or Admin.
         */
        List<ResourceId> existingServices = new ArrayList<ResourceId>();

        for (AdminParams ap: parameters.getAdminParams()) {
            if (ap.getStorageNodeId().equals(target)) {
                existingServices.add(ap.getAdminId());
            }
        }

        for (RepNodeParams rnp: parameters.getRepNodeParams()) {
            if (rnp.getStorageNodeId().equals(target)) {
                existingServices.add(rnp.getRepNodeId());
            }
        }

        if (existingServices.size() > 0) {
            throw new IllegalCommandException
                ("Cannot remove " + target + " because it hosts these " +
                 " components: " + existingServices);
        }
    }

    @Override
    void preExecutionSave() {

        /* 
         * Nothing to do, topology and params are saved after the SN is removed.
         */        
    }

    public StorageNodeId getTarget() {
        return target;
    }
}
