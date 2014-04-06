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
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.task.DeploySN;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;

import com.sleepycat.persist.model.Persistent;

@Persistent
public class DeploySNPlan extends TopologyPlan {

    private static final long serialVersionUID = 1L;

    /* The original inputs. */
    private StorageNodeParams inputSNP;

    private StorageNodeParams newSNParams;
    private StorageNodeId snId;

    public DeploySNPlan(AtomicInteger idGen,
                        String planName,
                        Planner planner,
                        Topology topology,
                        DatacenterId datacenterId,
                        StorageNodeParams inputSNP) {
        super(idGen, planName, planner, topology);
        this.inputSNP = inputSNP;

        /* Error Checking */

        Datacenter dc = topology.get(datacenterId);
        if (dc == null) {
            throw new IllegalCommandException
                (datacenterId +
                 " is not a valid Zone id.  " +
                 "Please provide the id of an existing Zone.");
        }

        /* Create an updated topology and DataCenterParams */
        StorageNode sn = new StorageNode(dc,
                                         inputSNP.getHostname(),
                                         inputSNP.getRegistryPort());
        /**
         * Copy only those parameters that are relevant to the Storage Node
         * being deployed.
         */
        newSNParams = new StorageNodeParams(inputSNP.getFilteredMap());

        StorageNode alreadyExists = alreadyInTopology(sn);
        StorageNode useStorageNode = null;
        boolean isFirst;
        if (alreadyExists != null) {
            useStorageNode = alreadyExists;
            guardAgainstDifferentParams(alreadyExists.getResourceId());

            /*
             * If this is the first SN in the topology, it will host the
             * admin.
             */
            isFirst = (getTopology().getStorageNodeMap().size() == 1);
        } else {

            /*
             * This SN does not exist in the topology. Do appropriate
             * checks. For example, we allow only a single StorageNode to be
             * deployed before an Admin is deployed.
             */

            isFirst = (topology.getStorageNodeMap().size() == 0);
            if (planner.getAdmin().getAdminCount() == 0 && !isFirst) {
                throw new IllegalCommandException
                    ("An Admin service instance must be deployed on the " +
                     "first deployed StorageNode before any further " +
                     "StorageNode deployments can take place.");
            }
            useStorageNode  = topology.add(sn);
        }

        snId = useStorageNode.getStorageNodeId();
        newSNParams.setStorageNodeId(snId);

        /* Create Tasks */
        addTask(new DeploySN(this, isFirst));

        /*
         * Note that we will save the topology and params after the task
         * executes and successfully creates and registers the SN. Most other
         * plans save topology and params before execution, to make sure that
         * the topology is consistent and saved in the admin db before any
         * kvstore component can access it. DeploySNPlan is a special case
         * where it is safe to store the topology after execution because only
         * one task is executed in the plan (and therefore no issues about
         * atomicity of tasks). Also, there can be no earlier reference to the
         * SN before the plan finishes.
         */
    }

    /*
     * No-arg ctor for use by DPL.
     */
    @SuppressWarnings("unused")
    private DeploySNPlan() {
    }

    /**
     * Additional parameters are added to the storage node's param set
     * after the bootstrap/registration handshake is done. This method
     * returns the parameters instance that should be augmented.
     */
    public StorageNodeParams getRegistrationParams() {
        return newSNParams;
    }

    @Override
    public String getDefaultName() {
        return "Deploy Storage Node";
    }

    public StorageNodeId getStorageNodeId() {
        return snId;
    }

    /**
     * @return the hostname
     */
    public StorageNodeParams getInputStorageNodeParams() {
        return inputSNP;
    }

    /**
     * @return a StorageNode component object if this SN already exists in the
     * topology.
     */
    private StorageNode alreadyInTopology(StorageNode newStorageNode) {
        for (StorageNode s : getTopology().getStorageNodeMap().getAll()) {
            if (newStorageNode.propertiesEquals(s)) {
                return s;
            }
        }
        return null;
    }

    /**
     * @throw IllegalCommandException if params for this SN already exist, and
     * are different from the new ones proposed.
     */
    private void guardAgainstDifferentParams(StorageNodeId existingId) {

        StorageNodeParams existingParams =
            getAdmin().getStorageNodeParams(existingId);

        if (existingParams == null) {
            return;
        }

        /*
         * When comparing params, exclude those that are set later in the
         * process, such as the storage node id, which is generated when the
         * node is added to the topology, and after SNA registration.
         */
        ParameterMap existingParamsMapCopy =
            existingParams.getFilteredMap().copy();
        existingParamsMapCopy.remove(ParameterState.COMMON_SN_ID);
        existingParamsMapCopy.remove(ParameterState.SN_COMMENT);
        for (String paramName : StorageNodeParams.REGISTRATION_PARAMS) {
            existingParamsMapCopy.remove(paramName);
        }

        ParameterMap newParamsMapCopy = newSNParams.getMap();
        newParamsMapCopy.remove(ParameterState.COMMON_SN_ID);
        newParamsMapCopy.remove(ParameterState.SN_COMMENT);

        if (!existingParamsMapCopy.equals(newSNParams.getMap())) {
            throw new IllegalCommandException
                ("A storage node on " + existingParams.getHostname() +
                 ":" + existingParams.getRegistryPort() + " already " +
                 "exists, but has different parameters. The " +
                 "storage node can't be deployed again unless the " +
                 "parameters are identical. Existing parameters:\n" +
                 existingParamsMapCopy.showContents(true) + "New parameters:\n" +
                 newParamsMapCopy.showContents(true));
        }
    }

    @Override
    void preExecutionSave() {

        /*
         * Nothing to do, in this special case, topology is saved after
         * execution, and after the SN is created and registered.
         */
    }
    
    @Override
    void stripForDisplay() {
        super.stripForDisplay();
        inputSNP = null;
        newSNParams = null;
    }
}
