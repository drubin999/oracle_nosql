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

import oracle.kv.impl.admin.PlannerAdmin;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.persist.model.Persistent;

/**
 * A task for asking a storage node to write the new global parameters to
 * configuration file.
 */
@Persistent
public class WriteNewGlobalParams extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private boolean continuePastError;
    private AbstractPlan plan;
    private ParameterMap newParamMap;
    private StorageNodeId targetSNId;

    public WriteNewGlobalParams(AbstractPlan plan,
                                ParameterMap newParams,
                                StorageNodeId targetSNId,
                                boolean continuePastError) {
        this.plan = plan;
        this.newParamMap = newParams;
        this.continuePastError = continuePastError;
        this.targetSNId = targetSNId;
    }

    /* No-arg ctor for DPL */
    @SuppressWarnings("unused")
    private WriteNewGlobalParams() {
    }

    @Override
    public boolean continuePastError() {
        return continuePastError;
    }

    @Override
    public State doWork() throws Exception {
        final PlannerAdmin admin = plan.getAdmin();
        final GlobalParams gp = admin.getCurrentParameters().getGlobalParams();
        final ParameterMap gpMap = gp.getMap();

        final ParameterMap diff =
            gpMap.diff(newParamMap, true /* notReadOnly */);
        plan.getLogger().info("Changing Global params for " + targetSNId +
                              ": " + diff);

        /*
         * Merge and store the changed global params in the admin db before
         * sending them to the SNA.
         */
        gpMap.merge(newParamMap, true /* notReadOnly */);
        admin.updateParams(gp);

        /* Ask the SNA to write a new configuration file. */
        Topology topology = admin.getCurrentTopology();
        LoginManager loginMgr = admin.getLoginManager();
        RegistryUtils registryUtils = new RegistryUtils(topology, loginMgr);

        StorageNodeAgentAPI sna =
            registryUtils.getStorageNodeAgent(targetSNId);
        sna.newGlobalParameters(gpMap);

        return State.SUCCEEDED;
    }

    @Override
    public String toString() {
       return super.toString() +
           " write new global parameters into the Admin database and SN " +
           targetSNId + "'s configuration file : " +
           newParamMap.showContents();
    }
}
