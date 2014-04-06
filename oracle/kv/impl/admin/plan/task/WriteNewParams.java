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
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.persist.model.Persistent;

/**
 * A task for asking a storage node to write a new configuration file.
 *
 * version 0: original.
 * version 1: Changed inheritance chain.
 */
@Persistent(version=1)
public class WriteNewParams extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private AbstractPlan plan;
    private ParameterMap newParams;
    private StorageNodeId targetSNId;
    private RepNodeId rnid;
    private boolean continuePastError;

    public WriteNewParams(AbstractPlan plan,
                          ParameterMap newParams,
                          RepNodeId rnid,
                          StorageNodeId targetSNId,
                          boolean continuePastError) {
        super();
        this.plan = plan;
        this.newParams = newParams;
        this.rnid = rnid;
        this.targetSNId = targetSNId;
        this.continuePastError = continuePastError;
    }

    /*
     * No-arg ctor for use by DPL.
     */
    @SuppressWarnings("unused")
    private WriteNewParams() {
    }

    /**
     */
    @Override
    public State doWork()
        throws Exception {

        PlannerAdmin admin = plan.getAdmin();
        RepNodeParams rnp = admin.getRepNodeParams(rnid);
        ParameterMap rnMap = rnp.getMap();
        RepNodeParams newRnp = new RepNodeParams(newParams);
        newRnp.setRepNodeId(rnid);
        ParameterMap diff = rnMap.diff(newParams, true);
        plan.getLogger().info("Changing params for " + rnid + ": " + diff);

        /*
         * Merge and store the changed rep node params in the admin db before
         * sending them to the SNA.
         */
        rnMap.merge(newParams, true);
        admin.updateParams(rnp);

        /* Ask the SNA to write a new configuration file. */
        Topology topology = admin.getCurrentTopology();
        LoginManager loginMgr = admin.getLoginManager();
        RegistryUtils registryUtils = new RegistryUtils(topology, loginMgr);

        StorageNodeAgentAPI sna =
            registryUtils.getStorageNodeAgent(targetSNId);
        sna.newRepNodeParameters(rnMap);

        return State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return continuePastError;
    }

    @Override
    public String toString() {
       return super.toString() + 
           " write new " + rnid + " parameters into the Admin database: " +
           newParams.showContents();
    }
}
