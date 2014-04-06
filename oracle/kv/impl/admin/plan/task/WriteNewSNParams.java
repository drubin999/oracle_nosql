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

import static oracle.kv.impl.param.ParameterState.BOOTSTRAP_MOUNT_POINTS;
import static oracle.kv.impl.param.ParameterState.COMMON_SN_ID;

import oracle.kv.impl.admin.PlannerAdmin;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
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
public class WriteNewSNParams extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private AbstractPlan plan;
    private ParameterMap newParams;
    private StorageNodeId targetSNId;

    public WriteNewSNParams(AbstractPlan plan,
                            StorageNodeId targetSNId,
                            ParameterMap newParams) {
        super();
        this.plan = plan;
        this.newParams = newParams;
        this.targetSNId = targetSNId;
    }

    /*
     * No-arg ctor for use by DPL.
     */
    @SuppressWarnings("unused")
    private WriteNewSNParams() {
    }

    /**
     */
    @Override
    public State doWork()
        throws Exception {

        /*
         * Store the changed params in the admin db before sending them to the
         * SNA.  Merge rather than replace in this path.  If nothing is changed
         * return.
         */
        PlannerAdmin admin = plan.getAdmin();
        StorageNodeParams snp = admin.getStorageNodeParams(targetSNId);
        ParameterMap snMap = snp.getMap();
        ParameterMap mountMap =
            newParams.getName().equals(BOOTSTRAP_MOUNT_POINTS) ? newParams :
            null;
        if (mountMap != null) {
            plan.getLogger().info("Changing storage directories for " +
                                  targetSNId + ": " + mountMap);
            /*
             * Snid may have been stored, remove it
             */
            mountMap.remove(COMMON_SN_ID);
            snp.setMountMap(mountMap);
            snMap = null;
        } else {
            ParameterMap diff = snMap.diff(newParams, true);
            snMap.merge(newParams, true);
            plan.getLogger().info("Changing these params for " + targetSNId +
                                  ": " + diff);
        }

        /* Update the admin DB and call the SNA using the merged map */
        admin.updateParams(snp, null);
        Topology topology = admin.getCurrentTopology();
        LoginManager loginMgr = admin.getLoginManager();
        RegistryUtils registryUtils = new RegistryUtils(topology, loginMgr);
        StorageNodeAgentAPI sna =
            registryUtils.getStorageNodeAgent(targetSNId);

        /* Only one or the other map will be non-null */
        sna.newStorageNodeParameters(snMap != null ? snMap : mountMap);

        return State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    public String toString() {
       return super.toString() +
           " write new " + targetSNId +
           " parameters into the Admin database: " + newParams.showContents();
    }
}
