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

import java.util.Set;

import oracle.kv.impl.admin.PlannerAdmin;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.ChangeAdminParamsPlan;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.persist.model.Persistent;

/**
 * A task for asking a storage node to write a new configuration file to
 * include new AdminParams..
 *
 * version 0: original.
 * version 1: Changed inheritance chain.
 */
@Persistent(version=1)
public class WriteNewAdminParams extends SingleJobTask {
    private static final long serialVersionUID = 1L;

    private ChangeAdminParamsPlan plan;
    private ParameterMap newParams;
    private StorageNodeId targetSNId;
    private AdminId aid;

    WriteNewAdminParams() {
    }

    public WriteNewAdminParams(ChangeAdminParamsPlan plan,
                               ParameterMap newParams,
                               AdminId aid,
                               StorageNodeId targetSNId) {
        super();
        this.plan = plan;
        this.newParams = newParams;
        this.aid = aid;
        this.targetSNId = targetSNId;
    }

    @Override
    public State doWork()
        throws Exception {

        /*
         * Merge and store the changed rep node params in the admin db before
         * sending them to the SNA.
         */
        PlannerAdmin admin = plan.getAdmin();
        Parameters parameters = admin.getCurrentParameters();
        AdminParams current = parameters.get(aid);
        ParameterMap currentMap = current.getMap();

        Set<AdminId> needsAction = plan.getNeedsActionSet();

        /* Set the id in a copy of newParams, since the original lacks it. */
        new AdminParams(newParams.copy()).setAdminId(aid);

        ParameterMap diff = currentMap.diff(newParams, true);
        if (currentMap.merge(newParams, true) == 0) {
            plan.getLogger().info("No difference in Admin parameters");
            return State.SUCCEEDED;
        }
        admin.updateParams(current);
        plan.getLogger().info("Changing params for " + aid + "y: " + diff);

        /* Ask the SNA to write a new configuration file. */
        StorageNodeAgentAPI sna = 
            RegistryUtils.getStorageNodeAgent(parameters, targetSNId,
                                              admin.getLoginManager());
        sna.newAdminParameters(currentMap);

        /* Tell subsequent tasks that we did indeed change the parameters. */
        needsAction.add(aid);

        return State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    public String toString() {
       return super.toString() + 
           " write new " + aid + " parameters into the Admin database: " +
           newParams.showContents();
    }
}
