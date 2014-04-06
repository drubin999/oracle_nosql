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
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.PortTracker;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.persist.model.Persistent;

/**
 * A task for creating and starting an instance of an Admin service on a
 * StorageNode.
 */
@Persistent(version=1)
public class DeployAdmin extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    /* Hook to inject failures at different points in task execution */
    public static TestHook<Integer> FAULT_HOOK;

    private AbstractPlan plan;
    private AdminId adminId;
    private StorageNodeId snId;

    public DeployAdmin(AbstractPlan plan,
                       StorageNodeId snId,
                       AdminId adminId) {
        super();
        this.plan = plan;
        this.snId = snId;
        this.adminId = adminId;
    }

    /*
     * No-arg ctor for use by DPL.
     */
    public DeployAdmin() {
        super();
    }

    @Override
    public State doWork()
        throws Exception {

        PlannerAdmin admin = plan.getAdmin();

        Topology topology = admin.getCurrentTopology();
        Parameters parameters = admin.getCurrentParameters();

        /*
         * If needed, set a helper host and nodeHostPort for this Admin.
         */
        AdminParams ap = parameters.get(adminId);
        if (ap.getHelperHosts() == null) {
            initParams(topology, parameters, admin, ap);
        }
        assert TestHookExecute.doHookIfSet(FAULT_HOOK, 1);

        /* Now ask the SNA to create it. */
        LoginManager loginMgr = admin.getLoginManager();
        RegistryUtils registryUtils = new RegistryUtils(topology, loginMgr);
        StorageNodeAgentAPI sna = registryUtils.getStorageNodeAgent(snId);
        sna.createAdmin(ap.getMap());
        assert TestHookExecute.doHookIfSet(FAULT_HOOK, 2);

        return State.SUCCEEDED;
    }

    private void initParams(Topology topology,
                            Parameters parameters,
                            PlannerAdmin admin,
                            AdminParams ap) {

        String nodeHostPort;
        String helperHost;
        if (adminId.getAdminInstanceId() == 1) {

            /*
             * If this is the first admin to be deployed, it already has a
             * NodeHostPort setting.  Its helperHost should be itself.
             */
            nodeHostPort = admin.getParams().getAdminParams().getNodeHostPort();
            helperHost = nodeHostPort;

        } else {
            /*
             * If this is a secondary admin deployment, we'll ask the
             * PortTracker to give out a HA port number for it.
             */
            PortTracker portTracker = new PortTracker(topology,
                                                      parameters,
                                                      snId);

            String haHostName = parameters.get(snId).getHAHostname();
            int haPort = portTracker.getNextPort(snId);
            nodeHostPort = haHostName + ":" + haPort;

            /* Assemble the list of helper hosts */
            helperHost = Utils.findAdminHelpers(parameters, adminId);
        }

        ap.setJEInfo(nodeHostPort, helperHost);
        plan.getAdmin().updateParams(ap);
    }

    @Override
    public String toString() {
        return super.toString() +  " AdminId[" + adminId +
            "] on SNId[" + snId + "]";
    }

    public StorageNodeId getSnId() {
        return snId;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }
}
