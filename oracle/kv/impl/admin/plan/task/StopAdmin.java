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

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import oracle.kv.impl.admin.PlannerAdmin;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.ChangeAdminParamsPlan;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.je.utilint.StoppableThread;
import com.sleepycat.persist.model.Persistent;

/**
 * A task for stopping a given Admin
 *
 * version 0: original.
 * version 1: Changed inheritance chain.
 */
@Persistent(version=1)
public class StopAdmin extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private AbstractPlan plan;
    private StorageNodeId snId;
    private AdminId adminId;
    private boolean continuePastError;

    /**
     * We expect that the target Admin exists before StopAdmin is
     * executed.
     * @param continuePastError if true, if this task fails, the plan
     * will stop.
     */
    public StopAdmin(AbstractPlan plan,
                     StorageNodeId snId,
                     AdminId adminId,
                     boolean continuePastError) {
        super();
        this.plan = plan;
        this.snId = snId;
        this.adminId = adminId;
        this.continuePastError = continuePastError;
    }

    /* DPL */
    StopAdmin() {
    }

    @Override
    public State doWork()
        throws Exception {

        boolean needsAction = true;
        if (plan instanceof ChangeAdminParamsPlan) {
            needsAction = ((ChangeAdminParamsPlan) plan).
                getNeedsActionSet().contains(adminId);
        }

        /*
         * We won't perform the action unless the aid is set to NO needsAction
         * by a ChangeAdminParamsPlan.
         */
        if (needsAction) {
            final PlannerAdmin admin = plan.getAdmin();

            /*
             * If we are running on the admin to be shut down, just shut down
             * without directly involving the SNA.  The SNA will restart the
             * AdminService immediately, but the mastership will transfer to
             * a replica, if there are any.
             */
            if (adminId.equals
                        (admin.getParams().getAdminParams().getAdminId())) {

                new StoppableThread("StopAdminThread") {
                    @Override
                    public void run() {
                        admin.awaitPlan(plan.getId(), 10000, TimeUnit.SECONDS);
                        admin.stopAdminService(false);
                    }
                    @Override
                    protected Logger getLogger() {
                        return plan.getLogger();
                    }
                }.start();

                return State.INTERRUPTED;
            }

            /*
             * Update the admin params to indicate that this node is now
             * disabled, and save the changes.
             */
            Parameters parameters = admin.getCurrentParameters();
            AdminParams ap = parameters.get(adminId);
            ap.setDisabled(true);
            admin.updateParams(ap);

            /* Tell the SNA to start the admin. */
            StorageNodeAgentAPI sna =
                RegistryUtils.getStorageNodeAgent(parameters, snId,
                                                  plan.getLoginManager());

            sna.stopAdmin(false);
        }

        return State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return continuePastError;
    }

    @Override
    public String toString() {
       return super.toString() +
           " shutdown " + adminId;
    }
}
