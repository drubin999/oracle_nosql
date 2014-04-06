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

import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.ServiceUtils;

import com.sleepycat.persist.model.Persistent;

/**
 * Monitors the state of an Admin, blocking until a certain state has been
 * reached.
 *
 * version 0: original.
 * version 1: Changed inheritance chain.
 */
@Persistent(version=1)
public class WaitForAdminState extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    /**
     * The node that is to be monitored
     */
    private AdminId targetAdminId;

    /**
     * The state the node must be in before finishing this task
     */
    private ServiceStatus targetState;
    private AbstractPlan plan;
    private StorageNodeId snId;

    public WaitForAdminState() {
    }

    /**
     * Creates a task that will block until a given Admin has reached
     * a given state.
     *
     * @param desiredState the state to wait for
     */
    public WaitForAdminState(AbstractPlan plan,
                             StorageNodeId snId,
                             AdminId targetAdminId,
                             ServiceStatus desiredState) {
        this.plan = plan;
        this.targetAdminId = targetAdminId;
        this.snId = snId;
        this.targetState = desiredState;
    }

    @Override
    public State doWork()
        throws Exception {

        Parameters parameters = plan.getAdmin().getCurrentParameters();
        StorageNodeParams snp = parameters.get(snId);

        /* Get the timeout from the currently running Admin's myParams. */
        AdminParams ap = plan.getAdmin().getParams().getAdminParams();
        long waitSeconds =
            ap.getWaitTimeoutUnit().toSeconds(ap.getWaitTimeout());

        String msg =
            "Waiting " + waitSeconds + " seconds for Admin" + targetAdminId +
            " to reach " + targetState;

        plan.getLogger().fine(msg);

        try {
            ServiceUtils.waitForAdmin(snp.getHostname(), snp.getRegistryPort(),
                                      plan.getLoginManager(),
                                      waitSeconds, targetState);
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                throw (InterruptedException) e;
            }

            plan.getLogger().info("Timed out while " + msg);

            return State.ERROR;
        }

        return State.SUCCEEDED;
    }

    @Override
    public String toString() {
       return super.toString() + " waits for Admin " + targetAdminId +
           " to reach " + targetState + " state";
    }

    @Override
    public boolean continuePastError() {
        return true;
    }
}
