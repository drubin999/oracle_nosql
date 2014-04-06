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

import com.sleepycat.persist.model.Persistent;

import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.util.ServiceUtils;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;

/**
 * Send a simple newGlobalParameters call to the Admin to refresh its global
 * parameters without a restart.
 */
@Persistent
public class NewAdminGlobalParameters extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private AdminId targetAdminId;
    private AbstractPlan plan;
    private String hostname;
    private int registryPort;

    /* For DPL */
    @SuppressWarnings("unused")
    private NewAdminGlobalParameters() {
    }

    public NewAdminGlobalParameters(AbstractPlan plan,
                                    String hostname,
                                    int registryPort,
                                    AdminId targetAdminId) {
        this.plan = plan;
        this.hostname = hostname;
        this.registryPort = registryPort;
        this.targetAdminId = targetAdminId;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    public State doWork() throws Exception {
        plan.getLogger().fine(
            "Sending newGlobalParameters to Admin " + targetAdminId);

        final CommandServiceAPI cs = ServiceUtils.waitForAdmin(
            hostname, registryPort, plan.getLoginManager(),
            40, ServiceStatus.RUNNING);

        cs.newGlobalParameters();
        return State.SUCCEEDED;
    }

    @Override
    public String toString() {
       return super.toString() + " cause Admin " + targetAdminId +
           " to refresh its global parameter state without restarting";
    }
}
