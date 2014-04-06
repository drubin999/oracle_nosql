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
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.persist.model.Persistent;

/**
 * A task for asking a storage node to destroy an Admin that it hosts.
 */
@Persistent(version=0)
public class DestroyAdmin extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private AbstractPlan plan;
    private StorageNodeId snid;
    private AdminId victim;

    public DestroyAdmin() {
    }

    public DestroyAdmin(AbstractPlan plan,
                        StorageNodeId snid,
                        AdminId victim) {
        this.plan = plan;
        this.snid = snid;
        this.victim = victim;
    }

    @Override
    public State doWork()
        throws Exception {

        PlannerAdmin admin = plan.getAdmin();
        Parameters parameters = admin.getCurrentParameters();

        StorageNodeAgentAPI sna =
            RegistryUtils.getStorageNodeAgent(parameters, snid,
                                              plan.getLoginManager());

        sna.destroyAdmin(victim, true);

        return State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    public String toString() {
       return super.toString() +
           " Destroy the Admin replica " + victim;
    }
}

