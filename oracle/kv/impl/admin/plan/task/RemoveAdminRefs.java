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
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.topo.AdminId;

import com.sleepycat.je.rep.MemberNotFoundException;
import com.sleepycat.persist.model.Persistent;

/**
 * A task for removing references to an Admin that is being removed.
 * 1. Remove the Admin from its rep group.
 * 2. Remove the relevant Parameters entry from the Admin Database.
 */
@Persistent(version=0)
public class RemoveAdminRefs extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private AbstractPlan plan;
    private AdminId victim;

    public RemoveAdminRefs() {
    }

    public RemoveAdminRefs(AbstractPlan plan,
                           AdminId victim) {
        this.plan = plan;
        this.victim = victim;
    }

    @Override
    public State doWork()
        throws Exception {

        PlannerAdmin admin = plan.getAdmin();

        /*
         * Remove the replica from the rep group before shutting it down.  This
         * sequence is necessary if there is a two-node group, so that majority
         * can be maintained until the node is removed, from JE HA's
         * perspective.
         */
        try {
            admin.removeAdminFromRepGroup(victim);
        } catch (MemberNotFoundException mnfe) {
            /* This would happen if the plan was interrupted and re-executed. */
            plan.getLogger().info("The admin " + victim +
                                " was not found in the repgroup.");
        }

        try {
            admin.removeAdminParams(victim);
        } catch (MemberNotFoundException mnfe) {
            /* This could happen if the plan was interrupted and re-executed. */
            plan.getLogger().info("The admin " + victim + " was not found.");
        }

        return State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    public String toString() {
       return super.toString() +
           " Remove references to Admin " + victim;
    }
}

