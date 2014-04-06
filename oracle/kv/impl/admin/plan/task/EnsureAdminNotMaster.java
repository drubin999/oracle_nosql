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
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.topo.AdminId;

import com.sleepycat.je.utilint.StoppableThread;
import com.sleepycat.persist.model.Persistent;

/**
 * A task to transfer mastership of the Admin.  If the task is running on the
 * given Admin, cause it to lose mastership.
 *
 * version 0: original.
 */
@Persistent(version=0)
public class EnsureAdminNotMaster extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private AbstractPlan plan;
    private AdminId adminId;

    public EnsureAdminNotMaster(AbstractPlan plan,
                                AdminId adminId) {
        super();
        this.plan = plan;
        this.adminId = adminId;
    }

    /* DPL */
    EnsureAdminNotMaster() {
    }

    @Override
    public State doWork()
        throws Exception {

        final PlannerAdmin admin = plan.getAdmin();

        /*
         * If we are running on the given Admin, the mastership needs to be
         * transferred away before we can do anything else.  If there are no
         * replicas, the enclosing plan cannot be completed.
         */
        if (adminId.equals
            (admin.getParams().getAdminParams().getAdminId())) {

            if (admin.getAdminCount() <= 1) {
                throw new IllegalStateException
                    ("Can't change Admin mastership if there are no replicas.");
            }

            /*
             * The thread will wait for the plan to enter INTERRUPTED state,
             * then proceed with the master transfer.
             */
            new StoppableThread("EnsureAdminNotMasterThread") {
                @Override
                public void run() {
                    admin.awaitPlan(plan.getId(), 10000, TimeUnit.SECONDS);
                    admin.transferMaster();
                }
                @Override
                protected Logger getLogger() {
                    return plan.getLogger();
                }
            }.start();

            return State.INTERRUPTED;
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
           " ensure admin is not the master " + adminId;
    }
}
