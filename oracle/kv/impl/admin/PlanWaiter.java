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

package oracle.kv.impl.admin;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.ExecutionListener;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.TaskRun;
import oracle.kv.impl.admin.plan.task.Task;

/**
 * PlanWaiter is used to wait for a plan to finish execution.
 */
public class PlanWaiter implements ExecutionListener {

    private final CountDownLatch done;

    public PlanWaiter() {
        done = new CountDownLatch(1);
    }

    public boolean waitForPlanEnd() throws InterruptedException {
        return waitForPlanEnd(0, null);
    }

    /**
     * Call after plan execution has begin. Will return when
     *  A. the method is interrupted.
     *  B. the timeout period has expired.
     *  C. the plan has finished, successfully or not.
     *
     * @param timeout if 0, wait without a timeout
     * @throws InterruptedException
     */
    public boolean waitForPlanEnd(int timeout, TimeUnit timeoutUnit)
        throws InterruptedException {

        if (timeout == 0) {
            done.await();
            return true;
        }

        return done.await(timeout, timeoutUnit);
    }

    @Override
    public void planStart(Plan plan) {
        /* Do nothing */
    }

    @Override
    public void planEnd(Plan plan) {
        done.countDown();
        
        /* 
         * Check if the logger is set; in some test cases, the plan may not
         * be executed by the PlannerImpl, and the logger may not be set.
         */
        Logger useLogger = ((AbstractPlan)plan).getLogger();
        if (useLogger != null) {
            useLogger.log(Level.FINE,
                         "PlanWaiter.planEnd called for {0}, state={1}",
                          new Object[]{plan, plan.getState()});
        }
    }

    @Override
    public void taskStart(Plan plan,
                          Task task,
                          int taskNum,
                          int totalTasks) {
        /* Do nothing */
    }

    @Override
    public void taskEnd(Plan plan,
                        Task task,
                        TaskRun taskRun,
                        int taskNum,
                        int totalTasks) {
        /* Do nothing */
    }
}
