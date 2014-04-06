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

import java.util.concurrent.Callable;

import oracle.kv.impl.admin.plan.PlanExecutor.ParallelTaskRunner;

/**
 * A JobWrapper is used to present one phase of a multi-phase task as a
 * Callable that can be executed by a executor service. When call() is invoked,
 * the wrapper invokes the doJob() method. The doJob() method will return a
 * NextJob, which holds both the status of the work just executed, along with a
 * possible follow-on phase, and parameters to use in scheduling the next
 * phase. When phase 1 finishes, the follow-on phase will be scheduled.
 *
 * For example, suppose a task wants to
 *  - phase 1: invoke a partition migration
 *  - phase 2: check migration status
 * then the doJob() method would invoke phase 1 and return a {@link NextJob}
 * that encases phase 2.
 */
abstract public class JobWrapper implements Callable<Task.State> {
    
    protected final int taskId;
    protected final ParallelTaskRunner runner;

    /* For debug and logging messages. */
    private final String description;
    
    /**
     * @param taskId of the owning task. Needed for submitting any follow on 
     * work for the ensuing phase.
     * @param runner - a parallel task runner that can be used to schedule and
     * execute any ensuing work.
     * @param description - used only for providing information to debugging 
     * and logging output
     */
    public JobWrapper(int taskId, 
                      ParallelTaskRunner runner,
                      String description) {

        this.taskId = taskId;
        this.runner = runner;
        this.description = description;
    }

    public abstract NextJob doJob() throws Exception;

    /**
     * Executes the work in the doJob()) method. Upon its completion, 
     * schedule and submits the next phase of work to be done, as 
     * specified by doJob().
     */
    @Override
    public Task.State call() throws Exception {
        NextJob nextAction = doJob();
        return runner.dispatchNextJob(taskId, nextAction);
    }

    public String getDescription() {
        return description;
    }
}