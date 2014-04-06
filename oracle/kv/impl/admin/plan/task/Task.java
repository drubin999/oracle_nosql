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

import java.util.Map;
import java.util.concurrent.Callable;

import oracle.kv.impl.admin.plan.PlanExecutor.ParallelTaskRunner;
import oracle.kv.impl.admin.plan.Planner;


/**
 * A step in the process of executing a plan. Tasks are assembled (using {@link
 * TaskList}s) to carry out the changes in a plan.  Tasks are a unit of work
 * that can be repeated, canceled, and recovered during plan execution.
 *
 * All Tasks must define a method called getFirstJob that returns a Callable
 * which performs all or a portion of the task work. If getFirstJob only does a
 * portion of the work, the job will be responsible for scheduling follow on
 * work when the first job is finished.
 */
public interface Task {

    /**
     * The possible values for status of a task.
     *
     *         PENDING
     *           |
     *         RUNNING
     *     /         \       \
     * INTERRUPTED SUCCEEDED ERROR
     *
     */
    public static enum State {
        PENDING,
        RUNNING,
        INTERRUPTED,
        SUCCEEDED,
        ERROR,
    }
    
    /* For formatting */
    public static int LONGEST_STATE = 11;
    public String getName();

    /**
     * Return true if a failure in this task should halt execution of the
     * entire plan.Some tasks always continue or stop, whereas the behavior
     * of other tasks is specified when they are constructed for a given plan.
     */
    public boolean continuePastError();

    /**
     * Because of nested tasks, there may be more tasks held within the
     * umbrella of this task. Tasks which hold nested tasks do no work
     * themselves, and only include their nested children in the task count.
     */
    public int getTotalTaskCount();

    /*
     * Nested tasks are used to create parallelism in the execution of plan.
     */
    public TaskList getNestedTasks();

    /**
     * Return the very first job, or phase, which will start off the task.
     * @param taskId is used to schedule any follow on jobs.
     * @param runner is used to schedule any follow on jobs and is only
     * needed by multiphase tasks.
     */
    public Callable<Task.State> getFirstJob(int taskId,
                                            ParallelTaskRunner runner)
        throws Exception;

    /**
     * If this task ends in ERROR or interrupt, it may have work to do to
     * return the store to a consistent state. This is not the same as a
     * rollback; it's permissible for a task to alter the store, and to leave
     * the store in that changed state even if an error occurs. Cleanup should
     * only be implemented if the task needs to ensure that something in the
     * store is consistent.
     * @return null if there is no cleanup to do.
     */
    public Runnable getCleanupJob();

    /**
     * Obtain any required topo locks before plan execution, to avoid 
     * conflicts in concurrent plans.
     */
    public void lockTopoComponents(Planner planner);

    /*
     * Format any detailed information collected about the task in a way
     * that's usable for plan reporting.
     * @return information to display, or null if there is no additional
     * info.
     */
    public String displayExecutionDetails(Map<String, String> details,
                                          String displayPrefix);
}
