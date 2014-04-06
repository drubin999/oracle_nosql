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

package oracle.kv.impl.admin.plan;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.plan.ExecutionState.ExceptionTransfer;
import oracle.kv.impl.admin.plan.task.Task;
import oracle.kv.impl.util.FormatUtils;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.persist.model.Persistent;

/**
 * Captures status of a single task execution. Tasks may be executed repeatedly
 * as the plan is re-executed.
 *
 * Note that any caller who modifies the TaskRun must synchronize against its
 * owning plan. This coordinates between threads that are:
 *  - concurrently updating the different tasks, because of parallel task
 *    execution
 *  - PlanExecutor threads that are saving the plan instance to the persistent
 *    DPL store.
 *
 * Readers of the task may not necessarily need to synchronize on the plan. It
 * may not be important that the reader get the latest task information, if
 * it's only needed for logging and reporting.
 */
@Persistent(version=1)
public class TaskRun implements Serializable {

    private static final long serialVersionUID = 1L;
    private String taskName;
    private long startTime;
    private long endTime;
    private Task.State state;

    /*
     * An identifier for the task, used to locate per-task status stored in the
     * PlanRun.
     */
    private int taskNum;

    /*
     * ExceptionTransfer packages a Throwable and an exception message.  It
     * is used to propagate the exception across thread boundaries, and also
     * PlanWaiter, and need not be stored persistently.
     */
    private transient ExceptionTransfer transfer;

    private long cleanupStartTime;
    private long cleanupEndTime;
    private String cleanupFailure;

    private Task task;
    
    /*
     * Task and details were added in AdminSchemaVersion 3, for plan status
     * report support. CustomInfo holds key->value pairs that can be store
     * metrics that pertain only to a particular type of task. See
     * PartitionMigrationStatus for an example of how to encapsulate the
     * information in the map, and its creation and extraction.
     * Note that modifications of the details map, like all other modifications
     * to the plan instance, must be synchronized on the plan instance.
     */
    private Map<String, String> details;

    TaskRun(Task task, Logger logger, int taskNum) {
        this.task = task;
        /* To be compatible with versions before AdminSchemaVersion 3 */
        this.taskName = task.getName();

        setState(Task.State.RUNNING, logger);
        startTime = System.currentTimeMillis();
        this.taskNum = taskNum;
        details = new HashMap<String, String>();
    }

    /** For DPL */
    public TaskRun() {
    }

    /**
     * Change the status of this task. Should only be called when synchronized
     * on the owning plan.
     *
     * @param newState the new status of this task.
     */
    void setState(Task.State newState, Logger logger) {
        if (state == null) {
            logger.log(Level.FINE, "TaskRun {0} starting in {1}",
                       new Object[] {taskName, newState});
        } else {
            logger.log(Level.FINE, "TaskRun {0} transitioning from {1} to {2}",
                       new Object[] {taskName, state, newState});
            /*
             * An assertion against a state transition that should never
             * happen.
             */
            if ((state == Task.State.ERROR) &&
                (newState != Task.State.ERROR)) {
                throw new IllegalStateException
                ("Illegal transition from " + state + " to " +
                 newState + " for " + taskName);
            }
        }

        state = newState;
        endTime = System.currentTimeMillis();
    }

    /**
     * Save a information about a task failure. Only call when synchronized on
     * the owning plan.Note that failures can be detected from two types of
     * threads: the thread executing the task and the PlanExecutor, when it is
     * examining the future.
     */
    void saveFailure(Throwable t, String problem, Logger logger) {

        transfer = new ExceptionTransfer(t, problem);

        if (t == null) {
            logger.log(Level.SEVERE,
                       "Task {0}/{1} ended in state {2} with {3}",
                       new Object[] {taskNum, taskName, state, problem});
        } else {
            logger.log(Level.SEVERE,
                       "Task {0}/{1} ended in state {2} with {3} {4}\n{5}",
                       new Object[] {taskNum, taskName, state, t, problem,
                                     LoggerUtils.getStackTrace(t)});
        }

    }

    ExceptionTransfer getTransfer() {
        return transfer;
    }

    String getFailureDescription() {
        ExceptionTransfer et = getTransfer();
        if (et == null) {
            return null;
        }

        return et.getDescription() + ": " + et.getStackTrace();
    }

    @Override
    public String toString() {
        String ret = taskName + " [" + state + "]";
        String failure = getFailureDescription();
        if (failure != null) {
            ret += " " + failure;
        }
        if (cleanupStartTime != 0) {
            ret += " cleanup started at " +
                FormatUtils.formatDateAndTime(cleanupStartTime);
        }
        if (cleanupEndTime != 0) {
            ret += " cleanup ended at " +
                FormatUtils.formatDateAndTime(cleanupEndTime);
        }

        if (cleanupFailure != null) {
            ret += "cleanup failure:" + cleanupFailure;
        }

        return ret;
    }

    String getTaskName() {
        return taskName;
    }

    Task.State getState() {
        return state;
    }

    int getTaskNum() {
        return taskNum;
    }

    void cleanupStarted() {
        cleanupStartTime = System.currentTimeMillis();
    }

    void cleanupEnded() {
        cleanupEndTime = System.currentTimeMillis();
    }

    void saveCleanupFailure(String failureInfo) {
        cleanupFailure = failureInfo;
    }

    long getStartTime() {
        return startTime;
    }

    Task getTask() {
        return task;
    }

    Map<String, String> getDetails() {
        return details;
    }

    long getEndTime() {
        return endTime;
    }

    /*
     * On upgrade, fill in these new fields.
     */
    void upgradeToV3(int tnum, Task t) {
        taskNum = tnum;
        task = t;
    }
    
    /* 
     * Provide a customized display of task execution for this task, if it
     * collected extra info, and supports such a display.
     * @return null if there is no detailed information.
     */
    public String displayTaskDetails(String displayPrefix) {
        return task.displayExecutionDetails(details, displayPrefix);
    }
}
