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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.NonfatalAssertionException;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.persist.model.Persistent;

/**
 * A plan that incurs a failure may be retried. There can be many plan execution
 * attempts that map to a single plan. ExecutionState preserves and manages
 * information about plan execution. A PlanRun is kept for each execution
 * attempt.
 *
 * The most recent plan state is held in:
 * - if this plan has never executed, the PreRunState field
 * - if this plan has executed, the most recent PlanRun.
 *
 * Synchronization of changes to this execution state are the responsibility 
 * of the caller; the execution state doesn't attempt to take any mutexes. 
 * ExecutionState methods generally only modify fields within this class.
 */
@Persistent
public class ExecutionState implements Serializable {

    private static final long serialVersionUID = 1L;

    /*
     * Plans have state (PENDING, APPROVED or CANCELED) before the first
     * execution attempt. If this plan has never executed, preRunState is the
     * authoritative state.
     */
    private Plan.State preRunState;

    /*
     * History tracks each plan execution run. If this plan has executed, the
     * most recent plan run holds the authorative plan state
     */
    private List<PlanRun> history;

    private String planName;

    public ExecutionState(String planName) {
        this.planName = planName;
        history = new ArrayList<PlanRun>();
        preRunState = Plan.State.PENDING;
    }

    /** For DPL */
    @SuppressWarnings("unused")
    private ExecutionState() {
    }

    public Plan.State getLatestState() {

        if (history.size() == 0) {
            /* Never been run before. */
            return preRunState;
        }

        /* It's been run, get the last run state. */
        return history.get(history.size() -1).getState();
    }

    public Date getLatestStartTime() {
        if (history.size() == 0) {
            return null;
        }

        return new Date(history.get(history.size() - 1).getStartTime());
    }

    public Date getLatestEndTime() {
        if (history.size() == 0) {
            return null;
        }

        return new Date(history.get(history.size() - 1).getEndTime());
    }

    /**
     * Check if a plan is in an appropriate state for execution.
     */
    void validateStartOfNewRun(Plan plan) {
        Plan.State latestState = getLatestState();
        if (!latestState.checkTransition(Plan.State.RUNNING)) {
            throw new IllegalCommandException
                (plan + " can't be run, last state was " +
                 latestState);
        }
    }

    /**
     * Setup a new plan run.
     */
    PlanRun startNewRun() {

        /* Sanity check that all previous runs are ERROR or INTERRUPT. */
        int numPastRuns = history.size();
        if (numPastRuns > 0) {
            for (int i = numPastRuns -1; i >= 0; i--) {
                Plan.State pastState = history.get(i).getState();
                if (!((pastState == Plan.State.ERROR) ||
                      (pastState == Plan.State.INTERRUPTED))) {
                    throw new OperationFaultException
                        ("This plan can only be retried if previous attempts " +
                         "were interrupted or failed. Run " + i +
                         " status=" + pastState + " Past run status: " +
                         showRuns());
                }
            }
        }

        PlanRun attempt = new PlanRun(history.size() +1, this);
        history.add(attempt);
        return attempt;
    }

    /* Display all plan  run history. */
    String showRuns() {
        StringBuilder sb = new StringBuilder();
        for (PlanRun run : history) {
            sb.append(run + "\n");
        }
        return sb.toString();
    }

    String getLatestRunFailureDescription() {
        PlanRun planRun = getLatestPlanRun();
        if (planRun == null) {
            return null;
        }
        return planRun.getFailureDescription(true);
    }

    void setPlanState(Planner planner,
                      Plan plan,
                      Plan.State newState,
                      String msg) {
        if (history.size() == 0) {
            preRunState = changeState(planner, plan, preRunState,
                                      newState, 0, msg);
            return;
        }
        getLatestPlanRun().setState(planner, plan, newState, msg);
    }

    /**
     * Plan state may be changed by different threads. An admin thread may
     * approve or cancel a pending plan. An asynchronous plan execution thread
     * may set an error or deem the plan to be finished.
     */
    Plan.State changeState(Planner planner,
                           Plan plan,
                           Plan.State oldState,
                           Plan.State newState,
                           int attemptNumber,
                           String msg) {
        if (oldState == newState) {
            return newState;
        }

        Plan.State.validateTransition(oldState, newState);
        PlanStateChange change = new PlanStateChange(plan.getId(),
                                                     plan.getName(),
                                                     newState,
                                                     attemptNumber,
                                                     msg);
        planner.getAdmin().getMonitor().publish(change);
        return newState;
    }

    public PlanRun getLatestPlanRun() {
        if (history.size() == 0) {
            return null;
        }
        return history.get(history.size()-1);
    }

    String getPlanName() {
        return planName;
    }

    public List<PlanRun> getHistory() {
        return history;
    }

    /* For unit test support */
    ExceptionTransfer getLatestExceptionTransfer() {
        PlanRun planRun = getLatestPlanRun();
        if (planRun == null){
            return null;
        }

        return planRun.getExceptionTransfer();
    }

    /**
     * ExceptionTransfer is used in PlanRun and TaskRun simply to keep a
     * failure message and stack trace.
     *
     * Also, Exceptions that occur during asynchronous plan execution may have
     * to be transfered across thread boundaries so that an appropriate
     * exception can be thrown by the thread that is waiting for a plan to
     * complete.  ExceptionTransfer is also used for this purpose.
     *
     * When an ExceptionTransfer refers to an currently running plan/task, for
     * which a PlanWaiter might be waiting, the Throwable field "failure" will
     * refer to the actual exception object that was thrown during execution of
     * the plan.  Thus when PlanWaiter.throwOpFaultEx calls getFailure(), this
     * field will contain the exception.
     *
     * However, if the ExceptionTransfer object has been reconstituted from the
     * database, the "failure" field will be null, and the stack trace from the
     * original exception will be available in the String field "stackTrace".
     *
     * It is considered an error to call getFailure in a context in which
     * "failure" is null and "stackTrace" is not.
     */
    @Persistent
    public static class ExceptionTransfer implements Serializable {

        private static final long serialVersionUID = 1L;

        private transient Throwable failure = null;
        private String stackTrace = null;
        private String description;

        ExceptionTransfer(Throwable t, String msg) {

            description = msg;
            if (t != null) {
                Throwable trueCause = t;
                if (t instanceof ExecutionException) {
                    trueCause = t.getCause();
                }

                failure = trueCause;
                if (trueCause.getMessage() != null) {
                    description += ": " + trueCause.getMessage();
                }
                stackTrace = LoggerUtils.getStackTrace(t);
            }
        }

        ExceptionTransfer() {
        }

        public Throwable getFailure() {
            if (failure == null && stackTrace != null) {
                throw new NonfatalAssertionException
                    ("Unexpected call to getFailure after reconstitution.");
            }
            return failure;
        }

        public String getDescription() {
            return description;
        }

        public String getStackTrace() {
            return stackTrace;
        }

        @Override
        public String toString() {
            return description + " " + stackTrace;
        }
    }
}
