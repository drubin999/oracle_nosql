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

import java.util.Date;
import java.util.Formatter;
import java.util.List;
import java.util.logging.Logger;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.PlanWaiter;
import oracle.kv.impl.admin.plan.ExecutionState.ExceptionTransfer;
import oracle.kv.impl.admin.plan.task.Task;
import oracle.kv.impl.admin.plan.task.TaskList;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.metadata.Metadata;

import com.sleepycat.je.Transaction;
import com.sleepycat.persist.EntityStore;

/**
 * Encapsulates a definition and mechanism for making a change to the KV Store.
 * Implementations of Plan will define the different types of plans that can be
 * carried out.
 */
public interface Plan {

    /** The default name for a plan. */
    final String NO_NAME = "none";

    /**
     * The available states of a Plan. Plan state is persistent.
     *
     *
     *             Pending ----+
     *               |         +--(cancel)---> Canceled (terminal state)
     *            Approved ----+
     *               |
     *               |
     *            Running       <-------------+
     *            /  |  \                     |
     *           /   |  (interrupt requested) |
     *          /    |  /  \                  |
     *         /     | /    \                 |
     *    Success  Error Interrupted      (retry)
     *  (terminal    |     |                  |
     *    state)     +-----+------------------+
     *                  |
     *                  |
     *              Canceled (terminal)
     *
     * All plans that have been created are retained in a history.
     *
     * Once a plan is running, there are only two terminal states: Success and
     * Canceled.
     *
     * When a plan hits an error or has been interrupted by the user, the user
     * must decide whether to retry the plan, or to give up and cancel the
     * plan. If a plan is retried, it has a fresh start, and is treated
     * identically to any other running plan, although we will keep track of
     * how many attempts there have been to run this plan.

     *
     * Note that Plan and {@link Task} state must be reinitialized when an
     * Admin comes up. For example, any RUNNING plans or tasks have presumably
     * been interrupted, and should be reset to INTERRUPTED state at plan
     * recovery.
     */
    public static enum State {
        PENDING(false),   // The plan has been created and is awaiting approval
        APPROVED(false),         // The plan may be run
        CANCELED(true) ,         // The plan did not run to completion, and is
                                 // now invalidated
        RUNNING(false),          // The plan is running
        INTERRUPTED(false),      // The user stopped the plan
        SUCCEEDED(true),         // The plan ran to completion successfully
        ERROR(false),            // The plan failed at any point

        /* Added in R2 */
        INTERRUPT_REQUESTED(false); // The plan is attempting to stop and
                                    // cleanup in response to an interrupt

        private final boolean isTerminal;

        State(boolean isTerminal) {
            this.isTerminal = isTerminal;
        }

        /**
         * Check a transition, and throw if it's not valid.
         */
        public static void validateTransition(State oldState,
                                              State nextState) {
            if (!oldState.checkTransition(nextState)) {
                throw new IllegalCommandException
                    ("Plan cannot transition from " + oldState +
                     " to " + nextState);
            }
        }

        /**
         * @return false if the transition is not valid.
         */
        boolean checkTransition(State nextState) {

            boolean isValid = false;

            switch(this) {
            case PENDING:
                if ((nextState == APPROVED) || (nextState == CANCELED)) {
                    isValid = true;
                }
                break;
            case APPROVED:
                if ((nextState == CANCELED) ||
                    (nextState == RUNNING) ||
                    (nextState == APPROVED)) {
                    isValid = true;
                }
                break;
            case CANCELED:
                /* terminal state, no transitions permitted. */
                break;
            case RUNNING:
                if ((nextState == SUCCEEDED) ||
                    (nextState == ERROR) ||
                    (nextState == INTERRUPT_REQUESTED)) {
                    isValid = true;
                }
                break;
            case SUCCEEDED:
                /* terminal state, no transitions permitted. */
                break;
            case ERROR:
            case INTERRUPTED:
                if ((nextState == RUNNING) || (nextState == CANCELED)) {
                    isValid = true;
                }
                break;
            case INTERRUPT_REQUESTED:
                if ((nextState == ERROR) || (nextState == INTERRUPTED)) {
                    isValid = true;
                }
                break;

            default:
                throw new OperationFaultException
                    ("Plan state transition for " + this +
                     " not defined. Attempting to transition to " +
                     nextState);
            }

            return isValid;
        }

        public boolean isTerminal() {
            return isTerminal;
        }

        /**
         * Provide a useful response to the CLI plan wait command.
         */
        public String getWaitMessage(int planId) {

            switch(this) {
            case PENDING:
                return "Plan " + planId + " is pending.";
            case APPROVED:
                return "Plan " + planId + " is approved, please execute.";
            case CANCELED:
                return "Plan " + planId + " has been canceled";
            case RUNNING:
                return "Plan " + planId + " is running";
            case SUCCEEDED:
                return "Plan " + planId + " ended successfully";
            case ERROR:
                return "Plan " + planId + " ended with errors." +
                    " Use \"show plan -id " + planId +
                    "\" for more information";
            case INTERRUPTED:
                return "Plan " + planId + " has been interrupted. Please " +
                    "cancel or retry the plan.";
            case INTERRUPT_REQUESTED:
                return "Plan " + planId + " is processing a request " +
                    "to interrupt the plan. Use \"show plan -id " + planId +
                    "\" for more information";
            default:
                throw new OperationFaultException
                    ("Message for " + this + " not defined.");
            }
        }

        /**
         * @return true if this run of this plan is finished.
         */
        boolean planExecutionFinished() {
            switch(this) {
            case PENDING:
            case APPROVED:
            case INTERRUPT_REQUESTED:
            case RUNNING:
                return false;
            case CANCELED:
            case SUCCEEDED:
            case ERROR:
            case INTERRUPTED:
                return true;
            }
            return false;
        }

        /**
         * @return true if this plan has been approved and can be executed
         * for the first or subsequent time.
         */
        boolean approvedAndCanExecute() {
            switch(this) {
            case APPROVED:
            case ERROR:
            case INTERRUPTED:
            case INTERRUPT_REQUESTED:
            case RUNNING:
                return true;
            default:
                return false;
            }
        }
    }

    /**
     * Return the unique identifier for this plan.
     */
    int getId();

    /**
     * Returns true if this Plan cannot be run while other plans are running.
     * Examples of plans that are exclusive are topology changing plans. An
     * example of a non-exclusive plan is one that restarts an existing RepNode.
     */
    boolean isExclusive();

    /**
     * Gets the current state of this plan, as far as it is known by the
     * system.  In the event that an Admin has failed, a Plan may temporarily
     * be in a RUNNING state before moving to INTERRUPTED.
     *
     * @return the most recently computed state
     */
    State getState();

    /**
     * Add a new task to the plan. Should only be used while the plan is being
     * generated.
     */
    void addTask(Task t);

    /**
     * Gets a description of all the tasks that will be run, in order, to carry
     * out this plan.
     *
     * @return the a cloned list of tasks that will be run. The tasks
     * may be modified by the caller.
     */
    TaskList getTaskList();

    /**
     * Plans are named with useful-to-human labels.
     */
    String getName();

    /**
     * A default name provided by the implementing class.
     */
    String getDefaultName();

    /**
     * Store the Plan objects in a BDB database. Plan is stored in the given
     * EntityStore using the id field as the primary key.
     *
     * @param estore the EntityStore that holds the Parameters
     * @param txn the transaction in progress
     */
    void persist(EntityStore estore, Transaction txn);

    /**
     * If this plan had a failure during execution, save a description of the
     * failure, complete with stack trace. This is persistent, for use when
     * viewing plan execution history.
     */
    String getLatestRunFailureDescription();

    /**
     * Add a listener to receive notification when the plan finishes. Also
     * return the PlanRun that belongs to the current execution.
     */
    PlanRun addWaiter(PlanWaiter waiter);

    /**
     * Remove a plan waiter.
     */
    void removeWaiter(PlanWaiter waiter);

    Date getCreateTime();

    Date getStartTime();

    Date getEndTime();

    /**
     * Set this plan's state to INTERRUPTED. Used when the Admin
     * recovers and finds plans that were in progress before its shutdown.
     */
    void markAsInterrupted();

    /**
     * Return a formatted string representing the history of execution attempts
     * for this plan.
     */
    String showRuns();

    /**
     * Expose the execution state object for display in the GUI.
     */
    ExecutionState getExecutionState();

    /**
     * Return the number of tasks in the plan.
     */
    int getTotalTaskCount();

    /**
     * Support the plan status command by returning a description of finished
     * tasks. Plans may choose to summarize or couch the information in a more
     * user friendly way, rather than necessarily returning a list of all
     * finished tasks.
     * @param fm information should be sent to this formatter.
     */
    void describeFinished(Formatter fm,
                                 List<TaskRun> finished,
                                 int errorCount,
                                 boolean verbose);

    /**
     * Support the plan status command by returning a description of running
     * tasks. Plans may choose to summarize or couch the information in a more
     * user friendly way, rather than necessarily returning a list of all
     * finished tasks.
     * @param fm information should be sent to this formatter.
     */
    void describeRunning(Formatter fm,
                                List<TaskRun> running,
                                boolean verbose);

    /**
     * Support the plan status command by returning a description of tasks that
     * have not started. Plans may choose to summarize or couch the information
     * in a more user friendly way, rather than necessarily returning a list of
     * all finished tasks.
     * @param fm information should be sent to this formatter.
     */
    void describeNotStarted(Formatter fm,
                                   List<Task> notStarted,
                                   boolean verbose);

    /**
     * Take any PlannerImpl locks needed. Used to coordinate plan execution.
     */
    void getCatalogLocks();

    /**
     * Do any validation needed before starting the plan. A logger is supplied
     * because the plan's own logger is not set at this point.
     * @param force if true, the -force flag was specified to the CLI. Each
     * plan may have a different interpretation of that force flag. For
     * topology plans, force affects whether the plan will run in the face of
     * topology violations.
     * @throw IllegalCommandException if there is a problem with running the
     * plan.
     */
    void preExecuteCheck(boolean force, Logger executeLogger);

    void saveFailure(PlanRun planRun,
                            Throwable t,
                            String problem,
                            Logger logger);

    ExceptionTransfer getLatestRunExceptionTransfer();

    /**
     * Called in the course of plan execution when metadata is
     * updated and persisted. If true is returned the plan will also be
     * persisted in the same transaction as the metadata. Plans should use this
     * method if they maintain persistent state based on metadata and need to
     * track changes.
     *
     * @param metadata the metadata being updated
     * @return true if the plan should be persisted
     */
    boolean updatingMetadata(Metadata<?> metadata);
}
