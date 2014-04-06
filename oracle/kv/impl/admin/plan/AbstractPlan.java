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
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.AdminServiceParams;
import oracle.kv.impl.admin.PlanWaiter;
import oracle.kv.impl.admin.PlannerAdmin;
import oracle.kv.impl.admin.plan.ExecutionState.ExceptionTransfer;
import oracle.kv.impl.admin.plan.task.Task;
import oracle.kv.impl.admin.plan.task.TaskList;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.util.FormatUtils;

import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.Transaction;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

/**
 * Encapsulates a definition and mechanism for making a change to the KV
 * Store. Subclasses of AbstractPlan will define the different types of
 * AbstractPlan that can be carried out.
 *
 * Synchronization
 * ---------------
 * Any modifications to the plan at execution time, including the execution
 * state, plan run, and task run instances contained within it, must
 * synchronize on the plan instance. At execution time, there will be multiple
 * thread modifying these state fields, as well as threads that are trying
 * to save the instance to its DPL database.
 *
 * The synchronization locking order is 
 *  1. the Admin service (oracle.kv.impl.admin.Admin)
 *  2. the plan instance
 *  3. JE locks in the AdminDB
 *
 * Because of that, any synchronized plan methods must be careful not to try to
 * acquire the Admin mutex after already owning the plan mutex, lest a deadlock
 * occur. [#22161]
 *
 * Likewise, the mutex on the plan instance must be taken before any JE locks
 * are acquired. [#23134] Creating a hierarchy between database locks and
 * mutexes is regrettable, but is needed to prevent modification of the plan
 * instance while it is being serialized as a precursor to the database
 * operation. For example, the plan mutex is taken by the persist() method, in
 * this way:
 *  - obtain plan mutex
 *  - DPL put (serialize plan, acquire write lock on plan record)
 *
 *  or the DeployTopoPlan.incrementEndCount()
 *  - obtain plan mutex
 *  - read current topology from Admin DB (acquire a topo read lock, then 
 *  release)
 *  - update field in plan based on current topology
 *  
 * Because of this, we must refrain from starting Admin transactions that
 * acquire JE locks and then attempt to obtain the plan mutex.
 */
@Entity
public abstract class AbstractPlan implements Plan, Serializable {

    private static final long serialVersionUID = 1L;

    private static final int MAXPLANS = 10;

    /**
     * A unique sequence id for a plan.
     */
    @PrimaryKey
    private int id;

    /**
     * A user defined name.
     */
    private String name;

    /**
     * The time this plan was created.
     */
    protected long createTime;

    /**
     * A list of tasks that will be executed to carry out this plan.
     */
    protected TaskList taskList;

    /**
     * Plans may be executed multiple times. ExecutionState tracks the status
     * of each run.
     */
    private ExecutionState executionState;

    /**
     * A transient reference to the owning Planner.  This field must be set
     * whenever a Plan is constructed or retrieved from the database.
     */
    protected transient Planner planner;

    /**
     * The plan execution listeners enable monitoring, testing, and
     * asynchronous plan execution. Their invocation must be ordered, because
     * the optional PlanWaiter signifies that all plan related execution is
     * done, and it must be the last callback executed. Listeners may be added
     * and viewed concurrently, so access to the list should be synchronized.
     */
    private transient LinkedList<ExecutionListener> listeners;

    /* Set only when the plan is run. */
    protected transient Logger logger;

    /**
     */
    public AbstractPlan(AtomicInteger idGenerator,
                        String name,
                        Planner planner) {
        this.id = idGenerator.getAndIncrement();
        planner.getAdmin().saveNextId(idGenerator.get());

        if (name == null) {
            this.name = getDefaultName() + " (" + id + ")";
        } else {
            this.name = name;
        }
        createTime = System.currentTimeMillis();
        taskList = new TaskList(TaskList.ExecutionStrategy.SERIAL);

        initTransientFields();

        this.planner = planner;
        executionState = new ExecutionState(name);

        initWithParams(planner.getAdmin().getParams());
    }

    /*
     * No-arg ctor for use by DPL, for plans instantiated from the database.
     */
    AbstractPlan() {
        initTransientFields();
    }

    private synchronized void initTransientFields() {
        listeners = new LinkedList<ExecutionListener>();
    }

    private void initWithParams(AdminServiceParams aServiceParams) {
        addListener(new PlanTracker(aServiceParams));
    }
    /**
     * Plans instantiated from the database.
     */
    public void initPlansFromDb(Planner planner1,
                                 AdminServiceParams aServiceParams) {
        planner = planner1;
        initWithParams(aServiceParams);
    }

    /**
     * Loggers are set just before plan execution.
     * @param admin unused here, but may be used by subclasses.
     */
    void setLogger(Logger logger1) {
        this.logger = logger1;
    }

    @Override
    public int getId() {
        return id;
    }

    synchronized void validateStartOfRun() {
        executionState.validateStartOfNewRun(this);
    }

    synchronized PlanRun startNewRun() {
        return executionState.startNewRun();
    }

    /**
     * Returns true if this plan cannot be run while other plans
     * are running
     */
    @Override
    public abstract boolean isExclusive();

    /**
     * Gets the current state of this plan, as far as it is known by the
     * system.  In the event that this Admin has failed and the plan has been
     * read from the database, a plan may temporarily be in a RUNNING state
     * before moving to INTERRUPTED.
     *
     * @return the most recently computed state
     */
    @Override
    public synchronized State getState() {
        return executionState.getLatestState();
    }

    /**
     * Note that checking and changing the state must be atomic, and is
     * synchronized.
     */
    synchronized void requestApproval() {
        State currentState = getState();
        if (currentState.approvedAndCanExecute()) {
            /* We're just trying to retry a plan, no need to approve again. */
            return;
        }

        executionState.setPlanState(planner, this,
                                    Plan.State.APPROVED, "approval requested");
    }

    /**
     * Note that checking and changing the state must be atomic, and is
     * synchronized.
     */
    synchronized void requestCancellation() {

        executionState.setPlanState(planner, this, Plan.State.CANCELED,
                                    "cancellation requested");
    }

    /**
     * Check if is possible to directly cancel this plan without doing
     * interrupt processing, because it has not
     * started, or was already canceled.
     * @return true if the plan was not started, and has been canceled,
     * false if this plan is running, and if steps must be taken to interrupt
     * it.
     */
     synchronized boolean cancelIfNotStarted() {

        /* Already canceled. */
        Plan.State state = executionState.getLatestState();
        if (state == Plan.State.CANCELED) {
            return true;
        }

        if ((state == Plan.State.PENDING) ||
            (state == Plan.State.APPROVED)) {
            executionState.setPlanState(planner, this, Plan.State.CANCELED,
                                        "cancellation requested");
            return true;
        }
        return false;
    }

    /**
     * Change a RUNNING or INTERRUPT_REQUESTED plan to INTERRUPTED.
     */
    @Override
    public synchronized void markAsInterrupted() {

        Plan.State state = executionState.getLatestState();
        if (state == Plan.State.RUNNING) {
            executionState.setPlanState(planner, this,
                                        Plan.State.INTERRUPT_REQUESTED,
                                        "plan recovery");
            executionState.setPlanState(planner, this, Plan.State.INTERRUPTED,
                                        "plan recovery");
       } else if (state == Plan.State.INTERRUPT_REQUESTED) {
           executionState.setPlanState(planner, this, Plan.State.INTERRUPTED,
                                       "plan recovery");
       }
    }

    /**
     * Be sure to synchronize properly when setting plan state, so other methods
     * which check plan state, like addWaiter(), will be correct.
     */
    synchronized void setState(PlanRun planRun,
                               Planner planner,
                               Plan.State newState,
                               String msg) {
        planRun.setState(planner, this, newState, msg);
    }

    /**
     * Set the request flag to start a plan interruption. CALLER must
     * synchronize on the Admin first, because the requestInterrupted may
     * attempt to save the plan to the database. Doing so requires the Admin
     * mutex. Since the lock hierachy is Admin->Plan, the caller must
     * synchronize on Admin, and then acquire the plan mutex with this call.
     */
    synchronized void requestInterrupt() {
        PlanRun planRun = executionState.getLatestPlanRun();

        /* This plan isn't running */
        if (planRun == null) {
            return;
        }

        if (getState() == Plan.State.RUNNING) {
            executionState.setPlanState
                (planner, this, Plan.State.INTERRUPT_REQUESTED,
                 "plan interrupt");
            planner.getAdmin().savePlan(this, Admin.CAUSE_INTERRUPT_REQUEST);
        }

        planRun.requestInterrupt();
    }

    /**
     * @see Plan#addTask
     */
    @Override
    public synchronized void addTask(Task t) {
        taskList.add(t);
    }

    /**
     * @return the TaskList for this plan.
     */
    @Override
    public TaskList getTaskList() {
        return taskList;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "Plan " + id + "[" + name + "]";
    }

    /**
     * @return the createTime
     */
    @Override
    public Date getCreateTime() {
        return new Date(createTime);
    }

    /**
     * @return the startTime
     */
    @Override
    public Date getStartTime() {
        return executionState.getLatestStartTime();
    }

    /**
     * @return the endTime
     */
    @Override
    public Date getEndTime() {
        return executionState.getLatestEndTime();
    }

    /**
     * Each plan must save any resulting params or topology to the admin at the
     * beginning and end of execution.
     * @param plannerAdmin
     */
    abstract void preExecutionSave();

    public Logger getLogger() {
        return logger;
    }

    /**
     * Store the Plan objects in the BDB environment. Plan is
     * stored in the given EntityStore using the id field as the primary key.
     * 
     * The method must be called while synchronized on the plan instance
     * to ensure that the plan instance is not modified while the object is 
     * being serialized into bytes before being stored into the database. Note 
     * that the synchronization hierarchy requires that no other JE locks are 
     * held before the mutex is acquired, so the caller to this method must be
     * careful. The synchronization is done explicitly by the caller, rather 
     * than making this method synchronized, to provide more flexibility for 
     * obeying the synchronization hierarchy.
     *
     * @param estore the EntityStore that holds the Parameters
     * @param txn the transaction in progress
     */
    @Override
    public void persist(EntityStore estore, Transaction txn) {
        PrimaryIndex<Integer, AbstractPlan> pi =
            estore.getPrimaryIndex(Integer.class, AbstractPlan.class);
        pi.put(txn, this);
    }

    /**
     * Fetches all non-terminal Plans in the given EntityStore as a Map.
     */
    public static Map<Integer, Plan> fetchActivePlans
       (EntityStore estore, Transaction txn, Planner planner,
        AdminServiceParams aServiceParams) {

        final Map<Integer, Plan> activePlans = new HashMap<Integer, Plan>();

        PrimaryIndex<Integer, AbstractPlan> pi =
            estore.getPrimaryIndex(Integer.class, AbstractPlan.class);

        EntityCursor<AbstractPlan> cursor =
            pi.entities(txn, CursorConfig.READ_COMMITTED);

        try {
            for (AbstractPlan p = cursor.first();
                 p != null;
                 p = cursor.next()) {

                if (!p.getState().isTerminal()) {
                    p.initPlansFromDb(planner, aServiceParams);
                    activePlans.put(new Integer(p.getId()), p);
                }
            }
        } finally {
            cursor.close();
        }

        return activePlans;
    }

    /**
     * Retrieve the beginning plan id and number of plans that satisfy the
     * request.
     *
     * Returns an array of two integers indicating a range of plan id
     * numbers. [0] is the first id in the range, and [1] number of
     * plan ids in the range.
     *
     * Operates in three modes:
     *
     *    mode A requests howMany plans ids following startTime
     *    mode B requests howMany plans ids preceding endTime
     *    mode C requests a range of plan ids from startTime to endTime.
     *
     *    mode A is signified by endTime == 0
     *    mode B is signified by startTime == 0
     *    mode C is signified by neither startTime nor endTime being == 0.
     *        howMany is ignored in mode C.
     */
    public static int[] getPlanIdRange(EntityStore estore,
                                       Transaction txn,
                                       final long startTime,
                                       final long endTime,
                                       final int howMany) {

        final int[] range = {0, 0};

        PrimaryIndex<Integer, AbstractPlan> pi =
            estore.getPrimaryIndex(Integer.class, AbstractPlan.class);

        EntityCursor<AbstractPlan> cursor =
            pi.entities(txn, CursorConfig.READ_COMMITTED);

        int n = 0;
        try {
            if (startTime == 0L) {
                /* This is mode B. */
                for (AbstractPlan p = cursor.last();
                     p != null && n < howMany;
                     p = cursor.prev()) {

                    long creation = p.getCreateTime().getTime();
                    if (creation < endTime) {
                        n++;
                        range[0] = p.getId();
                    }
                }
                range[1] = n;
            } else {
                for (AbstractPlan p = cursor.first();
                     p != null;
                     p = cursor.next()) {

                    long creation = p.getCreateTime().getTime();
                    if (creation >= startTime) {
                        if (range[0] == 0) {
                            range[0] = p.getId();
                        }
                        if (endTime != 0L && creation > endTime) {
                            /* Mode C */
                            break;
                        }
                        if (howMany != 0 && n >= howMany) {
                            /* Mode A */
                            break;
                        }
                        n++;
                    }
                }
                range[1] = n;
            }
        } finally {
            cursor.close();
        }

        return range;
    }

    /**
     * Returns a map of plans starting at firstPlanId.  The number of plans in
     * the map is the lesser of howMany, MAXPLANS, or the number of extant
     * plans with id numbers following firstPlanId.  The range is not
     * necessarily fully populated; while plan ids are mostly sequential, it is
     * possible for values to be skipped.
     */
    public static Map<Integer, Plan> getPlanRange
        (final EntityStore estore,
         final Transaction txn,
         final Planner planner,
         final AdminServiceParams aServiceParams,
         final int firstPlanId,
         int howMany) {

        if (howMany > MAXPLANS) {
            howMany = MAXPLANS;
        }

        Map<Integer, Plan> fetchedPlans = new HashMap<Integer, Plan>();

        PrimaryIndex<Integer, AbstractPlan> pi =
            estore.getPrimaryIndex(Integer.class, AbstractPlan.class);

        EntityCursor<AbstractPlan> cursor =
            pi.entities(txn,
                        new Integer(firstPlanId), true, null, false,
                        CursorConfig.READ_COMMITTED);

        try {
            for (AbstractPlan p = cursor.first();
                 p != null && howMany > 0;
                 p = cursor.next(), howMany--) {

                p.initPlansFromDb(planner, aServiceParams);
                p.stripForDisplay();
                fetchedPlans.put(new Integer(p.getId()), p);
            }
        } finally {
            cursor.close();
        }

        return fetchedPlans;
    }

    /**
     * Returns the Plan corresponding to the given id,
     * fetched from the database; or null if there is no corresponding plan.
     */
    public static Plan fetchPlanById(int id,
                                     EntityStore estore,
                                     Transaction txn,
                                     Planner planner,
                                     AdminServiceParams aServiceParams) {

        PrimaryIndex<Integer, AbstractPlan> pi =
            estore.getPrimaryIndex(Integer.class, AbstractPlan.class);

        AbstractPlan p = pi.get(txn, new Integer(id), LockMode.READ_COMMITTED);
        if (p != null) {
            p.initPlansFromDb(planner, aServiceParams);
        }
        return p;
    }

    /**
     * Return the <i>howMany</i> most recent plans in the plan history. 
     * The plan instance returned will be stripped of memory intensive 
     * components and will not be executable.
     * @deprecated in favor of getPlanRange.
     */
    @Deprecated
    public static Map<Integer, Plan> fetchRecentPlansForDisplay
        (int howMany,
         EntityStore estore,
         Transaction txn,
         Planner planner,
         AdminServiceParams aServiceParams) {

        Map<Integer, Plan> fetchedPlans = new HashMap<Integer, Plan>();

        PrimaryIndex<Integer, AbstractPlan> pi =
            estore.getPrimaryIndex(Integer.class, AbstractPlan.class);

        EntityCursor<AbstractPlan> cursor =
            pi.entities(txn, CursorConfig.READ_COMMITTED);

        try {
            int n = 0;
            for (AbstractPlan p = cursor.last();
                 p != null && n < howMany;
                 p = cursor.prev(), n++) {

                p.initPlansFromDb(planner, aServiceParams);
                p.stripForDisplay();
                fetchedPlans.put(new Integer(p.getId()), p);
            }
        } finally {
            cursor.close();
        }

        return fetchedPlans;
    }

    /** 
     * Null out components of the plan that are have a large memory footprint,
     * and that are not needed for display. Meant to reduce the cost of 
     * display a list of command in the Admin CLI.
     */
    abstract void stripForDisplay();

    /**
     * Return the Admin to which this planner belongs.
     */
    public PlannerAdmin getAdmin() {
        if (planner != null) {
            return planner.getAdmin();
        }
        return null;
    }

    /**
     * ExecutionListeners are used to generate monitoring information about
     * plan execution, and for testing.
     */
    synchronized void addListener(ExecutionListener listener) {
        listeners.addFirst(listener);
    }

    /**
     * Listeners must be called in a specific order.
     */
    synchronized List<ExecutionListener> getListeners() {
        /* Return a new list, to guard against concurrent modification. */
        return new ArrayList<ExecutionListener>(listeners);
    }

    /**
     * A PlanWaiter should be added at the end of the listener list, so it
     * executes after all the other listeners, because it signifies the end of
     * all plan related execution.
     */
    @Override
    public synchronized PlanRun addWaiter(PlanWaiter waiter) {
        listeners.addLast(waiter);

        /* If the plan has ended already, release the waiter. */
        if (logger != null) {
            logger.log(Level.FINE,
                       "Adding plan waiter to {0}/{1}, state={2}",
                       new Object[]{getId(), getName(), getState()});
        }
        if (getState().planExecutionFinished()) {
            waiter.planEnd(this);
        }

        return getExecutionState().getLatestPlanRun();
    }

    @Override
    public synchronized void removeWaiter(PlanWaiter waiter) {
        listeners.remove(waiter);
    }

    /**
     * @return any failures from the most recent run, for display.
     */
    @Override
    public String getLatestRunFailureDescription() {
        return executionState.getLatestRunFailureDescription();
    }

    /**
     * For unit test support. Get the saved Exception and description for a
     * plan failure.
     */
    ExceptionTransfer getExceptionTransfer() {
        return executionState.getLatestExceptionTransfer();
    }

    /**
     * Return a formatted string representing the history of execution attempts
     * for this plan.
     */
    @Override
    public String showRuns() {
        return executionState.showRuns();
    }

    /**
     * Return the execution state object, from which we can extract detailed
     * information about the plan's execution history.
     */
    @Override
    public ExecutionState getExecutionState() {
        return executionState;
    }

    /**
     * Return the total number of tasks in the plan, including nested tasks.
     */
    @Override
    public int getTotalTaskCount() {
        return taskList.getTotalTaskCount();
    }

    /**
     * Return true if an interrupt has been requested. Long running tasks
     * must check this and return promptly if it's set.
     */
    public synchronized boolean isInterruptRequested() {
        return executionState.getLatestPlanRun().isInterruptRequested();
    }

    /**
     * Return true if an interrupt has been requested after the cleanup
     * phase started, and therefore we should actually interrupt task cleanup.
     */
    public synchronized boolean cleanupInterrupted() {
        return executionState.getLatestPlanRun().cleanupInterrupted();
    }

    synchronized void setCleanupStarted() {
        executionState.getLatestPlanRun().setCleanupStarted();
    }

    /**
     * Describe all finished tasks, for a status report. Plans can override
     * this to provide a more informative, user friendly report for specific
     * plans.
     */
    @Override
    public void describeFinished(final Formatter fm,
                                 final List<TaskRun> finished,
                                 int errorCount,
                                 final boolean verbose) {
        if (verbose) {
            /* show all tasks */
            for (TaskRun tRun : finished) {
                if (tRun.getState() == Task.State.ERROR) {
                    describeOneFailedTask(fm, tRun);
                    continue;
                } 

                fm.format("   Task %3d %" + Task.LONGEST_STATE +
                          "s at %25s: %s\n",
                          tRun.getTaskNum(),
                          tRun.getState(),
                          FormatUtils.formatDateAndTime(tRun.getEndTime()),
                          tRun.getTask());
                
                String details = tRun.displayTaskDetails("              ");
                if (details != null) {
                    fm.format("%s\n", details); 
                }
            }
            return;
        }

        /* If not verbose, only list the errors */
        if (errorCount > 0) {
            fm.format("\nFailures:");
            for (TaskRun tRun : finished) {
                if (tRun.getState() == Task.State.ERROR) {
                    describeOneFailedTask(fm, tRun);
                }
            }
        }
    }

    void describeOneFailedTask(final Formatter fm,
                               TaskRun tRun) {
        String failDesc = tRun.getFailureDescription();
        if (failDesc == null) {
            fm.format("   Task %3d %" + Task.LONGEST_STATE +
                      "s at %25s: %s\n",
                      tRun.getTaskNum(),
                      tRun.getState(),
                      FormatUtils.formatDateAndTime(tRun.getEndTime()),
                      tRun.getTask());
        } else {
            fm.format("   Task %3d %" + Task.LONGEST_STATE +
                      "s at %25s: %s: %s\n",
                      tRun.getTaskNum(),
                      tRun.getState(),
                      FormatUtils.formatDateAndTime(tRun.getEndTime()),
                      tRun.getTask(), failDesc);
        }
    }

    /**
     * Describe all running tasks, for a status report. Plans can override
     * this to provide a more informative, user friendly report for specific
     * plans.
     */
    @Override
    public void describeRunning(Formatter fm,
                                final List<TaskRun> running,
                                boolean verbose) {

        for (TaskRun tRun : running) {
            fm.format("   Task %d/%s started at %s\n",
                      tRun.getTaskNum(), tRun.getTask(),
                      FormatUtils.formatDateAndTime(tRun.getStartTime()));

        }
    }

    /**
     * Describe all pending tasks, for a status report. Plans can override
     * this to provide a more informative, user friendly report for specific
     * plans.
     */
    @Override
    public void describeNotStarted(Formatter fm,
                                   final List<Task> notStarted,
                                   boolean verbose) {
        for (Task t : notStarted) {
            fm.format("   Task %s\n", t);
        }
    }

    /**
     * Get all the component locks that will serialize access to shards and
     * RNs from concurrently executing plans.
     */
    @Override
    public void getCatalogLocks() {
        getPerTaskLocks();
    }

    /**
     * Ask each task to lock what it needs.
     */
    protected void getPerTaskLocks() {
        for (Task t : taskList.getTasks()) {
            t.lockTopoComponents(planner);
        }
    }

    /**
     * By default, no checks to be done. A logger is supplied, instead of
     * using the plan's logger, because the plan's logger is not set yet.
     */
    @Override
    public void preExecuteCheck(boolean force, Logger plannerlogger) {
    }

    public void upgradeToV3() {
        for (PlanRun pr : executionState.getHistory()) {
            pr.upgradeToV3(taskList.getTasks());
        }
    }

    /**
     * Synchronize when updating task execution information.
     */
    synchronized void saveFailure(TaskRun taskRun,
                                         Throwable t,
                                         String problemDescription,
                                         Logger logger2) {
        taskRun.saveFailure(t, problemDescription, logger2);
    }

    /**
     * Synchronize when updating task execution information.
     */
    synchronized void setTaskState(TaskRun taskRun,
                                          Task.State taskState,
                                          Logger logger2) {
        taskRun.setState(taskState, logger2);
    }

    @Override
    public synchronized void saveFailure(PlanRun planRun,
                                         Throwable t,
                                         String problem,
                                         Logger logger2) {
        planRun.saveFailure(t, problem, logger2);
    }

    synchronized TaskRun startTask(PlanRun planRun,
                                          Task task,
                                          Logger logger2) {
        return planRun.startTask(task, logger2);
    }

    synchronized void setEndTime(PlanRun planRun) {
        planRun.setEndTime();
    }

    synchronized void incrementEndCount(PlanRun planRun,
                                        Task.State state) {
        planRun.incrementEndCount(state);
    }

    synchronized void cleanupEnded(TaskRun taskRun) {
        taskRun.cleanupEnded();
    }

    synchronized void cleanupStarted(TaskRun taskRun) {
        taskRun.cleanupStarted();
    }

    synchronized void saveCleanupFailure(TaskRun taskRun, String info) {
        taskRun.saveCleanupFailure(info);
    }

    @Override
    public synchronized ExceptionTransfer getLatestRunExceptionTransfer() {
        PlanRun run = executionState.getLatestPlanRun();
        if (run == null) {
            return null;
        }

        return run.getExceptionTransfer();
    }

    /**
     * Default implementation. The default behavior is that a plan is not
     * persisted when metadata is updated. Specific plans should override this
     * method if they maintain persistent state based on metadata.
     *
     * @param metadata the metadata being updated
     * @return false
     */
    @Override
    public boolean updatingMetadata(Metadata<?> metadata) {
        return false;
    }

    public LoginManager getLoginManager() {
        return getAdmin().getLoginManager();
    }

    /**
     * Must be supported by any plan that will modify and persist a topology.
     * TODO: this is an accommodation to deal with the fact that TopologyPlan,
     * introduced in R1 and DeployTopoPlan, introduced in R2, represent 
     * different class hierarchies. An alternative is to remove this from
     * AbstractPlan and introduce a new Plan interface, implemented by both
     * TopologyPlan and DeployTopoPlan which would support this method, but
     * interfaces have their own issues.
     */
    public DeploymentInfo getDeployedInfo() {
        throw new UnsupportedOperationException();
    }
}
