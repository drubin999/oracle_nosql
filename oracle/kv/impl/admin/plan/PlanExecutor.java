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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.PlannerAdmin;
import oracle.kv.impl.admin.plan.task.NextJob;
import oracle.kv.impl.admin.plan.task.Task;
import oracle.kv.impl.admin.plan.task.TaskList;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * The PlanExecutor manages the execution of a single plan. The PlannerImpl
 * invokes the PlanExecutor via an execution service, so that plan submission
 * is asynchronous and multiple plans can be issued by the PlannerImpl, if
 * desired.
 *
 * PlannerImpl +-------------> plan 1
 *             +------------ > plan 2
 *             +------------ > plan 3
 *
 * A PlanExecutor uses its own execution service to execute the tasks that
 * constitute the plan. Serial tasks are executed one by one, and the executor
 * waits for task completion before proceeding. A one level deep hierarchy of
 * tasks is supported. A serial task can contain a set of nested tasks,
 * which are to be executed in parallel.
 *
 * Serial task 1
 * Serial task 2
 * Serial task 3 contains nested, parallel tasks 4, 5, 6
 * Serial task 7
 *
 * Only one level of nesting is supported; nested tasks cannot have nested
 * tasks below. This constraint is motivated to keep the scope of require
 * testing bounded. If we needed more levels of nesting in the future, we could
 * do so.
 *
 * Both serial and parallel tasks are submitted to an execution service, even
 * though the serial tasks could be executed in place by the PlanExecutor
 * itself. Using the execution service for both types of tasks makes plan
 * interrupt and cancellation processing uniform.
 *
 * When a serial task is executed, the PlanExecutor submits it to the execution
 * service, and then immediately awaits its completion. When a set of nested,
 * parallel tasks are seen, the PlanExecutor submits the whole set, and then
 * awaits completion of all of the set.
 *
 * All threads in the diagram come from the execution service.
 *
 *      Plan Executor (thread 1)
 *              |
 *              v
 *          serial task
 *              v
 *          serial task
 *              v
 *          parallel task --+----- thread 2 -> nested task a
 *                          +----- thread 3 -> nested task b
 *                          +----- thread 4 -> nested task c
 *                                       |
 *                                (all nested tasks completed)
 *              +------------------------+
 *              |
 *          serial task
 *              v
 *          serial task
 *
 * Multiple phase tasks: Parallel tasks may be single or multiple phased tasks.
 * This concept is orthogonal to the parallel vs serial execution choice, but
 * in terms of implementation, we currently only support multiphase tasks when
 * they are run by the parallel task executor, just to limit the number of
 * cases we need to test. A multiple phase is executed as a series of jobs,
 * Callables. The sequence of jobs can be determined dynamically. The point of
 * grouping the jobs within a single task it to make the jobs atomic from the
 * point of task recovery and retries.
 *
 * An example usage for a multiple phase task is partition migration, which
 * requires invocations of multiple RN interfaces. The sequence of invocations
 * is dependent on dynamic state found during the actual task execution. Some
 * of the phases are polling jobs, and may be scheduled with a delay.
 *
 * Executing, interrupting, cleanup of incomplete or failed tasks
 * --------------------------------------------------------------
 * From the system administrator's point of view, a plan executes
 * asynchronously. There is no time limit for plan or task execution. To see
 * information about a running plan, the sys admin will use a CLI command to
 * request plan status. If desired, the sys admin can interrupt the plan, which
 * will make it halt its execution. An interrupt may take some time to notice
 * and process.
 *
 * Interrupting a plan puts it into the Plan.State.INTERRUPT_REQUESTED state.
 * The plan execution framework and each task are required to check for an
 * interrupt request at reasonable intervals. Tasks which return before
 * completing all their desired actions return with Task.State.INTERRUPTED.
 *
 * A task that ended unsuccessfully due to an interrupt, or an error incurred
 * during execution should attempt to leave the store in a consistent
 * state. This is not the same as a rollback; it's permissible for a task to
 * alter the store, and to leave the store in that changed state, as long as
 * the store is consistent. The execution framework will execute the task
 * cleanup job for any task that does not end successfully.
 *
 * Cleanups can take a while, and the user may issue additional interrupts to
 * halt the cleanup. In addition, the cleanup itself might fail. Because of
 * that, clean up is a best effort, and not guaranteed. If cleanup doesn't
 * happen, the user should be able to fix the problem by retrying the plan, or
 * by running some other, corrective plan.
 *
 * A plan that is INTERRUPT_REQUESTED finally transitions to INTERRUPTED after
 * all tasks are halted, and either all cleanups are executed, or the cleanup
 * phase has itself been interrupted.
 */
public class PlanExecutor implements Callable<Plan.State> {

    /* The interval to check if a task or task cleanup finished.*/
    private final static int TASK_CHECK_INTERVAL = 1;
    private final static TimeUnit TASK_CHECK_TIME_UNIT = TimeUnit.SECONDS;

    /*
     * TODO: should the size of the thread pool be a parameter? Serial task
     * lists only use one thread at a time. Parallel tasks spawn off tasks
     * concurrently, but those tasks use the multi-job mechanism, and should be
     * asynchronous. The only real issue is when a parallel task is doing a
     * cleanup, because task cleanups are currently implemented as a single
     * Runnable that can take an unbounded amount of time and can consume the
     * thread.
     */
    private final static int POOL_SIZE = 5;

    private final AbstractPlan plan;
    private final ScheduledExecutorService pool;
    private final PlannerAdmin plannerAdmin;
    private final BasicPlannerImpl plannerImpl;
    private final PlanRun planRun;
    private final Logger logger;
    private final PlanFaultHandler faultHandler;

    /* The number of tasks in the plan, including all nested tasks. */
    private final int totalTasks;

    /* cleanup jobs that must be executed during this plan run */
    private final Set<CleanupInfo> taskCleanups;

    public static TestHook<Integer> FAULT_HOOK;

    /**
     * A PlanExecutor lives for the duration of a single plan execution.
     * Retried executions spawn new PlanExecutors.
     */
    public PlanExecutor(PlannerAdmin plannerAdmin,
                        BasicPlannerImpl plannerImpl,
                        AbstractPlan plan,
                        PlanRun planRun,
                        Logger logger) {

        this.plannerAdmin = plannerAdmin;
        this.plannerImpl = plannerImpl;
        this.plan = plan;
        this.planRun = planRun;
        this.logger = logger;
        faultHandler = new PlanFaultHandler();
        totalTasks = plan.getTaskList().getTotalTaskCount();

        pool = new ScheduledThreadPoolExecutor
            (POOL_SIZE, new KVThreadFactory("PlanExecutor", logger));
        taskCleanups = new HashSet<CleanupInfo>();
    }

    /**
     * Run a single plan.
     */
    @Override
    public Plan.State call()
        throws Exception {

        plan.setLogger(logger);

        try {
            faultHandler.execute(new SimpleProcedure() {
               @Override
               public void execute() throws Exception {
                   taskCleanups.clear();
                   plan.setState(planRun, plannerImpl, Plan.State.RUNNING,
                                 "Plan is starting");

                   for (ExecutionListener listener : plan.getListeners()) {
                       listener.planStart(plan);
                   }

                   /*
                    * Make the fact that we are starting persistent, so that
                    * plan recovery will know that the first task may have
                    * started.
                    */
                   plannerAdmin.savePlan(plan, Admin.CAUSE_EXEC);

                   /*
                    * Since the Admin database is the authority for the
                    * topology and service params, persist any required new
                    * topologies and parameter changes before executing remote
                    * requests, and potentially distributing these changes Some
                    * plans may do the persistent before task execution, while
                    * other tasks require information that is only available at
                    * task execution time. In the latter case, topos and params
                    * are saved during task execution,
                    */
                   plan.preExecutionSave();

                   TaskList taskList = plan.getTaskList();

                   /*
                    * Currently, we assume that the initial, topmost task list
                    * is always executed serially. Guard against a plan that
                    * starts with a parallel task list.
                    */
                   if (taskList.getStrategy() ==
                       TaskList.ExecutionStrategy.PARALLEL) {
                       throw new IllegalStateException
                          (plan + "does not expect to see a parallel task " +
                          "list at the topmost level");
                   }

                   /* Each task is executed one by one */
                   executeSerialTaskList(taskList);
                }}
                );
        } finally {

            /*
             * Save the plan's final state, and let listeners know that the
             * plan is finished.
             */
            faultHandler.execute(new SimpleProcedure() {
                @Override
                public void execute() {

                    /*
                     * Task processing is complete, either because we executed
                     * each task, or because the plan was stopped before we got
                     * to all of them.
                     *
                     * If the plan is already in ERROR state, something bad
                     * happened during plan processing, outside the task
                     * execution. The error has already been saved, no need to
                     * do any further processing.
                     */
                    logger.log(Level.FINE,
                               "finish count={0}, totalTasks={1}, " +
                               "errors={2}, interrupts={3}",
                               new Object[] {planRun.getNumFinishedTasks(),
                                             totalTasks,
                                             planRun.getNumErrorTasks(),
                                             planRun.getNumInterruptedTasks()});

                    if (plan.getState() != Plan.State.ERROR) {

                        if (planRun.getNumErrorTasks() > 0) {

                            /*
                             * If any tasks failed, put the plan in ERROR.
                             * Tasks may also fail during cleanup.
                             */
                            plan.setState
                                (planRun, plannerImpl, Plan.State.ERROR,
                                 "Plan incurred " +
                                 planRun.getNumErrorTasks() +
                                 " failed tasks:" +
                                 planRun.getFailureDescription(false));
                        } else if (plan.getState() ==
                                   Plan.State.INTERRUPT_REQUESTED) {

                            /* Indicate that task cleanup has finished */
                            plan.setState(planRun, plannerImpl,
                                          Plan.State.INTERRUPTED,
                                          "Plan interrupted," +
                                          planRun.getNumInterruptedTasks() +
                                          " tasks were interrupted");

                        } else if (planRun.getNumFinishedTasks() == totalTasks){
                            plan.setState(planRun, plannerImpl,
                                          Plan.State.SUCCEEDED,
                                          "Plan finished.");
                        } else {

                            /*
                             * The plan died,even through an interrupt was
                             * not requested. We must go through interrupt
                             * requested state, as it's not permissible to
                             * go from RUNNING->INTERRUPTED.
                             */
                            int unstarted = totalTasks -
                                planRun.getNumFinishedTasks();
                            plan.setState(planRun, plannerImpl,
                                          Plan.State.INTERRUPT_REQUESTED,
                                          "Plan did not execute " + unstarted +
                                          " tasks even through interrupt not" +
                                          " requested");
                            plan.setState(planRun, plannerImpl,
                                          Plan.State.INTERRUPTED,
                                          "Plan interrupted, " + unstarted +
                                          " tasks not started");
                        }
                    }

                    plannerAdmin.savePlan(plan, Admin.CAUSE_EXEC);
                    plannerImpl.planFinished(plan);

                    for (ExecutionListener listener : plan.getListeners()) {
                        listener.planEnd(plan);
                    }
                }
            }
            );
            pool.shutdownNow();
        }
        return plan.getState();
    }

    /**
     * Each task is executed one by one. If a task contains nested tasks,
     * those tasks are execute in parallel. TODO: must match status report!!
     */
    private void executeSerialTaskList(final TaskList taskList)
        throws Exception {

        if (taskList.getStrategy() != TaskList.ExecutionStrategy.SERIAL) {
            throw new IllegalStateException
                (plan + " expects a serial task list, not " +
                 taskList.getStrategy());
        }

        /*
         * Execute each task serially, waiting for it to finish before
         * starting the next one.
         */
        for (Task task : taskList.getTasks()) {

            if (plan.isInterruptRequested()) {
                /* Stop spawning off tasks. */
                break;
            }

            TaskList nestedTaskList = task.getNestedTasks();

            /*
             * Note: this assumes that a task containing nested tasks has no
             * real work of its own to do. Its own doWork method is not called.
             * In addition, all nested tasks will be executed in parallel,
             * because there seems little purpose to nesting otherwise.
             */
            if (nestedTaskList != null) {
                if (executeParallelTaskList(nestedTaskList)) {
                    continue;
                }

                /* One or more tasks failed and are supposed to stop the plan.*/
                break;
            }

            TaskRun taskRun = plan.startTask(planRun, task, logger);
            Future<Task.State> future = null;
            try {

                /*
                 * The listener must be executed within the try/catch, so that
                 * any interrupts are properly handled.
                 */
                for (ExecutionListener listener : plan.getListeners()) {
                    listener.taskStart(plan, task, taskRun.getTaskNum(),
                                       totalTasks);
                }

                TestHookExecute.doHookIfSet(FAULT_HOOK, null);
                future = pool.submit(task.getFirstJob(taskRun.getTaskNum(),
                                                      null));
            } catch (RejectedExecutionException e) {

                /*
                 * This task didn't get started. Not enough threads, halt the
                 * plan for now.
                 */
                recordTaskFailure(taskRun, Task.State.PENDING, e, null);
                throw e;
            }

            /*
             * Wait for this task to finish and examine its result before
             * continuing.
             */
            boolean planShouldContinue =
                examineFuture(future, taskRun, task, null);

            /*
             * If the task did not finish successfully, either because it hit
             * an error, or was interrupted, examineFuture will have queued
             * this task cleanup. If the cleanup itself doesn't finish, stop
             * the plan no matter what the continueOnError flag says. All task
             * cleanups must execute successfully.
             */
            if (!waitForTaskCleanups()) {
                break;
            }

            /*
             * The task ended and any cleanups that were needed were done, but
             * there was a problem, and a failure in this task is supposed to
             * end the plan.
             */
            if (!planShouldContinue) {
                break;
            }
        }
    }

    /**
     * Execute the tasks concurrently. Will only return if all tasks have been
     * processed, or the plan is interrupted.
     * @return false if any of the tasks have failed, and if such a task
     * failure is supposed to stop the plan.
     */
    private boolean executeParallelTaskList(final TaskList taskList)
        throws Exception {

        if (taskList.getStrategy() != TaskList.ExecutionStrategy.PARALLEL) {
            throw new IllegalStateException
                (plan + " expects a parallel task list, not " +
                 taskList.getStrategy());
        }

        ParallelTaskRunner runner = new ParallelTaskRunner(taskList);

        /*
         * Submit each task right away. Following the results of the initial
         * execution, each task may then issue additional work.
         */
        try {
            for (Task t : taskList.getTasks()) {

                /*
                 * Only one level of nesting is permitted, guard against
                 * additional nesting.
                 */
                if (t.getNestedTasks() != null) {
                    throw new IllegalStateException
                        ("Only one level of task nesting is currently " +
                         "supported, but " + t + " has nested tasks");
                }
                runner.submitFirstJob(t);
            }
        } finally {

            /*
             * If some jobs weren't even submitted, make sure we don't wait
             * for them.
             */
            runner.clearUnsubmittedTasks();

            /*
             * Wait for all tasks to end. A plan interrupt does not short
             * change this wait, because all tasks should be checking the
             * interrupt flag, and should end if it is set.
             */
            boolean listFinished = false;
            do {
                try {
                    listFinished = runner.awaitFinish();
                } catch (InterruptedException e) {
                    logger.info("Interrupted while waiting for completion of " +
                                "parallel tasks: " +
                                LoggerUtils.getStackTrace(e));
                }

                runner.checkForDeadTasks();

            } while (!listFinished);

            logger.log(Level.FINE, "Parallel task submission: listFinished={0}",
                       listFinished);
        }

        boolean planShouldContinue = true;
        for (ParallelTaskRunner.TaskInfo taskInfo : runner.getTaskInfo()) {

           /*
             * Look at the state of each finished parallel task, and see if it
             * impacts the plan state.
             */
            if (examineFuture(taskInfo.future,
                              taskInfo.taskRun,
                              taskInfo.task,
                              taskInfo.additionalInfo) == false) {
                planShouldContinue = false;
            }
        }

        /*
         * If any tasks failed, they would have issued a cleanup.
         */
        waitForTaskCleanups();
        return planShouldContinue;
    }

    /**
     * Set task and plan states appropriately to save failure info. If an
     * exception occurred, rethrow it.
     * @param e because task failures can occur without an exception this
     * param may be null.
     * @throws Exception
     */
    private void recordTaskFailure(TaskRun taskRun,
                                   Task.State taskState,
                                   Exception e,
                                   String additionalInfo)
        throws Exception {

        Throwable trueCause = null;
        if (e != null) {
            trueCause = e;
            if (e instanceof ExecutionException) {
                trueCause = e.getCause();
            }
        }

        /* Problem is logged by saveFailure() */
        String problem = taskRun.getTaskNum() + "/" + taskRun.getTaskName() +
            " failed.";
        if (additionalInfo != null) {
            problem += " " + additionalInfo;
        }

        /* Set new task and plan states, and save the exception in the plan. */
        plan.setTaskState(taskRun, taskState, logger);
        plan.saveFailure(taskRun, trueCause, problem, logger);

        /*
         * Save the failure even if the plan is going to keep on running We
         * want to present the first failure that occurred, to make the error
         * situation more understandable to the user.  PlanRun.saveFailure will
         * save all failures from the plan.
         */
        plan.saveFailure(planRun, ((trueCause == null) ? e : trueCause),
                         problem, logger);
        logger.log(Level.FINE,
                   "Record failure of {0}/{1}, final state={2} problem={3} {4}",
                   new Object[] { taskRun.getTaskNum(),
                                  taskRun.getTaskName(), taskState, e,
                                  additionalInfo});
    }

    /*
     * Set this plan to ERROR state. Something bad happened during plan
     * execution, inbetween tasks.  Note that planRun.saveFailure will log the
     * exception.
     */
    private void putPlanInError(Throwable t) {

        /*
         * The plan may already be in ERROR state if a task failed. If so, just
         * log this new failure, no need to set state again.
         */
        String problem = "Problem during plan execution";
        plan.saveFailure(planRun, t, problem, logger);
        if (plan.getState() == Plan.State.ERROR) {
            logger.log(Level.SEVERE,
                       "Second error in plan execution, plan already in ERROR",
                       t);
        } else {
            plan.setState(planRun, plannerImpl, Plan.State.ERROR, problem);
        }
    }

    /**
     * @return true if all task cleanups executed cleanly. Each task should
     * check the planRun to see if a second interrupt request has occurred,
     * and if the cleanup itself should end prematurely.
     */
    boolean waitForTaskCleanups() {

        if (taskCleanups.isEmpty()) {
            return true;
        }

        boolean allSucceeded = true;
        plan.setCleanupStarted();

        for (CleanupInfo c : taskCleanups) {
            TaskRun tRun = c.getTaskRun();
            boolean cleanupDone = false;
            do {
                try {
                    logger.log(Level.INFO,
                               "Waiting for cleanup of task {0}/{1}",
                               new Object[] { tRun.getTaskNum(),
                                              tRun.getTaskName()});
                    Future<?> f = c.getFuture();
                    if (f != null) {

                        /*
                         * The future can be null if there was an exception
                         * before job was submitted.
                         */
                        f.get(TASK_CHECK_INTERVAL, TASK_CHECK_TIME_UNIT);
                    }
                    cleanupDone = true;
                } catch (InterruptedException retry) {
                    logger.log(Level.FINE,
                               "Cleanup of task {0}/{1} interrupted",
                               new Object[] { tRun.getTaskNum(),
                                              tRun.getTaskName()});
                } catch (TimeoutException retry) {
                    logger.log(Level.FINE,
                               "Cleanup of task {0}/{1} timed out, will retry",
                               new Object[] { tRun.getTaskNum(),
                                              tRun.getTaskName()});
                } catch (Exception e) {
                    allSucceeded = false;
                    cleanupDone = true;
                    String info = LoggerUtils.getStackTrace(e);
                    logger.log(Level.SEVERE,
                               "Cleanup of task {0}/{1} failed: {2}",
                               new Object[] { tRun.getTaskNum(),
                                              tRun.getTaskName(),
                                              info});
                    plan.saveCleanupFailure(tRun, info);
                } finally {
                    plan.cleanupEnded(tRun);
                }
            } while (!plan.cleanupInterrupted() && !cleanupDone);
        }
        return allSucceeded;
    }

    /**
     * Any RuntimeExceptions or Errors that occur when using the plan fault
     * handler will be saved as information within the PlanRun, and will
     * set the plan to ERROR.
     */
    private class PlanFaultHandler {

        void execute(SimpleProcedure proc) {
            try {
                proc.execute();
            } catch (Error e) {
                putPlanInError(e);
                throw e;
            } catch (RuntimeException re) {
                putPlanInError(re);
                throw re;
            } catch (Exception e) {
                putPlanInError(e);
                throw new OperationFaultException
                   ("Problem in plan execution", e);
            }
        }
    }

    /** For fault handling. */
    public interface SimpleProcedure {
        void execute() throws Exception;
    }

    /**
     * Execute a set of MultiJobTasks in parallel.
     *
     * The ParallelTaskRunner has the logic to concurrently start the first job
     * of each task, and then to process the results of each job. There is no
     * bound on the amount of time for each task. The runner provides a way to
     * query for the completion of all tasks.
     *
     * When the last job of a task is finished, that job returns a terminal
     * task status, and the runner updates TaskRun information, and marks
     * that job as complete.
     */
    public class ParallelTaskRunner  {

        private final CountDownLatch waitForCompletion;
        private final Map<Integer, TaskInfo> taskInfoMap;
        private final int numParallelTasks;
        private static final String RUNNER = "Parallel Task Runner:";
        private int numSubmitted = 0;

        /**
         * @param taskList the set of tasks to execute in parallel.
         */
        ParallelTaskRunner(TaskList taskList) {

            numParallelTasks = taskList.getTotalTaskCount();
            waitForCompletion = new CountDownLatch(numParallelTasks);
            taskInfoMap = new HashMap<Integer, TaskInfo>();
        }

        public void clearUnsubmittedTasks() {
            if (numSubmitted < numParallelTasks) {
                logger.log(Level.INFO,
                           "{0} only {1} out of {2} tasks started, reduce " +
                           "number of tasks to wait for.",
                           new Object[] { RUNNER, numSubmitted,
                                          numParallelTasks});

                for (int i = 0; i < (numParallelTasks - numSubmitted); i++) {
                    waitForCompletion.countDown();
                }
            }
        }

        /**
         * Mark the task start, and submit the first job of a multi job task
         * the execution service.
         */
        void submitFirstJob(Task task)
            throws Exception {

            numSubmitted++;
            TaskRun taskRun = plan.startTask(planRun, task, logger);
            try {

                /*
                 * The listener must be executed within the try/catch, so that
                 * any interrupts that occur during the listener execution are
                 * properly handled.
                 */
                for (ExecutionListener listener : plan.getListeners()) {
                    listener.taskStart(plan, task, taskRun.getTaskNum(),
                                       totalTasks);
                }

                /*
                 * Initialize the taskInfoMap for this task slot, so it's setup
                 * before the first job is executed.
                 */
                setTaskInfo(taskRun, task, null);

                /*
                 * The first phase of each task is always the
                 * MultiJobTask.startwork() method.
                 */
                logger.log(Level.FINE, "{0} submitted {1} for task {2}",
                           new Object[] { RUNNER, task.getName(),
                                          taskRun.getTaskNum()});
                TestHookExecute.doHookIfSet(FAULT_HOOK, null);
                Future<Task.State> f =
                    pool.submit(task.getFirstJob(taskRun.getTaskNum(), this));

                /* Keep track of the future */
                setTaskInfo(taskRun, task, f);

            } catch (RejectedExecutionException e) {
                logger.log(Level.SEVERE, "{0} task {2}/job={3} got {4}",
                           new Object[] { RUNNER, taskRun.getTaskNum(),
                                          task.getName(), e});
                throw new IllegalStateException
                    ("Unexpected " + e + ", ScheduledExecutionService should " +
                     "have an unbounded work queue");
            } catch (Exception e) {

                /*
                 * Since the schedule execution service should accept any
                 * number of starting jobs, any exception that occurs here is
                 * either a problem in one of the plan listeners, or a bug in
                 * the method. Even though the task didn't execute, manufacture
                 * an error state, and put the task in error mode, because
                 * otherwise the exception will be lost and the task will
                 * appear to hang.
                 */
                recordTaskFailure(taskRun, Task.State.ERROR, e,
                                  "Problem with concurrent start of parallel " +
                                  "tasks");
            }
        }

        /**
         * Examine the NextJob information provided by the previous job, and
         * either schedule a follow on job,or deem this task to be completed.
         * Called by the thread that is executing a phase.
         */
        public Task.State dispatchNextJob(int taskId, NextJob nextJob) {

            TestHookExecute.doHookIfSet(FAULT_HOOK, null);

            /* Save the plan to preserve task status generated by this job. */
            plannerAdmin.savePlan(plan, Admin.CAUSE_EXEC);

            switch (nextJob.getPrevJobTaskState()) {

            case RUNNING:
            case PENDING:
                if (plan.isInterruptRequested()) {
                    logger.log(Level.INFO,
                               "{0}.dispatch: plan is interrupted, " +
                               "{1}/{2} job={3} will " +
                               "not be executed",
                               new Object[] { RUNNER,
                                              taskId,
                                              taskInfoMap.get(taskId).getName(),
                                              nextJob.getDescription()});

                    completeTaskInfo(taskId, nextJob.getAdditionalInfo());
                    waitForCompletion.countDown();
                    return Task.State.INTERRUPTED;
                }

                logger.log(Level.FINE,
                           "{0} task {1}/{2} job={3} will run in {4} {5}",
                           new Object[] { RUNNER,
                                          taskId,
                                          taskInfoMap.get(taskId).getName(),
                                          nextJob.getDescription(),
                                          nextJob.getDelay(),
                                          nextJob.getTimeUnit()});

                Future<Task.State> f = pool.schedule
                    (nextJob.getNextCallable(),
                     nextJob.getDelay(),
                     nextJob.getTimeUnit());
                updateTaskInfo(taskId, f);
                break;

            case SUCCEEDED:
            case INTERRUPTED:
            case ERROR:
                completeTaskInfo(taskId, nextJob.getAdditionalInfo());
                waitForCompletion.countDown();

                /*
                 * We only really need this logging info when there are
                 * errors.
                 */
                Level logLevel = Level.INFO;
                if (nextJob.getPrevJobTaskState() == Task.State.SUCCEEDED) {
                    logLevel = Level.FINE;
                }

                logger.log(logLevel,
                           "{0} task {1}/job={2} finished, state={3}",
                           new Object[] { RUNNER, taskId, nextJob,
                                          nextJob.getPrevJobTaskState()});
                break;
            }

            return nextJob.getPrevJobTaskState();
        }

        /**
         * Tasks are responsible for periodically checking if the plan
         * interrupt request flag is set.
         * @return true if all tasks have reported themselves as finished.
         * Tasks may have finished successfully or in error, but one way or
         * another, they are done.
         */
        boolean awaitFinish()
            throws InterruptedException {

            logger.log(Level.FINE,
                       "{0} Wait for {1} out of {2} tasks to complete",
                       new Object[] { RUNNER, waitForCompletion.getCount(),
                                      numParallelTasks});
            boolean done = waitForCompletion.await(TASK_CHECK_INTERVAL,
                                                   TASK_CHECK_TIME_UNIT);
            logger.log(Level.FINE,
                       "{0} {1} out of {2} tasks still outstanding",
                       new Object[] { RUNNER, waitForCompletion.getCount(),
                                      numParallelTasks});


            return done;
        }

        protected Task getTask(int taskId) {
            synchronized (taskInfoMap) {
                return taskInfoMap.get(taskId).task;
            }
        }

        public Map<String, String> getDetails(int taskId) {
            synchronized (taskInfoMap) {
                return taskInfoMap.get(taskId).taskRun.getDetails();
            }
        }

        private void setTaskInfo(TaskRun taskRun,
                                 Task task,
                                 Future<Task.State> f) {
            synchronized (taskInfoMap) {
                taskInfoMap.put(taskRun.getTaskNum(),
                                new TaskInfo(taskRun, task, f));
            }
        }

        private void updateTaskInfo(int taskId, Future<Task.State> f) {
            synchronized (taskInfoMap) {
                TaskInfo oldInfo = taskInfoMap.get(taskId);
                taskInfoMap.put(taskId,
                                new TaskInfo(oldInfo.taskRun, oldInfo.task, f));
            }
        }

        private void completeTaskInfo(int taskId, String additionalInfo) {
            synchronized (taskInfoMap) {
                TaskInfo oldInfo = taskInfoMap.get(taskId);
                oldInfo.addInfo(additionalInfo);
                oldInfo.completed = true;
            }
        }

        Collection<TaskInfo> getTaskInfo() {
            return taskInfoMap.values();
        }

        /**
         * Find tasks that have died ungracefully, and failed to execute the
         * final completion steps. Mark them as completed now, and countdown
         * on the waitForCompletion latch.
         */
        void checkForDeadTasks() {
            synchronized (taskInfoMap) {
                for (TaskInfo ti : taskInfoMap.values()) {

                    boolean doCleanup = false;
                    if (ti.future == null) {

                        /*
                         * The future may be null if the task did not truly
                         * start.
                         */
                        if (!ti.completed) {
                            logger.log
                                (Level.INFO,
                                 "{0} cleaning up unstarted task {1} {2}",
                                 new Object[] { RUNNER,
                                                ti.taskRun.getTaskNum(),
                                                ti.task});
                            doCleanup = true;
                        }
                    } else if (ti.future.isDone() && (!ti.completed)) {
                        logger.log(Level.INFO,
                                   "{0} cleaning up dead task {1} {2}",
                                   new Object[] { RUNNER,
                                                  ti.taskRun.getTaskNum(),
                                                  ti.task});
                        doCleanup = true;
                    }

                    if (doCleanup) {
                        ti.completed = true;
                        waitForCompletion.countDown();
                    }
                }
            }
        }

        /**
         * A struct to keep all the pieces of information for task execution
         * together.
         */
        private class TaskInfo {
            final TaskRun taskRun;
            final Task task;
            final Future<Task.State> future;
            boolean completed;
            String additionalInfo;

            TaskInfo(TaskRun taskRun,
                     Task task,
                     Future<Task.State> future) {
                this.taskRun = taskRun;
                this.task = task;
                this.future = future;
                completed = false;
            }

            void addInfo(String info) {
                if (additionalInfo == null) {
                    additionalInfo = info;
                } else {
                    additionalInfo += info + " ";
                }
            }

            String getName() {
                return taskRun.getTaskName();
            }
        }
    }

    /**
     * Return when the task finishes executing.
     */
    private Task.State waitForFinish(Future<Task.State> future,
                                     TaskRun taskRun)
        throws Exception {

        if (future == null) {
            return Task.State.ERROR;
        }

        Task.State tState = Task.State.RUNNING;

        while (tState == Task.State.RUNNING) {
            logger.log(Level.FINEST,
                       "start wait for {0}/{1}",
                       new Object[] {taskRun.getTaskNum(),
                                     taskRun.getTaskName()});

            try {
                tState = future.get(TASK_CHECK_INTERVAL,
                                    TASK_CHECK_TIME_UNIT);
            } catch (InterruptedException retry) {

                /*
                 * Wait again. If there is a true timeout, the future would
                 * have thrown a TimeoutException -- this is just a hiccup.
                 */
                logger.log(Level.FINE,
                           "wait for finish of {0}/{1} got {2}",
                           new Object[] {taskRun.getTaskNum(),
                                         taskRun.getTaskName(), retry});

                /* Need to retry, so consider this task to be running. */
                tState = Task.State.RUNNING;
            } catch (TimeoutException e) {
                logger.log(Level.FINE,
                           "wait for finish of {0}/{1} got {2}",
                           new Object[] {taskRun.getTaskNum(),
                                         taskRun.getTaskName(), e});

                /*
                 * Check the interrupt flag here. It's the duty of the task to
                 * monitor the plan interrupt flag, but if the task has a bug,
                 * it might never finish, which would make this plan
                 * uninterruptible.
                 */
                if (plan.isInterruptRequested()) {
                    return Task.State.INTERRUPTED;
                }

                /* Need to retry, so consider this task to be running. */
                tState = Task.State.RUNNING;
            } catch (Exception e) {
                /* this task has failed. */
                throw e;
            }
        }

        return tState;
    }

    /**
     * Now that a task is finished, look at the result, which has been
     * lodged in the future. May be called concurrently, so must be thread
     * safe.
     *
     * Information about the task execution that we may want to save and
     * display is held within the future and in the additionalInfo field.
     *
     * @return true if the plan should continue.
     */
    private boolean examineFuture(Future<Task.State> future,
                                  TaskRun taskRun,
                                  Task task,
                                  String additionalInfo)
        throws Exception {

        /**
         * The plan should stop if (a) a failure occurs for a task that is
         * configured to stop the plan upon failure, or (b) an interrupt
         * is seen. In all other cases (task succeeds, or task fails but is
         * not supposed to stop the plan), the plan will continued.
         */
        boolean planShouldContinue = task.continuePastError();

        try {
            Task.State tState = waitForFinish(future, taskRun);

            /* The task returned, set the task state appropriately */
            if (tState == null) {
                plan.setTaskState(taskRun, Task.State.INTERRUPTED, logger);
                plan.saveFailure(taskRun,
                                 null, /* throwable */
                                 "Null status returned from future.get for " +
                                 task, logger);
                planShouldContinue = false;
            } else if (tState == Task.State.INTERRUPTED) {
                taskRun.setState(Task.State.INTERRUPTED, logger);
                plan.saveFailure(taskRun,
                                 null, /* throwable */
                                 "Task didn't complete, plan was interrupted",
                                 logger);
                planShouldContinue = false;
            } else if (tState == Task.State.ERROR) {
                recordTaskFailure(taskRun,
                                  tState,
                                  null,
                                  additionalInfo);

            } else {
                taskRun.setState(tState, logger);
            }
        } catch (Exception e) {

            /*
             * The thread that executed the task ran into a failure. Mark
             * this task as failed.
             */
            recordTaskFailure(taskRun, Task.State.ERROR, e, additionalInfo);

        } finally {

            if ((future != null) &&
                ((taskRun.getState() == Task.State.ERROR) ||
                 (taskRun.getState() == Task.State.INTERRUPTED))) {

                /*
                 * If this task did execute and has any cleanup work, do it.
                 */
                Runnable cleanupJob = task.getCleanupJob();
                if (cleanupJob != null) {
                    plan.cleanupStarted(taskRun);
                    logger.log(Level.INFO,
                               "Task {0}/{1} ended in {2}, cleaning up.",
                               new Object[] { taskRun.getTaskNum(),
                                              taskRun.getTaskName(),
                                              taskRun.getState()});
                    Future<?> f= pool.submit(cleanupJob);
                    taskCleanups.add(new CleanupInfo(taskRun, f));
                }
            }

            plan.incrementEndCount(planRun, taskRun.getState());

            /*
             * The Future is either done in which case this is a no-op, or
             * it was interrupted or timed out, in which case it needs to
             * be Canceled.
             */
            if ((future != null) && !future.isDone()) {
                future.cancel(true);
            }

            /*
             * Keep updating the plan end time, after each task, so it's
             * available even when plans fail.
             */
            plan.setEndTime(planRun);

            /*
             * If the plan is still running, check whether the task
             * succeeded.  Plans may attempt to run all tasks, even if some
             * have failed, but a plan is only considered successful if
             * there are no failed tasks.
             */
            synchronized (plan) {
                if (planRun.getState() == Plan.State.RUNNING){
                    if ((taskRun.getState() != Task.State.SUCCEEDED) &&
                        (!task.continuePastError())) {
                        planShouldContinue = false;
                    } else {
                        planShouldContinue = true;
                    }
                }

                /* Save the plan after each task. */
                plannerAdmin.savePlan(plan, Admin.CAUSE_EXEC);
            }

            for (ExecutionListener listener : plan.getListeners()) {
                listener.taskEnd(plan, task, taskRun, taskRun.getTaskNum(),
                                 totalTasks);
            }
        }

        return planShouldContinue;
    }

    /**
     * A struct to bundle together information needed for task cleanup
     */
    private class CleanupInfo {

        private final TaskRun taskRun;

        /* The future used for executing the cleanup. */
        private final Future<?> future;

        CleanupInfo(TaskRun taskRun, Future<?> future) {
            this.taskRun = taskRun;
            this.future = future;
        }

        TaskRun getTaskRun() {
            return taskRun;
        }

        Future<?> getFuture() {
            return future;
        }
    }

    /**
     * Get a list of unstarted tasks, for status reporting, in precisely the
     * order that they will be spawned. Because of that, this must match
     * executeSerialTaskList and executeParallelTaskList.
     *
     * @param startTask the first task to return.
     */
    public static List<Task> getFlatTaskList(Plan plan, int startTask) {
        TaskList taskList = plan.getTaskList();
        List<Task> unstarted = new ArrayList<Task>();
        int taskCount = 0;
        for (Task task : taskList.getTasks()) {

            TaskList nestedTasks = task.getNestedTasks();

            if (nestedTasks != null) {
                for (Task nested : nestedTasks.getTasks()) {
                    if (taskCount >= startTask) {
                        unstarted.add(nested);
                    }
                    taskCount++;
                }
                continue;
            }

            if (taskCount >= startTask) {
                unstarted.add(task);
            }
            taskCount++;
        }
        return unstarted;
    }
}
