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
import java.util.Formatter;
import java.util.List;

import oracle.kv.impl.admin.plan.task.Task;
import oracle.kv.impl.util.FormatUtils;

/**
 * A status report about a running plan, in particular meant to help the
 * user assess the progress of a long running elasticity plan.
 * Plans such as DeployTopoPlan should extend the basic descriptor methods
 * implemented by AbstractPlan to improve the usabilty of the information.
 */
public class StatusReport {

    /*
     * An old fashion bitmask is used to express options, for flexibility
     * while avoiding an RMI change.
     */
    public static final int VERBOSE_BIT = 0x1;
    public static final int SHOW_FINISHED_BIT= 0x2;

    private final List<TaskRun> finished;
    private final List<TaskRun> running;
    private final List<Task> pending;
    private final boolean verbose;
    private final boolean showFinished;

    private final Plan plan;
    private final PlanRun planRun;

    private static String NUM_LABEL = "%-22s %-30d\n";
    static String STRING_LABEL = "%-22s %-30s\n";

    /**
     * Use an old fashioned bitmask for options in order to have room for
     * customization without changing the RMI interface.
     *
     * Look at the latest plan run, and report on plan and task status.
     */
    public StatusReport(Plan plan, long optionFlags) {
        this.plan = plan;

        /* Hang onto this planRun in case another run starts */
        this.planRun = plan.getExecutionState().getLatestPlanRun();
        finished = new ArrayList<TaskRun>();
        running = new ArrayList<TaskRun>();
        int nStarted = 0;
        if (planRun != null) {
            List<TaskRun> allStarted = planRun.getTaskRuns();
            nStarted = allStarted.size();

            for (TaskRun tRun : allStarted) {
                if (tRun.getState().equals(Task.State.RUNNING)) {
                    running.add(tRun);
                } else {
                    finished.add(tRun);
                }
            }
        }

        pending = PlanExecutor.getFlatTaskList(plan, nStarted);
        this.verbose = ((optionFlags & VERBOSE_BIT)!= 0);
        this.showFinished = ((optionFlags & SHOW_FINISHED_BIT )!= 0);
    }

    /**
     * Obtain the report.
     *
     * A copy of the current output is:
     *
     * Plan rebalanceAttempt
     * State:                 ERROR
     * Attempt number:        3
     * Started:               2012-08-19 00:46:30 UTC
     * Ended:                 2012-08-19 00:46:30 UTC
     * Plan failures:         1 failure
     *	Failure 1: Problem during plan execution: Test exception injected
     * Total tasks:           5
     *  Successful:           2
     *  Incomplete:           1
     *  Not started:          2
     * Incomplete tasks
     *    Task 3/RelocateRN move rg2-rn3 from sn3(localhost:13271) to
     *            sn4(localhost:13291) started at 2012-08-19 00:46:30 UTC
     * Tasks not started
     *   Task DeleteRepNode remove rg2-rn3 from sn3(localhost:13271)
     *   Task BroadcastTopo
     *
     * Ideally the incomplete and unstarted tasks would be described in ways
     * more understandable to the user, and would stray from task-by-task
     * reporting.. For example, we'd list the number of outstanding partition
     * migrations, the number of relocated RNS, and skip smaller tasks like
     * BroadcastTopo
     *
     * Note that we carefully call them "incomplete" rather than inprogress,
     * and unstarted, rather than pending. That's because a plan that is incurs
     * an error within the plan execution framework, like this example, leaves
     * the tasks just dangling. The incomplete task will never finish, so it's
     * not inprogess, and the unstarted tasks will never be issued.
     */
    public String display() {
        StringBuilder sb = new StringBuilder();
        Formatter fm = new Formatter(sb);
        if (planRun == null) {
            fm.format("Plan %s has not been started\n", plan.getName());
            return sb.toString();
        }

        /* Plan info */
        fm.format("Plan %s\n", plan.getName());
        fm.format(STRING_LABEL, "State:", plan.getState());
        fm.format(NUM_LABEL, "Attempt number:", planRun.getAttempt());
        fm.format(STRING_LABEL, "Started:",
                  FormatUtils.formatDateAndTime(planRun.getStartTime()));
        if (planRun.getInterruptTime() != 0) {
            fm.format(STRING_LABEL, "Interrupt requested:",
                      FormatUtils.formatDateAndTime
                      (planRun.getInterruptTime()));
        }
        if (planRun.getEndTime() != 0) {
            fm.format(STRING_LABEL, "Ended:",
                      FormatUtils.formatDateAndTime(planRun.getEndTime()));
            String failure = planRun.getFailureDescription(verbose);
            if (failure != null) {
                fm.format(STRING_LABEL, "Plan failures:", failure);
            }
        }

        /* Task info */
        int successCount = 0;
        int errorCount = 0;
        int interruptCount = 0;
        for (TaskRun tRun : finished) {
            if (tRun.getState() == Task.State.SUCCEEDED) {
                successCount++;
            } else if (tRun.getState() == Task.State.ERROR) {
                errorCount++;
            } else if (tRun.getState() == Task.State.INTERRUPTED) {
                interruptCount++;
            }
        }

        fm.format(StatusReport.NUM_LABEL, "Total tasks:",
                  plan.getTotalTaskCount());
        if (successCount > 0) {
            fm.format(StatusReport.NUM_LABEL, " Successful:", successCount);
        }

        if (errorCount > 0) {
            fm.format(StatusReport.NUM_LABEL, " Failed:", errorCount);
        }

        if (interruptCount > 0) {
            fm.format(StatusReport.NUM_LABEL, " Interrupted:", interruptCount);
        }

        if (running.size() > 0) {
            fm.format(StatusReport.NUM_LABEL, " Incomplete:", running.size());
        }

        if (pending.size() > 0) {
            fm.format(StatusReport.NUM_LABEL, " Not started:", pending.size());
        }

        if (showFinished) {
            plan.describeFinished(fm, finished, errorCount, verbose);
        }

        if (running.size() > 0) {
            fm.format("Incomplete tasks\n");
            plan.describeRunning(fm, running, verbose);
        }

        if (pending.size() > 0) {
            fm.format("Tasks not started\n");
            plan.describeNotStarted(fm, pending, verbose);
        }
        return sb.toString();
    }
}
