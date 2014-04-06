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
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.PlannerAdmin;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.task.MigratePartition;
import oracle.kv.impl.admin.plan.task.Task;
import oracle.kv.impl.admin.topo.Rules;
import oracle.kv.impl.admin.topo.Rules.Results;
import oracle.kv.impl.admin.topo.TopologyCandidate;
import oracle.kv.impl.admin.topo.TopologyDiff;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.rep.migration.PartitionMigrationStatus;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.FormatUtils;
import oracle.kv.impl.util.TopologyPrinter;

import com.sleepycat.persist.model.Persistent;

/**
 * Deploys a target topology.
 */
@Persistent
public class DeployTopoPlan extends AbstractPlan {

    private static final long serialVersionUID = 1L;

    /*
     * Collection of shardIds generated during plan execution for brand new
     * shards.
     *
     * When tasks are generated to implement a candidate topology, the tasks
     * must refer to not-yet-created shards by a shard index. During task
     * execution, shards are created, and a RepGroupId is instantiated. Later
     * tasks that implement the RNs lookup the newly created shard ids by
     * index value.
     */
    private final List<RepGroupId> newlyGeneratedShardIds =
        new ArrayList<RepGroupId>();
    private String candidateName;

    /*
     * The topology sequence number identifying the current topology in effect
     * when the plan's tasks are generated.
     *
     * Plan tasks are generated when the user creates the plan. It's possible
     * that the plan may actually be executed later on. The generated tasks are
     * only valid if the current, deployed topology at execution time is
     * identical to the current topology at task generation time. This field is
     * used to check this condition.
     *
     * Once the plan as started, the seq # is updated during the plan execution
     * whenever the topology is updated. This allows the plan to be re-executed
     * even after parts of the plan have completed successfully.
     */
    private int sourceTopoSequence;

    private transient DeploymentInfo deploymentInfo;

    /*
     * No-arg ctor for use by DPL.
     */
    @SuppressWarnings("unused")
    private DeployTopoPlan() {
    }

    /**
     */
    public DeployTopoPlan(AtomicInteger idGen,
                          String planName,
                          Planner planner,
                          Topology current,
                          TopologyCandidate candidate) {

        super(idGen, planName, planner);

        sourceTopoSequence = current.getSequenceNumber();
        TopoTaskGenerator generator =
            new TopoTaskGenerator(this, current, candidate,
                                  planner.getAdmin().getParams());
        generator.generate();

        candidateName = candidate.getName();
    }

    public boolean isNewShardCreated(int planShardIdx) {
        return newlyGeneratedShardIds.get(planShardIdx) != null;
    }

    public void setNewShardId(int planShardIdx, RepGroupId shardId) {
        newlyGeneratedShardIds.add(planShardIdx, shardId);
    }

    public RepGroupId getShardId(int planShardIdx) {
        if (newlyGeneratedShardIds.size() > planShardIdx) {
            return newlyGeneratedShardIds.get(planShardIdx);
        }

        return null;
    }

    @Override
    public void preExecutionSave() {
    }

    @Override
    public String getDefaultName() {
        return "Deploy Topo";
    }

    @Override
    public boolean isExclusive() {
      return false;
    }

    @Override
    public DeploymentInfo getDeployedInfo() {
        return deploymentInfo;
    }

    @Override
    synchronized PlanRun startNewRun() {
        deploymentInfo = DeploymentInfo.makeDeploymentInfo(this, 
                                                           candidateName);
        return super.startNewRun();
    }

    @Override
    public void getCatalogLocks() {
        planner.lockElasticity(getId(), getName());
        getPerTaskLocks();
    }

    /**
     * Log information about the current and future topologies, and a preview
     * ofo the work to be done. Check if the target topology introduces new
     * violations. If it does, refuse to execute it unless the force flag is
     * set.
     * @param force if false, the plan will check if the proposed topology
     * introduces new topology violations. If it does, the plan will not
     * execute. If true, the plan will skip any violation checks.
     */
    @Override
    public void preExecuteCheck(boolean force, Logger executeLogger) {
        PlannerAdmin admin = planner.getAdmin();
        Topology current = admin.getCurrentTopology();
        Parameters params = admin.getCurrentParameters();
        TopologyCandidate candidate = admin.getCandidate(candidateName);
        Topology future = candidate.getTopology();

        /*
         * For ease of debugging, log information about the changes
         * this plan would execute. Always log this information, before doing
         * any validation, so that we have a consistent set of information in
         * the log
         */
        executeLogger.log(Level.INFO, "{0} deploying topology candidate {1}.",
                          new Object[]{this.toString(), candidateName});

        executeLogger.log(Level.INFO,
                          "Current topology: {0}",
                          TopologyPrinter.printTopology(current));

        /*
         * Don't do validation when diffing the topology. Validation will be
         * done later, within the context of the force flag.
         */
        TopologyDiff diff = new TopologyDiff(current, null /*sourceName*/,
                                             candidate, params,
                                             false /*validate*/);
        executeLogger.log(Level.INFO,
                          "Preview of changes to be executed by {0}:\n{1}",
                           new Object[]{this.toString(),
                                        diff.display(true /*verbose*/)});

        executeLogger.log(Level.INFO,
                          "Target topology candidate: {0}\n{1}",
                          new Object[]{candidate.getName(),
                                       TopologyPrinter.printTopology(future)});

        if (current.getSequenceNumber() != sourceTopoSequence) {
            throw new IllegalCommandException
                ("Plan " + this + " was based on the system topology at " +
                 "sequence " + sourceTopoSequence +
                 " but the current topology is at sequence " +
                 current.getSequenceNumber() +
                 ". Please cancel this plan and create a new plan with the " +
                 "command \"plan deploy-topology -name " + candidateName);
        }

        if (force) {
            /* Don't bother checking */
            executeLogger.log(Level.INFO,
                              "-force specified for {0} so no topology " +
                              "validation will be done.", this.toString());
            return;
        }

        Results statusQuo = Rules.validate(current, params, true);
        Results futureState = Rules.validate(future, params, false);

        Results newIssues = futureState.remove(statusQuo);
        int newViolations = newIssues.getViolations().size();
        if (newViolations > 0) {
            final String errorDesc = (newViolations == 1) ? "1 new violation" :
                newViolations + " new violations: " + newIssues;

            throw new IllegalCommandException
                ("Deploying topology candidate \"" + candidateName +
                 "\" will introduce " + errorDesc + "\nTo deploy anyway, " +
                 "use plan deploy-topology <candidateName> [force]. " +
                 "Use topology validate [<candidate name>] " +
                 "to view violations in the candidate \"" + candidateName +
                 "\" and the current, deployed topology.");
        }
    }

    /**
     * Describe all running tasks, for a status report.
     */
    @Override
    public void describeRunning(Formatter fm,
                                final List<TaskRun> running,
                                boolean verbose) {

        /*
         * Treat the partition migration tasks specially by summarizing and
         * showing details for those tasks. Display all the non-migrations
         * first. In R2, migrations only run concurrently with other
         * migrations, so the migration list will end up containing all the
         * tasks.
         */
        List<TaskRun> migrations = new ArrayList<TaskRun>();

        for (TaskRun tRun : running) {
            if (tRun.getTask() instanceof MigratePartition) {
                migrations.add(tRun);
            } else {
                fm.format("   Task %d/%s started at %s\n",
                          tRun.getTaskNum(), tRun.getTask(),
                          FormatUtils.formatDateAndTime(tRun.getStartTime()));
            }
        }

        /* Now show migration information. */
        int numQueued = 0;
        int numRunning = 0;
        int numSucceeded = 0;
        int numFailed = 0;
        int numUnknown = 0;
        long succeededTime = 0;

        /*
         * Process all the migrations, and show a summary in both verbose and
         * non-verbose mode. Save the details in a separate StringBuilder for
         * appending to the end of the description.
         */
        StringBuilder detailBd = new StringBuilder();
        Formatter taskDetailFM = new Formatter(detailBd);

        for (TaskRun m : migrations) {

            /* Get the target's details, to see what the migration state was.*/
            Map<String, String> details = m.getDetails();
            PartitionMigrationStatus targetStatus =
                PartitionMigrationStatus.parseTargetStatus(details);
            if (targetStatus == null) {
                continue;
            }

            if (verbose) {
                /* Only get the source's details if we are in verbose mode.*/
                PartitionMigrationStatus sourceStatus =
                    PartitionMigrationStatus.parseSourceStatus(details);
                if ((sourceStatus != null) &&
                    (sourceStatus.getStartTime() > 0)) {
                    taskDetailFM.format("   Task %d/%s:\n     %s\n     %s\n",
                                        m.getTaskNum(), m.getTask(),
                                        targetStatus,  sourceStatus);
                } else {
                    taskDetailFM.format("   Task %d/%s:\n     %s\n",
                              m.getTaskNum(), m.getTask(), targetStatus);
                }
            }

            switch (targetStatus.getState()) {
            case PENDING:
                numQueued++;
                break;
            case RUNNING:
                numRunning++;
                break;
            case SUCCEEDED:
                numSucceeded++;
                succeededTime += (targetStatus.getEndTime() -
                                  targetStatus.getStartTime());
                break;
            case ERROR:
                numFailed++;
                break;
            case UNKNOWN:
                numUnknown++;
                break;
            }
        }

        /* Show summary */
        if (numQueued != 0) {
            fm.format("   %d partition migrations queued\n", numQueued);
        }

        if (numRunning != 0) {
            fm.format("   %d partition migrations running\n", numRunning);
        }

        if (numSucceeded != 0) {
            fm.format("   %d partition migrations succeeded, " +
                      "avg migration time = %d ms.\n",
                      numSucceeded, (succeededTime / numSucceeded));
        }

        if (numUnknown != 0) {
            fm.format("   %d partition migrations could not be " +
                      "contacted for status\n", numUnknown);
        }

        if (numFailed != 0) {
            fm.format("   %d partition migrations failed\n", numFailed);
        } else if (numSucceeded > 0) {
            final long elapsedMS = System.currentTimeMillis() - createTime;
            final long elapsedMSPerTask = elapsedMS / numSucceeded;
            final long estMSRemaining =
                (numQueued + numRunning + numUnknown) * elapsedMSPerTask;
            fm.format(StatusReport.STRING_LABEL, "Estimated completion:",
                      FormatUtils.formatDateAndTime
                      (estMSRemaining + System.currentTimeMillis()));
        }

        if (verbose && (detailBd.length() > 0)) {
            fm.format("%s", detailBd.toString());
        }

        taskDetailFM.close();
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

        int migrationCount = 0;
        for (Task t : notStarted) {
            boolean showDetail = true;

            if (!verbose && (t instanceof MigratePartition)) {
                migrationCount++;
                showDetail = false;
            }
            if (showDetail) {
                fm.format("   Task %s\n", t);
            }
        }
        if ((!verbose) && (migrationCount > 0)) {
            fm.format("   %d partition migrations waiting", migrationCount);
        }
    }

    public String getCandidateName() {
        return candidateName;
    }

    /**
     * Update the sourceTopoSequence after every task, in case it has been
     * incremented by topo updates.
     */
    @Override
    synchronized void incrementEndCount(PlanRun planRun,
                                        Task.State state) {

        updatingTopo(planner.getAdmin().getCurrentTopology());
        super.incrementEndCount(planRun, state);
    }

    @Override
    public boolean updatingMetadata(Metadata<?> metadata) {
        return metadata.getType().equals(MetadataType.TOPOLOGY) ?
                        updatingTopo((Topology)metadata) : false;
    }

    private boolean updatingTopo(Topology topology) {

        /**
         * If the topology is updated, then update the source seq # and
         * return true, which will persist the plan (and the new source seq #).
         */
        final int seqNum = topology.getSequenceNumber();

        if (seqNum <= sourceTopoSequence) {
            return false;
        }
        sourceTopoSequence = seqNum;
        return true;
    }

    /**
     * Add custom task status information to the TaskRun which records
     * information about each task execution. Must be synchronized on the
     * plan instance, to coordinate between different threads who are modifying
     * task state and persisting the plan instance.
     */
    public synchronized void addTaskDetails(Map<String, String> taskRunStatus,
                                            Map<String, String> info) {
        taskRunStatus.putAll(info);
    }

    @Override
    void stripForDisplay() {
        /* 
         * Nothing much to do, seem that the deploymentInfo is worth leaving,
         * might want to display it.
         */
    }
}
