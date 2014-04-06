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

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.logging.Level;

import oracle.kv.impl.admin.PlannerAdmin;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.plan.DeployTopoPlan;
import oracle.kv.impl.admin.plan.PlanExecutor.ParallelTaskRunner;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.rep.admin.RepNodeAdmin.PartitionMigrationState;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.rep.migration.PartitionMigrationStatus;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.Ping;

import com.sleepycat.persist.model.Persistent;

/**
 * Move a partition from one RepGroup to another, for topology redistribution.
 */
@Persistent
public class MigratePartition extends AbstractTask {

    private static final long serialVersionUID = 1L;

    private DeployTopoPlan plan;
    private RepGroupId sourceRGId;
    private RepGroupId targetRGId;
    private PartitionId partitionId;

    /* for logging messages. */
    private transient RepNodeId targetRNId;

    /**
     * We expect that the target RepNode exists before MigratePartition is
     * executed.
     *
     * @param plan
     * @param sourceRGId ID of the current rep group of the partition
     * @param targetRGId ID of the new rep group of the partition
     * @param partitionId ID the partition to migrate
     * will stop.
     */
    public MigratePartition(DeployTopoPlan plan,
                            RepGroupId sourceRGId,
                            RepGroupId targetRGId,
                            PartitionId partitionId) {
        super();
        this.plan = plan;
        this.sourceRGId = sourceRGId;
        this.targetRGId = targetRGId;
        this.partitionId = partitionId;
    }

    /* DPL */
    protected MigratePartition() {
    }

    /**
     * Find the master of the target RepGroup.
     * @return null if no master can be found, otherwise the admin interface
     * for the master RN.
     * @throws NotBoundException
     * @throws RemoteException
     */
    private RepNodeAdminAPI getTarget()
        throws RemoteException, NotBoundException {

        PlannerAdmin admin = plan.getAdmin();
        Topology topology = admin.getCurrentTopology();
        RepNode targetMasterRN = Ping.getMaster(topology, targetRGId);
        if (targetMasterRN == null) {
            targetRNId = null;
            return null;
        }

        targetRNId = targetMasterRN.getResourceId();
        LoginManager loginMgr = admin.getLoginManager();
        RegistryUtils registryUtils = new RegistryUtils(topology, loginMgr);
        return registryUtils.getRepNodeAdmin(targetMasterRN.getResourceId());
    }

    /**
     * Find the master of the source RepGroup. Differs slightly from
     * getTarget() in that there is no need to save the targetRNId field.
     */
    private RepNodeAdminAPI getSource()
        throws RemoteException, NotBoundException {

        PlannerAdmin admin = plan.getAdmin();
        Topology topology = admin.getCurrentTopology();
        RepNode masterRN = Ping.getMaster(topology, sourceRGId);
        if (masterRN == null) {
            return null;
        }

        LoginManager loginMgr = admin.getLoginManager();
        RegistryUtils registryUtils = new RegistryUtils(topology, loginMgr);
        return registryUtils.getRepNodeAdmin(masterRN.getResourceId());
    }

    /**
     * Partition migration goes through these steps:
     *
     * Step 1: Invoke RepNodeAdminAPI.migration. Retry if target is not
     *         available
     * Step 2: Query target for migration status. Retry if status is not
     *         terminal.
     * Step 2a:If migration status indicates error, cancel the source.
     * Step 3: Update topology in admin db. Retry if there are any failures.
     * Step 4: Broadcast topology to all RNs. Retry if there are any failures.
     *
     * The task is idempotent; if the migration already occurred, step 1 will
     * just return a success status.
     *
     * startWork() begins with Step 1
     */
    @Override
    public Callable<Task.State> getFirstJob(int taskId,
                                            ParallelTaskRunner runner) {
        return makeRequestMigrationJob(taskId, runner);
    }

    /**
     * Do Step 1 - start the migration.
     */
    private NextJob requestMigration(int taskId,
                                     ParallelTaskRunner runner) {
        AdminParams ap = plan.getAdmin().getParams().getAdminParams();
        RepNodeAdminAPI target = null;
        try {
            target = getTarget();
            if (target == null) {
                /* No master available, try step 1 again later. */
                return new NextJob(Task.State.RUNNING,
                                   makeRequestMigrationJob(taskId, runner),
                                   ap.getRNFailoverPeriod());
            }

            /* Start a migration */
            plan.getLogger().log(Level.INFO,
                                 "{0}: target={1} migration submitted",
                                 new Object[] {partitionId, targetRNId});
            PartitionMigrationState mState =
                target.migratePartition(partitionId, sourceRGId);

            /* Plan on going to Step 2 or 3 */
            return checkMigrationState(target, mState, taskId, runner, ap);

        } catch (RemoteException e) {
            /* RMI problem, try step 1 again later. */
            return new NextJob(Task.State.RUNNING,
                               makeRequestMigrationJob(taskId, runner),
                               ap.getServiceUnreachablePeriod());

        } catch (NotBoundException e) {
            /* RMI problem, try step 1 again later. */
            return new NextJob(Task.State.RUNNING,
                               makeRequestMigrationJob(taskId, runner),
                               ap.getServiceUnreachablePeriod());
        }
    }

    /**
     * @return a wrapper that will invoke a migration job.
     */
    private JobWrapper makeRequestMigrationJob
        (final int taskId, final ParallelTaskRunner runner) {
        return new PartitionJob(taskId, runner, "request migration") {
            @Override
            public NextJob doJob() {
                return requestMigration(taskId, runner);
            }
        };
    }

    /**
     * Take action based on the migration state, which has been obtained via
     * the original migratePartition call, or subsequent queries to the target
     * RN for migration state. Depending on the migration state, the next
     * step is Step 2 or Step 3
     * @throws RemoteException
     */
    private NextJob checkMigrationState(RepNodeAdminAPI target,
                                        PartitionMigrationState mState,
                                        final int taskId,
                                        final ParallelTaskRunner runner,
                                        AdminParams ap) {

        NextJob nextJob = null;

        /*
         * We check the migration state for exception information irregardless
         * of the state value, although in general we really only expect
         * exceptions with ERROR and UNKNOWN. The motivation is just to make
         * this as general purpose as possible, in case there are unexpected
         * cases where exception info is passed in the future.
         *
         * TODO: for now the plan executor is only saving exception info when
         * the task ends in error. An UNKNOWN state actually may have an
         * exception, which we need to propagate. The problem is that this
         * leaves the task running. Perhaps the error information should be
         * saved in the taskRun details?
         */

        String errorInfo = null;
        if (mState.getCause() != null) {
            errorInfo = LoggerUtils.getStackTrace(mState.getCause());
        }

        /*
         * Obtain more details about the migration and save it in the TaskRun
         * This is purely for reporting purposes, so bail out if any exceptions
         * happen.
         */
        try {
            getMigrationDetails(target, mState, taskId,  runner);
        } catch (Exception e) {
            plan.getLogger().log
                (Level.INFO,
                 "{0}: target={1} source={2} migration state={3} " +
                 " exception seen when getting detailed status: {4}",
                 new Object[] {partitionId, targetRNId, sourceRGId,
                               mState, LoggerUtils.getStackTrace(e)});
        }

        switch(mState) {
        case ERROR:

            /* The migration has failed, tell the source to cancel. */
            String additionalInfo = "target=" + targetRNId + " state="
                + mState;
            if (errorInfo != null) {
                additionalInfo += " " + errorInfo;
            }

            nextJob = cancelMigration(taskId, runner, additionalInfo, ap);
            break;

        case PENDING:
        case RUNNING:

            /*
             * Schedule Step 2, request migration status
             * The migration hasn't finished, so wait a bit, and then
             * poll for the migration state again.
             */
            nextJob = new NextJob(Task.State.RUNNING,
                                  makeStatusQueryJob(taskId, runner, ap),
                                  ap.getCheckPartitionMigrationPeriod(),
                                  errorInfo);
            break;
        case UNKNOWN:

            /*
             * Schedule Step 2, request migration status
             * The information was returned by a replica, which means that
             * there was some kind failover. Wait a bit, and poll again.
             */
            nextJob = new NextJob(Task.State.RUNNING,
                                  makeStatusQueryJob(taskId, runner, ap),
                                  ap.getRNFailoverPeriod(),
                                  errorInfo);
            break;

        case SUCCEEDED:
            /* Update the topology. */
            nextJob = updateTopoInAdminDB(taskId, runner);
            break;
        }
        return nextJob;
    }

    /**
     * Do Step 2: query for migration status.
     */
    private NextJob queryForStatus(int taskId,
                                   ParallelTaskRunner runner,
                                   AdminParams ap) {

        try {
            RepNodeAdminAPI target = getTarget();
            if (target == null) {
                /* No master to talk to, repeat step2 later. */
                return new NextJob(Task.State.RUNNING,
                                   makeStatusQueryJob(taskId, runner, ap),
                                   ap.getRNFailoverPeriod());
            }

            PartitionMigrationState mstate =
                target.getMigrationState(partitionId);
            plan.getLogger().log
                (Level.FINE,
                 "{0}: target={1} migration state={2}",
                 new Object[] {partitionId, targetRNId, mstate});
            return checkMigrationState(target, mstate, taskId, runner, ap);
        } catch (RemoteException e) {
            /* RMI problem, try step 2 again later. */
            return new NextJob(Task.State.RUNNING,
                               makeStatusQueryJob(taskId, runner, ap),
                               ap.getServiceUnreachablePeriod());
        } catch (NotBoundException e) {
            /* RMI problem, try step 1 again later. */
            return new NextJob(Task.State.RUNNING,
                               makeStatusQueryJob(taskId, runner, ap),
                               ap.getServiceUnreachablePeriod());
        }
    }

    /**
     * Query the target and source, if possible, for more details about
     * migration status, for reporting reasons.
     * @throws NotBoundException
     * @throws RemoteException
     */
    private void getMigrationDetails(RepNodeAdminAPI target,
                                     PartitionMigrationState mState,
                                     final int taskId,
                                     final ParallelTaskRunner runner)
        throws RemoteException, NotBoundException {

        PartitionMigrationStatus status;
        switch(mState) {
        case RUNNING:
        case SUCCEEDED:
            status = target.getMigrationStatus(partitionId);
            if (status != null) {
                plan.addTaskDetails(runner.getDetails(taskId), status.toMap());
            }

            RepNodeAdminAPI source;

            source = getSource();
            if (source != null) {
                status = source.getMigrationStatus(partitionId);
                if (status != null) {
                    plan.addTaskDetails(runner.getDetails(taskId), 
                                        status.toMap());
                }
            }
            break;
        case ERROR:
        case PENDING:
            /* Only the target has status, not the source. */
            status = target.getMigrationStatus(partitionId);
            if (status != null) {
                plan.addTaskDetails(runner.getDetails(taskId), status.toMap());
            }

            break;
        case UNKNOWN:
            /*
             * The information was returned by a replica, which means that
             * there was some kind of failover. There are no details to be
             * had from either source or target.
             */
            break;
        }

    }

    /**
     * @return a wrapper that will invoke a status query.
     */
    private JobWrapper makeStatusQueryJob(final int taskId,
                                          final ParallelTaskRunner runner,
                                          final AdminParams ap) {
        return new PartitionJob(taskId, runner, "query migration status") {
            @Override
            public NextJob doJob() {
                return queryForStatus(taskId, runner, ap);
            }
        };
    }

    /**
     * Do Step 2a: cancel a failed migration.
     */
    private NextJob cancelMigration(int taskId,
                                    ParallelTaskRunner runner,
                                    String cancelReason,
                                    AdminParams ap) {

        try {

            /* Find the source master. */
            PlannerAdmin admin = plan.getAdmin();
            Topology topology = admin.getCurrentTopology();
            RepNode sourceMasterRN = Ping.getMaster(topology, sourceRGId);
            if (sourceMasterRN == null) {
                /* Can't contact the source, retry later. */
                return new NextJob(Task.State.RUNNING,
                                   makeCancelJob(taskId, runner,
                                                 cancelReason, ap),
                                   ap.getRNFailoverPeriod());
            }

            LoginManager loginMgr = admin.getLoginManager();
            RegistryUtils registryUtils = new RegistryUtils(topology, loginMgr);
            RepNodeId sourceRNId = sourceMasterRN.getResourceId();
            RepNodeAdminAPI source = registryUtils.getRepNodeAdmin(sourceRNId);

            if (source == null) {
                /* No master to talk to, repeat step2 later. */
                return new NextJob(Task.State.RUNNING,
                                   makeCancelJob(taskId, runner,
                                                 cancelReason, ap),
                                   ap.getRNFailoverPeriod());
            }

            boolean done = source.canceled(partitionId, targetRGId);

            plan.getLogger().log
                (Level.INFO,
                 "{0}: source={1} cancellation confirmation={2}",
                 new Object[] {partitionId, sourceRNId, done});

            if (done) {
                return new NextJob(Task.State.ERROR, cancelReason);
            }

            /*
             * Retry until the cancel works. The user can stop a long running
             * cancel by issuing a plan interrupt.
             */
            return new NextJob(Task.State.RUNNING,
                               makeCancelJob(taskId, runner, cancelReason, ap),
                               ap.getCheckPartitionMigrationPeriod());
        } catch (RemoteException e) {
            /* RMI problem, try step 2a again later. */
            return new NextJob(Task.State.RUNNING,
                               makeCancelJob(taskId, runner, cancelReason, ap),
                               ap.getServiceUnreachablePeriod());

        } catch (NotBoundException e) {
            /* RMI problem, try step 2a again later. */
            return new NextJob(Task.State.RUNNING,
                               makeCancelJob(taskId, runner, cancelReason, ap),
                               ap.getServiceUnreachablePeriod());
        }
    }

    /**
     * @return a wrapper that will invoke a status query.
     */
    private JobWrapper makeCancelJob(final int taskId,
                                     final ParallelTaskRunner runner,
                                     final String cancelReason,
                                     final AdminParams ap) {
        return new PartitionJob(taskId, runner, "cancel migration") {
            @Override
            public NextJob doJob() {
                return cancelMigration(taskId, runner, cancelReason, ap);
            }
        };
    }

    /**
     * Do Step 3: update the topology in the admin database. Since this is part
     * of the migration transfer of ownership protocol, this must succeed, so
     * if there is any failure, we will retry indefinitely (until the plan is
     * canceled.)
     */
    private NextJob updateTopoInAdminDB(final int taskId,
                                        final ParallelTaskRunner runner) {
        final PlannerAdmin admin = plan.getAdmin();
        try {

            /*
             * Partition change is done atomically within the PlannerAdmin.
             * If nothing changed, skip the broadcast.
             */
            if (!admin.updatePartition(partitionId, targetRGId,
                                       plan.getDeployedInfo(), plan)) {
                return NextJob.END_WITH_SUCCESS;
            }

            /* Success, go to step 4: broadcast to other RNs without delay. */
            return broadcastTopo(admin, taskId, runner);

        } catch (Exception e) {

            /*
             * Update failed, admin db is unavailable, retry step 3 after
             * delay.
             */
            AdminParams ap = admin.getParams().getAdminParams();
            return new NextJob(Task.State.RUNNING,
                               makePersistToDBJob(taskId, runner),
                               ap.getAdminFailoverPeriod());
        }
    }

    /**
     * @return a wrapper that will update the topology in the admin db.
     */
    private JobWrapper makePersistToDBJob(final int taskId,
                                          final ParallelTaskRunner runner) {
        return new PartitionJob(taskId, runner, "update topo in admin db") {
            @Override
            public NextJob doJob() {
                return updateTopoInAdminDB(taskId, runner);
            }
        };
    }

    /**
     * Do Step 4: broadcast to all RNs.
     * TODO: Utils.broadcastTopoChangesToRN now has a concept of retrying
     * within it. How does it mesh with this retry?
     */
    private NextJob broadcastTopo(final PlannerAdmin admin,
                                  final int taskId,
                                  final ParallelTaskRunner runner) {
        try {
            /* Send topology changes to all nodes. */
            if (!Utils.broadcastTopoChangesToRNs(plan.getLogger(),
                                            admin.getCurrentTopology(),
                                            this.toString(),
                                            admin.getParams().getAdminParams(),
                                            plan)) {
                return new NextJob(Task.State.INTERRUPTED,
                                   "task interrupted before new topology " +
                                   "was sent to enough nodes");
            }

            /* Success, finish task.*/
            return NextJob.END_WITH_SUCCESS;

        } catch (Exception e) {
            /* Broadcast failed, repeat step 4 */
            AdminParams ap = admin.getParams().getAdminParams();
            return new NextJob(Task.State.RUNNING,
                               makeBroadcastJob(taskId, runner, admin),
                               ap.getServiceUnreachablePeriod());
        }
    }

    /**
     * @return a wrapper that will broadcast a topology
     */
    private JobWrapper makeBroadcastJob(final int taskId,
                                        final ParallelTaskRunner runner,
                                        final PlannerAdmin admin) {
        return new PartitionJob(taskId, runner, "broadcast topology") {
            @Override
            public NextJob doJob() {
                return broadcastTopo(admin, taskId, runner);
            }
        };
    }

    @Override
    public boolean continuePastError() {
        return true;
    }

    @Override
    public String toString() {
        return getName() + " from " +  sourceRGId + " to " + targetRGId;
    }

    @Override
    public String getName() {
        return super.getName() + " " + partitionId;
    }

    @Override
    public Runnable getCleanupJob() {
        return new Runnable() {
        @Override
        public void run(){

            PartitionMigrationState targetState =
                                        PartitionMigrationState.UNKNOWN;

            while (!plan.cleanupInterrupted()) {
                try {

                    /*
                     * Try to cancel the target until we have some known state.
                     */
                    if (targetState == PartitionMigrationState.UNKNOWN) {
                        targetState = cancelTarget();
                    }

                    /*
                     * If the state is null, the source has no record of this
                     * partition migration (and doesn not have the partition).
                     * In this case we will assume the migration was never
                     * started and we do not need to cancel the source.
                     */
                    if (targetState == null) {
                        return;
                    }
                    switch (targetState) {

                        case SUCCEEDED:
                            /*
                             * Cancel can't be done because the partition
                             * migration has completed.
                             */
                            final PlannerAdmin admin = plan.getAdmin();

                            /* If nothing changed, skip the broadcast */
                            if (!admin.updatePartition(partitionId, targetRGId,
                                                       plan.getDeployedInfo(),
                                                       plan))
                            {
                                return;
                            }
                            try {
                                if (Utils.broadcastTopoChangesToRNs
                                            (plan.getLogger(),
                                             admin.getCurrentTopology(),
                                             this.toString(),
                                             admin.getParams().getAdminParams(),
                                             plan)) {
                                    /* all done, no cancel */
                                    return;
                                }

                            } catch (InterruptedException e) {}

                            /* Hmm, the broadcast didn't work, retry. */
                            break;

                        case ERROR:
                            /*
                             * Migration was canceled, so make sure the source
                             * is canceled as well.
                             */
                            if (cancelSource()) {
                                /* all done, canceled */
                                return;
                            }
                            /* Canceling the source didn't work, retry */
                            break;

                        default:
                            /* Canceling the target didn't work, retry */
                            targetState = PartitionMigrationState.UNKNOWN;
                    }
                } catch (Exception e) {
                    plan.getLogger().log
                        (Level.SEVERE,
                         "{0}: target={1} problem when cancelling migration:"+
                         "{2}",
                         new Object[] {partitionId, targetRNId,
                                       LoggerUtils.getStackTrace(e)});
                }

                /*
                 * TODO: would be better to schedule a job, rather
                 * than sleep.
                 */
                try {
                    Thread.sleep(CLEANUP_RETRY_MILLIS);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
        };
    }

    /**
     * Attempts to cancel the partition migration on the target.
     *
     * @return the state of the partition migration or null
     */
    private PartitionMigrationState cancelTarget()
            throws RemoteException, NotBoundException {

        final RepNodeAdminAPI target = getTarget();
        if (target == null) {
            plan.getLogger().log
                (Level.INFO,
                 "{0}: attempted to cancel migration, but can't contact " +
                 "target RN", partitionId);
            return PartitionMigrationState.UNKNOWN;
        }
        final PartitionMigrationState state = target.canCancel(partitionId);

        /*
         * The state return is a little confusing -- SUCCEEDED means that the
         * migration has happened, and ERROR means that it will be stopped.
         */
        String meaning = (state == PartitionMigrationState.SUCCEEDED) ?
            "migration finished, can't be canceled" :
            ((state == PartitionMigrationState.ERROR) ?
             "migration will be stopped" : "problem canceling migration");

        plan.getLogger().log
             (Level.INFO,
              "{0}: target={1} request to cancel migration: {2} {3}",
              new Object[] {partitionId, targetRNId, state, meaning});
        return state;
    }

    /**
     * Attempts to cancel the partition migration on the source.
     *
     * @return true if the cancel was successful
     */
    private boolean cancelSource() throws RemoteException, NotBoundException {
        final RepNodeAdminAPI source = getSource();
        if (source == null) {
            return false;
        }

        final boolean canceled = source.canceled(partitionId, targetRGId);
        plan.getLogger().log
            (Level.INFO,
             "{0}: source={1} target={2} cancel at source={3}",
             new Object[] {partitionId, sourceRGId, targetRNId, canceled});
        return canceled;
    }

    /**
     * Prepend the partition id onto all status messages for that phase, or job.
     */
    private abstract class PartitionJob extends JobWrapper {

        public PartitionJob(int taskId, ParallelTaskRunner runner,
                            String description) {
            super(taskId, runner, description);
        }

        @Override
        public String getDescription() {
            return partitionId + ": " + super.getDescription();
        }
    }

    @Override
    public void lockTopoComponents(Planner planner) {
        planner.lockShard(plan.getId(), plan.getName(), sourceRGId);
        planner.lockShard(plan.getId(), plan.getName(), targetRGId);
    }
    
    /*
     * Return detailed stats collected on the source and target about migration
     * execution.
     * 
     * @return null if there are no details.
     */
    @Override
    public String displayExecutionDetails(Map<String, String> details,
                                          String displayPrefix) {
        PartitionMigrationStatus targetStatus =
                PartitionMigrationStatus.parseTargetStatus(details);
        if (targetStatus == null) {
            return null;
        }
        
        return targetStatus.display(displayPrefix);
    }
}
