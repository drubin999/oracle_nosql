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

package oracle.kv.impl.rep.migration;

import com.sleepycat.je.DatabaseException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import oracle.kv.impl.api.rgstate.RepNodeState;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.admin.RepNodeAdmin.PartitionMigrationState;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.rep.migration.PartitionMigrations.MigrationRecord;
import oracle.kv.impl.rep.migration.PartitionMigrations.TargetRecord;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * An executor for running tasks to monitor migration targets. There is
 * only one monitor task running at a time.
 */
public class TargetMonitorExecutor extends ScheduledThreadPoolExecutor {

    private final static long POLL_PERIOD = 2L;   /* 2 seconds */

    private final MigrationManager manager;
    private final RepNode repNode;
    private final Logger logger;
    private final RepGroupId sourceRGId;

    TargetMonitorExecutor(MigrationManager manager,
                          RepNode repNode,
                          Logger logger) {
        super(1, new KVThreadFactory(" target monitor", logger));
        this.manager = manager;
        this.repNode = repNode;
        this.logger = logger;
        sourceRGId = new RepGroupId(repNode.getRepNodeId().getGroupId());
        setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }

    /**
     * Schedules a target monitor task.
     */
    void monitorTarget() {
        try {
            schedule(new TargetMonitor(), POLL_PERIOD, TimeUnit.SECONDS);
        } catch (RejectedExecutionException ree) {
            logger.log(Level.WARNING, "Failed to schedule monitor", ree);
        }
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);

        if (t != null) {
            logger.log(Level.INFO, "Target monitor execution failed", t);
            return;
        }

        if (!manager.isMaster()) {
            return;
        }
        @SuppressWarnings("unchecked")
        final Future<TargetMonitor> f = (Future<TargetMonitor>)r;

        TargetMonitor monitor = null;
        try {
            monitor = f.get();
        } catch (Exception ex) {
            logger.log(Level.SEVERE, "Exception getting monitor", ex);
        }

        /*
         * If the monitor was returned, then it should be re-run if the queue
         * is empty. If there are other monitors in the queue, drop this one
         * and let the other(s) take its place.
         */
        if ((monitor != null) && getQueue().isEmpty()) {

            try {
                schedule(monitor, POLL_PERIOD, TimeUnit.SECONDS);
            } catch (RejectedExecutionException ree) {
                logger.log(Level.WARNING, "Failed to restart monitor", ree);
            }
        }
    }

    /**
     * Callable which checks on completed source migrations. The check calls
     * the target of each completed source migration to see if the migration
     * has completed successfully or if there was an error. In the case of an
     * error, the source record is removed and the local topology updated.
     * This will restore the partition to this node.
     *
     * Monitoring will continue until 1) there are no completed source records,
     * 2) there are no targets which have not completed, or 3) the node
     * is no longer a master (or is shutdown).
     */
    private class TargetMonitor implements Callable<TargetMonitor> {

        @Override
        public TargetMonitor call() {
            boolean keepChecking = true;

            if (!manager.isMaster()) {
                return null;
            }

            final PartitionMigrations migrations = manager.getMigrations();

            if (migrations == null) {
                return this;
            }
            keepChecking = false;

            final Iterator<MigrationRecord> itr = migrations.completed();

            /*
             * Since check() does a remote call, recheck for state
             * change and shutdown.
             */
            while (itr.hasNext() && manager.isMaster()) {
                if (check(itr.next())) {
                    keepChecking = true;
                }
            }
            return keepChecking ? this : null;
        }

        /**
         * Checks whether the specified record represents a completed migration
         * source, and if so contacts the target to see if the operation
         * completed there. If the record needs to be checked again true is
         * returned.
         *
         * @param record
         * @return true if the record needs to be checked again
         */
        private boolean check(MigrationRecord record) {

            if (record instanceof TargetRecord) {
                return false;
            }

            assert record.getSourceRGId().equals(sourceRGId);

            logger.log(Level.FINE, "Check target for {0}", record);

            Exception ex;
            try {
                final RegistryUtils registryUtils =
                    new RegistryUtils(repNode.getTopology(),
                                      repNode.getLoginManager());
                final RepGroupId targetRGId = record.getTargetRGId();
                final RepNodeState rns = repNode.getMaster(targetRGId);

                if (rns == null) {
                    logger.log(Level.FINE,
                               "Master not found for {0}, sending NOP to " +
                               "update group table", targetRGId);
                    repNode.sendNOP(targetRGId);
                    return true;
                }

                final RepNodeAdminAPI rna = registryUtils.
                                            getRepNodeAdmin(rns.getRepNodeId());

                final PartitionMigrationState state =
                                rna.getMigrationState(record.getPartitionId());
                switch (state) {
                    case ERROR:
                        /* Remove record, update local topo */
                        failed(record, state);
                        return false;

                    case SUCCEEDED:
                        /*
                         * If the target has completed successfully then we
                         * don't need to check again.
                         */
                        return false;

                    case UNKNOWN:
                        /*
                         * Likely do not have the master. Sending a NOP will
                         * cause the group table to be updated so that next time
                         * around we will have the correct master.
                         */
                        logger.log(Level.FINE,
                                   "Received UNKNOWN status from {0}, " +
                                   "sending NOP to update group table",
                                   targetRGId);
                        repNode.sendNOP(targetRGId);
                        return true;

                    default:
                        /*
                         * Got here means the migration is still running on the
                         * target. However, if the target node has changed,
                         * then the migration this record represents has failed
                         * AND the target mastership has changed.
                         */
                        if (record.getTargetRNId().equals(rns.getRepNodeId())) {
                            /* Running on same node, keep checking */
                            return true;
                        }
                        failed(record, state);
                        return false;
                }
            } catch (NotBoundException nbe) {
                ex = nbe;
            } catch (RemoteException re) {
                ex = re;
            } catch (DatabaseException de) {
                ex = de;
            }
            logger.log(Level.INFO,
                       "Exception while monitoring target for {0}: {1}",
                       new Object[]{record, ex});
            return true;
        }

        private void failed(MigrationRecord record,
                            PartitionMigrationState state) {
            logger.log(Level.INFO,
                       "Migration source detected failure of {0}, " +
                       "target returned {1} ({2}), removing completed record",
                       new Object[] {record, state, state.getCause()});
            manager.removeRecord(record, true);
        }
    }
}

