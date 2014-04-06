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

package oracle.kv.impl.rep;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.Key;
import oracle.kv.impl.map.HashKeyToPartitionMap;
import oracle.kv.impl.map.KeyToPartitionMap;
import oracle.kv.impl.rep.RepNodeService.Params;
import oracle.kv.impl.topo.Partition;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.PartitionMap;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.SecondaryAssociation;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.InsufficientAcksException;
import com.sleepycat.je.rep.InsufficientReplicasException;
import com.sleepycat.je.rep.NoConsistencyRequiredPolicy;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.UnknownMasterException;

/**
 * Manages the partition database handles for the rep node.
 */
public class PartitionManager {

    /**
     * The amount of time to wait between retries when opening a db
     * handle at a replica.
     */
    public static final int DB_OPEN_RETRY_MS = 1000;

    private final RepNode repNode;

    /**
     * The database configuration used to create and access partition
     * databases.
     */
    private final DatabaseConfig partitionDbConfig;

    private final Logger logger;

    /**
     * A map from partitionId to the canonical partition database handle. Once
     * initialized, the map is only modified as a result of partition
     * migration.
     */
    private final Map<PartitionId, Database> partitionDbMap =
                        new ConcurrentHashMap<PartitionId, Database>();

    /*
     * Maps a key to a partition id. Set the first time the DB handles are
     * updated.
     */
    private KeyToPartitionMap mapper = null;

    private UpdateThread updateThread = null;

    PartitionManager(RepNode repNode,
                     SecondaryAssociation secondaryAssociation,
                     Params params) {
        this.repNode = repNode;
        partitionDbConfig =
            new DatabaseConfig().setTransactional(true).
                                 setAllowCreate(true).
                                 setBtreeComparator(Key.BytesComparator.class).
                                 setKeyPrefixing(true).
                                 setSecondaryAssociation(secondaryAssociation).
                                 setCacheMode(
                                    params.getRepNodeParams().getJECacheMode());
        logger = LoggerUtils.getLogger(this.getClass(), params);
        logger.log(Level.INFO,
                   "Partition database cache mode: {0}",
                   partitionDbConfig.getCacheMode());
    }

    /**
     * Returns the partition Db config
     *
     * @return the partition Db config
     */
    DatabaseConfig getPartitionDbConfig() {
        return partitionDbConfig;
    }

    Set<PartitionId> getPartitions() {
        return partitionDbMap.keySet();
    }

    /**
     * Asynchronously opens the partition database handles associated with the
     * partitions stored at this rep node. If an update thread is running it is
     * not restarted, avoiding any wait for the thread to stop.
     */
    synchronized void updateDbHandles(Topology topology) {

        /* If an update is already in progress just let it continue. */
        if (updateThread != null) {
            return;
        }
        final ReplicatedEnvironment repEnv = repNode.getEnv(1);

        if (repEnv != null) {
            updateDbHandles(topology, repEnv, true);
        }
    }

    /**
     * Asynchronously opens the partition database handles associated with the
     * partitions stored at this rep node. The databases are created if they do
     * not already exist. All database handles are cached in the partitionDbMap
     * for use by subsequent partition level operations.
     * <p>
     * This method is invoked at startup. At this time, new databases may be
     * created if this node is the master and databases need to be created for
     * the partitions assigned to this node. If the node is a replica, it may
     * need to wait until the databases created on the master have been
     * replicated to it.
     * <p>
     * Post startup, this method is invoked to re-establish database handles
     * whenever the associated environment handle is invalidated and needs to
     * be re-established. Or via the TopologyManager's listener interface
     * whenever the Topology has been updated.
     *
     * @param topology the topology describing the current
     * @param repEnv the replicated environment handle
     * @param reuseExistingHandles true if any current db handles are valid
     * and should be reused.
     */
    synchronized void updateDbHandles(Topology topology,
                                      ReplicatedEnvironment repEnv,
                                      boolean reuseExistingHandles) {
        assert topology != null;
        assert repEnv != null;
        stopUpdate();
        updateThread = new UpdateThread(topology, repEnv, reuseExistingHandles);
        updateThread.start();

        /* Set the mapper if the number if partitions have been determined. */
        if (mapper ==  null) {
            final int nPartitions = topology.getPartitionMap().getNPartitions();

            if (nPartitions > 0) {
                mapper = new HashKeyToPartitionMap(nPartitions);
            }
        }
    }

    /**
     * Stops the updater and waits for its thread to exit.
     */
    private void stopUpdate() {
        assert Thread.holdsLock(this);

        if (updateThread != null) {
            updateThread.waitForStop();
            updateThread = null;
        }
    }

    /**
     * Closes all partition DB handles, typically as a precursor to closing the
     * environment handle itself. The caller is assumed to have made provisions
     * if any to ensure that the handles are no longer in use.
     */
    synchronized void closeDbHandles() {
        logger.log(Level.INFO, "Closing partition database handles");

        stopUpdate();

        /*
         * Note that closing databases will terminate any operations that
         * are in progress for that partition.
         */
        for (Database pDb : partitionDbMap.values()) {
            if (!closePartitionDB(pDb)) {
                /* Break out on an env failure */
                return;
            }
        }
    }

    /**
     * Closes a partition DB, handling exceptions. Returns true if there were
     * no environment failures.
     *
     * @param pDb a partition database
     * @return true if the environment associated with the DB was invalid or
     * closed
     */
    private boolean closePartitionDB(Database pDb) {
        final Environment env = pDb.getEnvironment();

        if ((env == null) || !env.isValid()) {
            return false;
        }

        TxnUtil.close(logger, pDb, "partition");
        return true;
    }

    /**
     * Returns the partition associated with the key
     *
     * @param keyBytes the key used for looking up the database
     *
     * @return the partitionId associated with the key
     */
    PartitionId getPartitionId(byte[] keyBytes) {
        return mapper.getPartitionId(keyBytes);
    }

    /**
     * Returns the database associated with the key. Returns null if the
     * key is not associated with a partition on this node.
     *
     * @param keyBytes the key used for looking up the database
     *
     * @return the database associated with the key or null
     */
    Database getPartitionDB(byte[] keyBytes) {
        return partitionDbMap.get(mapper.getPartitionId(keyBytes));
    }
    
    /**
     * Returns the database associated with the partition. Returns null if the
     * partition is not on this node.
     *
     * @param partitionId the partition used for looking up the database.
     *
     * @return the database associated with the partition or null
     */
    Database getPartitionDB(PartitionId partitionId) {
        return partitionDbMap.get(partitionId);
    }

    /**
     * Returns true if the database for the specified partition is open.
     *
     * @param partitionId a partition ID
     * @return true if the database for the specified partition is open
     */
    boolean isPresent(PartitionId partitionId) {
        return partitionDbMap.containsKey(partitionId);
    }

    /**
     * Handles an exception opening a replicated DB. Returns
     * true if the open should be retried otherwise the exception is
     * re-thrown.
     * 
     * @param re the exception from the open
     * @param logger a logger
     * @param dbName name of DB that was opened
     * @return true if the open should be retried
     */
    public static boolean handleException(RuntimeException re,
                                          Logger logger,
                                          String dbName) {
        try {
            throw re;
        } catch (ReplicaWriteException rwe) {

            /*
             * Master has not had a chance to create the database as
             * yet, or the current environment (in the replica, or
             * unknown) state is lagging or the node has become a 
             * replica. Wait, giving the environment
             * time to catch up and become current.
             */
            logger.log(Level.FINE,
                       "Failed to open database for {0}. {1}",
                       new Object[] {dbName, rwe.getMessage()});
            return true;
        } catch (UnknownMasterException ume) {

            /*
             * Master has not had a chance to create the database as
             * yet, or the current environment (in the replica, or
             * unknown) state is lagging or has become a replica. 
             * Wait, giving the environment time to catch up and 
             * become current.
             */
            logger.log(Level.FINE,
                       "Failed to open database for {0}. {1}",
                       new Object[] {dbName, ume.getMessage()});
            return true;
        } catch (InsufficientReplicasException ire) {
            logger.log(Level.FINE,
                       "Insufficient replicas when creating " +
                       "database {0}. {1}",
                       new Object[] {dbName, ire.getMessage()});
            return true;
        } catch (InsufficientAcksException iae) {
            logger.log(Level.FINE,
                       "Insufficient acks when creating database {0}. {1}",
                       new Object[] {dbName, iae.getMessage()});
            /*
             * Database has already been created locally, ignore
             * the exception.
             */
            return false;
        } catch (IllegalStateException ise) {
            logger.log(Level.FINE,
                       "Problem accessing database {0}. {1}",
                       new Object[] {dbName, ise.getMessage()});
            return true;
        }
    }
        
    private class UpdateThread extends Thread {

        private final Topology topology;
        private final ReplicatedEnvironment repEnv;
        private final boolean reuseExistingHandles;

        private volatile boolean stop = false;

        UpdateThread(Topology topology,
                     ReplicatedEnvironment repEnv,
                     boolean reuseExistingHandles) {
            super("KV partition handle updater");
            this.topology = topology;
            this.repEnv = repEnv;
            this.reuseExistingHandles = reuseExistingHandles;
            setDaemon(true);
            setUncaughtExceptionHandler(repNode.getExceptionHandler());
        }

        @Override
        public void run() {

            /* Retry as long as there are errors */
            while (update()) {
                try {
                    Thread.sleep(DB_OPEN_RETRY_MS);
                } catch (InterruptedException ie) {
                    /* Should not happen. */
                    throw new IllegalStateException(ie);
                }
            }
        }

        /**
         * Updates the partition database handles.
         *
         * @return true if there was an error
         */
        private boolean update() {
            final PartitionMap partitionMap = topology.getPartitionMap();

            logger.log(Level.FINE,
                       "Establishing partition database handles, " +
                       "topology seq#: {0}",
                       topology.getSequenceNumber());

            final int groupId = repNode.getRepNodeId().getGroupId();
            int errors = 0;
            int rnPartitions = 0;
            for (Partition p : partitionMap.getAll()) {

                /* Exit if the updater has been stopped, or the env is bad */
                if (stop || !repEnv.isValid()) {
                    logger.log(Level.INFO,
                               "Update terminated, established {0} " +
                               "partition database handles",
                               partitionDbMap.size());
                    return false;   // Will cause thread to exit
                }

                final PartitionId partitionId = p.getResourceId();
                if (p.getRepGroupId().getGroupId() != groupId) {
                    logger.log(Level.FINE,
                               "Removing partition database handle for {0}",
                               partitionId);

                    /* This node does not host the partition. */
                    final Database db = partitionDbMap.remove(partitionId);

                    /*
                     * If db != null then the partition had moved, so we can
                     * close the database.
                     *
                     * Note that if the partition has migrated the database will
                     * be removed once the topology hs been updated and the
                     * change made "official.
                     * See MigrationManager.localizeTopology().
                     */
                    if (db != null) {
                        logger.log(Level.INFO,
                                   "Closing database {0} for moved {1}",
                                   new Object[]{db.getDatabaseName(),
                                                partitionId});
                        /*
                         * The return can be ignored since the partition
                         * migration transfer is complete and there is nothing
                         * left to do.
                         */
                        closePartitionDB(db);
                    }
                    continue;
                }
                rnPartitions++;

                try {
                    final Database currentDB = partitionDbMap.get(partitionId);
                    if ((currentDB == null) || !reuseExistingHandles) {
                        updatePartitionHandle(partitionId);
                    }
                } catch (RuntimeException re) {
                    if (handleException(re, logger,
                                        partitionId.getPartitionName())) {
                        errors++;
                    }
                }
            }
            repNode.getRepEnvManager().updateRNPartitions(rnPartitions);

            /*
             * If there have been errors return true (unless the update has been
             * stopped) which will cause the update to be retried.
             */
            if (errors > 0) {
                logger.log(Level.INFO,
                           "Established {0} partition database handles, " +
                           "will retry in {1}ms",
                           new Object[] {partitionDbMap.size(),
                                         DB_OPEN_RETRY_MS});
                return !stop;
            }

            /* Success */
            logger.log(Level.INFO,
                       "Established {0} partition database handles, " +
                       "topology seq#: {1}",
                       new Object[]{partitionDbMap.size(),
                                    topology.getSequenceNumber()});
            return false;
        }

        /**
         * Opens the specified partition database and stores its handle in
         * partitionDBMap.
         */
        private void updatePartitionHandle(final PartitionId partitionId)
            throws ReplicaWriteException {

            /*
             * Use NO_CONSISTENCY so that the handle establishment is not
             * blocked trying to reach consistency particularly when the env is
             * in the unknown state and we want to permit read access.
             */
            final TransactionConfig txnConfig = new TransactionConfig().
               setConsistencyPolicy(NoConsistencyRequiredPolicy.NO_CONSISTENCY);

            Transaction txn = null;
            try {
                txn = repEnv.beginTransaction(null, txnConfig);
                final Database db =
                        repEnv.openDatabase(txn, partitionId.getPartitionName(),
                                            partitionDbConfig);
                txn.commit();
                txn = null;
                partitionDbMap.put(partitionId, db);
            } catch (IllegalStateException e) {

                /*
                 * The exception was most likely thrown because the environment
                 * was closed.  If it was thrown for another reason, though,
                 * then invalidate the environment so that the caller will
                 * attempt to recover by reopening it.
                 */
                if (repEnv.isValid()) {
                    EnvironmentFailureException.unexpectedException(
                        DbInternal.getEnvironmentImpl(repEnv), e);
                }
                throw e;

            } finally {
               TxnUtil.abort(txn);
            }
        }
        
        /**
         * Stops the updater and waits for the thread to exit.
         */
        void waitForStop() {
            assert Thread.currentThread() != this;

            stop = true;

            try {
                join();
            } catch (InterruptedException ie) {
                /* Should not happen. */
                throw new IllegalStateException(ie);
            }
        }
    }
}
