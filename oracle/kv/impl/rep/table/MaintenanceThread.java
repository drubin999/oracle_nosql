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

package oracle.kv.impl.rep.table;

import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.api.ops.MultiDeleteTable;
import oracle.kv.impl.api.ops.OperationHandler;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.table.SecondaryInfoMap.DeletedTableInfo;
import oracle.kv.impl.rep.table.SecondaryInfoMap.SecondaryInfo;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.util.TxnUtil;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.rep.InsufficientAcksException;
import com.sleepycat.je.rep.InsufficientReplicasException;
import com.sleepycat.je.rep.ReplicatedEnvironment;

/**
 * Base class for maintenance threads. A maintenance thread only runs on
 * the master.
 */
abstract class MaintenanceThread extends Thread {
    private static final int NUM_DB_OP_RETRIES = 100;

    /* DB operation delays */
    private static final long SHORT_RETRY_TIME = 500;

    private static final long LONG_RETRY_TIME = 1000;

    /* Number of records read from the primary during each populate call. */
    private static final int POPULATE_BATCH_SIZE = 100;

    /*
     * Number of records read from a secondary DB partition in a transaction
     * when cleaning after a partition migration.
     */
    private static final int CLEAN_BATCH_SIZE = 100;

    /*
     * Number of records deleted from the partition DB partition in a
     *  transaction during cleaning due to a removed table.
     */
    private static final int DELETE_BATCH_SIZE = 100;

    protected final TableManager tableManager;
    protected final RepNode repNode;
    protected final Logger logger;

    protected final ReplicatedEnvironment repEnv;

    protected volatile boolean stop = false;

    protected MaintenanceThread(String name,
                                TableManager tableManager,
                                RepNode repNode,
                                ReplicatedEnvironment repEnv,
                                Logger logger) {
        super(name);
        this.tableManager = tableManager;
        this.repNode = repNode;
        this.repEnv = repEnv;
        this.logger = logger;
    }

    @Override
    public void run() {
        logger.log(Level.FINE, "Starting {0}", this);

        DatabaseException de = null;
        long delay = 0;
        int retryCount = NUM_DB_OP_RETRIES;
        Database infoDb = null;
        while (!isStopped()) {
            try {
                infoDb = SecondaryInfoMap.openDb(repEnv);
                doWork(infoDb);
                stop = true;
                // TODO - need to separate this from doWork() in terms
                // of exception handling.
                tableManager.maintenanceThreadExit(repEnv, infoDb);
                return;
            } catch (InsufficientAcksException iae) {
                de = iae;
                delay = LONG_RETRY_TIME;
            } catch (InsufficientReplicasException ire) {
                de = ire;
                delay = LONG_RETRY_TIME;
            } catch (LockConflictException lce) {
                de = lce;
                delay = SHORT_RETRY_TIME;
            } finally {
                if (infoDb != null) {
                    TxnUtil.close(logger, infoDb, null);
                }
            }
            assert de != null;
            retrySleep(retryCount, delay, de);
            retryCount--;
        }
        logger.log(Level.FINE, "{0} stopped", this);
    }

    private void retrySleep(int count, long sleepTime, DatabaseException de) {
        logger.log(Level.INFO, "DB op caused {0} attempts left {1}",
                   new Object[]{de, count});

        /* If the cound has expired, re-throw the last exception */
        if (count <= 0) {
            throw de;
        }

        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException ie) {
            /* Should not happen. */
            throw new IllegalStateException(ie);
        }
    }

    /**
     * Does the actual work of the thread.
     *
     * @param infoDb the opened secondary info database
     */
    abstract void doWork(Database infoDb);

    /**
     * Returns true if the operation is to be stopped, either by external
     * request or by the environment changing.
     *
     * @return true if the thread is to be stopped
     */
    protected boolean isStopped() {
        return stop ||
               !repEnv.isValid() ||
               !repEnv.getState().isMaster();
    }

    /**
     * Stops the operation and waits for the thread to exit.
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

    /**
     * Thread to populate new secondary DBs.
     */
    static class PopulateThread extends MaintenanceThread {

        PopulateThread(TableManager tableManager,
                       RepNode repNode,
                       ReplicatedEnvironment repEnv,
                       Logger logger) {
            super("KV secondary populator",
                  tableManager, repNode, repEnv, logger);
        }

        /*
         * Populate the secondary databases.
         */
        @Override
        protected void doWork(Database infoDb) {

            /* The working secondary. */
            String currentSecondary = null;

            while (!isStopped()) {
                SecondaryDatabase db = null;
                Transaction txn = null;
                try {
                    txn = repEnv.beginTransaction(null,
                                        SecondaryInfoMap.SECONDARY_INFO_CONFIG);

                    final SecondaryInfoMap infoMap =
                        SecondaryInfoMap.fetch(infoDb, txn, LockMode.RMW);

                    if (infoMap == null) {
                        logger.info("Unable to read secondary info database");
                        return;
                    }

                    if (currentSecondary == null) {
                        currentSecondary = infoMap.getNextSecondaryToPopulate();

                        /* If no more, we are finally done */
                        if (currentSecondary == null) {
                            logger.info("Completed populating secondary " +
                                        "database(s)");
                            return;
                        }
                        logger.log(Level.INFO,  // TODO - FINE
                                   "Started populating {0}", currentSecondary);
                    }

                    final SecondaryInfo info =
                        infoMap.getSecondaryInfo(currentSecondary);
                    assert info != null;
                    assert info.needsPopulating();

                    if (info.getCurrentPartition() == null) {
                        for (PartitionId partition : repNode.getPartitions()) {
                            if (!info.isCompleted(partition)) {
                                info.setCurrentPartition(partition);
                                break;
                            }
                        }
                    }

                    if (info.getCurrentPartition() == null) {
                        info.donePopulation();
                        logger.log(Level.INFO, "Finished populating {0} {1}",
                                   new Object[]{currentSecondary, info});
                        infoMap.persist(infoDb, txn);
                        txn.commit();
                        txn = null;
                        
                        db = tableManager.getSecondaryDb(currentSecondary);
                        assert db != null;
                        db.endIncrementalPopulation();
                        currentSecondary = null;
                        continue;
                    }

                    final Database partitionDb =
                        repNode.getPartitionDB(info.getCurrentPartition());

                    /* If the partition is missing, something is up, punt. */
                    if (partitionDb == null) {
                        logger.log(Level.WARNING,
                                   "Failed to populate {0}, partition {1} is " +
                                   "missing",
                                   new Object[]{info,
                                                info.getCurrentPartition()});
                        return;
                    }

                    db = tableManager.getSecondaryDb(currentSecondary);

                    if (db == null) {
                        logger.log(Level.WARNING,
                                   "Failed to populate {0}, secondary " +
                                   "database {1} is missing",
                                   new Object[]{info, currentSecondary});
                        return;
                    }
                    logger.log(Level.FINE, "Populating {0} {1}",
                               new Object[]{currentSecondary, info});

                    if (!partitionDb.populateSecondaries(info.getLastKey(),
                                                         POPULATE_BATCH_SIZE)) {
                        logger.log(Level.FINE, "Finished partition for {0}",
                                   info);
                        info.completeCurrentPartition();
                    }
                    infoMap.persist(infoDb, txn);
                    txn.commit();
                    txn = null;
                } finally {
                    TxnUtil.abort(txn);
                }
            }
        }
    }

    /**
     * Thread to clean secondary databases. A secondary needs to be "cleaned"
     * when a partition has moved from this node. In this case secondary
     * records that are from primary records in the moved partition need to be
     * removed the secondary. Cleaning is done by reading each record in a
     * secondary DB and checking whether the primary key is from a missing
     * partition. If so, remove the secondary record. Cleaning needs to happen
     * on every secondary whenever a partition has migrated.
     */
    static class SecondaryCleanerThread extends MaintenanceThread {

        SecondaryCleanerThread(TableManager tableManager,
                               RepNode repNode,
                               ReplicatedEnvironment repEnv,
                               Logger logger) {
            super("KV secondary cleaner",
                  tableManager, repNode, repEnv, logger);
        }

        @Override
        protected void doWork(Database infoDb) {
            if (isStopped()) {
                return;
            }

            /*
             * The working secondary. Working on one secondary at a time is
             * an optimization in space and time. It reduces the calls to
             * getNextSecondaryToClean() which iterates over the indexes, and
             * makes it so that only one SecondaryInfo is keeping track of
             * the cleaned partitions.
             */
            String currentSecondary = null;

            while (!isStopped()) {
                Transaction txn = null;
                try {
                    txn = repEnv.beginTransaction(null,
                                        SecondaryInfoMap.SECONDARY_INFO_CONFIG);

                    final SecondaryInfoMap infoMap =
                        SecondaryInfoMap.fetch(infoDb, txn, LockMode.RMW);

                    if (infoMap == null) {
                        logger.info("Unable to read secondary info database");
                        return;
                    }

                    if (currentSecondary == null) {
                        currentSecondary = infoMap.getNextSecondaryToClean();
                    }

                    /* If no more, we are finally done */
                    if (currentSecondary == null) {
                        logger.info("Completed cleaning secondary database(s)");
                        return;
                    }

                    final SecondaryInfo info =
                        infoMap.getSecondaryInfo(currentSecondary);
                    assert info != null;
                    assert info.needsCleaning();

                    final SecondaryDatabase db =
                        tableManager.getSecondaryDb(currentSecondary);

                    if (db == null) {
                        logger.log(Level.WARNING,
                                   "Failed to clean {0}, secondary " +
                                   "database {1} is missing",
                                   new Object[]{info, currentSecondary});
                        return;
                    }

                    if (!db.deleteObsoletePrimaryKeys(info.getLastKey(),
                                                      info.getLastData(),
                                                      CLEAN_BATCH_SIZE)) {
                        logger.log(Level.INFO, "Completed cleaning {0}",
                                   currentSecondary);
                        info.doneCleaning();
                        currentSecondary = null;
                    }
                    infoMap.persist(infoDb, txn);
                    txn.commit();
                    txn = null;
                } finally {
                    TxnUtil.abort(txn);
                }
            }
        }
    }

    /**
     * Thread to clean primary records. This thread will remove primary
     * records associated with a table which has been marked for deletion.
     */
    static class PrimaryCleanerThread extends MaintenanceThread {

        PrimaryCleanerThread(TableManager tableManager,
                             RepNode repNode,
                             ReplicatedEnvironment repEnv,
                             Logger logger) {
            super("KV primary cleaner",
                  tableManager, repNode, repEnv, logger);
        }

        @Override
        protected void doWork(Database infoDb) {
            if (isStopped()) {
                return;
            }

            /*
             * NOTE: this code does not attempt to use partition migration
             * streams to propagate deletions because it is assumed to be
             * running as a remove-table plan.  Such plans are mutually
             * exclusive with respect to elasticity plans so there can be no
             * migrations in progress.  If that assumption ever changes this
             * code needs to initialize and use MigrationStreamHandle objects.
             */

            /* The working table. */
            String currentTable = null;

            while (!isStopped()) {
                Transaction txn = null;
                try {
                    txn = repEnv.beginTransaction(null,
                                        SecondaryInfoMap.SECONDARY_INFO_CONFIG);

                    final SecondaryInfoMap infoMap =
                        SecondaryInfoMap.fetch(infoDb, txn, LockMode.RMW);

                    if (infoMap == null) {
                        logger.info("Unable to read secondary info database");
                        return;
                    }

                    if (currentTable == null) {
                        currentTable = infoMap.getNextDeletedTableToClean();
                    }

                    /* If no more, we are finally done */
                    if (currentTable == null) {
                        logger.info("Completed cleaning partition database(s)" +
                                    " for all tables");
                        return;
                    }

                    final DeletedTableInfo info =
                                    infoMap.getDeletedTableInfo(currentTable);
                    assert info != null;
                    assert !info.isDone();
                    if (info.getCurrentPartition() == null) {
                        for (PartitionId partition : repNode.getPartitions()) {
                            if (!info.isCompleted(partition)) {
                                info.setCurrentPartition(partition);
                                break;
                            }
                        }
                    }

                    if (info.getCurrentPartition() == null) {
                        logger.log(Level.INFO,
                                   "Completed cleaning partition database(s) " +
                                   "for {0}", currentTable);
                        info.setDone();
                        currentTable = null;
                    } else {

                        // delete some...
                        if (deleteABlock(info, txn)) {
                            logger.log(Level.INFO,  // TODO FINE - as most other logging here
                                       "Completed cleaning " +
                                       info.getCurrentPartition() +
                                       " for {0}", currentTable);
                            // Done with this partition
                            info.completeCurrentPartition();
                        }
                    }

                    infoMap.persist(infoDb, txn);
                    txn.commit();
                    txn = null;
                } finally {
                    TxnUtil.abort(txn);
                }
            }
        }

        /*
         * Deletes up to DELETE_BATCH_SIZE number of records from some
         * primary (partition) database.
         */
        private boolean deleteABlock(DeletedTableInfo info, Transaction txn) {
            final MultiDeleteTable mdt =
                new MultiDeleteTable(info.getParentKeyBytes(),
                                     info.getTargetTableId(),
                                     info.getMajorPathComplete(),
                                     DELETE_BATCH_SIZE,
                                     info.getCurrentKeyBytes());
            final OperationHandler oh =
                new OperationHandler(repNode, tableManager.getParams());

            final Result result = mdt.execute(txn,
                                              info.getCurrentPartition(),
                                              oh);
            int num = result.getNDeletions();
            logger.info("Deleted " + num + " records in " +
                        "partition " + info.getCurrentPartition() +
                        (num < DELETE_BATCH_SIZE ?
                         ", partition is complete" : ""));
            if (num < DELETE_BATCH_SIZE) {
                /* done with this partition */
                info.setCurrentKeyBytes(null);
                return true;
            }
            info.setCurrentKeyBytes(mdt.getLastDeleted());
            return false;
        }
    }
}
