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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.RepNodeService.Params;
import oracle.kv.impl.rep.migration.MigrationManager.DBOperation;
import oracle.kv.impl.rep.migration.TransferProtocol.OP;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.NoConsistencyRequiredPolicy;
import com.sleepycat.je.rep.net.DataChannel;

/**
 * Migration source thread. This thread will read records from the Db and
 * send them along to the target. It will exit when the reading of the Db is
 * complete or the migration is canceled.
 *
 * The initial data transfer takes place on the channel passed to the
 * constructor. The communication on this channel is one-way (source to target)
 * and consists of messages defined by the OP enum.
 *
 * Once the transfer is complete, the source and target nodes participate in
 * the Transfer of Ownership (ToO) protocol to establish the partition at its
 * new location.
 *
 * Transfer of Ownership (ToO)
 *
 * #     Source                            Target                     Admin
 * 0 Servicing client OPS COPY(K/V)    Processing OPS               Polling for
 *   Sending on-disk K/V  PUT(K/V)  -> from source     No <-> Done? completion.
 *   and client ops       DELETE(K)
 *   Topology is at TA
 * 1 Read last K/V from
 *   on-disk P
 * 2 Persist TAx
 * 3 End Of Data                 EOD ->
 * 4 Forward RMW                 RMW ->
 * 5                                    Make partition durable
 * 6                                    Persist TAx
 * 7                                    Accept RMW
 * 8                                                        <- Done?
 * 9                                                   Yes ->
 * 10                                                                Update
 *                                                                   topology
 *                                                                   TA => TB
 * 11 Update to TB                      Update to TB
 * 12 Stop RMW
 *
 * In #0 a No response to Done? is PENDING or RUNNING
 * In #9 a Yes response to Done? is SUCCESS
 *
 * Forwarded client operations at #4 will fail until the target reaches #7.
 *
 * After #7 the migration can not be canceled because the target partition
 * may have been modified by forwarded client operations and therefore is the
 * only up-to-date copy of the data.
 *
 * To cancel a migration the admin can invoke cancelMigration(PartitionId) on
 * the target. The admin can consider the migration canceled Iff the return
 * value is PartitionMigrationState.ERROR. When the migration is canceled, the
 * admin must then invoke canceled(PartitionId, RepGroupId) on the source
 * repNode.
 *
 * After #2 the source will monitor the target by periodically invoking
 * getMigrationState(). If PartitionMigrationState.ERROR is returned the source
 * will remove the record for the migration, undoing step #2 and effectively
 * canceling the migration on the source side. (see TargetMonitorExecutor)
 */
class MigrationSource implements Runnable {

    private final Logger logger;

    private final DataChannel channel;

    private final DataOutputStream stream;

    /* The partition this source is transferring */
    private final PartitionId partitionId;

    /* The target this partition will be transfered to */
    private final RepNodeId targetRNId;

    private final RepNode repNode;

    private final MigrationService service;

    /* The partition db */
    private final Database partitionDb;

    /*
     * The thread executing the source runnable. Need to keep this because
     * we may have to wait for the thread to exit on shutdown.
     */
    private volatile Thread executingThread = null;

    /*
     * The last key sent. This is used to filter client operations. Client
     * operations less than or equal to the lastKey must be sent to the target.
     * All other ops can be ignored.
     */
    private DatabaseEntry lastKey = null;

    /* True if the migration has been canceled */
    private volatile boolean canceled = false;

    /* True if EOD has been sent */
    private volatile boolean eod = false;

    /* statistics */
    private final long startTime;
    private long endTime = 0;
    private int operations = 0;
    private final int filtered = 0;
    private int transactionConflicts = 0;
    private long recordsSent = 0;
    private long clientOpsSent = 0;

    MigrationSource(DataChannel channel,
                    PartitionId partitionId,
                    RepNodeId targetRNId,
                    RepNode repNode,
                    MigrationService service,
                    Params params)
        throws IOException {
        this.channel = channel;
        this.stream = new DataOutputStream(Channels.newOutputStream(channel));
        this.partitionId = partitionId;
        this.targetRNId = targetRNId;
        this.repNode = repNode;
        this.service = service;
        logger = LoggerUtils.getLogger(this.getClass(), params);
        partitionDb = repNode.getPartitionDB(partitionId);

        SocketChannel sChannel = channel.getSocketChannel();
        sChannel.configureBlocking(true);
        sChannel.socket().setSoTimeout(
                               params.getRepNodeParams().getReadWriteTimeout());
        sChannel.socket().setTcpNoDelay(false);
        startTime = System.currentTimeMillis();
    }

    /**
     * Gets statistics on this migration source.
     *
     * @return a statistics object
     */
    PartitionMigrationStatus getStatus() {
        return new PartitionMigrationStatus(partitionId.getPartitionId(),
                                            targetRNId.getGroupId(),
                                            repNode.getRepNodeId().getGroupId(),
                                            operations,
                                            startTime,
                                            endTime,
                                            recordsSent,
                                            clientOpsSent);
    }

    int getTargetGroupId() {
        return targetRNId.getGroupId();
    }

    /**
     * Returns true if there is a thread associated with this source.
     * Note that this doesn't mean that the thread is running.
     *
     * @return true if there is a thread associated with this source
     */
    boolean isAlive() {
        return (executingThread != null);
    }

    /**
     * Cancels the migration. If wait is true it will wait for the source
     * thread to exit. Otherwise it returns immediately.
     *
     * @param wait if true will wait for thread exit
     */
    synchronized void cancel(boolean wait) {
        canceled = true;

        if (!wait) {
            return;
        }

        final Thread thread = executingThread;

        /* Wait if there is a thread AND is is running */
        if ((thread != null) && thread.isAlive()) {
            assert Thread.currentThread() != thread;

            try {
                logger.log(Level.FINE, "Waiting for {0} to exit", this);
                thread.join(5000);

                if (isAlive()) {
                    logger.log(Level.FINE, "Cancel of {0} timed out", this);
                }
            } catch (InterruptedException ie) {
                /* Should not happen. */
                throw new IllegalStateException(ie);
            }
        }
    }

    /**
     * Logs the specified IO exception and cancels this source.
     *
     * @param ioe an IO exception
     */
    private void error(IOException ioe) {
        error("Unexpected exception, stopping " + this, ioe);
    }

    /**
     * Logs the specified exception with a message and cancels this source.
     *
     * @param message a message to log
     * @param ex an exception
     */
    private void error(String message, Exception ex) {
        logger.log(Level.INFO, message, ex);
        cancel(false);
    }

    @Override
    public void run() {
        executingThread = Thread.currentThread();

        Cursor cursor = null;

        try {
            cursor = partitionDb.openCursor(null, CursorConfig.READ_COMMITTED);
            final DatabaseEntry key = new DatabaseEntry();
            final DatabaseEntry value = new DatabaseEntry();

            while (!canceled) {
                assert TestHookExecute.doHookIfSet(service.readHook, lastKey);
                assert cursor != null;

                try {

                    switch (cursor.getNext(key, value, LockMode.DEFAULT)) {
                        case SUCCESS:
                            sendCopy(key, value);
                            break;

                        case NOTFOUND:
                            /*
                             * Must close cursor here because transfer complete
                             * will cause the underlying DB to be closed.
                             */
                            cursor.close();
                            cursor = null;

                            /* ToO #1 - Finished reading on-disk records */
                            transferComplete();
                            return;

                        case KEYEMPTY:
                            /* Key under cursor was deleted, move on */
                            break;

                        case KEYEXIST:
                            /* Can't happen but keeps Eclipse happy */
                            break;
                    }
                } catch (LockConflictException lce) {
                	if (cursor == null) {
                            return;
                	}

                    /* retry */
                    transactionConflicts++;
                }
            }
        } catch (DatabaseException de) {
            logger.log(Level.INFO,
                     this + " encountered database exception reading partition",
                     de);
        } catch (Exception ex) {
            logger.log(Level.INFO,
                       this + " encountered unexpected exception", ex);
        } finally {
            logger.log(Level.FINE, "{0} exiting", this);
            closeChannel();
            if (cursor != null) {
                try {
                    cursor.close();
                } catch (DatabaseException ex) {
                    logger.log(Level.WARNING,
                               "Exception closing partition migration cursor",
                               ex);
                }
            }
            executingThread = null;
        }
    }

    /**
     * Closes the channel, logging any resulting exceptions.
     */
    private void closeChannel() {
        try {
            channel.close();
        } catch (IOException ioe) {
            logger.log(Level.WARNING,
                       "Exception closing partition migration channel", ioe);
        }
    }

    /**
     * Signals the transfer of the on-disk records to the target is complete.
     */
    private void transferComplete() {
        logger.log(Level.INFO, "{0} completed transfer", this);
        endTime = System.currentTimeMillis();

        /*
         * ToO #2 - Persist the fact that the transfer to the target is
         * complete. This will update the local topology which will redirect
         * all ops to the new rep group on the target.
         *
         * When the transfer completed update propagates to the replicas they
         * will start to forward their client ops to the target - ToO #4
         */
        if (!persistTransferComplete()) {
            cancel(false);
            return;
        }

        /*
         * ToO #3 - Write the End of Data marker onto the migrations stream.
         * This will set the eod flag causing any in-progress client write ops
         * to fail. Once the local topology is updated the client ops will be
         * redirected.
         */
        sendEOD();

        if (logger.isLoggable(Level.INFO)) {
            final long seconds = (endTime - startTime) / 1000;
            final long opsPerSec = (seconds == 0) ? operations :
                                                    operations / seconds;
            logger.log(Level.INFO,
                       "Sent EOD for {0}, {1} total operations, {2} " +
                       "filtered, {3} transaction conflicts, {4} ops/second",
                       new Object[]{partitionId, operations,
                                    filtered, transactionConflicts, opsPerSec});
        }
    }

    /**
     * Returns true if the specified key can be filtered from the migration
     * stream.
     */
    private boolean filterOp(DatabaseEntry key) {

        /*
         * compareKeys() can throw an ISE if the database has been closed (due
         * to the migration completing). In this case the ISE will be caught in
         * RequestHandlerImpl.executeInternal() and the client operation will
         * be forwarded
         */
        return (lastKey == null) ? false :
                                   partitionDb.compareKeys(lastKey, key) < 0;
    }

    private synchronized void sendCopy(DatabaseEntry key, DatabaseEntry value) {
        try {
            writeOp(OP.COPY);
            writeDbEntry(key);
            writeDbEntry(value);
            lastKey = new DatabaseEntry(key.getData());
            recordsSent++;
        } catch (IOException ioe) {
            error(ioe);
        }
    }

    synchronized boolean sendPut(long txnId,
                                 DatabaseEntry key,
                                 DatabaseEntry value) {
        if (canceled) {
            return false;
        }
        if (filterOp(key)) {
            return false;
        }

        try {
            writeOp(OP.PUT, txnId);
            writeDbEntry(key);
            writeDbEntry(value);
            clientOpsSent++;
            return true;
        } catch (IOException ioe) {
            error(ioe);
        }
        return false;
    }

    synchronized boolean sendDelete(long txnId, DatabaseEntry key) {
        if (canceled) {
            return false;
        }
        if (filterOp(key)) {
            return false;
        }

        try {
            writeOp(OP.DELETE, txnId);
            writeDbEntry(key);
            clientOpsSent++;
            return true;
        } catch (IOException ioe) {
            error(ioe);
        }
        return false;
    }

    synchronized void sendPrepare(long txnId) {
        if (canceled) {
            return;
        }
        try {
            writeOp(OP.PREPARE, txnId);
        } catch (IOException ioe) {
            error(ioe);
        }
    }

    synchronized void sendResolution(long txnId, boolean commit) {
        if (canceled) {
            return;
        }
        try {
            writeOp(commit ? OP.COMMIT : OP.ABORT, txnId);
        } catch (IllegalStateException ise) {
            /*
             * At this point nothing can be done about an ISE. This can
             * come from the partition moving out from under the operation
             * but since the transaction has completed, it has been persisted
             * on the source. The target will have to detect this state and
             * abort the migration.
             */
            error("Exception attempting to send resolution, stopping " + this,
                  ise);
        } catch (IOException ioe) {
            error(ioe);
        }
    }

    private synchronized void sendEOD() {
        if (canceled) {
            return;
        }
        try {
            writeOp(OP.EOD);
            eod = true;
        } catch (IOException ioe) {
            error(ioe);
        }
    }

    private void writeOp(OP op) throws IOException {
        assert Thread.holdsLock(this);

        if (eod) {
            /*
             * Attempt to write an op after EOD has been sent. By throwing
             * an IllegalStateException RequestHandlerImpl.executeInternal()
             * will forward the client request to the new node.
             */
            throw new IllegalStateException(partitionId + " has moved");
        }
        stream.write(op.ordinal());
        operations++;
    }

    private void writeOp(OP op, long txnId) throws IOException {
        writeOp(op);
        stream.writeLong(txnId);
    }

    private void writeDbEntry(DatabaseEntry entry) throws IOException {
        assert Thread.holdsLock(this);

        stream.writeInt(entry.getSize());
        stream.write(entry.getData());
    }

    private boolean persistTransferComplete() {
        logger.log(Level.FINE,
                   "Persist transfer complete for {0}", partitionId);

        final RepGroupId sourceRGId =
                        new RepGroupId(repNode.getRepNodeId().getGroupId());
        final PartitionMigrationStatus status = getStatus();
        final TransactionConfig txnConfig = new TransactionConfig();
        txnConfig.setConsistencyPolicy(
                                 NoConsistencyRequiredPolicy.NO_CONSISTENCY);

        txnConfig.setDurability(
               new Durability(Durability.SyncPolicy.SYNC,
                              Durability.SyncPolicy.SYNC,
                              Durability.ReplicaAckPolicy.SIMPLE_MAJORITY));

        final MigrationManager manager = service.manager;

        final Boolean success =
            manager.tryDBOperation(new DBOperation<Boolean>() {

            @Override
            public Boolean call(Database db) {

                Transaction txn = null;
                try {
                    txn = db.getEnvironment().beginTransaction(null, txnConfig);

                    final PartitionMigrations pm =
                                      PartitionMigrations.fetch(db, txn);

                    pm.add(pm.newSource(status, partitionId, sourceRGId,
                                        targetRNId));
                    pm.persist(db, txn, true);
                    txn.commit();
                    txn = null;
                    return true;
                } finally {
                    TxnUtil.abort(txn);
                }
            }
        }, true);

        if ((success == null) || !success) {
            return false;
        }

        /*
         * The local topology must be updated to reflect the new location
         * of the partition before the source thread exits so that client
         * operations no longer access the local partition DB.
         */
        manager.criticalUpdate();

        /*
         * Now that the record has been persisted and the local topology
         * updated, monitor the target for failure so that this may be undone.
         */
        manager.monitorTarget();
        return true;
    }

    @Override
    public String toString() {
        return "MigrationSource[" + partitionId + ", " + targetRNId +
               ", " + eod + "]";
    }
}
