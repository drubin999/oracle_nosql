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

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.Channels;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.RepNodeService.Params;
import oracle.kv.impl.rep.RepNodeStatus;
import oracle.kv.impl.rep.admin.RepNodeAdmin.PartitionMigrationState;
import oracle.kv.impl.rep.migration.MigrationManager.DBOperation;
import oracle.kv.impl.rep.migration.PartitionMigrations.TargetRecord;
import oracle.kv.impl.rep.migration.TransferProtocol.OP;
import oracle.kv.impl.rep.migration.TransferProtocol.TransferRequest;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.Ping;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.NoConsistencyRequiredPolicy;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.net.DataChannelFactory.ConnectOptions;
import com.sleepycat.je.rep.utilint.HostPortPair;
import com.sleepycat.je.rep.utilint.RepUtils;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.Response;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.ServiceConnectFailedException;

/**
 * Partition migration target. This class is the destination side
 * of a source target pair.
 *
 * The migration process is initiated by the target which will attempt to
 * contact the source and send a migration request. This initial request
 * is the only time the target sends data to the source. All other
 * communication is one-way, source to target.
 *
 * Once a connection is established (migration stream) the source will send
 * records (keys and values) read from the partition's DB and client
 * operations targeted for the partition. Additional messages are needed to
 * handle client transactions.
 *
 * The source sends Ops until all records are read read and send to the
 * target. At that time the source will send an End of Data (EoD) message.
 * Once the target received the EoD Op it initiates the Transfer of Ownership
 * protocol (ToO).
 *
 * If the target encounters an error any time during the above steps
 * it may wait and retry. See the MigrationTarget.call() method.
 *
 * During a migration the target consists of two threads. One (Reader) will
 * read operation messages from the source migration stream. Most messages
 * result in Op objects being placed on a queue. The second "consumer" thread
 * (MigrationTarget) removes Ops from the queue and executes them. This
 * continues until an EoD message is encountered.
 */
class MigrationTarget implements Callable<MigrationTarget> {

    private final Logger logger;

    private static final int SECOND_MS = 1000;

    /* Number of times to retry after an error. */
    private static final int MAX_ERRORS = 10;

    /* Retry wait period for when the source or target is busy */
    private final long waitAfterBusy;

    /* Retry wait period for when there is an error */
    private final long waitAfterError;

    /* Configuration for speedy writes */
    private static final TransactionConfig weakConfig =
        new TransactionConfig().
               setConsistencyPolicy(NoConsistencyRequiredPolicy.NO_CONSISTENCY).
               setDurability(new Durability(Durability.SyncPolicy.NO_SYNC,
                                            Durability.SyncPolicy.NO_SYNC,
                                            Durability.ReplicaAckPolicy.NONE));

    /* The partition this target is going to get */
    private final PartitionId partitionId;

    /* The current rep group the partition resides on */
    private final RepGroupId sourceRGId;

    /* The ID of the TargetRecord associated with this target */
    private final long recordId;

    private final RepNode repNode;

    private final MigrationManager manager;

    private final ReplicatedEnvironment repEnv;

    private final ReaderFactory readerFactory;

    private DataChannel channel = null;

    /* The new partition db */
    private Database partitionDb = null;

    /*
     * The following three flags define the state of the migration:
     *                 running          done      canceled
     * PENDING          false           false       false
     * RUNNING          true            false       false
     * SUCCEEDED          -             true         -
     * ERROR              -             false       true
     *
     * The legal transitions are:
     *
     *                                    setDone()
     *          setRunning()            -------------> SUCCEEDED
     * PENDING --------------> RUNNING /
     *     ^                      |    \-------------> ERROR
     *     |______________________|      setCanceled() via error() or
     *          setStopped()              cancel()
     */

    /*
     * True when the target is executing.
     */
    private volatile boolean running = false;

    /*
     * True when the migration is complete and the partition has been
     * made durable. Once set the migration can not be canceled.
     */
    private volatile boolean done = false;

    /* Guard to keep from being canceled while finishing ToO (see setDone()) */
    private volatile boolean inDone = false;

    /*
     * True when the migration is canceled. Could be set from an admin
     * command or because of an unrecoverable error.
     */
    private volatile boolean canceled = false;

    /* Exception that caused the migration to terminate */
    private Exception errorCause = null;

    /*
     * Time (in milliseconds) to wait before retrying the target after an error
     * or busy response.
     */
    private long retryWait = -1;

    /*
     * For logging. When available, this will be set to the master RN of the
     * source group. Otherwise it will be the source group name.
     */
    private String sourceName;

    /* statistics */
    private final long requestTime;
    private long startTime = 0;
    private long endTime = 0;
    private long operations = 0;
    private int attempts = 0;
    private int busyResponses = 0;
    private int errors = 0;

    MigrationTarget(TargetRecord record,
                    RepNode repNode,
                    MigrationManager manager,
                    ReplicatedEnvironment repEnv,
                    Params params) {

        partitionId = record.getPartitionId();
        sourceRGId = record.getSourceRGId();
        /* Until a connection is made, the source name is just the group name */
        sourceName = sourceRGId.getGroupName();
        recordId = record.getId();
        this.repNode = repNode;
        this.manager = manager;
        this.repEnv = repEnv;
        logger = LoggerUtils.getLogger(this.getClass(), params);
        final RepNodeParams repNodeParams = params.getRepNodeParams();
        waitAfterBusy =  repNodeParams.getWaitAfterBusy();
        waitAfterError = repNodeParams.getWaitAfterError();
        readerFactory = new ReaderFactory();
        requestTime = System.currentTimeMillis();
    }

    /**
     * Gets the source group ID.
     *
     * @return the source group ID
     */
    RepGroupId getSource() {
        return sourceRGId;
    }

  /**
   * Gets the ID of the TargetRecord associated with this target.
   *
   * @return the target record ID
   */
    long getRecordId() {
        return recordId;
    }

    /**
     * Gets statistics on this migration target.
     *
     * @return a statistics object
     */
    PartitionMigrationStatus getStatus() {
        return getStatus(getState());
    }

    private PartitionMigrationStatus getStatus(PartitionMigrationState state) {
        return new PartitionMigrationStatus(state,
                                            partitionId.getPartitionId(),
                                            repNode.getRepNodeId().getGroupId(),
                                            sourceRGId.getGroupId(),
                                            operations,
                                            requestTime,
                                            startTime,
                                            endTime,
                                            attempts,
                                            busyResponses,
                                            errors);
    }

    /**
     * Gets the state of the migration. The admin will poll for status as part
     * of the ToO protocol. This returns SUCCESS iff the the EOD messages
     * is received and the partition has been made durable.
     *
     * @return migration state
     */
    PartitionMigrationState getState() {
        return done ? PartitionMigrationState.SUCCEEDED :
                 canceled ? PartitionMigrationState.ERROR.setCause(errorCause) :
                      running ? PartitionMigrationState.RUNNING :
                                PartitionMigrationState.PENDING;
    }

    /**
     * Attempts to cancel the migration. If wait is true, this method will wait
     * on the target thread to exit. Returns true if the migration can be
     * canceled, otherwise false is returned. The migration can be canceled any
     * time before the final commit of the partition and switch to topology x.
     *
     * Note that this does not remove the record from the db, as this cancel
     * could be due to shutdown, and not a failure or admin cancel.
     *
     * @param wait wait flag
     * @return true if migration was canceled
     */
    synchronized boolean cancel(boolean wait) {
        if (done || inDone) {
            return false;
        }
        setCanceled(wait, new Exception("Migration canceled"));
        return true;
    }

    /**
     * Cancels the migration by setting the canceled flag and cleans up the
     * target.
     *
     * @param wait wait flag
     */
    private synchronized void setCanceled(boolean wait, Exception cause) {
        assert !done;

        canceled = true;
        errorCause = cause;
        cleanup(wait);
    }

    /**
     * Cancels the migration due to an unrecoverable condition.
     *
     * @param msg message to log
     * @param ex exception to log
     */
    private void error(String msg, Exception cause) {
        assert !Thread.holdsLock(this);

        logger.log(Level.WARNING, msg, cause);

        setCanceled(false, new Exception(msg, cause));
        try {
            /* On an unrecoverable error, remove the record from the db */
            manager.removeRecord(partitionId, recordId, false);
        } catch (DatabaseException de) {
            logger.log(Level.INFO,
                       "Exception attempting to remove migration record for " +
                       partitionId,
                       de);
        }
    }

    /**
     * Cleans up this target. If wait is true, this method will wait on
     * the target thread to exit.
     *
     * @param wait wait flag
     */
    private synchronized void cleanup(boolean wait) {
        setStopped();

        /*
         * If requested to wait and the DB is open, wait until it is closed.
         * This is done to avoid DB errors caused but the target still running
         * after a cancel. (A DB error will cause the env to be invalidated,
         * something to avoid.
         *
         * By setting stopped (above) Reader.remove() will return null,
         * causing the main thread to exit and call cleanup(false) which will
         * then close the DB at an OK time.
         */
        if (wait && (partitionDb != null)) {
            try {
                logger.log(Level.INFO, "Waiting for {0} to exit", this);
                wait(2 * SECOND_MS);
            } catch (InterruptedException ie) {
                logger.log(Level.INFO, "Unexpected interrupt", ie);
            }
        }

        /* Close the channel if open. */
        if (channel != null) {
            try {
                channel.close();
            } catch (Exception ex) {
                logger.log(Level.WARNING, "Exception closing channel", ex);
            }
            channel = null;
        }

        /*
         * If we are not done, remove the partition migration DB if one exists.
         * (Note that a check for done is unnecessary as setDone() closes
         * the DB which clears db)
         */
        if (partitionDb != null) {
            assert !done;

            final String dbName = partitionId.getPartitionName();
            logger.log(Level.INFO, "Removing migrated DB {0}", dbName);

            try {
                closeDB();
            } catch (Exception ex) {
                logger.log(Level.WARNING, "Exception closing DB", ex);
            }

            try {
                repEnv.removeDatabase(null, dbName);
            } catch (DatabaseNotFoundException dnfe) {
                /* Shouldn't happen, but if it does, not really bad */
            } catch (Exception ex) {
                logger.log(Level.WARNING, "Exception removing DB", ex);
            }
        }
    }

    /**
     * Closes the partition migration DB if one exists.
     */
    private synchronized void closeDB() {
        if (partitionDb != null) {
            partitionDb.close();
            partitionDb = null;
        }

        /* A cleanup may be waiting for the DB to be closed */
        notifyAll();
    }

    /**
     * Sets the running flag. The running flag can be set to true only if
     * not done or canceled, otherwise it is set to false. Returns the value
     * of the running flag.
     *
     * @return running
     */
    private synchronized boolean setRunning() {
        assert !running;
        running = (done || canceled) ? false : true;
        return running;
    }

    /**
     * Sets the running flag to false.
     */
    private synchronized void setStopped() {
        running = false;
    }

    /**
     * Sets state to done (SUCCEEDED) and persists the partition DB.
     */
    private void setDone() {
        endTime = System.currentTimeMillis();

        if (logger.isLoggable(Level.INFO)) {
            final long seconds = (endTime - startTime) / 1000;
            logger.log(Level.INFO,
                       "Migration of {0} complete, {1} operations, " +
                       "transfer time: {2} seconds",
                       new Object[]{partitionId, operations, seconds});
        }
        try {
            /*
             * The normal monitor can't be held when writing the db (in
             * persistTargetDurable()) so we guard from being canceled by
             * setting inDone to true. We can't just set done to true because
             * that will cause getState() to return SUCCESS before things
             * are made durable and everyone is informed.
             */
            synchronized (this) {
                /* Doh! Too bad, we were almost there! */
                if (canceled) {
                    return;
                }
                inDone = true;
            }
            closeDB();

            /*
             * ToO #5 & #6 - Make the partition durable and persist that the
             * transfer from the source is complete.
             *
             * Persisting the transfer complete will allow the target node
             * to accept forwarded client ops - ToO #7
             *
             * ToO #9 - Setting done to true will cause SUCCEEDED be returned
             * from getMigrationState()
             */
            done = persistTargetDurable();

        } finally {
            inDone = false;
        }
    }

    /**
     * Main target loop. This may be called repeatedly during the life of the
     * partition migration, potentially in different threads. In the case of
     * a retry-able error or busy response from the source, this call will
     * return this object. In all other cases null is returned. If this
     * object is returned, getRetryWait() will return the time to wait before
     * attempting to retry.
     *
     * @return this object or null
     */
    @Override
    public MigrationTarget call() {
        if (!setRunning()) {
            return null;
        }

        attempts++;
        startTime = System.currentTimeMillis();
        endTime = 0;
        operations = 0;

        /*
         * Try clause for catching exceptions. The loop will open a channel
         * to the source, create a db if needed, and read operations from
         * the channel to populate the db.
         */
        try {
            final DataInputStream stream = openChannel();

            // TODO - Once lastKey is implemented, move createDB() to here
            // /* lastKey will be null for a clean db. */
            // final DatabaseEntry lastKey = createDb();

            TransferRequest.write(channel,
                                  partitionId.getPartitionId(),
                                  repNode.getRepNodeId(),
                                  null); // TODO - lastKey);

            final Response response = TransferRequest.readResponse(stream);
            switch (response) {

                /* OK */
                case OK:
                    logger.log(Level.INFO, "Starting {0}", this);

                    // TODO - move up when last key is implemented.
                    createDb();

                    /*
                     * Read loop. This returns when done, or throws an
                     * IOException.
                     */
                    consumeOps(readerFactory.newReader(stream));
                    break;

                /*
                 * If BUSY, the source is suitable for migration but is
                 * currently unavailable. Force the usual retry logic
                 * but without a retry limit since we assume that the
                 * source will eventually be able to service the
                 * request.
                 */
                case BUSY:
                    // TODO - make use of this info
                    TransferRequest.readNumStreams(stream);
                    setRetryWait(true,
                                 new Exception("Source busy: " +
                                           TransferRequest.readReason(stream)));
                    break;

                /*
                 * If UNKNOWN_SERVICE the node may be down/coming up or the
                 * partition is missing (from a previously failed migration).
                 * Treat as an error and retry, but not forever.
                 */
                case UNKNOWN_SERVICE:
                    setRetryWait(false,
                                 new Exception("Unknown service: " +
                                           TransferRequest.readReason(stream)));
                    break;

                /* Fatal */
                case FORMAT_ERROR:
                    error("Fatal response " + response + " from source: " +
                          TransferRequest.readReason(stream), null);
                    break;
                case AUTHENTICATE:
                	/* should not occur in this context */
                	error("Authenticate response encountered outside of " +
                              "hello sequence", null);
                	break;
                case PROCEED:
                	/* should not occur in this context */
                	error("Proceed response encountered outside of hello " +
                              "sequence", null);
                	break;
                case INVALID:
                    error("Fatal response " + response + " from source: " +
                          TransferRequest.readReason(stream), null);
                    break;
            }
        } catch (IOException ioe) {
            setRetryWait(false, ioe);
        } catch (DatabaseException de) {
            setRetryWait(false, de);
        } catch (ServiceConnectFailedException scfe) {
            error("Failed to connect to migration service at " + sourceName +
                  " for " + partitionId, scfe);
        } catch (Exception ex) {
            error("Unexpected exception, exiting", ex);
        }  finally {
            /* End time will not be set on error */
            if (endTime == 0) {
                endTime = System.currentTimeMillis();
            }

            /*
             * Before the try block exits, one of the following will be called:
             *    setDone() on success,
             *    setRetryWait() on a retry-able error, or busy response, or
             *    error() on an non-retry-able error.
             */
            cleanup(false);
        }

        /* If this should be retried, return the object. */
        return getRetryWait() >= 0 ? this : null;
    }

    /**
     * Calculates how long to wait before a retry attempt should be made. This
     * method should be called for any retry-able error conditions.
     *
     * @param busy true if the last failure was due to the source being busy
     * @param ex the exception that ended the run, if any
     */
    private void setRetryWait(boolean busy, Exception ex) {
        assert !done;

        if (canceled) {
            return;
        }

        if (busy) {
            busyResponses++;
        } else {
            errors++;
        }

        if (errors >= MAX_ERRORS) {
            error("Migration of " + partitionId + " failed. Giving up after " +
                  attempts + " attempt(s)", ex);
            return;
        }

        retryWait = busy ? waitAfterBusy : waitAfterError;

        logger.log(Level.FINE,
                   "Migration of {0} failed from: {1}",
                   new Object[]{partitionId, ex.getLocalizedMessage()});
    }

    /**
     * Gets the retry wait time in milliseconds. If the target is to be retried
     * the return value is >= 0, otherwise the value is -1.
     *
     * @return the retry wait time or -1
     */
    long getRetryWait() {
        assert !running;

        if (canceled || done) {
            return -1L;
        }
        assert retryWait >= 0;
        return retryWait;
    }

    /**
     * Establishes a channel with the partition source and creates an
     * input stream.
     *
     * @return an input stream
     * @throws IOException
     * @throws com.sleepycat.je.rep.utilint.ServiceDispatcher.ServiceConnectFailedException
     */
    private synchronized DataInputStream openChannel()
        throws IOException, ServiceConnectFailedException {

        final Topology topo = repNode.getTopology();

        if (topo == null) {
            throw new IOException("Target node not yet initialized");
        }
        final oracle.kv.impl.topo.RepNode rn = Ping.getMaster(topo, sourceRGId);

        final RepNodeStatus status = Ping.getMasterStatus(topo, sourceRGId);
        sourceName = (rn == null) ? sourceRGId.getGroupName() :
                                    rn.getResourceId().getFullName();

        if (status == null) {
            throw new IOException("Unable to get mastership status for " +
                                   sourceName);
        }
        final String haHostPort = status.getHAHostPort();

        /* getHAHostPort() returns null for a R1 node */
        if (haHostPort == null) {
            throw new IllegalStateException("Source node " + sourceName +
                                            " is running an incompatible " +
                                            "software version");
        }
        final InetSocketAddress sourceAddress =
                                        HostPortPair.getSocket(haHostPort);

        logger.log(Level.FINE,
                   "Opening channel to {0} to make migration request",
                   sourceAddress);

        final RepNodeParams repNodeParams = repNode.getRepNodeParams();
        final RepImpl repImpl = repNode.getEnvImpl(0L);
        if (repImpl == null) {
            throw new IllegalStateException("Attempt to migrate a partition " +
                                            "on a node that is not available");
        }

        ConnectOptions connectOpts = new ConnectOptions().
            setTcpNoDelay(true).
            setReceiveBufferSize(0).
            setReadTimeout(repNodeParams.getReadWriteTimeout()).
            setOpenTimeout(repNodeParams.getConnectTImeout());

        channel = RepUtils.openBlockingChannel(
            sourceAddress, repImpl.getChannelFactory(), connectOpts);

        ServiceDispatcher.doServiceHandshake(channel,
                                             MigrationService.SERVICE_NAME);

        return new DataInputStream(Channels.newInputStream(channel));
    }

    /**
     * Opens or creates the partition DB. If the DB already exists the last
     * key found in it is returned. Otherwise null is returned.
     *
     * @return a database entry or null
     */
    private synchronized DatabaseEntry createDb() {

        if (partitionDb != null) {
            Cursor cursor = partitionDb.openCursor(null, CursorConfig.DEFAULT);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry value = new DatabaseEntry();
            OperationStatus status = cursor.getLast(key, value, LockMode.RMW);
            cursor.close();

            return (status == OperationStatus.SUCCESS) ? key : null;
        }

        /* Retry until success. */
        final TransactionConfig txnConfig =
                    new TransactionConfig().setConsistencyPolicy(
                                NoConsistencyRequiredPolicy.NO_CONSISTENCY);

        /* Create DB */
        while (partitionDb == null) {

            Transaction txn = null;
            try {
                txn = repEnv.beginTransaction(null, txnConfig);
                partitionDb =
                        repEnv.openDatabase(txn,
                                            partitionId.getPartitionName(),
                                            repNode.getPartitionDbConfig());
                txn.commit();
                txn = null;

            } catch (ReplicaWriteException rwe) {
                /* Could be transitioning from master to replica. */
                final String msg = "Attempted to start partition migration " +
                                   "target for " + partitionId + " but node " +
                                   "has become a replica";
                logger.log(Level.WARNING, msg, rwe);
                throw new IllegalStateException(msg, rwe);
            } catch (UnknownMasterException ume) {
                /* Could be transitioning from master to replica. */
                final String msg = "Attempted to start partition migration " +
                                   "target for " + partitionId + " but node " +
                                   "has lost master status";
                logger.log(Level.WARNING, msg, ume);
                throw new IllegalStateException(msg, ume);
            } finally {
                TxnUtil.abort(txn);
            }
        }
        return null;
    }

    private void consumeOps(Reader reader) throws Exception {
        int count = 0;
        while (!done) {
            Op op = reader.remove();

            if (op == null) {
                throw new IOException("Reader returned null after " + count);
            }
            op.execute();
            count++;
        }
    }

    /**
     * Persists the target record in the db. Note the monitor can't be
     * held during this operation as the db's triggers call back into the
     * manager.
     *
     * @return true if the operation was successful
     */
    private boolean persistTargetDurable() {
        assert !Thread.holdsLock(this);
        logger.log(Level.FINE,
                   "Persist target transfer durable for {0}", partitionId);

        final TransactionConfig txnConfig = new TransactionConfig();
        txnConfig.setConsistencyPolicy(
                                 NoConsistencyRequiredPolicy.NO_CONSISTENCY);
        txnConfig.setDurability(
                   new Durability(Durability.SyncPolicy.SYNC,
                                  Durability.SyncPolicy.SYNC,
                                  Durability.ReplicaAckPolicy.SIMPLE_MAJORITY));

        final Boolean success =
                manager.tryDBOperation(new DBOperation<Boolean>() {

            @Override
            public Boolean call(Database db) {

                Transaction txn = null;
                try {
                    txn = db.getEnvironment().beginTransaction(null, txnConfig);

                    final PartitionMigrations pm =
                                   PartitionMigrations.fetch(db, txn);

                    final TargetRecord record = pm.getTarget(partitionId);

                    if(record == null) {
                        throw new IllegalStateException(
                                        "Unable to find migration record for " +
                                        partitionId);
                    }
                    record.setStatus(
                                getStatus(PartitionMigrationState.SUCCEEDED));

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

        /* With the migration record persisted, we can remove this target. */
        manager.removeTarget(partitionId);

        /*
         * This will update the local topology here (master) and update the
         * partition DBs. The replicas are updated through the DB triggers
         * from persisting the migration record.
         *
         * This is critical to complete, because if the local topo is not
         * updated no one will (source or target) will think they own the
         * partition. So fail the node if there is a problem and hopefully
         * the new master be correct.
         */
        manager.criticalUpdate();

        manager.setLastMigrationDuration(endTime - startTime);
        return true;
    }

    @Override
    public String toString() {
        return "MigrationTarget[" + partitionId + ", " + sourceName + ", " +
               attempts + ", " + running + ", " + done + ", " + canceled + "]";
    }

    /**
     * Encapsulates an operation.
     */
    private static abstract class Op {

        /**
         * Called to execute the operation by the consumer thread.
         */
        abstract void execute();
    }

    /**
     * Reader thread. This thread will read operations from the stream and
     * insert them onto the opQueue.
     */
    private class Reader implements Runnable {

        /*
         * Map of local transactions. No synchronization is needed since it is
         * onlt accessed by the read thread.
         */
        private final Map<Long, LocalTxn> txnMap =
                                        new HashMap<Long, LocalTxn>();

        /* The operation queue. This thread inserts ops, the target thread
         * removes them. Accesses to the queue must be synchronized.
         */
        private final Queue<Op> opQueue = new LinkedList<Op>();

        /*
         * The normal capacity of the op queue. This is overridden when the
         * consumer needs to wait for a transaction resolution.
         */
        private static final int DEFAULT_CAPACITY = 100;

        /* The capacity limit of the op queue. */
        private int capacity = DEFAULT_CAPACITY;

        private final DataInputStream stream;

        /* For general use to avoid constructing DatabaseEntrys in the OPS */
        private final DatabaseEntry keyEntry = new DatabaseEntry();
        private final DatabaseEntry valueEntry = new DatabaseEntry();

        Reader(DataInputStream stream) {
            this.stream = stream;
        }

        @Override
        public void run() {

            try {
                processStream();
            } catch (Exception ex) {
                /* If canceled, don't bother reporting. */
                if (!canceled) {
                    logger.log(Level.INFO,
                               "Exception processing migration stream for " +
                               partitionId, ex);
                }

                /*
                 * Setting running will cause remove() and PrepareOp.execute()
                 * to exit so that the target thread can handle the issue.
                 */
                setStopped();
            }
        }

        /**
         * Processes operations from the migration stream. This method does not
         * return normally until an End Of Data operation is received on the
         * stream.
         *
         * If an exception is thrown, the migration should be canceled because
         * the state of the source and the data is unknown.
         *
         * @throws Exception if there were any problems encountered processing
         * the migration stream
         */
        private void processStream() throws Exception {

            while (running) {
                OP op = OP.get(stream.readByte());

                if (op == null) {
                    throw new IOException("Bad op, or unexpected EOF");
                }
                operations++;

                switch (op) {
                    case COPY : {
                        insert(new CopyOp(readDbEntry(), readDbEntry()));
                        break;
                    }
                    case PUT : {
                        insert(new PutOp(readTxnId(),
                                         readDbEntry(),
                                         readDbEntry()));
                        break;
                    }
                    case DELETE : {
                        insert(new DeleteOp(readTxnId(), readDbEntry()));
                        break;
                    }
                    case PREPARE : {
                        insert(new PrepareOp(readTxnId()));
                        break;
                    }
                    case COMMIT : {
                        resolve(readTxnId(), true);
                        break;
                    }
                    case ABORT: {
                        resolve(readTxnId(), false);
                        break;
                    }
                    case EOD : {
                        logger.log(Level.INFO,
                                   "Received EOD for {0}", partitionId);

                        /*
                         * It is possible that a txn was started (via PUT or
                         * DELETE) but not resolved (a COMMIT or ABORT was never
                         * received). At this point the local txns in the map
                         * are those which have not been resolved.
                         *
                         * If the transaction has been prepared then we don't
                         * know if the operation has completed on the source.
                         * In this case the only option is to abort
                         * the migration and start over.
                         */
                        for (LocalTxn txn : txnMap.values()) {
                            assert !txn.resolved;

                            if (txn.prepared) {
                                throw new Exception("Encountered prepared, " +
                                                    "but unresolved " + txn);
                            }
                        }
                        insert(new EoD());
                        return;
                    }
                }
            }
        }

        /**
         * Reads a transaction ID from the migration stream.
         */
        private long readTxnId() throws IOException {
            return stream.readLong();
        }

        /**
         * Reads a DB entry (as a byte array) from the migration stream.
         */
        private byte[] readDbEntry() throws IOException {
            int size = stream.readInt();
            byte[] bytes = new byte[size];
            stream.readFully(bytes);
            return bytes;
        }

        /**
         * Inserts an operation onto the queue. This method will block if the
         * queue is at capacity.
         */
        private void insert(Op op) {

            synchronized (opQueue) {
                if (!running) {
                    return;
                }
                opQueue.add(op);
                opQueue.notifyAll();

                while ((opQueue.size() > capacity) && running) {

                    try {
                        opQueue.wait(SECOND_MS);
                    } catch (InterruptedException ie) {
                        logger.log(Level.WARNING, "Unexpected interrupt", ie);
                    }
                }
            }
        }

        /**
         * Removes an operation from the queue. This method will block if the
         * queue is empty.
         */
        private Op remove() {

            synchronized (opQueue) {
                while (running) {
                    Op op = opQueue.poll();
                    if (op != null) {
                        opQueue.notifyAll();
                        return op;
                    }

                    try {
                        opQueue.wait(SECOND_MS);
                    } catch (InterruptedException ie) {
                        logger.log(Level.WARNING, "Unexpected interrupt", ie);
                    }
                }
                return null;
            }
        }

        /**
         * Resolves a prepare operation.
         */
        private void resolve(long txnId, boolean commit) {

            final LocalTxn txn = txnMap.remove(txnId);
            assert txn != null;

            /*
             * If the consumer had reached the prepare operation, it is waiting
             * on the transaction. After marking it resolved wake it up.
             */
            synchronized (txn) {
                txn.resolve(commit);
                txn.notifyAll();
            }

            /*
             * Resolve dosen't place an op on the queue, so we need to wake
             * the consumer thread just in case it is waiting there.
             */
            synchronized (opQueue) {
                opQueue.notifyAll();
            }
        }

        @Override
        public String toString() {
            return "Reader[" + operations + ", " + opQueue.size() + "]";
        }

        /**
         * Copy operation (record read from the DB).
         */
        private class CopyOp extends Op {
            final byte[] key;
            final byte[] value;

            CopyOp(byte[] key, byte[] value) {
                this.key = key;
                this.value = value;
            }

            @Override
            void execute() {
                keyEntry.setData(key);
                valueEntry.setData(value);
                partitionDb.put(null, keyEntry, valueEntry);
            }

            @Override
            public String toString() {
                return "CopyOp[" + key.length + ", " + value.length + "]";
            }
        }

        /**
         * An operation associated with source-side transactions. When the first
         * object created with the specified txn a new local transaction is
         * started. Subsequent creations with the same txn will be associated
         * with same local transaction.
         */
        private abstract class TxnOp extends Op {
            final LocalTxn txn;

            protected TxnOp(long txnId) {
                LocalTxn t = txnMap.get(txnId);

                if (t == null) {
                    t = new LocalTxn(txnId);
                    txnMap.put(txnId, t);
                }
                this.txn = t;
            }

            /**
             * Gets the local transaction for this operation. The first time
             * this is called a new local transaction will be created and
             * started.
             */
            protected Transaction getTransaction() {
                return txn.getTransaction();
            }
        }

        /**
         * Put operation (client write).
         */
        private class PutOp extends TxnOp {
            final byte[] key;
            final byte[] value;

            PutOp(long txnId, byte[] key, byte[] value) {
                super(txnId);
                this.key = key;
                this.value = value;
            }

            @Override
            void execute() {
                keyEntry.setData(key);
                valueEntry.setData(value);
                partitionDb.put(getTransaction(), keyEntry, valueEntry);
            }

            @Override
            public String toString() {
                return "PutOp[" + txn.txnId + ", " + key.length + ", " +
                       value.length + "]";
            }
        }

        /**
         * Delete operation (client delete).
         */
        private class DeleteOp extends TxnOp {
            final byte[] key;

            DeleteOp(long txnId, byte[] key) {
                super(txnId);
                this.key = key;
            }

            @Override
            void execute() {
                keyEntry.setData(key);
                partitionDb.delete(getTransaction(), keyEntry);
            }

            @Override
            public String toString() {
                return "DeleteOp[" + txn.txnId + ", " + key.length + "]";
            }
        }

        /**
         * Prepare op. This operation indicates that a source-based transaction
         * is about to be committed. When the consumer reaches a PrepareOp it
         * must wait for it to be resolved before consuming any other
         * operations.
         *
         * While the consumer is waiting for the resolution the operation queue
         * becomes unbounded so that the read thread can continue and read the
         * resolution.
         */
        private class PrepareOp extends TxnOp {

            PrepareOp(long txnId) {
                super(txnId);
                txn.prepared = true;
            }

            @Override
            void execute() {

                /*
                 * Check if no transaction was started for this ID. This could
                 * happen due to key filtering at the source. In this case just
                 * exit.
                 */
                if (txn.transaction == null) {
                    logger.log(Level.FINE,
                               "Prepare with no txn for {0}, {1}",
                               new Object[]{txn, partitionId});
                    return;
                }

                synchronized (txn) {
                    if (!txn.resolved) {
                        synchronized (opQueue) {

                            /*
                             * Make the queue unbounded so we can find the
                             * resolution message.
                             */
                            capacity = Integer.MAX_VALUE;
                            opQueue.notifyAll();
                        }
                    }
                    while (!txn.resolved && running) {
                        logger.log(Level.FINE,
                                   "Waiting for resolution of {0}, {1} {2} ops",
                                   new Object[]{txn, partitionId, operations});
                        try {
                            txn.wait(SECOND_MS);
                        } catch (InterruptedException ie) {
                            logger.log(Level.WARNING,
                                       "Unexpected interrupt", ie);
                        }
                    }
                }
                capacity = DEFAULT_CAPACITY;
                txn.finish();
            }

            @Override
            public String toString() {
                return "PrepareOp[" + txn + "]";
            }
        }

        /**
         * End of data marker.
         */
        private class EoD extends Op {

            @Override
            void execute() {
                setDone();
            }

            @Override
            public String toString() {
                return "EoD[]";
            }
        }

        /**
         * Encapsulates a local transaction which is associated with a
         * transaction id.
         */
        private class LocalTxn {
            private final long txnId;
            private Transaction transaction = null;
            private boolean prepared = false;
            private boolean resolved = false;
            private boolean committed = false;

            LocalTxn(long txnId) {
                this.txnId = txnId;
            }

            /**
             * Gets the local transaction for this id. The first time this
             * is called a new local transaction will be created and started.
             */
            Transaction getTransaction() {
                if (transaction == null) {
                    transaction = repEnv.beginTransaction(null, weakConfig);
                }
                return transaction;
            }

            /**
             * Marks the transaction as resolved.
             */
            void resolve(boolean commit) {
                assert prepared;
                assert !resolved;
                resolved = true;
                committed = commit;
            }

            /**
             * Completes the transaction.
             */
            void finish() {
                assert resolved;
                assert transaction != null;
                if (committed) {
                    transaction.commit();
                } else {
                    TxnUtil.abort(transaction);
                }
            }

            @Override
            public String toString() {
                return "LocalTxn[" + txnId + ", " + transaction +
                       ", prepared=" + prepared + ", resolved=" + resolved +
                       ", committed=" + committed +"]";
            }
        }
    }

    private class ReaderFactory extends KVThreadFactory {

        ReaderFactory() {
            super(" migration stream reader for ", logger);
        }

        private Reader newReader(DataInputStream stream) {
            final Reader reader = new Reader(stream);
            newThread(reader).start();
            return reader;
        }
    }
}
