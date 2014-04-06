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

import java.io.IOException;
import java.nio.channels.Channel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.rep.IncorrectRoutingException;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.RepNodeService.Params;
import oracle.kv.impl.rep.migration.PartitionMigrations.SourceRecord;
import oracle.kv.impl.rep.migration.TransferProtocol.TransferRequest;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.utilint.RepUtils;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.Response;

/**
 * Migration service. This object is registered with the JE service framework
 * and handles requests to migrate partitions from target nodes. When a
 * service request is received the details of the request are read from the
 * newly established channel and if valid, a MigrationSource thread is
 * started to handle the actual data movement.
 *
 * The initial request is the only message sent from the target to the source
 * node. After that all communication is from the source to the target.
 */
class MigrationService implements Runnable {

    private final Logger logger;

    /* Name used to register with the JE service framework */
    static final String SERVICE_NAME = "PartitionMigration";

    /* Wait indefinitely for somebody to request the service. */
    private static long POLL_TIMEOUT = Long.MAX_VALUE;

    private final RepNode repNode;

    private final Params params;

    /* The maximum number of target streams which can run concurrently. */
    private final int concurrentSourceLimit;

    final MigrationManager manager;

    private ThreadFactory sourceThreadFactory = null;

    /*
     * Queue for the JE service framework. Channels for incoming requests are
     * placed on this queue by the framework, and are pulled in this thread's
     * run method.
     */
    private final BlockingQueue<DataChannel> queue =
                                new LinkedBlockingQueue<DataChannel>();

    /* Maps the partition Id with the migration source */
    private final Map<PartitionId, MigrationSource> sourceMap =
                                new HashMap<PartitionId, MigrationSource>();

    /* True if the service is accepting migration requests. */
    private volatile boolean enabled = false;

    /* Count of errors processing requests */
    private int requestErrors = 0;

    /* For unit tests */
    TestHook<DatabaseEntry> readHook;
    private TestHook<AtomicReference<Response>> responseHook;

    MigrationService(RepNode repNode, MigrationManager manager, Params params) {
        this.repNode = repNode;
        this.manager = manager;
        this.params = params;
        concurrentSourceLimit =
                        params.getRepNodeParams().getConcurrentSourceLimit();
        logger = LoggerUtils.getLogger(this.getClass(), params);
    }

    synchronized void getStatus(HashSet<PartitionMigrationStatus> status) {
        for (MigrationSource source : sourceMap.values()) {
            status.add(source.getStatus());
        }
    }

    synchronized PartitionMigrationStatus getStatus(PartitionId partitionId) {
        final MigrationSource source = sourceMap.get(partitionId);
        return (source == null) ? null : source.getStatus();
    }

    /**
     * Starts the service by registering with the JE service framework.
     */
    synchronized void start(ReplicatedEnvironment repEnv) {
        if (enabled) {
            throw new IllegalStateException("Service already started");
        }
        assert repEnv != null;

        final RepImpl repImpl = RepInternal.getRepImpl(repEnv);
        if (repImpl == null) {

            /*
             * Env was closed. A subsequent state transition when the
             * env is reopened, will register the dispatcher if necessary.
             */
            return;
        }
        final ServiceDispatcher dispatcher =
            repImpl.getRepNode().getServiceDispatcher();

        if (dispatcher.isRegistered(SERVICE_NAME)) {
            throw new IllegalStateException("Service already registered");
        }

        enabled = true;
        final Thread t = new KVThreadFactory(" migration service", logger).
                                                        newThread(this);
        dispatcher.register(dispatcher.new LazyQueuingService(SERVICE_NAME,
                                                              queue, t));
        logger.info("Migration service accepting requests.");
    }

    /**
     * Stops the service.
     *
     * @param shutdown true if the node is shutting down
     */
    synchronized void stop(boolean shutdown, boolean wait,
                           ReplicatedEnvironment repEnv) {
        assert repEnv != null;

        if (!enabled) {
            return;
        }
        enabled = false;

        /**
         * Since the rep node may be in an incomplete state during shutdown
         * do not attempt to cancel registration with the service dispatcher.
         */
        if (!shutdown) {
            final RepImpl repImpl = RepInternal.getRepImpl(repEnv);

            if (repImpl != null) {
                final ServiceDispatcher dispatcher =
                                    repImpl.getRepNode().getServiceDispatcher();

                if (dispatcher.isRegistered(SERVICE_NAME)) {
                    logger.log(Level.INFO, "Stopping {0}", this);

                    /* This will interrupt the service thread if needed */
                    dispatcher.cancel(SERVICE_NAME);
                }
            }
        }

        for (MigrationSource source : sourceMap.values()) {
            source.cancel(wait);
        }
        sourceMap.clear();
    }

    /**
     * Shuts down a source and waits for it to stop. This is used
     * when the admin needs to cleanup after a cancel or failure.
     *
     * @param partitionId
     * @param targetRGId
     */
    synchronized void cancel(PartitionId partitionId, RepGroupId targetRGId) {
        final MigrationSource source = sourceMap.get(partitionId);

        if ((source != null) &&
            (source.getTargetGroupId() == targetRGId.getGroupId())) {
            source.cancel(true);
            removeSource(partitionId);
        }
    }

    /**
     * Returns the migration source for the specified partition. If there
     * isn't a migration going on for that partition, null is returned.
     *
     * @param partitionId a partition ID
     * @return a migration source or null
     */
    synchronized MigrationSource getSource(PartitionId partitionId) {
        return sourceMap.get(partitionId);
    }

    synchronized void removeSource(PartitionId partitionId) {
        sourceMap.remove(partitionId);
    }

    @Override
    public void run() {

        /* This thread is run the first time a service request comes in for the
         * migration service. Once started it will remain running until the
         * service is unregistered or canceled. If unregistered the thread will
         * be interrupted.
         */
        logger.log(Level.INFO, "Migration service thread started.");

        try {
            while (enabled) {
                DataChannel channel = null;
                try {
                    channel = queue.poll(POLL_TIMEOUT, TimeUnit.MILLISECONDS);

                    if (channel == RepUtils.CHANNEL_EOF_MARKER) {
                        logger.info("EOF marker - shutdown");
                        return;
                    }

                    if (channel != null) {
                        processRequest(channel);
                    }
                } catch (IOException ioe) {
                    closeChannel(channel);
                    logger.log(Level.INFO,
                               "IOException processing migration request: ",
                               ioe);
                } catch (InterruptedException ie) {
                    logger.info("Migration service interrupted");
                    return;
                }
            }
        } finally {
            logger.info("Migration service thread exit");
        }
    }

    /**
     * Closes the specified channel, logging any resulting exceptions.
     *
     * @param channel a channel
     */
    private void closeChannel(Channel channel) {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException ioe) {
                logger.log(Level.WARNING, "Exception during cleanup", ioe);
            }
        }
    }

    /**
     * Processes the initial service request. The migration details are read
     * from the channel and if valid a migration source thread is created
     * and started.
     *
     * @param channel a channel
     * @throws IOException resulting from operations on the channel
     */
    private void processRequest(DataChannel channel) throws IOException {

        TransferRequest request = TransferRequest.read(channel);

        AtomicReference<Response> hookedResponse =
                                        new AtomicReference<Response>();
        assert TestHookExecute.doHookIfSet(responseHook, hookedResponse);

        if ((responseHook != null) && (hookedResponse.get() != null)) {
            final Response response = hookedResponse.get();
            if (response.equals(Response.BUSY)) {
                reportBusy(concurrentSourceLimit, "Test busy", channel);
            } else {
                reportError(response, "Test error: " + response, channel);
            }
            return;
        }
        final PartitionId partitionId = new PartitionId(request.partitionId);
        final int targetGroupId = request.targetRNId.getGroupId();

        /* Check to make sure the requested partition is here */
        try {
            repNode.getPartitionDB(partitionId);
        } catch (IncorrectRoutingException ire) {

            /*
             * If the request is for an unknown partition, it may be due to
             * completed transfer, in which case the source should reset
             * and let the target try again later.
             */
            if (checkForRestart(partitionId, targetGroupId)) {
                reportBusy(concurrentSourceLimit,
                           "Migration source resetting " + partitionId,
                           channel);
                return;
            }
            reportError(Response.UNKNOWN_SERVICE,
                        "Request for unknown: " + ire.getLocalizedMessage(),
                        channel);
            return;
        }

        /*
         * If the source doesn't know about the target rep group, report back
         * BUSY. Eventually the topology will be updated and we can proceed.
         * The check is necessary because once the migration is completed the
         * source needs to know about the target group in order to forward
         * requests there.
         */
        if (repNode.getTopology().get(new RepGroupId(targetGroupId)) == null) {
            reportBusy(0,
                       "Migration source needs updated topology, target " +
                       "group " + targetGroupId + " unknown", channel);
            return;
        }

        DatabaseEntry lastKey = request.lastKey;

        logger.log(Level.FINE,
                   "Received migration request for {0} to {1}, lastKey= {2}",
                   new Object[]{partitionId, targetGroupId, lastKey});

        synchronized (this) {

            if (!enabled) {
                /* Report 0 streams until enabled */
                reportBusy(0,
                           "Migration source not enabled for " + partitionId,
                           channel);
                return;
            }

            int running = 0;

            Iterator<MigrationSource> itr = sourceMap.values().iterator();

            while (itr.hasNext()) {
                if (itr.next().isAlive()) {
                    running++;
                } else {
                    itr.remove();
                }
            }

            /*
             * Limit the number of concurrent streams. The target will retry
             * until they can get in.
             */
            if (running >= concurrentSourceLimit) {
                reportBusy(concurrentSourceLimit,
                           "Migration source busy. Number of streams= " +
                           sourceMap.size() +
                           ", max= " + concurrentSourceLimit, channel);
                return;
            }

            MigrationSource source = sourceMap.get(partitionId);

            /*
             * If there is a source already running, try to cancel it and ask
             * the target to try again later.
             */
            if (source != null) {
                source.cancel(false);
                reportBusy(concurrentSourceLimit,
                           "Source for " + partitionId + " already running: " +
                           source.toString(), channel);
                return;
            }
            //TODO - add lastKey
            source = new MigrationSource(channel, partitionId,
                                         request.targetRNId,
                                         repNode, this, params);
            sourceMap.put(partitionId, source);

            try {
                TransferRequest.writeACKResponse(channel);
            } catch (IOException ioe) {
                sourceMap.remove(partitionId);
                closeChannel(channel);
                throw ioe;
            }

            if (sourceThreadFactory == null) {
                sourceThreadFactory =
                     new KVThreadFactory(" partition migration source", logger);
            }
            logger.log(Level.INFO, "Starting {0}", source);

            /* Start streaming K/Vs to target */
            sourceThreadFactory.newThread(source).start();
        }
    }

    /*
     * Checks to see if there is a source migration record for the specified
     * partition and target. If so, the record is removed and true is returned.
     */
    private boolean checkForRestart(PartitionId partitionId,
                                    int targetGroupId) {
        final PartitionMigrations migrations = manager.getMigrations();

        if (migrations == null) {
            return false;
        }

        final SourceRecord record = migrations.getSource(partitionId);

        /* If no record, or the target does not match, then return no match */
        if ((record == null) ||
           !record.getTargetRGId().equals(new RepGroupId(targetGroupId))) {
            return false;
        }
        try {
            logger.log(Level.INFO,
                       "Migration source detected restart of {0}, "+
                       "removing completed record",
                       record);
            manager.removeRecord(record, true);
        } catch (DatabaseException de) {
            logger.log(Level.WARNING, "Exception removing " + record, de);
        }
        return true;
    }

    /*
     * Reports a busy condition to the client. The message is also logged
     * at FINE, and the channel is closed.
     */
    private void reportBusy(int numStreams,
                            String message,
                            DataChannel channel) {
        requestErrors++;
        logger.log(Level.FINE, message);
        try {
            TransferRequest.writeBusyResponse(channel, numStreams, message);
        } catch (IOException ioe) {
            logger.log(Level.WARNING, "Exception sending busy response", ioe);
        }
        closeChannel(channel);
    }

    /*
     * Reports and error condition to the client. The message is also logged
     * at INFO, and the channel is closed.
     */
    private void reportError(Response response,
                             String message,
                             DataChannel channel) {
        assert response.equals(Response.FORMAT_ERROR) ||
               response.equals(Response.UNKNOWN_SERVICE);

        requestErrors++;
        logger.log(Level.INFO, message);
        try {
            TransferRequest.writeErrorResponse(channel, response, message);
        } catch (IOException ioe) {
            logger.log(Level.WARNING, "Exception sending error response", ioe);
        }
        closeChannel(channel);
    }

    /* -- Unit test -- */

    void setReadHook(TestHook<DatabaseEntry> hook) {
        readHook = hook;
    }

    void setResponseHook(TestHook<AtomicReference<Response>> hook) {
        responseHook = hook;
    }

    @Override
    public String toString() {
        return "MigrationService[" + enabled + ", " + sourceMap.size() +
               ", " + requestErrors + "]";
    }
}
