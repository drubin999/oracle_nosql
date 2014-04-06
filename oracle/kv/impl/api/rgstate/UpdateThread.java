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

package oracle.kv.impl.api.rgstate;

import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.FaultException;
import oracle.kv.impl.api.RequestDispatcher;
import oracle.kv.impl.api.RequestHandlerAPI;
import oracle.kv.impl.rep.admin.IllegalRepNodeServiceStateException;
import oracle.kv.impl.rep.admin.RepNodeAdminFaultException;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.ResourceId.ResourceType;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.RateLimitingLogger;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * Base class for update threads.
 *
 * TODO: pass stats out to Monitor when in an RN. Log when in a client? TODO:
 * Limit the rate of log messages on error?
 */
public abstract class UpdateThread extends Thread {

    /**
     * The max number of RNs to track for the purposes of log rate limiting.
     */
    private static final int LIMIT_RNS = 100;

    /**
     * 1 min in millis
     */
    private static final int ONE_MINUTE_MS = 60 * 1000;
    
    /**
     * The dispatcher associated with this updater thread.
     */
    protected final RequestDispatcher requestDispatcher;

    /**
     * The thread used to resolve request handles in parallel.
     */
    protected final ThreadPoolExecutor threadPool;

    /**
     * Shutdown can only be executed once. The shutdown field protects against
     * multiple invocations.
     */
    protected final AtomicBoolean shutdown = new AtomicBoolean(false);

    /**
     * The minimum number of threads used by the pool. This minimum number
     * permits progress in the presence of a few bad network connections.
     */
    private static final int MIN_POOL_SIZE = 6;

    /**
     *  The time interval between executions of a concurrent update pass.
     */
    private final int periodMs;

    /**
     *  The amount of time to wait when resolving a handle.
     */
    private final int resolutionTimeoutMs = 10000;

    /**
     * Statistics
     */
    private volatile int poolRejectCount;

    private volatile int resolveCount;
    private int resolveFailCount;
    private int resolveExceptionCount;

    protected final Logger logger;

    /*
     * Encapsulates the above logger to limit the rate of log messages
     * associated with a specific RN.
     */
    private final RateLimitingLogger<RepNodeId> rateLimitingLogger;
    
    /**
     * Creates the RN state update thread.
     *
     * @param requestDispatcher the request dispatcher associated with this
     * thread.
     *
     * @param periodMs the time period between passes over the RNs to see if
     * they need updating.
     *
     * @param logger the logger used by the update threads.
     */
    protected UpdateThread(RequestDispatcher requestDispatcher,
                           int periodMs,
                           UncaughtExceptionHandler handler,
                           final Logger logger) {
        super();
        this.requestDispatcher = requestDispatcher;
        this.periodMs = periodMs;
        this.logger = logger;
        this.rateLimitingLogger = new RateLimitingLogger<RepNodeId>
            (ONE_MINUTE_MS, LIMIT_RNS, logger);
        final String name = "KV_" + requestDispatcher.getDispatcherId() +
                            "_" + getClass().getSimpleName();
        this.setName(name);
        this.setDaemon(true);
        this.setUncaughtExceptionHandler(handler);
        
        /* Reclaim threads if they have not been used over 5 time periods. */
        final int keepAliveTimeMs =  periodMs * 5;
        threadPool =
            new ThreadPoolExecutor
              (0, /* core size */
               MIN_POOL_SIZE, /* max size can be adjusted dynamically in loop */
               keepAliveTimeMs, TimeUnit.MILLISECONDS,
               new LinkedBlockingQueue<Runnable>(10),
               new UpdateThreadFactory(logger, name, handler),
               new CountDiscardsPolicy());
    }

    public int getResolveCount() {
        return resolveCount;
    }

    public int getResolveFailCount() {
        return resolveFailCount;
    }

    public int getResolveExceptionCount() {
        return resolveExceptionCount;
    }

    @Override
    public void run() {

        logger.log(Level.INFO, "{0} started", this);
        try {
            while (!shutdown.get()) {
                doUpdate();
                
                if (shutdown.get()) {
                    return;
                }
                try {
                    Thread.sleep(periodMs);
                } catch (InterruptedException e) {
                    logger.log(Level.WARNING, "{0} interrupted", this);
                    throw new IllegalStateException(e);
                }
            }
        } catch (Throwable t) {
            requestDispatcher.shutdown(t);
        } finally {
            shutdown();
        }
    }

    /**
     * Called periodically to perform update.
     */
    protected abstract void doUpdate();

    /**
     * Returns the list of RNs whose state must be maintained. If the
     * request dispatcher belongs to the client the state associated with all
     * RNs is maintained. If it's associated with an RN only state associated
     * with its RG is maintained. This behavior will need to be updated with
     * support for partition migration, or if support light weight clients
     * which don't themselves maintain RN state.
     *
     * @return the list of RNs whose state is to be maintained.
     */
    protected Collection<RepNodeState> getRNs() {
        
        final ResourceId dispatcherId = requestDispatcher.getDispatcherId();
        
        if (dispatcherId.getType() == ResourceType.CLIENT) {
            final RepGroupStateTable rgst =
                                    requestDispatcher.getRepGroupStateTable();
            return rgst.getRepNodeStates();
            
        } else if (dispatcherId.getType() == ResourceType.REP_NODE) {
            final RepNodeId rnId = (RepNodeId)dispatcherId;
            return getRNs(new RepGroupId(rnId.getGroupId()));
            
        } else {
            throw new IllegalStateException
                ("Unexpected dispatcher: " + dispatcherId);
        }
    }
    
    /**
     * Returns the list of RNs in the specified group.
     * 
     * @param groupId a group Id
     * @return the list of RNs in the group
     */
    protected Collection<RepNodeState> getRNs(RepGroupId groupId) {
        final RepGroupStateTable rgst =
                                    requestDispatcher.getRepGroupStateTable();
        final RepGroupState rgs = rgst.getGroupState(groupId);
        return rgs.getRepNodeStates();
    }

    /**
     * Returns true if the handle of the specified state needs resolution.
     * If true a thread is started to resolve asynchronously, so that a single
     * network problem does not tie up resolution of all handles.
     * 
     * @param rnState target node
     * @return true if the handle needs resolution
     */
    protected boolean needsResolution(RepNodeState rnState) {
        if (!rnState.reqHandlerNeedsResolution()) {
            return false;
        }
        threadPool.execute(new ResolveHandler(rnState));
        return true;
    }
    
    public void shutdown() {
        if (!shutdown.compareAndSet(false, true)) {
            /* Already shutdown. */
            return;
        }
        logger.log(Level.INFO, "{0} shutdown", this);
        threadPool.shutdownNow();
    }
    
    /**
     * Action to be taken when an "execute" request is rejected by the
     * thread pool.
     */
    private class CountDiscardsPolicy extends ThreadPoolExecutor.DiscardPolicy {
        @Override
        public void rejectedExecution(Runnable r,
                                      ThreadPoolExecutor e) {
            final int count = poolRejectCount++;

            final Level level = ((count % 100) == 0) ? Level.INFO : Level.FINE;

            logger.log(level,
                       "RN state update thread pool rejected {0} requests. " +
                       "Pool size: {1} Max pool size: {2}",
                       new Object[]{count,
                                    threadPool.getPoolSize(),
                                    threadPool.getMaximumPoolSize()});
        }
    }

    /**
     * The task that's actually executed by the threads in the thread pool to
     * resolve handles.
     */
    private class ResolveHandler implements Runnable {
        final RepNodeState rns;

        ResolveHandler(RepNodeState rns) {
            super();
            this.rns = rns;
        }

        @Override
        public void run() {
            try {
                final RegistryUtils regUtils = requestDispatcher.getRegUtils();
                if (regUtils == null) {

                    /*
                     * The request dispatcher has not initialized itself as
                     * yet. Retry later.
                     */
                    return;
                }
                RequestHandlerAPI ref =
                        rns.resolveReqHandlerRef(regUtils, resolutionTimeoutMs);
                if (ref == null) {
                    resolveFailCount++;
                } else {
                    resolveCount++;
                }
            } catch (Exception e) {
                logger.log(Level.WARNING,
                           "Exception in ResolveHandlerThread " +
                           "when contacting:" + rns.getRepNodeId(), e);
                resolveExceptionCount++;
            }
        }
    }

    /**
     * Produces a brief message (whenever possible) describing the exception.
     * In particular, it avoids use of FaultException.toString which always
     * logs the complete stack trace.
     *
     * @param level the logging level to be used
     * @param msgPrefix the message prefix text
     * @param exception the exception
     */
    protected void logBrief(RepNodeId rnId,
                            Level level,
                            String msgPrefix,
                            Exception exception) {

        if (exception instanceof FaultException) {
            final FaultException fe = (FaultException) exception;
            final String fcName = fe.getFaultClassName();
            if ((fcName != null) && (fe.getMessage() != null)) {
                /* Avoid use of FaultException.toString and stack trace */
                rateLimitingLogger.log(rnId, level,
                                       msgPrefix +
                                       " Fault class:" + fcName +
                                       " Problem:" + fe.getMessage());
                return;
            }
        }

        /* Default to use of toString */
        logger.log(level, "{0} Problem:{1}",
                   new Object[]{msgPrefix, exception.toString()});
    }
    
    /**
     * Logs a suitable message on failure.
     */
    protected void logOnFailure(RepNodeId rnId,
                                Exception exception,
                                final String changesMsg) {
        if (exception == null) {
            logger.info("Error. "  + changesMsg);
            return;
        }

        final String irnsseName =
            IllegalRepNodeServiceStateException.class.getName();

        if ((exception instanceof RepNodeAdminFaultException) &&
            ((RepNodeAdminFaultException) exception).
            getFaultClassName().equals(irnsseName)) {
            /* log an abbreviated message. */
            rateLimitingLogger.log(rnId, Level.INFO,
                                   changesMsg + "Exception message:" +
                                   exception.getMessage());
        } else {
            logBrief(rnId, Level.INFO, changesMsg, exception);
        }
    }

    /**
     * Name the update thread pool threads, and pass any uncaught exceptions
     * to the logger.
     */
    private class UpdateThreadFactory extends KVThreadFactory {
        private final String name;
        private final UncaughtExceptionHandler handler;

        UpdateThreadFactory(Logger logger,
                            String name,
                            UncaughtExceptionHandler handler) {
            super(null, logger);
            this.name = name  + "_Updater";
            this.handler = handler;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public UncaughtExceptionHandler makeUncaughtExceptionHandler() {
            return handler;
        }
    }
}
