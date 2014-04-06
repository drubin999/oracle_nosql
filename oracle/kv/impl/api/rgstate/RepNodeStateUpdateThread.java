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

import java.rmi.RemoteException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.RequestTimeoutException;
import oracle.kv.impl.api.RequestDispatcher;
import oracle.kv.impl.api.TopologyInfo;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.topo.change.TopologyChange;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * This thread is associated with a RequestDispatcher on a KV client or a RN.
 * Its primary purpose is to keep the RepNodeState as current as possible, so
 * that the request dispatcher has current information that's used as the basis
 * for making dispatch decisions.
 * <p>
 * The thread also provides the mechanism to take "high latency" network
 * operations involving the setup or repair of network connections and move
 * them out of the request's thread of control. Whenever the RequestDispatcher
 * encounters a problem with a RN it will typically move on to the next
 * available RN that can satisfy the request, leaving it to this thread to fix
 * any problems asynchronously outside of the request's thread of control.
 * <p>
 * The behavior of this thread is slightly different depending upon whether
 * it's part of a client or RN request dispatcher. Since a client can dispatch
 * to any RN in a KVS, the thread scans all connections to an RN on a periodic
 * basis and will resolve any connections that are in error. An RN on the other
 * hand will only dispatch to other members of its rep group in R1. As a
 * result, it only needs to maintain RN state for RNs in its rep group.
 *
 * TODO: pass stats out to Monitor when in an RN. Log when in a client? TODO:
 * Limit the rate of log messages on error?
 */
public class RepNodeStateUpdateThread extends UpdateThread {

    /**
     * The time between changes to the RN that will be used to pull topology,
     * if the topo seq number itself is unchanged. It strikes a balance
     * between switching RNs too frequently and having a reasonably current
     * RN so that a pull is likely to succeed.
     */
    private static final int PULL_TOPO_RN_RENEW_MS = 10000;

    /**
     * The minimum number of threads used by the pool. This minimum number
     * permits progress in the presence of a few bad network connections.
     */
    private static final int MIN_POOL_SIZE = 6;

    /**
     *  The amount of time to wait for a nop request.
     */
    private final int nopTimeoutMs = 1000;

    /**
     * Statistics
     */

    private volatile int refreshCount;
    private volatile int refreshExceptionCount;

    /* Push/pull full topo counts. */
    private volatile int pullFullTopologyCount;

    private volatile int pushFullTopologyCount;

    /**
     * Testing flag to control whether state should be updated.
     */
    private static boolean updateState = true;

    /* Updated to indicate a full topo pull may be needed. */
    private final AtomicReference<FullTopoInfo> pullFullTopoInfo;

    /* Used to ensure that pull full topology requests. */
    private final Semaphore fullPullInProgress = new Semaphore(1);

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
    public RepNodeStateUpdateThread(RequestDispatcher requestDispatcher,
                                    int periodMs,
                                    UncaughtExceptionHandler handler,
                                    final Logger logger) {
        super(requestDispatcher, periodMs, handler, logger);
        this.pullFullTopoInfo = new AtomicReference<FullTopoInfo>
            (new FullTopoInfo(null, Integer.MIN_VALUE));
    }

    public int getRefreshCount() {
        return refreshCount;
    }

    public int getRefreshExceptionCount() {
        return refreshExceptionCount;
    }

    /**
     * For unit test use only.
     */
    static public void setUpdateState(boolean updateState) {
        RepNodeStateUpdateThread.updateState = updateState;
    }

    public int getPullFullTopologyCount() {
        return pullFullTopologyCount;
    }

    public int getPushFullTopologyCount() {
        return pushFullTopologyCount;
    }

    /**
     * Queues a request to pull a full topology, because incremental changes
     * were not available for transmission in a response.
     *
     * @param rnId the rep node that has this full topology but cannot
     * supply changes
     *
     * @param topoSeqNum the top seq number at that RN
     */
    public void pullFullTopology(RepNodeId rnId, int topoSeqNum) {
        final FullTopoInfo update = new FullTopoInfo(rnId, topoSeqNum);

        while (true) {
            final FullTopoInfo curr = pullFullTopoInfo.get();

            if (update.topoSeqNum < curr.topoSeqNum) {
                return;
            }

            if ((update.topoSeqNum == curr.topoSeqNum) &&
                (((update.timeMs - curr.timeMs) < PULL_TOPO_RN_RENEW_MS) ||
                 update.rnId.equals(curr.rnId))) {
                /* RN is same or recent enough. */
                return;
            }

            if (pullFullTopoInfo.compareAndSet(curr, update)) {
                logger.info("Current topo#: " + curr.topoSeqNum +
                            " Need to pull full topo#: " + update.topoSeqNum +
                            " Full topo source RN: " + rnId);
                return;
            }
        }
    }

    @Override
    protected void doUpdate() {

        final Topology topology =
            requestDispatcher.getTopologyManager().getTopology();

        final Collection<RepNodeState> rns = getRNs();

        threadPool.setMaximumPoolSize(Math.max(MIN_POOL_SIZE,
                                               rns.size()/10));
        for (RepNodeState rnState : rns) {
            if (shutdown.get()) {
                return;
            }
            if (needsResolution(rnState)) {
                /* The handle will be updated async, so skip for now */
            } else if (updateState) {
                if (rnState.isObsoleteVLSNState()) {

                    /*
                     * The handle is resolved, but the RN state needs to be
                     * refreshed. Send a NOP request.
                     */
                    threadPool.execute(new RefreshRepNodeState(rnState));
                }

                final int rnTopoSeqNum = rnState.getTopoSeqNum();
                if ((topology != null) &&
                    (rnTopoSeqNum >= 0) &&
                    (rnTopoSeqNum < topology.getSequenceNumber())) {
                    threadPool.execute(new PushTopology(rnState,
                                                        topology));
                }
            }
        }

        if (updateState && (topology != null) &&
            (pullFullTopoInfo.get().topoSeqNum >
             topology.getSequenceNumber())) {
            /* Need to pull over a full topology. */
            threadPool.execute(new PullTopology(pullFullTopoInfo.get()));
        }
    }

    /**
     * Task used to refresh the VLSN and Topology state associated with an RN.
     * This state is typically refreshed as a result of normal request
     * processing, but it may become stale if the KVS is idle, or if the
     * dispatcher lives in an RN and there has not been any request forwarding
     * activity in a while.
     */
    private class RefreshRepNodeState implements Runnable {
        final RepNodeState rns;

        RefreshRepNodeState (RepNodeState rns) {
            super();
            this.rns = rns;
        }

        @Override
        public void run() {
            Exception exception = null;
            Level level = Level.SEVERE;

            try {
                /* The rn state is updated as a result of the response. */
                requestDispatcher.executeNOP(rns, nopTimeoutMs,
                                             (LoginManager) null);
                refreshCount++;
            } catch (java.rmi.ConnectException e) {

                /*
                 * Routine, the node is down or unreachable and cannot be
                 * contacted.
                 */
                exception = e;
                level = Level.FINE;
            } catch (RemoteException e) {
                exception = e;
                level = Level.INFO;
            } catch (RequestTimeoutException e) {
                exception = e;
                level = Level.INFO;
            } catch (Exception e) {
                exception = e;
                level = Level.WARNING;
            } finally {
                if (exception != null) {
                    refreshExceptionCount++;

                    final String msg =
                        "Exception in RefreshRepNodeStateThread " +
                        "when contacting:" + rns.getRepNodeId() +
                        " Exception " + exception.getClass().getName();

                    if (level.intValue() >= Level.WARNING.intValue()) {
                        logger.log(Level.WARNING, msg, exception);
                    } else {
                        logBrief(rns.getRepNodeId(), level, msg, exception);
                    }
                }
            }
        }
    }

    /**
     * Pull a full topology from a RN. This more expensive form of Topology
     * transfer is only done when the incremental changes are not available.
     * This situation typically arises because an RN has been down for a long
     * time and needs to catch up with the topology changes, but the other
     * nodes in the store have moved on and pruned their list of incremental
     * changes.
     */
    private class PullTopology implements Runnable {

        final FullTopoInfo fullTopoInfo;

        public PullTopology(FullTopoInfo fullTopoInfo) {
            this.fullTopoInfo = fullTopoInfo;
        }

        @Override
        public void run() {
            boolean success = false;
            Exception exception = null;
            if (!fullPullInProgress.tryAcquire()) {
                /* A pull is already in progress. */
                return;
            }

            try {
                final RegistryUtils regUtils = requestDispatcher.getRegUtils();
                if (regUtils == null) {

                    /*
                     * The request dispatcher has not initialized itself as
                     * yet. Retry later.
                     */
                    return;
                }
                final RepNodeAdminAPI rnAdmin =
                    regUtils.getRepNodeAdmin(fullTopoInfo.rnId);
                final Topology newTopo = rnAdmin.getTopology();
                requestDispatcher.getTopologyManager().update(newTopo);
                pullFullTopologyCount++;
                success = true;
            } catch (Exception e) {
                exception = e;
            } finally {
                fullPullInProgress.release();
                if (success) {
                    logger.info("Pulled full topology." +
                    		" Topology updated to: " +
                    		requestDispatcher.getTopologyManager().
                    		getTopology().getSequenceNumber());
                } else {
                    logOnFailure(fullTopoInfo.rnId,
                                 exception,
                                 " full topo pull from: + rnId + " +
                                 " for topo seqNum:" +
                                 fullTopoInfo.topoSeqNum);
                }
            }
        }
    }

    /**
     * Task used to push topology changes to a RN. Incremental changes are
     * sent if available, otherwise, the entire topology is pushed.
     */
    private class PushTopology implements Runnable {

        /* The RN to which the topology changes are to be pushed. */
        private final RepNodeState rnState;

        /* The topology to be pushed. */
        private final Topology topology;

        /* The original topo sequence number. */
        private int rnTopoSeqNum;

        PushTopology(RepNodeState rns,
                     Topology topology) {
            super();

            this.rnState = rns;
            rnTopoSeqNum = rnState.getTopoSeqNum();

            /*
             * Optimistically update the sequence number, so no new updates are
             * queued while the request is in progress. It will be reverted in
             * the run method below if the update fails.
             */
            rnState.updateTopoSeqNum(topology.getSequenceNumber());
            this.topology = topology;
        }

        @Override
        public void run() {

            String changesMsg = "Unknown failure";
            int startSeqNum = -1;
            int endSeqNum = -1;
            boolean success = false;
            Exception exception = null;

            try {

                final RegistryUtils regUtils = requestDispatcher.getRegUtils();
                if (regUtils == null) {
                    /*
                     * The request dispatcher has not initialized itself as
                     * yet. Retry later.
                     */
                    return;
                }

                final List<TopologyChange> changes =
                    topology.getChanges(rnTopoSeqNum + 1);

                final RepNodeId targetRNId = rnState.getRepNodeId();
                final RepNodeAdminAPI rnAdmin =
                    regUtils.getRepNodeAdmin(targetRNId);

                if (changes == null) {
                    changesMsg =
                        "entire topology push to " + targetRNId +
                        " updating from topo seq#: " +
                        rnTopoSeqNum  + " to " + topology.getSequenceNumber();
                    rnAdmin.updateMetadata(topology);
                    pushFullTopologyCount++;
                    success = true;
                } else {
                    startSeqNum = changes.get(0).getSequenceNumber();
                    endSeqNum =
                        changes.get(changes.size() - 1).getSequenceNumber();
                    changesMsg = "topology changes [" + startSeqNum +
                        " .. " + endSeqNum + "]" +
                        " to node: " + targetRNId;
                    final TopologyInfo topoInfo =
                        new TopologyInfo(topology, changes);
                    final int targetSeqNum = rnAdmin.updateMetadata(topoInfo);
                    if ((startSeqNum - targetSeqNum) > 1) {
                        changesMsg = "Push to be retried for rn: " +
                                     targetRNId +
                                    " Target topo seq number:" + targetSeqNum +
                                    " Cached topo seq number:" + rnTopoSeqNum;

                        /*
                         * Set it to the actual lower sequence number so
                         * that the push is retried with the extended change
                         * sequence.
                         */
                        rnTopoSeqNum = targetSeqNum;
                        success = false;
                    } else {
                        success = true;
                    }
                }
            } catch (Exception e) {
                exception = e;
            } finally {

                if (success) {
                    logger.log(Level.FINE, "Pushed {0}", changesMsg);
                } else {
                    logOnFailure(rnState.getRepNodeId(),
                                 exception, "Failed pushing " +  changesMsg);
                    /* Revert it so that retries are queued. */
                    rnState.resetTopoSeqNum(rnTopoSeqNum);
                }
            }
        }
    }

    /**
     * Information about the RN that cannot deliver incremental changes but
     * has a more recent topology. The RNSUT uses this information to pull
     * the changes it needs from the RN.
     */
    private static class FullTopoInfo {
        final int topoSeqNum;
        final RepNodeId rnId;
        /* The time that the request was noted. */
        final long timeMs = System.currentTimeMillis();

        FullTopoInfo(RepNodeId rnId, int topoSeqNum) {
            super();
            this.rnId = rnId;
            this.topoSeqNum = topoSeqNum;
        }
    }
}
