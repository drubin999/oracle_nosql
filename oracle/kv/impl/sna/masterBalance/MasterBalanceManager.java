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

package oracle.kv.impl.sna.masterBalance;

import java.rmi.RemoteException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;

/**
 * Implements the MasterBalancingInterface. This class is designed to be an
 * independent service; the current implementation creates an instance of this
 * class when the SNA starts up, and is shutdown as part of the SNA's
 * shutdown. That is, its lifecycle is coincident with the SNA's lifecycle.
 * <p>
 * This instance must be created before the SNA starts up any of its RNs so
 * that none of the state changes are missed and an accurate count of the
 * masters at the SN can be maintained.
 * <p>
 * @see MasterBalancingInterface
 */
public class MasterBalanceManager implements MasterBalanceManagerInterface {

    /**
     * The storage node being balanced.
     */
    private final SNInfo snInfo;

    /**
     * The component that coordinates the use of leases during a master
     * rebalance operation.
     */
    private final MasterLeaseManager masterLeaseManager;

    /**
     * The cached copy of the topology used to identify RNs at this SN and
     * other SNs. We need this cached copy, since it's not stored persistently
     * on the SNA.
     */
    private final TopoCache topoCache;

    /**
     * The thread instance used to initiate rebalancing whenever the BMD is
     * exceeded.
     */
    private final RebalanceThread rebalanceThread;

    /**
     * Coordinates shutdown activities.
     */
    final AtomicBoolean shutdown = new AtomicBoolean(false);

    final Logger logger;

    final LoginManager loginMgr;

    /**
     * An instance of MasterBalancing. There is one per SN. Its lifecycle
     * follows that of the SNA.
     *
     * @param snInfo information about the SN that is being balanced
     * @param logger the logger to be used
     */
    private MasterBalanceManager(SNInfo snInfo,
                                 Logger logger,
                                 LoginManager loginMgr) {

        this.snInfo = snInfo;
        this.logger = logger;
        this.loginMgr = loginMgr;

        masterLeaseManager = new MasterLeaseManager(logger);
        topoCache = new TopoCache(snInfo, logger, getLoginManager()) {
            @Override
            Set<RepNodeId> getActiveRNs() {
               return rebalanceThread.getActiveRNs();
            }
        };
        rebalanceThread = new RebalanceThread(this);
        rebalanceThread.start();
    }

    /**
     * Factory method to create a MBM
     *
     * @param enabled if true MB is enabled
     * @param snInfo information about the SN that is being balanced
     * @param logger the logger to be used
     */
    public static MasterBalanceManagerInterface create(boolean enabled,
                                                       SNInfo snInfo,
                                                       Logger logger,
                                                       LoginManager loginMgr) {
        return enabled ?
            new MasterBalanceManager(snInfo, logger, loginMgr) :
            new MasterBalanceManagerDisabled(logger);
    }

    SNInfo getSnInfo() {
        return snInfo;
    }

    /**
     * Returns the topo cache maintained for balancing
     */
    TopoCache getTopoCache() {
        return topoCache;
    }

    /**
     * Cleanup on exit.
     */
    @Override
    public void shutdown() {
        if (shutdown.getAndSet(true)) {
            return;
        }

        topoCache.shutdown();
        masterLeaseManager.shutdown();
        rebalanceThread.shutdownThread(logger);
        logger.info("Master balance manager shutdown");
    }

    /**
     * Invoked to note that a particular RN has exited. If the RN exit was not
     * abrupt, this call may have been preceded by a noteState change called
     * communicating the DETACHED state. The method is resilient in the face
     * of such redundant calls.
     */
    @Override
    public void noteExit(RepNodeId rnId) {
        if (shutdown.get()) {
            /* Ignore requests after it's been shutdown. */
            return;
        }

        masterLeaseManager.cancel(rnId);
        try {
            rebalanceThread.noteExit(rnId);
        } catch (InterruptedException e) {
            logger.info("NoteExit call interrupted");
        }
    }

    @Override
    public synchronized void noteState(StateInfo stateInfo,
                                       final short serialVersion) {
        noteState(stateInfo, null, serialVersion);
    }

    @Override
    public synchronized void noteState(StateInfo stateInfo,
                                       AuthContext authContext,
                                       final short serialVersion) {

        if (shutdown.get()) {
            /* Ignore requests after it's been shutdown. */
            return;
        }

        final RepNodeId rnId = stateInfo.rnId;
        /* Track latest current topology. */
        topoCache.noteLatestTopo(stateInfo);

        try {
            if (stateInfo.state.isMaster()) {
                /* cancel any outstanding lease for this node */
                masterLeaseManager.cancel(rnId);
            }
            rebalanceThread.noteState(stateInfo);
        } catch (InterruptedException e) {
            logger.info("State change call interrupted");
        }
    }

    @Override
    public MDInfo getMDInfo(short serialVersion) {
        return getMDInfo(null, serialVersion);
    }

    @Override
    public synchronized MDInfo getMDInfo(AuthContext authContext,
                                         short serialVersion) {

        if (!topoCache.isInitialized() || shutdown.get()) {
            return null;
        }

        return new MDInfo(rebalanceThread.getMasterCount(),
                          masterLeaseManager.leaseCount(),
                          topoCache.getRnCount(),
                          rebalanceThread.getBMD());
    }

    @Override
    public synchronized boolean cancelMasterLease(StorageNode lesseeSN,
                                                  RepNode rn,
                                                  short serialVersion) {
        return cancelMasterLease(lesseeSN, rn, null, serialVersion);
    }

    @Override
    public synchronized boolean cancelMasterLease(StorageNode lesseeSN,
                                                  RepNode rn,
                                                  AuthContext authContext,
                                                  short serialVersion) {
        return masterLeaseManager.cancel(lesseeSN, rn);
    }

    @Override
    public synchronized boolean getMasterLease(MasterLeaseInfo masterLease,
                                               short serialVersion) {
        return getMasterLease(masterLease, null, serialVersion);
    }

    @Override
    public synchronized boolean getMasterLease(MasterLeaseInfo masterLease,
                                               AuthContext authContext,
                                               short serialVersion) {

        if (!topoCache.isInitialized() || shutdown.get()) {
            return false;
        }

        MDInfo mdInfo = getMDInfo(serialVersion);
        if ((mdInfo == null) ||
            /*
             * Check if this (the target) SN's PTMD has increased since the
             * time the decision to request the lease was made by the source
             * SN. If it has, reject the lease request since that decision may
             * no longer be valid. The source SN may retry after obtaining the
             * increased PTMD.
             */
            (mdInfo.getPTMD() > masterLease.limitPTMD)) {
            return false;
        }

        /* Establish a lease, if it's in replica state. */
        return rebalanceThread.isReplica(masterLease.getRepNodeId())
            ? masterLeaseManager.getMasterLease(masterLease) : false;
    }

    @Override
    public void overloadedNeighbor(StorageNodeId storageNodeId,
                                   short serialVersion)
        throws RemoteException {

        overloadedNeighbor(storageNodeId, null, serialVersion);
    }

    @Override
    public void overloadedNeighbor(StorageNodeId storageNodeId,
                                   AuthContext authContext,
                                   short serialVersion)
        throws RemoteException {

        rebalanceThread.overloadedNeighbor(storageNodeId);
    }

    public LoginManager getLoginManager() {
        return loginMgr;
    }

    /**
     * Convenience "struct" used to package info about the SN.
     */
    public static class SNInfo {
        final String storename;
        final StorageNodeId snId;
        final String snHostname;
        final int snRegistryPort;

        public SNInfo(String storename,
                      StorageNodeId snId,
                      String snHostname,
                      int snRegistryPort) {
            super();
            this.storename = storename;
            this.snId = snId;
            this.snHostname = snHostname;
            this.snRegistryPort = snRegistryPort;
        }
    }
}
