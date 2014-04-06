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

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.sna.masterBalance.MasterBalanceManager.SNInfo;
import oracle.kv.impl.sna.masterBalance.MasterBalancingInterface.MDInfo;
import oracle.kv.impl.sna.masterBalance.MasterBalancingInterface.MasterLeaseInfo;
import oracle.kv.impl.sna.masterBalance.MasterBalancingInterface.StateInfo;
import oracle.kv.impl.sna.masterBalance.ReplicaLeaseManager.ReplicaLease;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.utilint.StoppableThread;

/**
 * The thread that does the rebalancing when one is necessary. The thread
 * reacts to changes that are posted in the stateTransitions queue and
 * initiates master transfer operations in response to the state changes.
 */
class RebalanceThread extends StoppableThread {

    private static final int THREAD_SOFT_SHUTDOWN_MS = 10000;

    /**
     * The time associated with a remote master lease. This is also the
     * amount of time, associated with the master transfer request.
     */
    private static final int MASTER_LEASE_MS = 5 * 60 * 1000;

    /**
     * The poll period used to poll for a rebalance in the absence of any
     * HA state transitions by the nodes on the SN. This permits an imbalanced
     * SN to check whether other SNs who can help ameliorate the local
     * imbalance have come online.
     */
    private static final int POLL_PERIOD_DEFAULT_MS = 1 * 60 * 1000;

    private static int pollPeriodMs = POLL_PERIOD_DEFAULT_MS;

    /**
     * The SN that is being brought into balance, by this rebalance thread.
     */
    private final SNInfo snInfo;

    /**
     * The set of replica RNs at the SN.
     */
    private final Set<RepNodeId> activeReplicas;

    /**
     * The set of master RNs at the SN. Some subset of the masters may have
     * replicaLeases associated with them, if they are in the midst of a master
     * transfer.
     */
    private final Set<RepNodeId> activeMasters;

    /**
     * The shared topo cache at the SN
     */
    private final TopoCache topoCache;

    /**
     * The leases on an RN as it transitions from master to replica as part of
     * a master transfer operation.
     */
    private final ReplicaLeaseManager replicaLeases;

    /*
     * Determines if this SN has been contacted by overcommitted SNs that
     * could potentially transfer masters to it.
     */
    private final AtomicBoolean overloadedNeighbor = new AtomicBoolean(false);

    /**
     * The queue used to post state changes to the thread that attempts to
     * correct a MD imbalance.
     */
    private final BlockingQueue<StateInfo> stateTransitions =
            new ArrayBlockingQueue<StateInfo>(100);

    /**
     * The master balance manager associated with this thread. The
     * RebalanceThread is a component of the MasterBalanceManager.
     */
    private final MasterBalanceManager manager;

    private final Logger logger;

    RebalanceThread(MasterBalanceManager manager) {
        super("MasterRebalanceThread");
        snInfo = manager.getSnInfo();
        topoCache = manager.getTopoCache();
        this.manager = manager;

        activeReplicas = Collections.synchronizedSet(new HashSet<RepNodeId>());
        activeMasters = Collections.synchronizedSet(new HashSet<RepNodeId>());

        this.logger = manager.logger;
        replicaLeases = new ReplicaLeaseManager(logger);
    }

    @Override
    protected Logger getLogger() {
       return logger;
    }

    /**
     * Used to vary the poll period in unit tests.
     */
    static void setPollPeriodMs(int pollPeriodMs) {
        RebalanceThread.pollPeriodMs = pollPeriodMs;
    }

    /**
     * Take note of an RN that has exited by simulating a state change to
     * DETACHED
     */
    void noteExit(RepNodeId rnId)
        throws InterruptedException {
        logger.info("Rebalance thread notes " + rnId + " exited");
        noteState(new StateInfo(rnId, ReplicatedEnvironment.State.DETACHED, 0));
    }

    /**
     * Take note of the new state placing it in the stateTransitions queue.
     */
    void noteState(StateInfo stateInfo)
        throws InterruptedException {

        while (!stateTransitions.offer(stateInfo, 60, TimeUnit.SECONDS)) {
            logger.info("State transition queue is full retrying. " +
                        "Capacity:" + stateTransitions.size());
        }
        logger.info("added:" + stateInfo);
    }

    /**
     * Part of the thread shutdown protocol.
     */
    @Override
    protected int initiateSoftShutdown() {
        if (!manager.shutdown.get()) {
            throw new IllegalStateException("Expected manager to be shutdown");
        }

        /* Provoke the thread into looking at the shutdown state. */
        stateTransitions.offer(new StateInfo(null, null, 0));

        return RebalanceThread.THREAD_SOFT_SHUTDOWN_MS;
    }

    /**
     * Returns true if this SN could potentially be rebalanced.
     */
    private boolean needsRebalancing(boolean overload) {

        if (activeMasters.size() == 0) {
            return false;
        }

        final int leaseAdjustedMD = getLeaseAdjustedMD();
        final int BMD = getBMD();

        final boolean needsRebalancing =
            ((activeMasters.size() > 1) &&   /* More than one master. */
             (leaseAdjustedMD > BMD)) /* Imbalance */ ;

        if (needsRebalancing) {

            logger.info(snInfo.snId +
                        " masters: " + activeMasters +
                        " replica leases: " + replicaLeases.leaseCount() +
                        " resident RNs: " + topoCache.getRnCount() +
                        " needs rebalancing. lease adjusted MD: " +
                         leaseAdjustedMD + " > BMD:" + BMD);
            return true;
        }

        /**
         * If one of this SNs neighbors is imbalanced, try harder and seek to
         * move a master off this SN, so that the imbalanced neighbor can in
         * turn move a master to this SN. Note that as currently implemented
         * the imbalance is only propagated to immediate neighbors as a
         * simplification. In future SNs may propagate imbalance to their
         * neighbors in turn, if they cannot move a master RN off the SN based
         * on local considerations.
         */
        final int incrementalMD = (100 / topoCache.getRnCount());
        if (overload &&
            ((leaseAdjustedMD + incrementalMD) > BMD)) {

            logger.info(snInfo.snId +
                        " masters: " + activeMasters +
                        " replica leases: " + replicaLeases.leaseCount() +
                        " resident RNs: " + topoCache.getRnCount() +
                        " Unbalanced neighbors initiated rebalancing. " +
                        "lease adjusted MD: " + leaseAdjustedMD +
                        " + " + incrementalMD + "> BMD:"
                        + BMD);
            return true;
        }

        return false;
    }

    /**
     * The run thread. It reacts to state changes posted to the
     * stateTransitions queue.
     */
    @Override
    public void run() {
        logger.info("Started " + this.getName());
        try {
            while (true) {

                /*
                 * Wait for RN state transitions
                 */
                final StateInfo stateInfo =
                    stateTransitions.poll(pollPeriodMs, TimeUnit.MILLISECONDS);

                /* Check if "released" for shutdown. */
                if (manager.shutdown.get()) {
                    return;
                }

                if (stateInfo != null) {
                    processStateInfo(stateInfo);
                }

                if (!topoCache.ensureTopology()) {
                    continue;
                }

                final boolean overload = overloadedNeighbor.getAndSet(false);

                if (!needsRebalancing(overload)) {
                    continue;
                }

                final List<Transfer> choice = candidateTransfers(overload);

                if (choice.size() > 0) {
                    if (transferMaster(choice) != null) {
                        continue;
                    }
                }

                /* needs balancing, but no master transfer. */
                final String masters =
                    new ArrayList<RepNodeId>(activeMasters).toString();
                logger.info("No suitable RNs: " + masters +
                            " for master transfer.");

                if ((replicaLeases.leaseCount() == 0) &&
                    (getRawMD() > getBMD())) {
                    /*
                     * Unbalanced, but no progress, inform our MSCN neighbors
                     * of this predicament in case they can help by
                     * vacating a master RN
                     */
                    informNeighborsOfOverload();
                }
            }
        } catch (InterruptedException e) {

            if (manager.shutdown.get()) {
                return;
            }

            logger.info("Lease expiration task interrupted.");
        } catch (Exception e) {
            logger.log(Level.SEVERE, this.getName() +
                       " thread exiting due to exception.", e);
            /* Shutdown the manager. */
            manager.shutdown();
        } finally {
            logger.info(this.getName() + " thread exited.");
        }
    }

    private void processStateInfo(final StateInfo stateInfo) {
        /* Process the state change. */
        final RepNodeId rnId = stateInfo.rnId;

        switch (stateInfo.state) {

            case MASTER:
                activeReplicas.remove(rnId);
                activeMasters.add(rnId);
                break;

            case REPLICA:
                activeMasters.remove(rnId);
                activeReplicas.add(rnId);
                replicaLeases.cancel(rnId);
                break;

            case UNKNOWN:
            case DETACHED:
                activeMasters.remove(rnId);
                activeReplicas.remove(rnId);
                replicaLeases.cancel(rnId);
                break;
        }

        logger.info("sn: " + snInfo.snId +
                    " state transition: " + stateInfo +
                    " active masters:" + activeMasters +
                    " active replicas:" + activeReplicas.size() +
                    " replica leases:" + replicaLeases.leaseCount());
    }

    /**
     * Returns true if the node is known to be the master
     */
    @SuppressWarnings("unused")
    private boolean isMaster(RepNodeId repNodeId) {
        return activeMasters.contains(repNodeId);
    }

    /**
     * Returns true if the node is known to be a replica
     */
    boolean isReplica(RepNodeId repNodeId) {
        return activeReplicas.contains(repNodeId);
    }

    /**
     * Request a single master transfer from the ordered list of transfer
     * candidates.
     */
    private Transfer transferMaster(List<Transfer> candidateTransfers) {

        final Topology topo = topoCache.getTopology();
        final RegistryUtils regUtils =
            new RegistryUtils(topo, manager.getLoginManager());
        final StorageNode localSN = topo.get(snInfo.snId);

        for (Transfer transfer : candidateTransfers) {

            /* Contact the local RN and request the transfer. */
            logger.info("Requesting master RN transfer : " + transfer);

            StorageNodeAgentAPI sna = null;
            RepNodeId sourceRNId = null;

            boolean masterTransferInitiated = false;
            try {
                sna = regUtils.getStorageNodeAgent
                        (transfer.targetSN.getResourceId());
                final MasterLeaseInfo masterLease =
                        new MasterLeaseInfo(localSN,
                                            transfer.targetRN,
                                            transfer.ptmd,
                                            MASTER_LEASE_MS);
                if (!sna.getMasterLease(masterLease)) {
                    continue;
                }

                sourceRNId = transfer.sourceRN.getResourceId();
                final ReplicaLease replicaLease = new ReplicaLeaseManager.
                        ReplicaLease(sourceRNId, MASTER_LEASE_MS);

                replicaLeases.getReplicaLease(replicaLease);

                RepNodeAdminAPI rna = regUtils.getRepNodeAdmin(sourceRNId);
                masterTransferInitiated =
                        rna.initiateMasterTransfer(transfer.targetRN.getResourceId(),
                                                   MASTER_LEASE_MS,
                                                   TimeUnit.MILLISECONDS);
                if (masterTransferInitiated) {
                    return transfer;
                }

            } catch (RemoteException e) {
               continue;
            } catch (NotBoundException e) {
               continue;
            } finally {
                if (!masterTransferInitiated) {
                    /* Clean up leases. */
                    if (sna != null) {
                        try {
                            sna.cancelMasterLease(localSN, transfer.targetRN);
                        } catch (RemoteException e) {
                            logger.info("Failed master lease cleanup: " +
                                        e.getMessage());
                        }
                    }
                    if (sourceRNId != null) {
                        replicaLeases.cancel(sourceRNId);
                    }
                }
            }
        }

        return null;
    }

    /**
     * Selects suitable target SNs for the master transfer, if doing so will
     * reduce the imbalance across the KVS.
     *
     * It contacts all the SNs in MCSNs to get a snapshot of their current
     * post-transfer MDs (the MD after an additional master has been
     * transferred to it). If this candidate SN has a lower post-transfer MD
     * than the post-transfer MD (the MD with one less master) of the
     * overloaded SN, the SN is selected as a potential target for the master
     * transfer. In the case of a tie, the selection process may use other
     * criteria, like switch load balancing, as a tie breaker.
     *
     * Note that if a neighbor SN is overloaded, then the MD density associated
     * with this SN is boosted by one in order to consider a master transfer
     * in situations where it may not have otherwise.
     *
     * @param overload used to indicate whether a neighbor SN is overloaded
     *
     * @return an ordered list of potential transfers. The transfers are
     * ordered by increasing PTMD
     */
    private List<Transfer> candidateTransfers(boolean overload) {

        final Set<StorageNode> mscns = getMCSNs();
        logger.info("MSCNS:" + mscns);
        final TreeMap<Integer, List<StorageNode>> ptmds = orderMSCNs(mscns);
        final List<Transfer> transfers = new LinkedList<Transfer>();

        if (ptmds.size() == 0) {
            /* None of the SNs were available. */
            return transfers;
        }

        /* Select an RN to move */
        final int loweredMD =
            ((activeMasters.size() +
              (overload ? 1 : 0) -
              replicaLeases.leaseCount() - 1) * 100) /
            topoCache.getRnCount();

        if (loweredMD <= 0) {
            return transfers;
        }

        final Topology topology = topoCache.getTopology();

        for (Entry<Integer, List<StorageNode>> entry : ptmds.entrySet()) {

            final int ptmd = entry.getKey();
            logger.info("loweredMD:" + loweredMD + " ptmd:" + ptmd +
                        " SNS:" + entry.getValue());
            /*
             * If the PTMD of the target SN reaches a balanced state,
             * allow a transfer to it, even if it results in a target with a
             * lower MD than the one on this node. See SR 22888 for details.
             */
            if ((ptmd > getBMD()) && (ptmd > loweredMD)) {
                /* No further candidates. */
                return transfers;
            }

            if (overload && (ptmd > getBMD())) {
                /*
                 * Don't push under neighbor overload if it will result in the
                 * target SN becoming overloaded.
                 */
                return transfers;
            }

            for (final StorageNode targetSN :  entry.getValue()) {
                final StorageNodeId targetSnId = targetSN.getResourceId();

                /* Pick a master RN to transfer. */
                for (RepNodeId mRnId : activeMasters) {
                    if (replicaLeases.hasLease(mRnId)) {
                        /* A MT for this RN is already in progress. */
                        continue;
                    }
                    final RepNode sourceRN = topology.get(mRnId);

                    if (sourceRN == null) {
                        /*
                         * The SNA heard about the master state transition
                         * before it had a chance to update its cached topology.
                         */
                        continue;
                    }
                    /* Pick the target for the source. */
                    final Collection<RepNode> groupNodes =
                        topology.get(sourceRN.getRepGroupId()).getRepNodes();
                    for (RepNode targetRN : groupNodes) {

                        if (targetRN.getStorageNodeId().equals(targetSnId)) {
                            final Transfer transfer =
                                new Transfer(sourceRN, targetRN, targetSN,
                                             ptmd);
                            logger.info("Candidate transfer:" + transfer);
                            transfers.add(transfer);
                        }
                    }
                }
            }
        }
        return transfers;
    }

    /**
     * Returns the MSCNs ordered by their current PTMDs. SNs that cannot be
     * contacted are eliminated from this computation.
     */
    private TreeMap<Integer, List<StorageNode>>
        orderMSCNs(Set<StorageNode> mscns) {

        final TreeMap<Integer, List<StorageNode>> ptmds =
                new TreeMap<Integer, List<StorageNode>>();

        for (StorageNode sn : mscns) {
            try {
                final StorageNodeAgentAPI sna =
                    RegistryUtils.getStorageNodeAgent(
                        snInfo.storename, sn, manager.getLoginManager());
                MDInfo mdInfo = sna.getMDInfo();
                if (mdInfo == null) {
                    continue;
                }
                final int ptmd = mdInfo.getPTMD();
                List<StorageNode> sns = ptmds.get(ptmd);
                if (sns == null) {
                    sns = new LinkedList<StorageNode>();
                    ptmds.put(ptmd, sns);
                }

                sns.add(sn);

            } catch (RemoteException e) {
                logger.info("Could not contact SN to compute PTMD: " + sn +
                            " reason:" + e.getMessage());
                continue;
            } catch (NotBoundException e) {
                /* Should not happen. */
                logger.warning(sn + " missing from registry.");
                continue;
            }
        }
        return ptmds;
    }

    /**
     * Returns the set of SNs that are candidates for a MasterTransfer. That
     * is, they host replica RNs that could trade state with master RNs at this
     * SN.
     */
    private Set<StorageNode> getMCSNs() {
        final Set<StorageNode> mscns = new HashSet<StorageNode>();

        final Topology topology = topoCache.getTopology();

        for (RepNodeId mRnId : activeMasters) {

            final RepNode mrn = topology.get(mRnId);

            if (mrn == null) {
                /*
                 * A topology race, have heard of master but don't yet have
                 * an updated topology.
                 */
                continue;
            }
            final RepGroupId mRgId = mrn.getRepGroupId();

            if (replicaLeases.hasLease(mRnId)) {
                /* Already a master transfer candidate. */
                continue;
            }

            for (RepNode rn : topology.get(mRgId).getRepNodes()) {

                final StorageNodeId targetSNId = rn.getStorageNodeId();


                if (snInfo.snId.equals(targetSNId)) {
                    continue;
                }

                final StorageNode targetSN = topology.get(targetSNId);
                if (!topology.get(targetSN.getDatacenterId()).
                    getDatacenterType().isPrimary()) {
                    /* Non-primary datacenters can't host masters. */
                    continue;
                }

                mscns.add(targetSN);
            }
        }
        return mscns;
    }

    /**
     * Returns the raw (unadjusted for any in-flight master transfers) MD
     */
    private int getRawMD() {
        return (!topoCache.isInitialized() || manager.shutdown.get()) ?
            Integer.MAX_VALUE :
            ((activeMasters.size() * 100) / topoCache.getRnCount());
    }

    /**
     * Subtract replica leases to account for master transfers that are in
     * play.
     */
    private int getLeaseAdjustedMD() {
        return ((activeMasters.size() - replicaLeases.leaseCount()) * 100) /
            topoCache.getRnCount();
    }

    /**
     * Returns the balanced master density.
     */
    int getBMD() {
        return 100 / topoCache.getPrimaryRF();
    }

    /**
     * Returns a count of the current masters.
     */
    int getMasterCount() {
        return activeMasters.size();
    }

    /**
     * Returns the masters and replica that are active at the SN
     */
    Set<RepNodeId> getActiveRNs() {
        final Set<RepNodeId> activeRNs = new HashSet<RepNodeId>();
        activeRNs.addAll(activeMasters);
        activeRNs.addAll(activeReplicas);

        return activeRNs;
    }

    /**
     * Contact all MSCNs exhorting them to try harder to shed a master,
     * permitting this SN in turn to move a master over.
     */
    private void informNeighborsOfOverload() {

        for (StorageNode sn : getMCSNs()) {
            try {
                /* Should we request a push to just one SN and then exit? */
                final StorageNodeAgentAPI sna =
                    RegistryUtils.getStorageNodeAgent(
                        snInfo.storename, sn, manager.getLoginManager());
                sna.overloadedNeighbor(snInfo.snId);
                logger.info("master rebalance push requested: " + sn);
            } catch (RemoteException e) {
                logger.info("Could not contact SN to request master " +
                            "rebalance push: " + sn + " reason:" +
                            e.getMessage());
                continue;
            } catch (NotBoundException e) {
                /* Should almost never happen. */
                logger.warning(sn + " missing from registry.");
                continue;
            }
        }
    }

    /* Note unbalanced neighbor SN */
    void overloadedNeighbor(StorageNodeId storageNodeId) {
        logger.info("Master unbalanced neighbor sn:" + storageNodeId);
        overloadedNeighbor.set(true);
    }

    /**
     * The struct containing the RNs and the target SN involved in a master
     * transfer.
     */
    private static final class Transfer {
        final RepNode sourceRN;
        final RepNode targetRN;
        final StorageNode targetSN;
        final private int ptmd;

        Transfer(RepNode sourceRN, RepNode targetRN, StorageNode targetSN, int ptmd) {
            super();
            this.sourceRN = sourceRN;
            this.targetRN = targetRN;
            this.targetSN = targetSN;
            this.ptmd = ptmd;
        }

        @Override
        public String toString() {
            return "<Transfer master from " + sourceRN + " to " +
                   targetRN + " at " + targetSN + " ptmd: " + ptmd + ">";
        }
    }
}
