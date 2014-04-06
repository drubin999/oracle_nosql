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
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.sna.masterBalance.MasterBalanceManager.SNInfo;
import oracle.kv.impl.sna.masterBalance.MasterBalancingInterface.StateInfo;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;


/**
 * This is the topology cache used by the master balancing service. To minimize
 * the communication overheads, the cache is populated from one of the local
 * RNs resident at the SN which typically hosts the master balancing service.
 */
abstract class TopoCache {

    private final SNInfo snInfo;

    /**
     * A cached copy of the topology. There is a single writer to this iv; it's
     * the ensureTopology() method. All other methods read the iv and can
     * choose to copy it after reading it to ensure consistent access.
     */
    private volatile Topology topology;

    /**
     *  The latest Topo sequence number/RN that the manager is aware of.
     */
    private final AtomicReference<StateInfo> latestStateInfo;

    /**
     * The number of RNs hosted at this SN. It's computed from the cached
     * topology.
     */
    private int rnCount;

    /**
     * Coordinates shutdown activities.
     */
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    /**
     * The login manager of the sna
     */
    private final LoginManager loginMgr;

    /**
     * The time at which the cache was last validated.
     */
    private long lastValidationTimeMs = 0l;

    /**
     * The maximum period between re-validations of the cache.
     */
    private static long validationPeriodMs = 60 * 1000;

    private final Logger logger;

    TopoCache(SNInfo snInfo, Logger logger, LoginManager loginMgr) {
        super();
        this.snInfo = snInfo;
        this.logger = logger;
        this.loginMgr = loginMgr;
        latestStateInfo =
            new AtomicReference<StateInfo>(new StateInfo(null, null, 0));
    }

    /**
     * Returns true if the cache has been populated with a copy of the topology
     */
    boolean isInitialized() {
        return topology != null;
    }

    /**
     * Returns a copy of the cached topology. Users of this interface must
     * ensure that the cache has been initialized, typically via a call to
     * {@link #ensureTopology} before invoking this method.
     */
    Topology getTopology() {
        if (topology == null) {
            throw new IllegalStateException("no topology in topology cache");
        }
        return topology;
    }

    /**
     * Used to track the latest topology associated with the RNs at the SN.
     * This method provokes {@link #ensureTopology} to fetch a more up to
     * date copy of the topology. Fetching of the topology, which can be time
     * consuming, since it involves network communications, is typically done
     * asynchronously in a separate thread.
     *
     * @param stateInfo the RN state transition that is also used to
     * piggy-back top information
     */
    void noteLatestTopo(StateInfo stateInfo) {
        while (true) {
            final StateInfo si = latestStateInfo.get();
            if (stateInfo.topoSequenceNumber > si.topoSequenceNumber) {
                if (latestStateInfo.compareAndSet(si, stateInfo)) {
                    return;
                }
                continue;
            }
            return;
        }
     }

    /**
     * Returns the combined RF of primary DCs.
     */
    int getPrimaryRF() {
        /* Stabilize the topo handle. */
        final Topology topo = this.topology;

        if (topo == null) {
            throw new IllegalStateException("no topology in topology cache");
        }

        int rf = 0;
        for (final Datacenter dc : topo.getDatacenterMap().getAll()) {
            if (dc.getDatacenterType().isPrimary()) {
                rf += dc.getRepFactor();
            }
        }
        return rf;
    }

    abstract Set<RepNodeId> getActiveRNs();

    /**
     * Internal only. For use in unit tests.
     */
    public static long getValidationIntervalMs() {
        return validationPeriodMs;
    }

    /**
     * Internal only. For use in unit tests.
     */
    public static void setValidationIntervalMs(long validationIntervalMs) {
        TopoCache.validationPeriodMs = validationIntervalMs;
    }

    /**
     * Ensures that there is a reasonably current cached copy of the Topology.
     * A copy of a Topology is obtained from one of the resident active RNs.
     * This method must be called on a periodic basis, to ensure that the
     * cache is current.
     *
     * <p>
     * If there is no cached topology, the method will be called repeatedly
     * until a local copy has been established. If the Topology is merely
     * obsolete, it will make an attempt to get a newer version but will give
     * up and fall back to the obsolete cached copy, if it fails to update the
     * copy after contacting all the known RNs.
     *
     * @return true if a topology is present, false if no topology is present.
     */
    boolean ensureTopology() {

        if ((topology != null) &&
            (topology.getSequenceNumber() >=
             latestStateInfo.get().topoSequenceNumber) &&
             ((System.currentTimeMillis() - lastValidationTimeMs) <
               validationPeriodMs)) {
            return true;
        }

        lastValidationTimeMs = System.currentTimeMillis() ;
        if (topology == null) {
            logger.info("Acquiring initial topology from RNs");
        }

        final List<RepNodeId> topoRNs =
                new LinkedList<RepNodeId>(getActiveRNs());
        StateInfo si = latestStateInfo.get();
        final RepNodeId topoRnId = si.rnId;
        final int topoSeqNum = si.topoSequenceNumber;
        /* Move topoRnId to the front, if it has been set. */
        if (topoRnId != null) {
            topoRNs.remove(topoRnId);
            topoRNs.add(0, topoRnId);
        }

        for (RepNodeId rnId : topoRNs) {
            try {
                final RepNodeAdminAPI topoRn = RegistryUtils.
                        getRepNodeAdmin(snInfo.storename,
                                        snInfo.snHostname,
                                        snInfo.snRegistryPort,
                                        topoRnId,
                                        loginMgr);

                final int rnTopoSeqNum = topoRn.getTopoSeqNum();
                if ((topology != null) &&
                    (topology.getSequenceNumber() >= rnTopoSeqNum)) {
                    /* cache is current wrt this RN */
                    continue;
                }

                /*
                 * Cache is out of date.
                 */
                final Topology newTopology = topoRn.getTopology();
                if ((newTopology != null) &&
                     newTopology.getSequenceNumber() >= topoSeqNum) {

                    final int topoVersion = newTopology.getVersion();
                    if (topoVersion == 0) {
                        logger.warning
                            ("Ignoring r1 topology from: " + rnId);
                        /* Try find a compatible version elsewhere. */
                        continue;
                    }

                    topology = newTopology;
                    final StateInfo newStateInfo = new
                        StateInfo(rnId, null,
                                  newTopology.getSequenceNumber());
                    noteLatestTopo(newStateInfo);
                    rnCount = 0;
                    /* update the rn count for this sn. */
                    for (RepNodeId id : topology.getRepNodeIds()) {
                        RepNode rn = topology.get(id);
                        if (rn.getStorageNodeId().equals(snInfo.snId)) {
                            rnCount++;
                        }
                    }
                    logger.info("Topology acquired from RN: " + rnId +
                                " Topo seq#: " +
                                newTopology.getSequenceNumber());
                    return true;
                }
            } catch (RemoteException e) {
                /* Try getting the topo from another RN */
                continue;
            } catch (NotBoundException e) {
                /* Try getting the topo from another RN */
                continue;
            }
        }
        return topology != null;
    }

    /**
     * Returns the number of RNs hosted at this SN
     */
    int getRnCount() {
        assert topology != null;
        return rnCount;
    }

    /**
     * Shuts down the topology cache
     */
    void shutdown() {
        shutdown.set(true);
    }
}
