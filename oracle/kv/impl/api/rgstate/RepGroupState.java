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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import oracle.kv.Consistency;
import oracle.kv.Consistency.Time;
import oracle.kv.Consistency.Version;
import oracle.kv.impl.api.Request;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.Topology;

import com.sleepycat.je.rep.ReplicatedEnvironment.State;

/**
 * RepGroupState represents the state associated with the replication group.
 * The only (non-structural) state associated with the rep group is the
 * identity of the node currently serving as the master and the time/seq#
 * associated that event.
 * <p>
 * There are two types of updates that can be made to the rep group state:
 * <ol>
 * <li>Structural changes arising from a change in Topology.</li>
 * <li>HA state changes in response to the change in the replication state
 * associated with nodes in the group.</li></ol>
 * Two overloaded update methods are defined by this class to deal with these
 * two types of group level changes.
 *
 * TODO:
 * 1) Configurable partition-aware dispatching as an option to exploit cache
 * locality on replicas
 */
public class RepGroupState {

    private final RepGroupId repGroupId;

    /**
     * The master of the group, or null if the master is unknown. This iv,
     * along with the lastChange iv below, is directly derived from the
     * information in a StateChangeEvent that either originated at this node,
     * due to as Listener invocation, or due to a StateChangeEvent at a distant
     * node that was communicated to this node as part of a Response.
     */
    private volatile RepNodeId groupMasterId;

    /**
     * Represents the time of the last change in the MASTER/REPLICA rep state
     * associated with the group. It's used to determine whether an update is
     * newer than the current known state.
     */
    private long lastChange;

    /**
     * The states for RNs that are members of the group. The collection starts
     * out empty and is filled in via calls to the update methods.
     */
    private final Map<RepNodeId, RepNodeState> rns;

    /**
     * Coordinates access to the RepGroupState.
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     *  Used to select a random RN from within a rep group
     */
    static volatile int randomRN;

    public RepGroupState(RepGroupId repGroupId) {
        this.repGroupId = repGroupId;
        rns = new ConcurrentHashMap<RepNodeId, RepNodeState>(3);
    }

    public RepGroupId getResourceId() {
       return repGroupId;
    }

    /**
     * Returns the RN states associated with the RG. The returned collection
     * may be empty if the group has not yet been initialized with Topology
     * information.
     *
     * Note: this method is intended for testing. It provides unsynchronized
     * internal representation state.
     */
    public Collection<RepNodeState> getRepNodeStates() {
        return rns.values();
    }

    /**
     * Returns the node state associated with the rnId.
     */
    public RepNodeState get(RepNodeId rnId) {

        assert rnId.getGroupId() == repGroupId.getGroupId();
        try {
            lock.readLock().lock();
            final RepNodeState repNodeState = rns.get(rnId);
            if (repNodeState != null) {
                return repNodeState;
            }
        } finally {
            lock.readLock().unlock();
        }

        try {
            lock.writeLock().lock();
            RepNodeState repNodeState = rns.get(rnId);
            if (repNodeState == null) {
                repNodeState = new RepNodeState(rnId);
                rns.put(rnId, repNodeState);
            }
            return repNodeState;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Returns the node state for the master associated with the RepGroup.
     *
     * @return the RepNodeState for the master if it knows of one. Or null if
     * it does not know of one.
     */
    public RepNodeState getMaster() {
        final RepNodeId stableGroupMasterId = groupMasterId;
        return (stableGroupMasterId == null) ? null : get(stableGroupMasterId);
    }

    /**
     * Updates the group state with information from the topology. If the
     * topology contains information about rns that it does not currently
     * contain, it updates its information. Updating the information means
     * that it will start using the new RNs during load balancing for requests.
     */
    public void update(RepGroup rg, Topology topology) {
        try {
            lock.writeLock().lock();
            final Collection<RepNode> shardRNs = rg.getRepNodes();
            for (final RepNode rn : shardRNs) {
                final RepNodeId rnId = rn.getResourceId();
                RepNodeState rnState = rns.get(rnId);
                if (rnState == null) {
                    rnState = new RepNodeState(rnId);
                    rns.put(rnId, rnState);
                }
                final StorageNode sn = topology.get(rn.getStorageNodeId());
                rnState.setZoneId(sn.getDatacenterId());
            }

            /* Remove any RNs that no longer appear in the shard */
            if (shardRNs.size() < rns.size()) {
                final Iterator<RepNodeId> iter = rns.keySet().iterator();
                while (iter.hasNext()) {
                    final RepNodeId rnId = iter.next();
                    if (rg.get(rnId) == null) {
                        iter.remove();
                    }
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Updates the HA state of the node, and possibly other nodes in the group.
     * <p>
     * If the event indicates there is a new master, the state of the nodes in
     * the group is updated to indicate that it's the sole new master. If some
     * node was previously in the master state, it's changed to being in the
     * Replica state.
     * <p>
     * Note that the update relies on time associated with change events, to
     * ensure that older updates that were delayed do not overwrite newer
     * updates. The use of time is essential for tracking the authoritative
     * master during a network partition. The non-authoritative master will
     * always have an older time associated with its state change event and
     * will therefore be superseded by the authoritative master which will have
     * a later time associated with its state change event, since its election
     * was initiated after the intervening network partition.
     *
     * @param rnId the node whose state is to be updated
     * @param masterId the master if its known or null
     * @param state the new HA state associated with rnId
     * @param time the time used to sequence the change
     */
    public void update(RepNodeId rnId,
                       RepNodeId masterId,
                       State state,
                       long time) {

        try {
            lock.writeLock().lock();
            if (time <= lastChange) {

                /*
                 * An out of sequence change, or while we are relying on a time
                 * sequence a clock skew.
                 */
                return;
            }

            if (!state.isActive()) {
                /* A localized state change to UNKNOWN, DETACHED, etc. */
                assert masterId == null;
                final RepNodeState rnState = get(rnId);
                rnState.updateRepState(state);
                return;
            }

            /*
             * Only update the time as a result of information from active
             * nodes which have information about the master state. Transitions
             * to a non active state do not update time, we wait for nodes
             * transitioning to active states to provide us with the correct
             * master when it becomes available; the information is slightly
             * inaccurate during any interregnum and request forwarding/retry
             * will deal with it.
             */
            lastChange = time;

            assert State.MASTER.equals(state) ? rnId.equals(masterId) : true;
            assert State.REPLICA.equals(state) ?
                   ((masterId !=  null) && !rnId.equals(masterId)) : true;

            masterId = State.MASTER.equals(state) ? rnId : masterId;
            groupMasterId = masterId;

            for (RepNodeState rnState : rns.values()) {
                if (rnState.getRepNodeId().equals(rnId)) {
                    rnState.updateRepState(state);
                    continue;
                } else if (rnState.getRepNodeId().equals(masterId)) {
                    /* The new master. */
                    rnState.updateRepState(State.MASTER);
                    continue;
                }

                /*
                 * Some other node, if it was a MASTER, change its state to a
                 * REPLICA, since we know it's no longer a master.
                 */
                if (rnState.getRepState().isMaster()) {
                    rnState.updateRepState(State.REPLICA);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Returns the load balanced RepNode for a request.
     *
     * @param request the request being dispatched
     * @param excludeRNs the set of RNs that must be excluded
     *
     * @return the load balanced active (master or replica) RN or null
     */
    public RepNodeState getLoadBalancedRN(Request request,
                                          Set<RepNodeId> excludeRNs) {
        try {
            lock.readLock().lock();
            return getLeastBusyRN(request, excludeRNs);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns the RN that is currently least busy as determined by the number
     * of active connections.
     * <p>
     * This rather simple strategy turns out to yield superior throughput
     * performance when compared to the strategy embodied in
     * {@link #getBestRespTimeRN(Set)}. This strategy provides a throughput
     * improvement of 25% on workloadA of the YCSB benchmark, effectively
     * saturating the disk with random IOs. One possible explanation is that
     * response times necessarily represent past history, while the number of
     * currently outstanding requests is a more current measure of the expected
     * response time from the node. This permits the dispatcher to react faster
     * to instantaneous changes on an RN or the network path. It's worth noting
     * that the number of open active connections is an indirect indicator of
     * response times. An RN that serves requests more promptly is more likely
     * to have fewer active connections and will therefore be chosen more often
     * as the target of a request dispatch.
     * <p>
     * This strategy may result in even activity distribution but unequal loads
     * across the RNs, if each operation consumes a different amount of
     * resources versus instead of a constant amount. For example, a single key
     * lookup versus a range lookup. In this case a more sophisticated strategy
     * that "weights" each active connection based upon its type could result
     * in a more even distribution. Thus using a single key lookup as the basic
     * unit operation, an active connection performing a "range" query would be
     * weighted by a factor of say three. More sophisticated techniques could
     * be used to weight the connections dynamically based upon the response
     * time associated with a specific operation type. For now, we favor
     * simplicity and treat all operation types as consuming the same resources.
     *
     * @param request the request that is to be dispatched to the RN
     *
     * @param excludeRNs the set of RNS to be excluded from consideration.
     *
     * @return the least busy active (master or replica) RN or null if there is
     * no clear choice, usually due to insufficient or out of date state
     * information
     */
    private RepNodeState getLeastBusyRN(Request request,
                                        Set<RepNodeId> excludeRNs) {

        long minRespTime = Integer.MAX_VALUE;
        RepNodeState minRN = null;
        int minActiveRequestCount = Integer.MAX_VALUE;

        for (RepNodeState rn : rns.values()) {
            if (((excludeRNs != null) &&
                  excludeRNs.contains(rn.getRepNodeId())) ||
                  rn.reqHandlerNeedsRepair() ||
                 !rn.getRepState().isActive()) {
                continue;
            }

            if (!request.isPermittedZone(rn.getZoneId())) {
                continue;
            }

            final Consistency consistency = request.getConsistency();

            if (!inConsistencyRange(rn, consistency)) {

                /*
                 * Filter out nodes that are laggards and are unlikely to
                 * satisfy consistency requirements
                 */
                continue;
            }

            final int avRespTimeMs = rn.getAvReadRespTimeMs();
            final int activeRequestCount = rn.getActiveRequestCount();

            if ((activeRequestCount > minActiveRequestCount) ||
                ((activeRequestCount == minActiveRequestCount) &&
                 (avRespTimeMs > minRespTime))) {
                 /* Use resp time as a tie breaker */
                continue;
            }

            /* A new least busy RN */
            minRN = rn;
            minRespTime = avRespTimeMs;
            minActiveRequestCount = activeRequestCount;
        }
        return minRN;
    }

    /**
     * Returns true if the rn looks like it's able to satisfy any consistency
     * requirements associated with the request
     *
     * @param rn the RN being evaluated
     *
     * @param consistency the consistency requirement that must be satisfied by
     * the RN
     */
    private boolean inConsistencyRange(RepNodeState rn,
                                       Consistency consistency) {

        if (consistency == null) {
            /* No consistency requirements to be satisfied. */
            return true;
        }
        if (consistency == Consistency.NONE_REQUIRED) {
            /* Any accessible node can satisfy this consistency. */
            return true;
        }

        if (consistency == Consistency.NONE_REQUIRED_NO_MASTER) {
            /* Only a replica can satisfy this consistency. */
            return !(rn.getRepNodeId().equals(groupMasterId));
        }

        if (consistency == Consistency.ABSOLUTE) {
            /* Only the master can satisfy this consistency. */
            return rn.getRepNodeId().equals(groupMasterId);
        }

        if (consistency instanceof Version) {
            final Version vConsistency = (Version) consistency;
            return rn.inConsistencyRange(System.currentTimeMillis(),
                                         vConsistency);
        }

        if (consistency instanceof Time) {
            final Time tConsistency = (Time) consistency;
            return rn.inConsistencyRange(System.currentTimeMillis(),
                                         tConsistency, getMaster());
        }

        throw new IllegalStateException("Unexpected consistency: " +
                                        consistency);
    }

    /**
     * Returns the RN that has the best responsive time, but has not exceeded
     * an activity threshold. It does this by prorating the allowed number of
     * active requests based upon relative responsive times. The lower the av
     * resp times at the node, the higher the proportion of activity directed
     * towards it. The average response time and therefore the target activity
     * percent is being continuously re-adjusted as a result of each response
     * from a node.
     * <p>
     * It's important to ensure that at least some minimal set of requests is
     * always directed at a node in order to maintain a reasonably current
     * average response time.
     * <p>
     * NOTE: We do not use this strategy currently, preferring the one that's
     * currently embodied in {@link #getLeastBusyRN(Set)} instead. The
     * reasons for this choice are embedded in the method's comments. The
     * method is retained here in case we find circumstances in future where
     * this strategy proves advantageous.
     *
     * @param excludeRNs the set of RNS to be excluded from consideration.
     *
     * @return the RN with the best response times
     */
    @SuppressWarnings("unused")
    private RepNodeState getBestRespTimeRN(Set<RepNodeId> excludeRNs) {
        final RepNodeState[] activeRNs = new RepNodeState[rns.size()];
        int sumAvRespTimesMs = 0;
        int totalActivity = 0;

        int index = 0;
        for (RepNodeState rn : rns.values()) {
            if (((excludeRNs != null) &&
                 excludeRNs.contains(rn.getRepNodeId())) ||
                !rn.getRepState().isActive()) {
                continue;
            }
            activeRNs[index++] = rn;
            sumAvRespTimesMs += rn.getAvReadRespTimeMs();
            totalActivity += rn.getActiveRequestCount();
        }

        /*
         * Try twice, first with thresholds in place and again, without
         * them.
         */
        for (boolean checkThreshold : new boolean[]{true, false}) {

            long minRespTime = Integer.MAX_VALUE;
            RepNodeState minRN = null;
            int minActiveRequestCount = Integer.MAX_VALUE;

            for (RepNodeState rn : activeRNs) {
                if (rn == null) {
                    break;
                }

                final int avRespTimeMs = rn.getAvReadRespTimeMs();
                final int activeRequestCount = rn.getActiveRequestCount();

                /*
                 * Response time is the principal consideration. Use
                 * current activity as a tie breaker. That is, direct
                 * direct request to the nodes that's less busy.
                 */
                if ((minRespTime <  avRespTimeMs) ||
                    ((minRespTime == avRespTimeMs) &&
                     (activeRequestCount > minActiveRequestCount))) {
                    continue;
                }

                /*
                 * Ensure that requests are distributed in proportion to
                 * their load.
                 */
                if (checkThreshold &&
                    !belowActivityThreshold(avRespTimeMs,
                                            sumAvRespTimesMs,
                                            activeRequestCount,
                                            totalActivity)) {
                    continue;
                }

                minRN = rn;
                minRespTime = avRespTimeMs;
                minActiveRequestCount = activeRequestCount;
            }

            if (minRN != null) {
                return minRN;
            }

            /* Try again without accounting for activity thresholds. */
        }
        return null;
    }

    /**
     *
     * Returns true if the activity at that node is below the current activity
     * threshold. It does this by prorating the allowed number of active
     * requests based upon relative responsive times. The lower the av resp
     * times at the node, the higher the proportion of activity directed
     * towards it. The average response time and therefore the target activity
     * percent is being continuously re-adjusted as a result of each response
     * from a node.
     * <p>
     * It's important to ensure that at least some minimal set of requests is
     * always directed at a node in order to maintain a reasonably current av
     * response time.
     * <p>
     *
     * @param avRespTimeMs
     * @param sumAvRespTimesMs
     * @param activeRequestCount
     * @param totalActivity
     */
    private boolean belowActivityThreshold(int avRespTimeMs,
                                           int sumAvRespTimesMs,
                                           int activeRequestCount,
                                           int totalActivity) {

        if ((totalActivity == 0) || (sumAvRespTimesMs == 0)) {
            /* No current activity, or < 1 ms resp times. */
            return true;
        }

        /* The lower the av resp times, the larger the target activity */
        final int targetActivityPercent =
            ((sumAvRespTimesMs - avRespTimeMs) * 100) / sumAvRespTimesMs;

        return ((activeRequestCount * 100) / totalActivity) <
            targetActivityPercent;
    }

    /**
     * Returns a random RN from within the group. This method is used as a
     * fallback when no state information is available for any of the nodes in
     * the RG. It represents a "last ditch" attempt to pick some RN in the
     * rep group to which a request can be dispatched. If no RNs are in the
     * group, or all are excluded, null is returned.
     *
     * @param request the request being dispatched, or null for a NOP request
     * @param excludeRNs the set of RNs that must be excluded
     * @return a randomly chosen RN or null
     */
    public RepNodeState getRandomRN(Request request,
                                    Set<RepNodeId> excludeRNs) {

        if (rns.isEmpty()) {
            return null;
        }

        try {
            lock.readLock().lock();
            int rnIndex = randomRN++ % rns.size();
            for (RepNodeState rn : rns.values()) {
                rnIndex--;
                if ((excludeRNs != null) &&
                    excludeRNs.contains(rn.getRepNodeId())) {
                    continue;
                }
                if ((request != null) &&
                    !request.isPermittedZone(rn.getZoneId())) {
                    continue;
                }
                if (rnIndex < 0) {
                    return rn;
                }
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }
}
