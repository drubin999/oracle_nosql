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

package oracle.kv.impl.admin.topo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.param.StorageNodePool;
import oracle.kv.impl.admin.topo.Validations.OverCapacity;
import oracle.kv.impl.admin.topo.Validations.RulesProblem;
import oracle.kv.impl.admin.topo.Validations.UnderCapacity;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.Partition;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;

/**
 * Topologies are governed by constraints on the relationships of componenents
 * its use of physical resources.
 *
 * Component relationship constraints:
 * ----------------------------------
 * Each datacenter must have a complete copy of the data.
 * A shard should be tolerant of a single point of failure, and to facilitate
 *  that, each RN of a shard must be on a different SN
 * The number of partitions is fixed for the lifetime of a store.
 * Each shard must have an RN in an SN in a primary data center
 *
 * Physical resource constraints:
 * ------------------------------
 * Each SN can only host a maximum of <capacity> number of RNs
 * Additional rules will be added about network affinity, physical fault groups,
 * etc
 *
 * Inputs to a topology:
 * DC   Data center
 * NS   Network switch  Optional
 * SN   Storage node    Associated with a DC, possibly a NS
 * RF   Replication factor              Per data center, associated with S
 * P    Partition       Administrator   # fixed for life of store
 * C    Capacity        Administrator   Per SN
 * MP   Mount point     Administrator   Optional, per SN
 * ADM  Admin service   Administrator
 *
 * Outputs (calculated by topology builder)
 * S    Shard (formally replication group)
 * RN   Replication node Associated with a S, assigned to a SN
 * ARB  Arbiter          Associated with a S, assigned to a SN
 *
 * The following rules are used to enforce the constraints:
 *
 *  1. Each datacenter must have the same set of shards.
 *  2. Each shard must have repFactor number of RNs
 *  3. Each RN of an S must reside on a different SN.
 *  3a. If, and only if, the DC RF is 2, an ARB must be deployed for each S (on
 *      an SN different from the SNs hosting the shard's RNs).
 *  4. The #RNSN <= CSN
 *  5. Each shard must have an RN in an SN in a primary datacenter
 *
 * Warnings:
 *
 * A SN is under capacity.
 * A store should have a additional shards.
 * A shard has no partitions.
 *
 *
 * Not yet implemented:
 *  If NS are present, then depending on the network affinity mode2,
 *        1. each RN of an S must reside on the same NS or
 *        2. each RN of an S must reside on different NSs.
 *
 * The decision to deploy an ARB is per-DC; therefore each S could have an ARB
 * in each DC.
 *
 * There are two network affinity modes, a) optimize for performance, requiring
 * that replication nodes of the same shard reside on the same switch, and b)
 * optimize for availability, requiring that replication nodes of the same
 * shard reside on different switches.
 */
public class Rules {
    public static int MAX_REPLICATION_FACTOR = 20;

    /**
     * Validation checks that apply to a single topology are:
     * -Every datacenter that is present in the pool of SNs used to deploy the
     *   topology has a complete copy of the data.
     * -all shards house at least one partition
     * -all shards have the datacenter-specified repfactor in each datacenter
     * -all shards have their RNs on different nodes.
     * -each SN is within capacity.
     */
    public static Results validate(Topology topo, Parameters params,
                                   boolean topoIsDeployed) {

        Results results = new Results();

        /*
         * Do some processing of the topology and parameters to map out
         * relationships that are embodied in the topo and params, but are
         * not stated in a convenient way for validation.
         */

        /*
         * Hang onto which shards are in which datacenters, and how many
         * RNs are in each shard (by datacenter).
         *
         * Also sort RNs by SN, for memory size validation.
         */
        Map<DatacenterId, Map<RepGroupId, Integer>> shardsByDC =
            new HashMap<DatacenterId, Map<RepGroupId, Integer>>();

        for (Datacenter dc: topo.getSortedDatacenters()) {
            shardsByDC.put(dc.getResourceId(),
                           new HashMap<RepGroupId,Integer>());
        }

        Map<StorageNodeId, Set<RepNodeId>> rnsPerSN =
            new HashMap<StorageNodeId, Set<RepNodeId>>();

        for (RepNode rn : topo.getSortedRepNodes()) {
            StorageNodeId snId = rn.getStorageNodeId();
            StorageNode hostSN = topo.get(snId);
            DatacenterId dcId = hostSN.getDatacenterId();
            RepGroupId rgId = rn.getRepGroupId();

            Map<RepGroupId,Integer> shardCount = shardsByDC.get(dcId);
            Integer count = shardCount.get(rgId);
            if (count == null) {
                shardCount.put(rgId, new Integer(1));
            } else {
                shardCount.put(rgId, new Integer(count.intValue() + 1));
            }

            Set<RepNodeId> rnIds = rnsPerSN.get(snId);
            if (rnIds == null) {
                rnIds = new HashSet<RepNodeId>();
                rnsPerSN.put(snId, rnIds);
            }
            rnIds.add(rn.getResourceId());
        }

        /* Do all DCs have the same set of shards? */
        Set<RepGroupId> allShardIds = topo.getRepGroupIds();
        for (Map.Entry<DatacenterId, Map<RepGroupId,Integer>> dcInfo:
                 shardsByDC.entrySet()) {

            /*
             * Find the shards that are in the topology, but are not in
             * this datacenter.
             */
            Set<RepGroupId> shards = dcInfo.getValue().keySet();
            Set<RepGroupId> missing =  relComplement(allShardIds, shards);
            DatacenterId dcId = dcInfo.getKey();
            int repFactor =  topo.get(dcId).getRepFactor();
            for (RepGroupId rgId : missing) {
                results.add(new Validations.InsufficientRNs(dcId, repFactor,
                                                           rgId,repFactor));
            }
        }

        /* Do all shards for a DC have the right repfactor number of RNs? */
        for (Map.Entry<DatacenterId,Map<RepGroupId,Integer>> dcInfo:
                 shardsByDC.entrySet()) {

            /* RepFactor is set per datacenter */
            DatacenterId dcId = dcInfo.getKey();
            int repFactor = topo.get(dcId).getRepFactor();

            for (Map.Entry<RepGroupId,Integer> shardInfo :
                     dcInfo.getValue().entrySet()) {
                int numRNs = shardInfo.getValue();
                if (numRNs < repFactor) {
                    results.add
                        (new Validations.InsufficientRNs(dcId,
                                                       repFactor,
                                                       shardInfo.getKey(),
                                                       repFactor - numRNs));
                } else if (numRNs > repFactor) {
                    results.add
                        (new Validations.ExcessRNs(dcId,
                                                 repFactor,
                                                 shardInfo.getKey(),
                                                 numRNs - repFactor));
                }
            }
        }

        /* All RNs of a single shard must be on different SNs. */
        for (RepGroupId rgId : topo.getRepGroupIds()) {

            /* Organize RNIds by SNId */
            Map<StorageNodeId, Set<RepNodeId>> rnIdBySNId = new
                HashMap<StorageNodeId, Set<RepNodeId>>();

            for (RepNode rn : topo.get(rgId).getRepNodes()) {
                StorageNodeId snId = rn.getStorageNodeId();
                Set<RepNodeId> rnIds = rnIdBySNId.get(snId);
                if (rnIds == null) {
                    rnIds = new HashSet<RepNodeId>();
                    rnIdBySNId.put(rn.getStorageNodeId(), rnIds);
                }
                rnIds.add(rn.getResourceId());
            }

            /*
             * If any of the SNs used by this shard have more than one RN,
             * report it.
             */
            for (Map.Entry<StorageNodeId, Set<RepNodeId>> entry :
                rnIdBySNId.entrySet()) {
                if (entry.getValue().size() > 1) {
                    StorageNodeId snId = entry.getKey();
                    results.add(new Validations.RNProximity
                                 (snId, rgId,
                                  new ArrayList<RepNodeId>(entry.getValue())));
                }
            }
        }

        /* Each shard must have an RN in an SN in a primary DC */
        final Set<RepGroupId> shardsWithoutPrimaryDC = topo.getRepGroupIds();
        for (final Map.Entry<DatacenterId, Map<RepGroupId, Integer>> dcInfo :
                 shardsByDC.entrySet()) {
            final DatacenterId dcId = dcInfo.getKey();
            final Datacenter dc = topo.get(dcId);
            if (dc.getDatacenterType().isPrimary()) {
                final Map<RepGroupId, Integer> repGroupInfo =
                    dcInfo.getValue();
                shardsWithoutPrimaryDC.removeAll(repGroupInfo.keySet());
            }
        }
        for (final RepGroupId rgId : shardsWithoutPrimaryDC) {
            results.add(new Validations.NoPrimaryDC(rgId));
        }

        /* Are all SNs are within capacity? */
        CapacityProfile profile = getCapacityProfile(topo, params);
        for (RulesProblem p : profile.getProblems()) {
            results.add(p);
        }

        /* Warn about under capacity SNs. */
        for (RulesProblem w: profile.getWarnings()) {
            results.add(w);
        }

        /*
         * Check partition assignments. If a redistribution plan is interrupted
         * shards may have non-optimal numbers of partitions.
         */
        int totalPartitions = topo.getPartitionMap().size();
        if (totalPartitions != 0) {
            Set<RepGroupId> allShards = topo.getRepGroupIds();
            int totalShards = allShards.size();
            int minPartitions = Rules.calcMinPartitions(totalPartitions,
                                                        totalShards);
            int maxPartitions = Rules.calcMaxPartitions(totalPartitions,
                                                        totalShards);

            Map<RepGroupId, AtomicInteger> partCounter =
                new HashMap<RepGroupId, AtomicInteger>();
            for (RepGroupId rgId: allShards) {
                partCounter.put(rgId, new AtomicInteger(0));
            }

            /* Get a partition count for each shard */
            for (Partition p: topo.getPartitionMap().getAll()) {
                AtomicInteger count = partCounter.get(p.getRepGroupId());
                count.incrementAndGet();
            }

            /*
             * Review the count for each shard, and flag those that are over or
             * under the minimum and maximum.
             */
            for (RepGroupId rgId: allShards) {
                int actualCount = partCounter.get(rgId).get();
                if ((actualCount < minPartitions) ||
                    (actualCount > maxPartitions)) {
                    results.add(new Validations.NonOptimalNumPartitions
                                (rgId, actualCount, minPartitions,
                                 maxPartitions));
                }
            }
        }

        if (topoIsDeployed) {
            /*
             * Check environment placement.
             */
            findMultipleRNsInRootDir(topo, params, results);

            /*
             * SN memoryMB and RN heap are optional parameters. If these have
             * been specified, make sure that the sum of all RN JVM heaps does
             * not exceed the memory specified on the SN.
             */
            checkMemoryUsageOnSN(rnsPerSN, params, results);
        }
        return results;
    }

    private static void findMultipleRNsInRootDir(Topology topo,
                                                 Parameters params,
                                                 Results results) {
        /*
         * Highlight SNs which have multiple RNs in the root directory. This
         * validation step is only possible when validate has been called on a
         * deployed topology. Before a deployment, the RepNodeParams mountPoint
         * field is not initialized, and the RepNodeParams themselves may not
         * exist.
         */
        Map<StorageNodeId, List<RepNodeId>> rnsOnRoot =
            new HashMap<StorageNodeId, List<RepNodeId>>();

        for (RepNode rn: topo.getSortedRepNodes()) {
            StorageNodeId snId = rn.getStorageNodeId();
            RepNodeId rnId = rn.getResourceId();

            RepNodeParams rnp = params.get(rnId);
            if (rnp == null) {
                continue;
            }

            if (rnp.getMountPointString() == null) {
                List<RepNodeId> onRoot = rnsOnRoot.get(snId);
                if (onRoot == null) {
                    onRoot = new ArrayList<RepNodeId>();
                    rnsOnRoot.put(snId, onRoot);
                }
                onRoot.add(rnId);
            }
        }

        for (Map.Entry<StorageNodeId, List<RepNodeId>> snRoot :
                 rnsOnRoot.entrySet()) {
            if (snRoot.getValue().size() == 1) {
                /* 1 RN on the root, no issue with that. */
                continue;
            }
            StorageNodeId snId = snRoot.getKey();
            results.add(new Validations.MultipleRNsInRoot
                        (snId,
                         snRoot.getValue(),
                         params.get(snId).getRootDirPath()));
        }
    }

    /*
     * For SNs that have a memory_mb param value, check that the heap of the
     * RNs hosted on that SN don't exceed the SN memory.
     */
    private static void checkMemoryUsageOnSN(Map<StorageNodeId,
                                             Set<RepNodeId>> rnsPerSN,
                                             Parameters params,
                                             Results results) {

        for (Map.Entry<StorageNodeId, Set<RepNodeId>> entry :
            rnsPerSN.entrySet()) {
            StorageNodeId snId = entry.getKey();
            StorageNodeParams snp = params.get(snId);
            if (snp.getMemoryMB() == 0) {
                continue;
            }

            long totalRNHeapMB = 0;
            Set<RepNodeId> rnIds = entry.getValue();

            for (RepNodeId rnId : rnIds) {
                RepNodeParams rnp = params.get(rnId);
                if (rnp == null) {
                    continue;
                }
                totalRNHeapMB += params.get(rnId).getMaxHeapMB();
            }

            if (totalRNHeapMB > snp.getMemoryMB()) {
                StringBuilder sb = new StringBuilder();
                for (RepNodeId rnId : rnIds) {
                    sb.append(rnId).append(" ").
                    append(params.get(rnId).getMaxHeapMB()).append("MB,");
                }
                results.add(new Validations.RNHeapExceedsSNMemory
                            (snId, snp.getMemoryMB(), rnIds, totalRNHeapMB,
                             sb.toString()));
            }
        }
    }

    /**
     * Check that a transition from source to candidate topology is possible.
     *
     * -Once deployed, the number of partitions in a store cannot change.
     * -A shard cannot be concurrently involved in a partition migration AND
     * have an RN that is being moved from one SN to another - TODO: move to
     * task generator class
     *
     * -For R2, we do not support contraction, so there cannot be
     *   -shards that are in the source, but are not in the candidate.
     *   -RNs that are in the source, but are not in the candidate.
     */

    public static void validateTransition(Topology source,
                                          TopologyCandidate candidate,
                                          Parameters params) {

        Topology target = candidate.getTopology();

        /*
         * Make sure that all the SNs and mount points in the candidate
         * still exist. It's possible that the user has removed them.
         */
        for (StorageNodeId snId : target.getStorageNodeIds()) {
            if (params.get(snId) == null) {
                throw new IllegalCommandException
                    ("Topology candidate " + candidate.getName() + " uses " +
                     snId + " but " + snId + " has been removed.");
            }
        }

        checkAllMountPointsExist(candidate, params);

        if (source.getRepGroupMap().size() == 0) {
            /*
             * Right now, if there are no shards in the source, there is
             * nothing else to check in the transition.
             */
            return;
        }

        /*
         * If the store has already been deployed, check that the number of
         * partitions has not changed.
         */

        Collection<PartitionId> sourcePartitionIds =
            source.getPartitionMap().getAllIds();
        Collection<PartitionId> targetPartitionIds =
            target.getPartitionMap().getAllIds();

        /* The source and target must have the same number of partitions. */
        if ((sourcePartitionIds.size() != 0) && 
            (sourcePartitionIds.size() != targetPartitionIds.size())) {
            throw new IllegalCommandException
                ("The current topology has been initialized and has " +
                 sourcePartitionIds.size() +
                 " partitions while the target (" + candidate.getName() +
                 ")has " + targetPartitionIds.size() +
                 " partitions. The number of partitions in a store cannot be"+
                 " changed");
        }

        /* The source and target must have the same set of partitions. */
        Set<PartitionId> mustExistCopy =
            new HashSet<PartitionId>(sourcePartitionIds);
        mustExistCopy.removeAll(targetPartitionIds);
        if (!mustExistCopy.isEmpty()) {
            throw new IllegalCommandException
                ("These partitions are missing from " +
                 candidate.getName() + ":" + mustExistCopy);
        }

        /* Any RNs that are in the source must be in the destination. */
        Set<RepNodeId> sourceRNsCopy =
            new HashSet<RepNodeId>(source.getRepNodeIds());
        Set<RepNodeId> targetRNs = target.getRepNodeIds();
        sourceRNsCopy.removeAll(targetRNs);
        if (!sourceRNsCopy.isEmpty()) {
            throw new IllegalCommandException
                ("These RepNodes are missing from " +
                 candidate.getName() + ":" + sourceRNsCopy);
        }
    }

    public static void checkAllMountPointsExist(TopologyCandidate candidate,
                                                Parameters params) {
        for (Map.Entry<RepNodeId, String> entry :
                 candidate.getAllMountPoints().entrySet()) {
            RepNodeId rnId = entry.getKey();
            StorageNodeId snId =
                candidate.getTopology().get(rnId).getStorageNodeId();
            String mountPoint = entry.getValue();
            if (!mountPointExists(mountPoint,
                                  params.get(snId).getMountPoints())) {
                throw new IllegalCommandException
                    ("Topology candidate " + candidate.getName() +
                     " uses storage directory " + snId + ", " + mountPoint +
                     " but it has been removed.");
            }
        }
    }

    private static boolean mountPointExists(String mountPoint,
                                            List<String> mpList) {
        for (String mp : mpList) {
            if (mp.equals(mountPoint)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Ensure that the replication factor is in the valid range.
     */
    public static void validateReplicationFactor(final int repFactor) {
        if (repFactor <= 0 || repFactor > MAX_REPLICATION_FACTOR) {
            throw new IllegalArgumentException
                ("Illegal replication factor: " + repFactor +
                 ", valid range is 1 to " + MAX_REPLICATION_FACTOR);
        }
    }

    /**
     * Check if "rn" can be placed on this SN. Account for constraints that can
     * be considered in the scope of a single SN, like capacity and RN
     * consanguinity.
     */
    static boolean checkRNPlacement(StorageNodeParams snp,
                                    Set<RepNodeId>assignedRNs,
                                    int proposedRG,
                                    boolean ignoreCapacity,
                                    Logger logger) {

        if (!ignoreCapacity &&
            (assignedRNs.size() >= snp.getCapacity())) {
            logger.log(Level.FINEST, "{0} capacity={1}, num hosted RNS={2}",
                       new Object[]{snp.getStorageNodeId(), snp.getCapacity(),
                                    assignedRNs.size()});
            return false;
        }

        for (RepNodeId r : assignedRNs) {
            if (r.getGroupId() == proposedRG) {
                logger.log(Level.FINEST,
                           "{0} already has RN {1} from same shard as {2}",
                           new Object[]{snp.getStorageNodeId(), r, proposedRG});
                return false;
            }
        }

        return true;
    }

    /**
     * Return the relative complement of A and B. That is,return all the items
     * in A that are not also in B.
     */
    private static Set<RepGroupId> relComplement(Set<RepGroupId> a,
                                                 Set<RepGroupId> b) {

        Set<RepGroupId> copyA = new HashSet<RepGroupId>(a);
        copyA.removeAll(b);
        return copyA;
    }

    /**
     * Create a profile which contains over and under utilized SNs, capacity
     * problems and warnings, and the excessive capacity count for a topology.
     *
     * This does not account for datacenter topology (one datacenter may gate
     * the others in terms of how many shards can be supported, so some RN
     * slots in a given datacenter may intentionally be unused), so this method
     * would have to be updated when there is datacenter support.
     */
    static CapacityProfile getCapacityProfile(Topology topo,
                                              Parameters params) {

        Map<StorageNodeId,Integer> rnCount =
            new HashMap<StorageNodeId,Integer>();

        for (StorageNodeId snId : topo.getStorageNodeIds()) {
            rnCount.put(snId, new Integer(0));
        }

        for (RepNode rn: topo.getSortedRepNodes()) {
            StorageNodeId snId = rn.getStorageNodeId();
            int currentCount = rnCount.get(snId);
            rnCount.put(snId, new Integer(++currentCount));
        }

        CapacityProfile profile = new CapacityProfile();
        for (StorageNodeId snId : topo.getStorageNodeIds()) {
            int count = rnCount.get(snId);
            int capacity = params.get(snId).getCapacity();
            profile.noteUtilization(snId, capacity, count);
        }
        return profile;
    }

    /**
     * Given a total number of shards and partitions, calculate the minimum
     * number of partition should have.
     */
    static int calcMinPartitions(int totalPartitions, int totalShards) {
        return totalPartitions/totalShards;
    }

    /**
     * Given a total number of shards and partitions, calculate the maximum
     * number of partition should have.
     */
    static int calcMaxPartitions(int totalPartitions, int totalShards) {
        int max = totalPartitions/totalShards;
        if ((totalPartitions % totalShards) != 0) {
            return max + 1;
        }
        return max;
    }

    /**
     * Packages information about the capacity characteristics of a topology.
     * List SNs that are over or under utilized, and capacity violations and
     * warnings.
     *
     * Also includes an excessCapacity count, which is a measure of all unused
     * slots. Note that this count can be < 0 if the SNs are under utilized.
     * Likewise, a 0 excessCapacity doesn't mean that the topology is totally
     * in compliance. It could be a result of a combination of over and under
     * capacity SNs.
     */
    static class CapacityProfile {

        private int excessCapacity;
        private final List<StorageNodeId> overUtilizedSNs;
        private final List<StorageNodeId> underUtilizedSNs;
        private final List<RulesProblem> problems;
        private final List<RulesProblem> warnings;

        CapacityProfile() {
            overUtilizedSNs = new ArrayList<StorageNodeId>();
            underUtilizedSNs = new ArrayList<StorageNodeId>();
            problems = new ArrayList<RulesProblem>();
            warnings = new ArrayList<RulesProblem>();
        }

        public List<RulesProblem> getWarnings() {
            return warnings;
        }

        List<RulesProblem> getProblems() {
            return problems;
        }

        /**
         * If the capacity and RN count don't match, record the discrepancy.
         */
        private void noteUtilization(StorageNodeId snId,
                                     int snCapacity,
                                     int rnCount) {
            int capacityDelta = snCapacity - rnCount;
            excessCapacity += capacityDelta;
            if (capacityDelta > 0) {
                UnderCapacity under =
                    new UnderCapacity(snId, rnCount, snCapacity);
                underUtilizedSNs.add(snId);
                warnings.add(under);
            } else if (capacityDelta < 0) {

                OverCapacity over = new OverCapacity(snId, rnCount, snCapacity);
                overUtilizedSNs.add(snId);
                problems.add(over);
            }
        }

        int getExcessCapacity() {
            return excessCapacity;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("excess=").append(excessCapacity).append("\n");
            sb.append("problems:").append(problems).append("\n");
            sb.append("warnings:").append(warnings).append("\n");
            return sb.toString();
        }
    }

    /**
     * Return the total sum capacity of all the SNs in the specified pool.
     * Do not consider whether there are any RNs assigned to those SNs yet.
     */
    static int calculateMaximumCapacity(StorageNodePool snPool,
                                        Parameters params) {

        int capacityCount = 0;
        for (StorageNodeId snId : snPool.getList()) {
            capacityCount += params.get(snId).getCapacity();
        }
        return capacityCount;
    }

    /**
     * A Boolean predicate for rules problems.
     */
    public interface RulesProblemFilter<T extends RulesProblem> {

        /**
         * Returns whether the problem matches the predicate.
         */
        boolean match(T problem);
    }

    public static class Results {
        private final List<RulesProblem> violations;
        private final List<RulesProblem> warnings;

        Results() {
            violations = new ArrayList<RulesProblem>();
            warnings = new ArrayList<RulesProblem>();
        }

        void add(RulesProblem p) {
            if (p.isViolation()) {
                violations.add(p);
            } else {
                warnings.add(p);
            }
        }

        public List<RulesProblem> getViolations() {
            return violations;
        }

        public List<RulesProblem> getWarnings() {
            return warnings;
        }

        /**
         * Return both violations and warnings.
         */
        public List<RulesProblem> getProblems() {
            List<RulesProblem> all = new ArrayList<RulesProblem>(violations);
            all.addAll(warnings);
            return all;
        }

        public int numProblems() {
            return violations.size() + warnings.size();
        }

        public int numViolations() {
            return violations.size();
        }

        /**
         * Return a list of all of this kind of problem.
         */
        public <T extends RulesProblem> List<T> find(Class<T> problemClass) {
            List<T> p = new ArrayList<T>();
            for (RulesProblem rp : violations) {
                if (rp.getClass().equals(problemClass)) {
                    p.add(problemClass.cast(rp));
                }
            }

            for (RulesProblem rp : warnings) {
                if (rp.getClass().equals(problemClass)) {
                    p.add(problemClass.cast(rp));
                }
            }

            return p;
        }

        /**
         * Return a list of the specified kind of problem that match the
         * filter.
         */
        public <T extends RulesProblem> List<T> find(
            final Class<T> problemClass, final RulesProblemFilter<T> filter) {

            final List<T> p = new ArrayList<T>();
            for (final RulesProblem rp : violations) {
                if (rp.getClass().equals(problemClass)) {
                    final T crp = problemClass.cast(rp);
                    if (filter.match(crp)) {
                        p.add(crp);
                    }
                }
            }

            for (final RulesProblem rp : warnings) {
                if (rp.getClass().equals(problemClass)) {
                    final T crp = problemClass.cast(rp);
                    if (filter.match(crp)) {
                        p.add(problemClass.cast(rp));
                    }
                }
            }

            return p;
        }

        @Override
        public String toString() {
            if (numProblems() == 0) {
                return "No problems";
            }

            StringBuilder sb = new StringBuilder();

            int numViolations = violations.size();
            if (numViolations > 0) {
                sb.append(numViolations);
                sb.append((numViolations == 1) ? " violation.\n" :
                          " violations.\n");

                for (RulesProblem r : violations) {
                    sb.append(r).append("\n");
                }
            }

            int numWarnings = warnings.size();
            if (numWarnings > 0) {
                sb.append(numWarnings);
                sb.append((numWarnings == 1) ? " warning.\n" :
                          " warnings.\n");
                for (RulesProblem r : warnings) {
                    sb.append(r).append("\n");
                }
            }

            return sb.toString();
        }


        /**
         * Find all the issues that are in this results class, but are
         * not in "other".
         */
        public Results remove(Results other) {
            Results diff = new Results();
            List<RulesProblem> pruned = new ArrayList<RulesProblem>(violations);
            pruned.removeAll(other.violations);
            diff.violations.addAll(pruned);

            pruned = new ArrayList<RulesProblem>(warnings);
            pruned.removeAll(other.warnings);
            diff.warnings.addAll(pruned);
            return diff;
        }
    }
}
