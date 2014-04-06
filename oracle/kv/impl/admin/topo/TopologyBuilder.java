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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.AdminServiceParams;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.param.StorageNodePool;
import oracle.kv.impl.admin.topo.Rules.Results;
import oracle.kv.impl.admin.topo.Rules.RulesProblemFilter;
import oracle.kv.impl.admin.topo.Validations.InsufficientRNs;
import oracle.kv.impl.admin.topo.Validations.OverCapacity;
import oracle.kv.impl.admin.topo.Validations.RNProximity;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.Partition;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.TopologyPrinter;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * Generate a topology candidate which uses the resources provided to the
 * builder.
 * TODO: ensure that the parameters are not modified while we are building a
 * topology. Get a copy of the params?
 */
public class TopologyBuilder {

    private final Topology sourceTopo;
    private final String candidateName;
    private final StorageNodePool snPool;
    private final int numPartitions;
    private final Parameters params;
    private final Logger logger;

    /**
     * This constructor is for an initial deployment where the number of
     * partitions is specified by the user.
     */
    public TopologyBuilder(Topology sourceTopo,
                           String candidateName,
                           StorageNodePool snPool,
                           int numPartitions,
                           Parameters params,
                           AdminServiceParams adminParams) {
        this(sourceTopo, candidateName, snPool, numPartitions, params,
             adminParams, true);
        if ((sourceTopo.getPartitionMap().size() > 0) &&
            (sourceTopo.getPartitionMap().size() != numPartitions)) {
            throw new IllegalCommandException
                ("The number of partitions cannot be changed.");
        }
    }

    private TopologyBuilder(Topology sourceTopo,
                            String candidateName,
                            StorageNodePool snPool,
                            int numPartitions,
                            Parameters params,
                            AdminServiceParams adminParams,
                            boolean isInitial) {
        this.sourceTopo = sourceTopo;
        this.candidateName = candidateName;
        this.snPool = snPool;
        this.numPartitions = numPartitions;
        this.params = params;

        logger = LoggerUtils.getLogger(this.getClass(), adminParams);

        /*
         * Validate inputs
         */
        if (snPool.size() < 1) {
            throw new IllegalCommandException(
                "Storage pool " + snPool.getName() + " must not be empty");
        }

        /*
         * Ensure that the snpool is a superset of those that already host RNs.
         * and that all SNs exist and were not previously remove.
         */
        checkSNs();

        /*
         * The number of partitions must be >= the total capacity of the SN
         * pool divided by the total replication factor of all of the data
         * centers.
         */
        int totalCapacity = 0;
        int totalRF = 0;
        boolean foundPrimaryDC = false;
        final Set<DatacenterId> dcs = new HashSet<DatacenterId>();
        for (final StorageNodeId snId : snPool) {
            final StorageNodeParams snp = params.get(snId);
            totalCapacity += snp.getCapacity();
            final DatacenterId dcId = sourceTopo.get(snId).getDatacenterId();
            if (dcs.add(dcId)) {
                totalRF += sourceTopo.get(dcId).getRepFactor();
                final Datacenter dc = sourceTopo.get(dcId);
                if (dc.getDatacenterType().isPrimary()) {
                    foundPrimaryDC = true;
                }
            }
        }
        if (!foundPrimaryDC) {
            throw new IllegalCommandException(
                "Storage pool " + snPool.getName() +
                " must contain SNs in a primary zone");
        }
        final int minPartitions = totalCapacity / totalRF;
        if (numPartitions < minPartitions) {
            if (isInitial) {
                throw new IllegalCommandException(
                    "The number of partitions requested (" + numPartitions +
                    ") is too small.  There must be at least as many" +
                    " partitions as the total SN capacity in the storage node" +
                    " pool (" + totalCapacity + ") divided by the total" +
                    " replication factor (" + totalRF + "), which is " +
                    minPartitions + ".");
            }
            if (numPartitions == 0) {
                throw new IllegalCommandException(
                    "topology create must be run before any other topology " +
                    "commands.");
            }
            throw new IllegalCommandException(
                "The number of partitions (" + numPartitions +
                ") cannot be smaller than the total SN capacity in the" +
                " storage node pool (" + totalCapacity +
                ") divided by the total replication factor (" +
                totalRF + "), which is " + minPartitions + ".");
        }
    }

    /**
     * Use for an existing store, when we have to rebalance or
     * redistribute. The number of partitions in the store is fixed, and
     * determined by the initial deployment.
     */
    public TopologyBuilder(TopologyCandidate origCandidate,
                           StorageNodePool snPool,
                           Parameters params,
                           AdminServiceParams adminParams) {
        this(origCandidate.getTopology(),
             origCandidate.getName(),
             snPool,
             origCandidate.getTopology().getPartitionMap().size(),
             params,
             adminParams,
             false);
    }

    /**
     * Correct any non-compliant aspects of the topology.
     */
    public TopologyCandidate rebalance(DatacenterId dcId) {
        return assignMountPoints(
            rebalance(
                new TopologyCandidate(candidateName, sourceTopo.getCopy()),
                dcId));
    }

    /**
     * This flavor used when rebalance is one step for other topology
     * building commands, and does not assign mount points.
     */
    private TopologyCandidate rebalance(final TopologyCandidate startingPoint,
                                        final DatacenterId dcId) {
        if (dcId != null) {
            if (startingPoint.getTopology().get(dcId) == null) {
                throw new IllegalCommandException(dcId +
                                                  " is not a valid zone");
            }
        }

        final Results results =
            Rules.validate(startingPoint.getTopology(), params, false);

        /* Check for rebalance problems in the validation results. */
        final List<RNProximity> proximity = results.find(
            RNProximity.class,
            new RulesProblemFilter<RNProximity>() {
                @Override
                public boolean match(final RNProximity p) {
                    return filterByDC(dcId, p.getSNId());
                }
            });
        final List<OverCapacity> overCap = results.find(
            OverCapacity.class,
            new RulesProblemFilter<OverCapacity>() {
                @Override
                public boolean match(final OverCapacity p) {
                    return filterByDC(dcId, p.getSNId());
                }
            });
        final List<InsufficientRNs> insufficient = results.find(
            InsufficientRNs.class,
            new RulesProblemFilter<InsufficientRNs>() {
                @Override
                public boolean match(final InsufficientRNs p) {
                    return ((dcId == null) || dcId.equals(p.getDCId()));
                }
            });

        if (proximity.isEmpty() && overCap.isEmpty() &&
            insufficient.isEmpty()) {
            logger.info(startingPoint + " has nothing to rebalance");
            return startingPoint;
        }

        /*
         * Map out the current layout -- that is, the relationship between all
         * topology components. This is derived from the topology, but fills in
         * the relationships that are implicit but are not stored as fields in
         * the topology.
         */
        final StoreDescriptor currentLayout =
            new StoreDescriptor(startingPoint.getTopology(), params, snPool);
        final TopologyCandidate candidate =
            new TopologyCandidate(startingPoint.getName(),
                                  startingPoint.getTopology().getCopy());
        final Topology candidateTopo = candidate.getTopology();

        /*
         * Fix the violations first.
         */

        /*
         * RNProximity: Find SNs with RNs that are from the same shard and move
         * all but the first one. They should all be fairly equal in cost.
         */
        logger.log(Level.FINE, "{0} has {1} RN proximity problems to fix",
                   new Object[]{candidate, proximity.size()});

        for (RNProximity r : proximity) {
            final int siblingCount = r.getRNList().size();
            moveRNs(candidateTopo, r.getSNId(), currentLayout,
                    r.getRNList().subList(1, siblingCount));
        }

        /*
         * OverCapacity: move enough RNs off this SN to get it down to
         * budget. Since this decision is done statically, it's hard to predict
         * if any RNs should be preferred as targets over another, so just pick
         * them arbitrarily
         */
        logger.log(Level.FINE, "{0} has {1} over capacity problems to fix",
                   new Object[]{candidate, overCap.size()});

        for (OverCapacity c : overCap) {
            final int needToMove = c.getExcess();
            final SNDescriptor owningSND = currentLayout.getSND(c.getSNId(),
                                                                candidateTopo);
            moveRNs(candidateTopo, c.getSNId(), currentLayout,
                    owningSND.getNRns(needToMove));
        }

        /*
         * InsufficientRNs: add RNs that are missing
         */
        logger.log(Level.FINE,
                   "{0} has {1} shards with insufficient rep factor",
                   new Object[]{candidate, insufficient.size()});

        for (InsufficientRNs ir : insufficient) {
            addRNs(candidateTopo, ir.getDCId(), ir.getRGId(),
                   currentLayout, ir.getNumNeeded());
        }

        return candidate;
    }

    /**
     * Build an initial store, or redistribute an existing store. Rebalance is
     * executed first, so that the starting topology is as clean as possible
     * before we try a redistribute.  Note that the incoming topology may
     * nevertheless have rule violations, and the resulting topology may also
     * have rule violations. The calling layer must be sure to warn about and
     * display any violations found in the candidate.
     *
     * If the builder is unable to improve the topology, it will return the
     * original topology.
     */
    public TopologyCandidate build() {

        logger.log(Level.FINE, "Build {0} using {1}, numPartitions={2}",
                  new Object[]{candidateName, snPool.getName(), numPartitions});

        /* Make sure the starting point has been rebalanced. */
        final TopologyCandidate startingPoint = rebalance(
            new TopologyCandidate(candidateName, sourceTopo.getCopy()),
            null);
        final Topology startingTopo = startingPoint.getTopology();

        TopologyCandidate candidate = null;
        try {

            /*
             * Calculate the maximum number of shards this store can host,
             * looking only at the physical resources available, and ignoring
             * any existing, assigned RNs.
             */
            final int currentShards = startingTopo.getRepGroupMap().size();
            final EmptyLayout ideal =
                new EmptyLayout(startingTopo, params, snPool);
            final int maxShards = ideal.getMaxShards();

            /*
             * Don't permit the creation of an empty, initial topology. If
             * deployed, it would have no partitions, and the number of
             * partitions can't be changed after the initial deployment.
             */
            if ((currentShards == 0) && (maxShards == 0)) {
                throw new IllegalCommandException
                    ("It is not possible to create any shards for this " +
                     "initial topology with the available set of storage " +
                     "nodes. Please increase the number or capacity of " +
                     "storage nodes");
            }


            /* Can we add any shards? */
            if (maxShards <= currentShards) {
                logger.info("Couldn't improve topology. Store can only " +
                            "support " +  maxShards  + " shards." +
                            "\nShard calculation based on smallest " +
                            "zone: " +
                            ideal.showDatacenters());

                /*
                 * Even if we couldn't add shards, see if we need any
                 * redistribution.
                 */
                final TopologyCandidate fixPartitions =
                    new TopologyCandidate(candidateName,
                                          startingTopo.getCopy());
                redistributePartitions(fixPartitions);
                return assignMountPoints(fixPartitions);
            }

            /*
             * The maximum number of shards may or may not be achievable.
             * Existing RNs may be too costly to move, or this redistribution
             * implementation may be too simplistic to do all the required
             * moves. Repeat the attempts until each datacenter has been mapped
             * for more shards than previously existed. If not possible, give
             * up. For example, suppose the existing store has 20 shards, and
             * the first calculations thinks we can get to 25 shards. Try to
             * map each datacenter with first 25 shards, then 24, etc, until
             * we've arrived at a number that is > 20, and which all
             * datacenters can support.
             *
             * TODO: handle the case where even the current number of shards is
             * not ideal, yet partitions still need to be moved.
             */
            boolean success = false;
            for (int currentGoal = maxShards; currentGoal > currentShards;
                 currentGoal--) {

                /*
                 * Create a starting candidate, and relationship descriptor
                 * which represent all the components in the current topology..
                 */
                candidate = new TopologyCandidate(candidateName,
                                                  startingTopo.getCopy());
                final StoreDescriptor currentLayout =
                    new StoreDescriptor(startingTopo, params, snPool);
                if (layoutShardsAndRNs(candidate, currentLayout, currentGoal)) {
                    success = true;
                    break;
                }
            }

            if (!success) {
                /*
                 * a failure during layout could have left the candidate's
                 * topology in an interim state. Reset it to the original,
                 * but retain its audit log and other useful information.
                 */
                if (candidate != null) {
                     candidate.resetTopology(startingTopo.getCopy());
                }
            }

            return assignMountPoints(candidate);
        } catch (RuntimeException e) {
            if (candidate == null) {
                logger.log(Level.INFO,
                           "Topology build failed due to " + e);

            } else {
                logger.log(Level.INFO,
                           "Topology build failed due to {0}\n{1}",
                           new Object[] { e, candidate.showAudit()});
            }
            throw e;
        }
    }

    /**
     * Change the repfactor on an existing datacenter. This will cause shards
     * to become non-compliant because they do not have enough SNs, so do a
     * rebalance so enough SNs are added.
     */
    public TopologyCandidate changeRepfactor(int newRepFactor,
                                             DatacenterId dcId) {
        final Datacenter dc = sourceTopo.get(dcId);
        if (dc == null) {
            throw new IllegalCommandException(dcId +
                                              " is not a valid zone");
        }

        if (dc.getRepFactor() > newRepFactor) {
            throw new IllegalCommandException
                ("The proposed replication factor of " + newRepFactor +
                 " is less than the current replication factor of " +
                 dc.getRepFactor() + " for " + dc +
                 ". Oracle NoSQL Database doesn't yet " +
                 " support the ability to reduce replication factor");
        }

        Rules.validateReplicationFactor(newRepFactor);

        /* Update the replication factor */
        final TopologyCandidate startingPoint =
            new TopologyCandidate(candidateName, sourceTopo.getCopy());
        if (dc.getRepFactor() != newRepFactor) {
            startingPoint.getTopology().update(
                dcId,
                Datacenter.newInstance(dc.getName(), newRepFactor,
                                       dc.getDatacenterType()));
        }

        /* Add RNs to fulfill the desired replication factor */
        return assignMountPoints(rebalance(startingPoint, dcId));
    }

    /**
     * Move the specified RN off its current SN. Meant as a limited way for
     * the user to manually modify the topology. The prototypical use case is
     * that there is a hardware fault with that RN, but not with the whole
     * SN (which pretty much means the mount point), and the user would
     * like to move the RN away before attempting repairs.
     *
     * If a SN is specified, the method will attempt to move to that specific
     * node. This will be a hidden option in R2; the public option will only
     * permit moving the RN off an SN, onto some SN chosen by the
     * TopologyBuilder.
     */
    public TopologyCandidate relocateRN(RepNodeId rnId,
                                        StorageNodeId proposedSNId) {

        final Topology topo = sourceTopo.getCopy();
        final RepNode rn = topo.get(rnId);
        if (rn == null) {
            throw new IllegalCommandException(rnId + " does not exist");
        }

        final StorageNodeId oldSNId = rn.getStorageNodeId();
        final StoreDescriptor currentLayout =
            new StoreDescriptor(topo, params, snPool);

        final SNDescriptor owningSND = currentLayout.getSND(oldSNId, topo);
        final DatacenterId dcId = currentLayout.getOwningDCId(oldSNId, topo);

        final List<SNDescriptor> possibleSNDs;
        if (proposedSNId == null) {
            possibleSNDs = currentLayout.getAllSNDs(dcId);
        } else {
            final StorageNode proposedSN = topo.get(proposedSNId);
            if (proposedSN == null) {
                throw new IllegalCommandException("Proposed target SN " +
                                                  proposedSNId +
                                                  " does not exist");
            }

            if (!dcId.equals(proposedSN.getDatacenterId())) {
                throw new IllegalCommandException
                    ("Can't move " + rnId + " to " + proposedSN +
                     " because it is in a different zone");
            }
            possibleSNDs = new ArrayList<SNDescriptor>();
            possibleSNDs.add(currentLayout.getSND(proposedSNId, topo));
        }

        for (SNDescriptor snd : possibleSNDs) {
            if (snd.getId().equals(oldSNId)) {
                continue;
            }

            logger.log(Level.FINEST, "Trying to move {0} to {1}",
                       new Object[]{rn, snd});
            if (snd.canAdd(rnId)) {
                /* Move the RN in the descriptions. */
                snd.claim(rnId, owningSND);
                changeSNForRN(topo, rnId, snd.getId());
                break;
            }
        }

        return assignMountPoints(new TopologyCandidate(candidateName, topo));
    }

    /**
     * Return true if this snId is in this datacenter.
     * if filterDC == null, return true.
     * if filterDC != null, return true if snId is in this datacenter
     */
    private boolean filterByDC(DatacenterId filterDCId, StorageNodeId snId) {
        if (filterDCId == null) {
            return true;
        }

        return (sourceTopo.get(snId).getDatacenterId().equals(filterDCId));
    }

    /**
     * Check that the snpool provided for the new topology is a superset of
     * those that already host RNs, and that all SNs exist.
     */
    private void checkSNs() {

        for (StorageNodeId snId : snPool.getList()) {
            if (params.get(snId) == null) {
                throw new IllegalCommandException
                    ("Storage Node " + snId + " does not exist. " +
                     "Please remove " + "it from " + snPool.getName());
            }

            if (sourceTopo.get(snId) == null) {
                throw new IllegalCommandException
                    ("Topology candidate " + candidateName +
                     " does not know about " + snId +
                     " which is a member of storage node pool " +
                     snPool.getName() +
                     ". Please use a different storage node pool or " +
                     "re-clone your candidate using the command " +
                     "topology clone -current -name <candidateName>");
            }
        }

        final Set<StorageNodeId> missing = new HashSet<StorageNodeId>();
        for (RepNode rn : sourceTopo.getSortedRepNodes()) {
            missing.add(rn.getStorageNodeId());
        }

        final Set<StorageNodeId> inPool =
            new HashSet<StorageNodeId>(snPool.getList());
        missing.removeAll(inPool);
        if (missing.size() > 0) {
            throw new IllegalCommandException
                ("The storage pool provided for topology candidate " +
                 candidateName +
                 " must contain the following SNs which are already in use " +
                 "in the current topology: " + missing);
        }
    }

    /**
     * Attempt to assign RNs to SNs, creating and moving RNs where
     * needed. Update both the relationship descriptors and the candidate
     * topology.
     *
     * @return true if successful
     */
    private boolean layoutShardsAndRNs(TopologyCandidate candidate,
                                       StoreDescriptor currentLayout,
                                       int desiredMaxShards) {

        final Topology candidateTopo = candidate.getTopology();
        final List<SNDescriptor> fullSNs = new ArrayList<SNDescriptor>();
        final int highestExistingShardId = currentLayout.getHighestShardId();

        /* Figure out how many shards we are placing */
        final int numNewShards = desiredMaxShards -
            candidateTopo.getRepGroupMap().size();

        /* Save newly created shards for partition assignment later */
        final List<RepGroup> newShards = new ArrayList<RepGroup>(numNewShards);

        for (int i = 1; i <= numNewShards; i++) {

            final int shardNumber = highestExistingShardId + i;

            /* Create a RepGroup to represent the shard. */
            final RepGroup repGroup = new RepGroup();
            newShards.add(repGroup);
            candidateTopo.add(repGroup);

            /* Keep track of replicas created for earlier data centers */
            int previousDcReplicas = 0;

            /* Add RNs for each data center */
            for (final DCDescriptor dcDesc : currentLayout.getDCDesc()) {
                final int repFactor = dcDesc.getRepFactor();
                final SNLoopIterator snIter = new SNLoopIterator(
                    new LinkedList<SNDescriptor>(dcDesc.getSortedSNs()));

                final List<SNDescriptor> snTargets =
                    layoutOneShard(candidate, previousDcReplicas, dcDesc,
                                   shardNumber, fullSNs, snIter, true);

                /*
                 * Before we add this shard to the topology, check that there
                 * are at least <repfactor> number of SNs available for this
                 * shard. If not, we couldn't lay out the shard. That may
                 * happen, since we may not be able to create the ideal maximum
                 * number of shards.
                 */
                if (snTargets.size() != repFactor) {
                    /* Give up, we couldn't place a shard. */
                    return false;
                }

                /* Create the RNs in a shard in the topology */
                for (int rn = 0; rn < repFactor; rn++) {
                    repGroup.add(new RepNode(snTargets.get(rn).getId()));
                }
                previousDcReplicas += repFactor;
            }

            candidate.log("added shard " + repGroup.getResourceId());
        }

        final int existingPartitions = candidateTopo.getPartitionMap().size();
        if (existingPartitions == 0) {
            /*
             * Brand new store, make new partitions. Note that this
             * implementation much match the actual execution of how we
             * create partitions in the AddPartitions task, so that a deploy
             * of an initial topology is idempotent.
             *
             * For example, suppose we create an initial topo where shardA had
             * partitions 1-10, and shard B has partitions 11-20. The topology
             * diff mechanism can only note that each shard has 10 partitions,
             * and cannot note the actual partition id, because partition ids
             * are assigned within the topology, and can't be specified by the
             * call to create the partition. AddPartitions must assume create
             * the partitions in exactly the same order as below.
             */
            final int min = numPartitions / numNewShards;
            final int numXtraLarge = numPartitions - (min * numNewShards);
            for (int whichShard = 0; whichShard < numNewShards; whichShard++) {
                final int end = (whichShard < numXtraLarge) ? min + 1 : min;
                for (int numP = 0; numP < end; numP++) {
                    final Partition p =
                        new Partition(newShards.get(whichShard));
                    candidateTopo.add(p);
                }
            }
        } else {
            /* existing store, move partitions.*/
            redistributePartitions(candidate);
        }
        return true;
    }

    /**
     * Very simplistic assignment to redistribute shards in an existing,
     * non-new topology.
     */
    private void redistributePartitions(TopologyCandidate candidate) {

        final Topology candidateTopo = candidate.getTopology();

        /*
         * Figure out the ideal number of partitions. If the num partitions
         * doesn't divide evenly by shards, this will be a min/max measurement.
         */
        final int totalPartitions = candidateTopo.getPartitionMap().size();
        final int totalShards = candidateTopo.getRepGroupMap().size();
        if ((totalShards == 0) || (totalPartitions == 0)) {
            return;
        }

        final int minPartitionsPerShard =
            Rules.calcMinPartitions(totalPartitions, totalShards);
        final int maxPartitionsPerShard =
            Rules.calcMaxPartitions(totalPartitions, totalShards);

        logger.fine("TotalNumPartitions=" + totalPartitions +
                    " total shards=" + totalShards +
                    " minPartsPerShard=" + minPartitionsPerShard +
                    " maxPartsPerShard=" + maxPartitionsPerShard);

        /*
         * Create a set of group descriptors. The descriptors contain a
         * list of partitions for that shard.
         */
        final Map<RepGroupId, ShardDescriptor> shards =
                                new HashMap<RepGroupId, ShardDescriptor>();

        for (RepGroupId rgId : candidateTopo.getRepGroupIds()) {
            shards.put(rgId, new ShardDescriptor(rgId));
        }

        for (Partition p: candidateTopo.getPartitionMap().getAll()) {
            shards.get(p.getRepGroupId()).addPartition(p.getResourceId());
        }

        /*
         * Identify the shards that have too many or too few partitions and
         * create sorted lists for each set.
         */
        final TreeSet<ShardDescriptor> tooFew = new TreeSet<ShardDescriptor>();
        final TreeSet<ShardDescriptor> tooMany =
            new TreeSet<ShardDescriptor>();

        /*
         * Keep track of the ones that have the max and min number of
         * shards. We may still steal a partition from one that has
         * the max, or put a partition on a shard that has the min.
         */
        final List<ShardDescriptor> hasMax = new ArrayList<ShardDescriptor>();
        final List<ShardDescriptor> hasMin = new ArrayList<ShardDescriptor>();

        for (ShardDescriptor desc : shards.values()) {
            final int nParts = desc.getNumPartitions();
            if (nParts < minPartitionsPerShard) {
                tooFew.add(desc);
            } else if (nParts > maxPartitionsPerShard) {
                tooMany.add(desc);
            } else if (nParts == maxPartitionsPerShard) {
                hasMax.add(desc);
            } else {
                hasMin.add(desc);
            }
        }

        /*
         * Attempt to move partitions to balance out the shards. Empty out
         * the shards that are too large.
         */
        while (!tooMany.isEmpty()) {

            /* Pick the least utilized shard to move a partition to */
            ShardDescriptor target = tooFew.pollFirst();

            if (target == null) {
               /*
                * No under utilized shards, look among the ones that have the
                * minimum number of partitions.
                */
                if (hasMin.isEmpty()) {
                    break;
                }
                target = hasMin.get(0);
                hasMin.remove(0);
            }

            /* Pick the most over utilized shard to move a partition from */
            final ShardDescriptor source = tooMany.pollLast();
            final PartitionId moveTarget = source.removePartition();
            target.addPartition(moveTarget);

            /*
             * See if source and target shards are still unbalanced, if so,
             * re-insert them into their respective lists, sorting as needed.
             */
            if (source.getNumPartitions() > maxPartitionsPerShard) {
                tooMany.add(source);
            } else if (source.getNumPartitions() ==  maxPartitionsPerShard) {
                hasMax.add(source);
            }

            if (target.getNumPartitions() < minPartitionsPerShard) {
                tooFew.add(target);
            } else if (target.getNumPartitions() == minPartitionsPerShard) {
                hasMin.add(target);
            }

            /* Update the topology */
            candidateTopo.update(moveTarget, new Partition(target.rgId));
        }

        /*
         * Sanity check assertion: we should have been able to find locations
         * for all over-populated shards.
         */
        if (!tooMany.isEmpty()) {
            throw new OperationFaultException
                ("Unexpected state found when redistributing partitions for " +
                 candidateName + ". After first processing pass, shards " +
                 tooMany + " have more than " + maxPartitionsPerShard +
                 " partitions. Candidate looks like " +
                 TopologyPrinter.printTopology(candidateTopo, null, true));
        }

        /*
         * Since the number of partitions may not divide evenly into the
         * number of shards, there is a min and max partitions per shard value,
         * where min and max differ by 1. Even after we've reduce all shards
         * that are over the max, we may still have shards under the min. For
         * example, suppose we have 3 shards, and 10 partitions. Each shard
         * should have 3 or 4 partitions. After the first loop, it's possible
         * to have a split of
         *   shardA(2 partitions)
         *   shardB(4 partitions)
         *   shardC(4 partitions)
         * and we still have to move one more shard to get 3,3,4
         */
        while (!tooFew.isEmpty()) {

            /*
             * Pick the most under utilized shard as a target for a partition
             */
            final ShardDescriptor target = tooFew.pollFirst();

            if (target == null) {
                break;  // No under utilized shards - done.
            }

            /*
             * Get a partition from a shard that has the maximum number. By
             * now, there are no shards that are over the maximum number.
             */
            if (hasMax.isEmpty()) {
                break;
            }

            final ShardDescriptor source = hasMax.get(0);
            hasMax.remove(0);

            final PartitionId moveTarget = source.removePartition();
            target.addPartition(moveTarget);

            if (target.getNumPartitions() < minPartitionsPerShard) {
                tooFew.add(target);
            }

            /* Update the topology */
            candidateTopo.update(moveTarget, new Partition(target.rgId));
        }
    }

    /**
     * Assign RNs to SNs for a single shard in a single data center and update
     * the topology relationship descriptors. If there are not sufficient
     * resources to accommodate the required RN, the returned list size will be
     * less than the data center's repFactor.  Use existingReplicas to know how
     * many replicas have already been created for this shard in other data
     * centers, to know what the first replication node number should be.
     */
    private List<SNDescriptor> layoutOneShard(TopologyCandidate candidate,
                                              int existingReplicas,
                                              DCDescriptor dcDesc,
                                              int shardNumber,
                                              List<SNDescriptor> fullSNs,
                                              SNLoopIterator availableSNs,
                                              boolean updateTopology) {

        final int repFactor = dcDesc.getRepFactor();
        final List<SNDescriptor> snsForShard = new ArrayList<SNDescriptor>();

        candidate.log("laying out shard " + shardNumber + ", " +
                      availableSNs.size() + " SNs in pool");

        for (int i = 1; i <= repFactor; i++) {

            boolean snFound = false;
            StorageNodeId startingSN = null;
            final RepNodeId rId =
                new RepNodeId(shardNumber, existingReplicas + i);

            while (availableSNs.hasNext()) {
                final SNDescriptor snDesc = availableSNs.next();

                if (snDesc.isFull()) {
                    fullSNs.add(snDesc);
                    availableSNs.remove();
                    candidate.log(" remove " + snDesc +
                                  " from available list");
                    continue;
                }
                logger.log(Level.FINEST,
                           "Trying to add RN {0} for shard {1} to {2}",
                           new Object[]{rId, shardNumber, snDesc});

                if (startingSN == null) {
                    startingSN = snDesc.getId();
                } else {
                    if (startingSN.equals(snDesc.getId())) {
                        /*
                         * We've looped back to the first SN we started with
                         * and haven't found a home for this RN, so give up.
                         */
                        break;
                    }
                }

                if (snDesc.canAdd(rId)) {
                    snDesc.add(rId);
                    snsForShard.add(snDesc);

                    /*
                     * We're done with this RN, since we found an SN. Go on
                     * to the next RN in the shard.
                     */
                    snFound = true;
                    break;
                }
            }

            if (snFound) {
                /* Go on to the next RN in the shard. */
                continue;
            }

            /*
             * Couldn't house this RN, and there are no more free slots anywhere
             * in the store, so give up on this shard.
             */
            if (!availableSNs.hasNext()) {
                break;
            }

            /*
             * Couldn't house this RN, but there are still SNs with free
             * slots around.  Try to swap a RN off one of the full SNs onto one
             * of the capacious ones, so we can take that slot.
             */
            candidate.log("Problem placing " + rId + " trying swap");
            final List<SNDescriptor> hasRoom = availableSNs.getList();

            /*
             * Look through the SNs in the same data center that are full for a
             * RN that can be swapped onto one of the SNs that still have
             * slots.
             */
            final DatacenterId dcId = dcDesc.getDatacenterId();
            for (SNDescriptor fullSND : fullSNs) {
                if (!dcId.equals(fullSND.getStorageNode().getDatacenterId())) {
                    continue;
                }

                if (swapRNToEmptySlot(fullSND, hasRoom, rId, candidate,
                                      updateTopology)) {
                    snsForShard.add(fullSND);
                    snFound = true;
                    break;
                }
            }

            /*
             * We tried to do some swapping, and still couldn't house this SN.
             * End the attempt to place this shard.
             */
            if (!snFound) {
                break;
            }
        }

        if (snsForShard.size() == repFactor) {
            candidate.log("shard " + shardNumber +
                          " successfully assigned to " +  snsForShard);
        } else {
            candidate.log("shard " + shardNumber +
                          " incompletely assigned to " +  snsForShard);
        }
        return snsForShard;
    }

    /**
     * Attempt to move one of the RNs on fullSND to a currently empty slot, to
     * make room for targetRN on fullSND.
     *
     * @return whether a swap was performed successfully
     */
    private boolean swapRNToEmptySlot(SNDescriptor fullSND,
                                      List<SNDescriptor> availableSNDs,
                                      RepNodeId targetRN,
                                      TopologyCandidate candidate,
                                      boolean updateTopology)  {

        if (!fullSND.canAddIgnoreCapacity(targetRN)) {

            /*
             * This SN can't host this RN because of Rule problems,
             * so don't try a swap
             */
            return false;
        }

        /* Treat all the RNs on this SN as potential move targets. */
        for (RepNodeId target : fullSND.getRNs()) {

            /* Look through all the SNs that have room */
            for (SNDescriptor sndWithRoom : availableSNDs) {
                if (sndWithRoom.canAdd(target)) {
                    candidate.log("Swap: " + targetRN + " goes to " +
                                  fullSND +  ", " + target + " goes to " +
                                  sndWithRoom);

                    /* Change the relationship descriptors */
                    sndWithRoom.claim(target, fullSND);

                    /* Change the topology */
                    if (updateTopology) {
                        changeSNForRN(candidate.getTopology(), target,
                                      sndWithRoom.getId());
                    }

                    /*
                     * Update the descriptors with the targetRN now, so
                     * that attempts to place siblings from the same shard
                     * will know that this SN is no longer a valid destination
                     */
                    fullSND.add(targetRN);
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Attempt to move this RN to a different SN. Not guaranteed to be
     * successful.
     * @return true if the whole list could be moved.
     */
    private boolean moveRNs(Topology topo,
                            StorageNodeId currentSNId,
                            StoreDescriptor currentLayout,
                            List<RepNodeId> moveTargets) {

        final SNDescriptor owningSND = currentLayout.getSND(currentSNId, topo);
        final DatacenterId dcId =
            currentLayout.getOwningDCId(currentSNId, topo);
        final List<SNDescriptor> possibleSNDs = currentLayout.getAllSNDs(dcId);

        int moved = 0;
        for (RepNodeId rnToMove : moveTargets) {
            for (SNDescriptor snd : possibleSNDs) {
                logger.log(Level.FINEST,
                           "Trying to move {0} to {1}",
                           new Object[]{rnToMove, snd});
                if (snd.canAdd(rnToMove)) {
                    /* Move the RN in the descriptions. */
                    snd.claim(rnToMove, owningSND);
                    changeSNForRN(topo, rnToMove, snd.getId());
                    moved++;
                    break;
                }
            }
        }
        return moved == moveTargets.size();
    }

    /** Change the SN for this RN in topology */
    private void changeSNForRN(Topology topo,
                               RepNodeId rnToMove,
                               StorageNodeId snId) {
        logger.finest("Swapped " + rnToMove + " to " + snId);
        final RepNode updatedRN = new RepNode(snId);
        final RepGroupId rgId = topo.get(rnToMove).getRepGroupId();
        final RepGroup rg = topo.get(rgId);
        rg.update(rnToMove, updatedRN);
    }

    private void addOneRN(Topology topo, SNDescriptor snd,
                          RepGroupId rgId) {
        /* Add an RN in the candidate topology */
        final RepNode newRN = new RepNode(snd.getId());
        final RepNode added = topo.get(rgId).add(newRN);

        /* Add this new RN to the SN's list of hosted RNs */
        snd.add(added.getResourceId());
    }

    private void moveAnyRNAway(Topology topo,
                               StoreDescriptor currentLayout,
                               SNDescriptor snd)  {
        for (RepNodeId rnId : snd.getRNs()) {
            final List<RepNodeId> moveList = new ArrayList<RepNodeId>();
            moveList.add(rnId);
            if (moveRNs(topo, snd.getId(), currentLayout, moveList)) {
                return;
            }
        }
    }


    /**
     * Attempt to add RNs to this shard, in this datacenter, to bring its
     * rep factor up to snuff.
     */
    private void addRNs(Topology topo,
                        DatacenterId dcId,
                        RepGroupId rgId,
                        StoreDescriptor currentLayout,
                        int numNeeded) {

        final List<SNDescriptor> possibleSNDs = currentLayout.getAllSNDs(dcId);

        int fixed = 0;
        for (int i = 0; i < numNeeded; i++) {
            /* Try one time for each RN we need to add to this shard. */
            boolean success = false;
            for (SNDescriptor snd : possibleSNDs) {
                logger.log(Level.FINEST,
                           "Trying add an RN to {0} on {1}",
                           new Object[]{rgId, snd});
                if (snd.canAdd(rgId)) {
                    addOneRN(topo, snd, rgId);
                    fixed++;
                    success = true;
                    break;
                }
            }
            if (!success) {
                /*
                 * We went through all the available SNs once, and none of them
                 * could house something from this shard, so give up.
                 */
                break;
            }
        }

        /*
         * If some couldn't be added, try again, this time consenting to move
         * existing RNs.
         */
        final int remaining = numNeeded - fixed;
        for (int i = 0; i < remaining; i++) {
            boolean success = false;
            for (SNDescriptor snd : possibleSNDs) {
                if (!snd.hosts(rgId)) {
                    moveAnyRNAway(topo, currentLayout, snd);

                    if (snd.canAdd(rgId)) {
                        addOneRN(topo, snd, rgId);
                        fixed++;
                        success = true;
                        break;
                    }
                }
            }
            if (!success) {
                /* Give up, can't house something from this shard anywbere */
                break;
            }
        }
    }

    /**
     * Figure out mount points for relocated and newly created RNs.
     * @throws InvalidTopologyException
     */
    private TopologyCandidate assignMountPoints(TopologyCandidate candidate) {

        final Topology candTopo = candidate.getTopology();
        final List<RepNode> needsMountPoint = new ArrayList<RepNode>();

        /* Find the RN->mount point assignments that are still valid. */
        final Map<StorageNodeId, Set<String>> usedMountPoints =
            new HashMap<StorageNodeId, Set<String>>();

        /*
         * Look through all the RNs in the candidate topology and find the
         * ones that have moved or been created on an SN, and need a mount
         * point if one is available.
         */
        for (RepNode rn : candTopo.getSortedRepNodes()) {
            final RepNodeId rnId = rn.getResourceId();
            final StorageNodeId snId = rn.getStorageNodeId();
            final RepNodeParams rnp =  params.get(rnId);

            /* This RN didn't exist before. */
            if (rnp == null) {
                needsMountPoint.add(rn);
                continue;
            }

            /* This RN wasn't on this SN before */
            if (!rnp.getStorageNodeId().equals(snId)) {
                needsMountPoint.add(rn);
                continue;
            }

            /*
             * This RN didn't move. Record its mount point as in-use. If it has
             * no mount point, it is housed in the root directory.
             *
             * In R2, we do not automatically move an RN from one directory to
             * another on the same SN, which means that we do not currently
             * address the issue of multiple RNs in the root dir. In future
             * releases, we might want to see if this RN was on an over-crowded
             * root dir, and assign it a new mount point.
             */
            if (rnp.getMountPointString() != null) {
                final Set<String> used = getUsedSet(snId, usedMountPoints);
                used.add(rnp.getMountPointString());
                candidate.saveMountPoint(rnId, rnp.getMountPointString());
            }
        }

        for (RepNode rn : needsMountPoint) {
            final StorageNodeId snId = rn.getStorageNodeId();
            final StorageNodeParams snp = params.get(snId);

            /* No mount points on this SN to use, all RNS go in the root dir. */
            if (snp.getMountPoints() == null) {
                continue;
            }
            final Set<String> used = getUsedSet(snId, usedMountPoints);

            for (String possibleMountPoint : snp.getMountPoints()) {
                if (!used.contains(possibleMountPoint)) {
                    used.add(possibleMountPoint);
                    candidate.saveMountPoint(rn.getResourceId(),
                                             possibleMountPoint);
                    break;
                }
            }
        }

        /*
         * Now sanity check that only one RN has been assigned per mount
         * point. Shouldn't happen, but contain any errors here, rather than
         * letting a bad candidate be propagated.
         */
        final Map<String, RepNodeId> mountPointToRN =
            new HashMap<String, RepNodeId>();

        for (RepNode rn : candTopo.getSortedRepNodes()) {
            final RepNodeId rnId = rn.getResourceId();
            final StorageNodeId snId = rn.getStorageNodeId();
            final String assignedMP = candidate.getMountPoint(rnId);
            if (assignedMP == null) {
                continue;
            }

            final String key = snId + assignedMP;
            final RepNodeId clashingRN =  mountPointToRN.get(key);
            if (clashingRN == null) {
                mountPointToRN.put(key, rnId);
            } else {
                throw new OperationFaultException
                    ("Topology candidate " + candidate.getName() +
                     " is invalid because " + clashingRN + " and " +
                     rnId + " are both assigned to " + snId +
                     ", mount point " + assignedMP);
            }
        }

        /* a sanity check, as much to guard against bugs. */
        Rules.checkAllMountPointsExist(candidate, params);

        return candidate;
    }

    private Set<String> getUsedSet(StorageNodeId snId,
                                   Map<StorageNodeId, Set<String>> usedMap) {
        Set<String> used = usedMap.get(snId);
        if (used == null) {
            used = new HashSet<String>();
            usedMap.put(snId, used);
        }
        return used;
    }

    /**
     * This iterator will loop over the storage node list in a round robin
     * fashion. During the iteration, SNs will be removed from the list after
     * they have reached capacity. The iterator will only return null if all
     * the elements of the list have been removed.
     */
     private class SNLoopIterator implements Iterator<SNDescriptor> {

        private final LinkedList<SNDescriptor> available;
        private Iterator<SNDescriptor> iter;

        SNLoopIterator(LinkedList<SNDescriptor> available) {
            this.available = available;
            iter = available.iterator();
        }

        public List<SNDescriptor> getList() {
            return new ArrayList<SNDescriptor>(available);
        }

        int size() {
            return available.size();
        }

        @Override
        public boolean hasNext() {
            return (available.size() > 0);
        }

        @Override
        public SNDescriptor next() {
            if (!iter.hasNext()) {
                iter = available.iterator();
            }

            return iter.next();
        }

        @Override
        public void remove() {
            iter.remove();
        }
    }

    /**
     * Shard descriptor. When inserted into a TreeSet the descriptors will sort
     * in order of the number of partitions, least to most.
     */
    private static class ShardDescriptor
                                    implements Comparable<ShardDescriptor> {

        private final RepGroupId rgId;
        private final List<PartitionId> partitions =
                                            new ArrayList<PartitionId>();

        /* Constructor. The partition list will be empty. */
        ShardDescriptor(RepGroupId rgId) {
            this.rgId = rgId;
        }

        int getNumPartitions() {
            return partitions.size();
        }

        PartitionId removePartition() {
            return partitions.isEmpty() ? null : partitions.remove(0);
        }

        void addPartition(PartitionId partitionId) {
            partitions.add(partitionId);
        }

        @Override
        public int compareTo(ShardDescriptor gd) {
            if (this.equals(gd)) {
                return 0;
            }
            final int diff = this.partitions.size() - gd.partitions.size();
            return (diff == 0) ? 1 : diff;
        }

        @Override
        public String toString() {
            return "ShardDescriptor[" + rgId + ", " + partitions.size() + "]";
        }
    }

    /**
     * The top level descriptor of the store and the relationships of the
     * existing topology's components.
     */
    private class StoreDescriptor {

        private final Map<DatacenterId, DCDescriptor> dcMap;
        private final int highestShardId;

        /**
         * A list of available SNs by datacenter, used for rebalancing.
         */
        private final Map<DatacenterId, List<SNDescriptor>> sndMap;

        StoreDescriptor(Topology topo,
                        Parameters params,
                        StorageNodePool snPool) {

            dcMap = new HashMap<DatacenterId, DCDescriptor>();
            for (StorageNodeId snId : snPool) {
                final StorageNode sn = topo.get(snId);
                final DatacenterId dcId = sn.getDatacenterId();

                DCDescriptor dcDesc = dcMap.get(dcId);
                if (dcDesc == null) {
                    final int rf = topo.getDatacenter(snId).getRepFactor();
                    dcDesc = new DCDescriptor(dcId, rf);
                    dcMap.put(dcId, dcDesc);
                }
                dcDesc.add(sn, params.get(sn.getStorageNodeId()));
            }

            /*
             * Make it possible to find all the SNs in a datacenter.
             */
            sndMap = new HashMap<DatacenterId, List<SNDescriptor>>();
            for (Map.Entry<DatacenterId, DCDescriptor> entry :
                dcMap.entrySet()) {
                sndMap.put(entry.getKey(), entry.getValue().getSortedSNs());
            }


            for (DCDescriptor dcDesc: dcMap.values()) {
                dcDesc.initSNDescriptors(topo);
            }

            int findHighestShardId = 0;
            for (RepGroupId rgId: topo.getRepGroupIds()) {
                if (findHighestShardId < rgId.getGroupId()) {
                    findHighestShardId = rgId.getGroupId();
                }
            }
            highestShardId = findHighestShardId;
        }

        public int getHighestShardId() {
            return highestShardId;
        }

        Collection<DCDescriptor> getDCDesc() {
            return dcMap.values();
        }

        List<SNDescriptor> getAllSNDs(DatacenterId dcId) {
            final List<SNDescriptor> v = sndMap.get(dcId);
            return (v != null) ? v : Collections.<SNDescriptor> emptyList();
        }

        DatacenterId getOwningDCId(StorageNodeId snId, Topology topo) {
           return topo.get(snId).getDatacenterId();
        }

        SNDescriptor getSND(StorageNodeId snId, Topology topo) {
            final DatacenterId dcId = getOwningDCId(snId, topo);
            return dcMap.get(dcId).get(snId);
        }
    }

    /**
     * A set of descriptors that are initialized with the existing physical
     * resources, but not any of the existing topology components, so it looks
     * like a blank, lean store. It's used to calculate the ideal number of
     * shards possible for such a layout.
     */
    private class EmptyLayout {

        private final Map<DatacenterId, DCDescriptor> dcMap;
        private final int maxShards;

        EmptyLayout(Topology topo,
                    Parameters params,
                    StorageNodePool snPool) {

            dcMap = new HashMap<DatacenterId, DCDescriptor>();
            for (StorageNodeId snId : snPool) {
                final StorageNode sn = topo.get(snId);
                final DatacenterId dcId = sn.getDatacenterId();

                DCDescriptor dcDesc = dcMap.get(dcId);
                if (dcDesc == null) {
                    final int rf = topo.getDatacenter(snId).getRepFactor();
                    dcDesc = new DCDescriptor(dcId, rf);
                    dcMap.put(dcId, dcDesc);
                }
                dcDesc.add(sn, params.get(sn.getStorageNodeId()));
            }

            initClean();
            maxShards = calculateMaxShards
                (new TopologyCandidate("scratch", topo.getCopy()));
        }

        public String showDatacenters() {
            final StringBuilder sb = new StringBuilder();
            for (final DCDescriptor dcDesc : dcMap.values()) {
                sb.append(DatacenterId.DATACENTER_PREFIX + " id=").append(
                    dcDesc.getDatacenterId());
                sb.append(" maximum shards= ").append(dcDesc.getNumShards());
                sb.append('\n');
            }
            return sb.toString();
        }

        /**
         * Calculate the maximum number of shards this store could support.
         */
        private int calculateMaxShards(TopologyCandidate candidate) {
            int calculatedMax = Integer.MAX_VALUE;
            for (Map.Entry<DatacenterId, DCDescriptor> entry :
                 dcMap.entrySet()) {

                final DCDescriptor dcDesc = entry.getValue();
                dcDesc.calculateMaxShards(candidate, 0);
                final int dcMax = dcDesc.getNumShards();
                if (calculatedMax > dcMax) {
                    calculatedMax = dcMax;
                }
            }
            return calculatedMax;
        }

        private void initClean() {
            for (DCDescriptor dcDesc: dcMap.values()) {
                dcDesc.initSNDescriptors(null);
            }
        }


        public int getMaxShards() {
            return maxShards;
        }
    }

    /**
     * Information about the datacenter characteristics and the SNs in that DC.
     */
    private class DCDescriptor {

        /** The data center ID */
        private final DatacenterId dcId;

        /* Configured by the user. */
        private final int repFactor;

        /* The number of shards currently mapped onto this datacenter. */
        private int numShards;

        private final Map<StorageNodeId, SNDescriptor> sns;

        DCDescriptor(final DatacenterId dcId, final int repFactor) {
            this.dcId = dcId;
            this.repFactor = repFactor;
            sns = new HashMap<StorageNodeId, SNDescriptor>();
        }

        /**
         * Return the SNs sorted by SNId.
         */
        List<SNDescriptor> getSortedSNs() {
            final List<SNDescriptor> snList =
                new ArrayList<SNDescriptor>(sns.values());
            Collections.sort(snList, new Comparator<SNDescriptor>() {
                @Override
                    public int compare(SNDescriptor snA,
                                       SNDescriptor snB) {
                        return snA.getId().getStorageNodeId() -
                            snB.getId().getStorageNodeId();
                }});
            return snList;
        }

        DatacenterId getDatacenterId() {
            return dcId;
        }

        int getRepFactor() {
            return repFactor;
        }

        int getNumShards() {
            return numShards;
        }

        /**
         * Add this SN's params. Needed to get access to information like
         * capacity, and other physical constraints.
         */
        void add(StorageNode sn, StorageNodeParams snp) {
            sns.put(sn.getResourceId(), new SNDescriptor(sn, snp));
        }

        /**
         * The SNDescriptors describe how shards and RNs map to the SNs.
         * Initialize in preparation for topology building.  If the topo
         * argument is null, ignore any RNs that are already assigned to an SN.
         * We want to calculate the theoretical ideal layout so we start with a
         * blank slate.
         */
        private void initSNDescriptors(Topology topo) {

            numShards = 0;

            /* Ignore the existing RNs, we want to have a clean slate. */
            if (topo == null) {
                return;
            }

            for (SNDescriptor snd : getSortedSNs()) {
                snd.clearAssignedRNs();
            }

            for (RepNode rn: topo.getSortedRepNodes()) {
                final StorageNodeId snId = rn.getStorageNodeId();
                /* Only add RNs for SNs in this data center */
                if (dcId.equals(topo.get(snId).getDatacenterId())) {
                    final SNDescriptor snd = sns.get(snId);
                    snd.add(rn.getResourceId());
                }
            }

            // TODO: not sufficient for multi-datacenters. Numshards is not
            // necessarily to be initialed to the the same as the number
            // of shards in the topology, if this datacenter has fewer shards,
            // due to a previous abnormal plan end
            numShards = topo.getRepGroupMap().size();
        }

        /**
         * Each datacenter must have a complete copy of the data in the store
         * and has an individual rep factor requirement, so each datacenter has
         * its own notion of the maximum number of shards it can support.
         * Find out how many more shards can go on this datacenter.
         */
        void calculateMaxShards(TopologyCandidate candidate,
                                int highestShardId) {

            final LinkedList<SNDescriptor> available =
                new LinkedList<SNDescriptor>(getSortedSNs());
            final SNLoopIterator loopIter = new SNLoopIterator(available);
            final List<SNDescriptor> fullSNs = new ArrayList<SNDescriptor>();

            candidate.log("calculating max number of shards");

            int wholeShards = 0;
            int shardNumber = highestShardId;
            while (loopIter.hasNext()) {

                final List<SNDescriptor> snsForShard =
                    layoutOneShard(candidate, 0 /* existingReplicas */,
                                   this, ++shardNumber, fullSNs, loopIter,
                                   false);

                if (snsForShard.size() < repFactor) {
                    /* Couldn't lay out a complete shard this time, stop. */
                    break;
                }

                wholeShards++;
            }
            numShards += wholeShards;
        }

        SNDescriptor get(StorageNodeId snId) {
            return sns.get(snId);
        }
    }

    /**
     * Keeps track of the RNs assigned to an SN, consulting Rules for
     * permitted placements.
     */
    private class SNDescriptor {
        private final StorageNode sn;
        private final StorageNodeParams snp;
        private Set<RepNodeId> assignedRNs;

        SNDescriptor(StorageNode sn, StorageNodeParams snp) {
            this.sn = sn;
            this.snp = snp;
            assignedRNs = new HashSet<RepNodeId>();
        }

        public boolean hosts(RepGroupId rgId) {
            for (RepNodeId rnId : assignedRNs) {
                if (rnId.getGroupId() == rgId.getGroupId()) {
                    return true;
                }
            }
            return false;
        }

        private boolean canAdd(RepNodeId rId) {
            return Rules.checkRNPlacement(snp, assignedRNs, rId.getGroupId(),
                                          false, logger);
        }

        private boolean canAdd(RepGroupId rgId) {
            return Rules.checkRNPlacement(snp, assignedRNs, rgId.getGroupId(),
                                          false, logger);
        }

        private boolean canAddIgnoreCapacity(RepNodeId rId) {
            return Rules.checkRNPlacement(snp, assignedRNs, rId.getGroupId(),
                                          true, logger);
        }

        /**
         * Check for >= rather than == capacity in case the SN's capacity value
         * is changed during the topo building. In theory, that should be
         * prohibited, but be conservative on the check.
         */
        private boolean isFull() {
            return assignedRNs.size() >= snp.getCapacity();
        }

        private void add(RepNodeId rId) {
            assignedRNs.add(rId);
        }

        StorageNode getStorageNode() {
            return sn;
        }

        StorageNodeId getId() {
            return sn.getStorageNodeId();
        }

        void clearAssignedRNs() {
            assignedRNs = new HashSet<RepNodeId>();
        }

        /** Move RN from owning SNDescriptor to this one. */
        void claim(RepNodeId rnId, SNDescriptor owner) {
            owner.assignedRNs.remove(rnId);
            assignedRNs.add(rnId);
            logger.log(Level.FINE,
                       "Moved {0} from {1} to {2}",
                       new Object[]{rnId, owner, this});
        }

        /**
         * Get N of the RNs on this SN.
         */
        List<RepNodeId> getNRns(int numRequested) {

            if (numRequested > assignedRNs.size()) {
                throw new IllegalStateException
                    ("Requesting too many RNs (" + numRequested +
                     ") during topology building:" + this);
            }

            final List<RepNodeId> rnList =
                new ArrayList<RepNodeId>(assignedRNs);
            return rnList.subList(0, numRequested);
        }

        Set<RepNodeId> getRNs() {
            return assignedRNs;
        }

        @Override
        public String toString() {
            return sn.getResourceId() + " hosted RNs=" + assignedRNs +
                " capacity= " + snp.getCapacity();
        }
    }
}
