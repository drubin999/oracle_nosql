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

package oracle.kv.impl.admin.plan;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.AdminServiceParams;
import oracle.kv.impl.admin.plan.task.AddPartitions;
import oracle.kv.impl.admin.plan.task.BroadcastMetadata;
import oracle.kv.impl.admin.plan.task.BroadcastTopo;
import oracle.kv.impl.admin.plan.task.CheckRNMemorySettings;
import oracle.kv.impl.admin.plan.task.DeployNewRN;
import oracle.kv.impl.admin.plan.task.DeployShard;
import oracle.kv.impl.admin.plan.task.MigratePartition;
import oracle.kv.impl.admin.plan.task.NewNthRNParameters;
import oracle.kv.impl.admin.plan.task.NewRepNodeParameters;
import oracle.kv.impl.admin.plan.task.ParallelBundle;
import oracle.kv.impl.admin.plan.task.RelocateRN;
import oracle.kv.impl.admin.plan.task.Task;
import oracle.kv.impl.admin.plan.task.UpdateDatacenter;
import oracle.kv.impl.admin.plan.task.UpdateHelperHost;
import oracle.kv.impl.admin.plan.task.UpdateNthRNHelperHost;
import oracle.kv.impl.admin.topo.TopologyCandidate;
import oracle.kv.impl.admin.topo.TopologyDiff;
import oracle.kv.impl.admin.topo.TopologyDiff.RelocatedPartition;
import oracle.kv.impl.admin.topo.TopologyDiff.RelocatedRN;
import oracle.kv.impl.admin.topo.TopologyDiff.ShardChange;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * Populate the target plan with the sequence of tasks and the new param
 * instances required to move from the current to a new target topology.
 */
class TopoTaskGenerator {

    /* This plan will be populated with tasks by the generator. */
    private final DeployTopoPlan plan;
    private final TopologyCandidate candidate;
    private final Logger logger;
    private final TopologyDiff diff;

    /**
     * @throws InvalidTopologyException
     *
     */
    TopoTaskGenerator(DeployTopoPlan plan,
                      Topology source,
                      TopologyCandidate candidate,
                      AdminServiceParams adminServiceParams) {

        this.plan = plan;
        this.candidate = candidate;
        logger = LoggerUtils.getLogger(this.getClass(), adminServiceParams);
        diff = new TopologyDiff(source, null, candidate,
                                plan.getAdmin().getCurrentParameters());
        logger.log(Level.FINE, "task generator sees diff of {0}",
                   diff.display(true));
    }

    /**
     * Compare the source and the candidate topologies, and generate tasks that
     * will make the required changes.
     *
     * Note that the new repGroupIds in the candidate topology cannot be taken
     * verbatim. The topology class maintains a sequence used to create ids for
     * topology components; one cannot specify the id value for a new
     * component. Since we want to provide some independence between the
     * candidate topology and the topology deployment -- we don't want to
     * require that components be created in precisely some order to mimic
     * the candidate -- we refer to the shards by ordinal value. That is,
     * we refer to the first, second, third, etc shard.
     *
     * The task generator puts the new shards into a list, and generates the
     * tasks with a plan shard index that is the order from that list. The
     * DeployTopoPlan will create and maintain a list of newly generated
     * repgroup/shard ids, and generated tasks will use their ordinal value,
     * as indicated by the planShardIdx and to find the true repgroup id
     * to use.
     */
    void generate() {

        makeDatacenterUpdates();

        /*
         * Execute the RN relocations before creating new RNs, so that a given
         * SN does not become temporarily over capacity, or end up housing
         * two RNs in the same mount point.
         */
        makeRelocatedRNTasks();
        makeCreateRNTasks();

        /*
         * Broadcast all of the above topo changes now so any migrations run
         * smoothly
         */
        plan.addTask(new BroadcastTopo(plan));

        /*
         * Since all RNs are expected to be active now, broadcast the security
         * metadata to them, so that the login authentication is able to work.
         */
        final SecurityMetadata md = plan.getAdmin().getMetadata(
                SecurityMetadata.class, MetadataType.SECURITY);
        plan.addTask(new BroadcastMetadata<SecurityMetadata>(plan, md));

        makePartitionTasks();
    }

    private void makeDatacenterUpdates() {
        /*
         * Add tasks to check to see if all datacenter attributes are up to date
         * namely the repFactor, which is stored only in the topology. Add
         * the task unconditionally, so it's checked at plan execution time.
         */
        for (Datacenter dc : candidate.getTopology().getSortedDatacenters()) {
            plan.addTask(new UpdateDatacenter(plan, dc.getResourceId(),
                                              dc.getRepFactor()));
        }
    }

    /**
     * Create tasks to execute all the RN creations.
     */
    private void makeCreateRNTasks() {

        Topology target = candidate.getTopology();

        /* These are the brand new shards */
        List<RepGroupId> newShards = diff.getNewShards();
        for (int planShardIdx = 0;
             planShardIdx < newShards.size();
             planShardIdx++) {

            RepGroupId candidateShard = newShards.get(planShardIdx);
            ShardChange change = diff.getShardChange(candidateShard);
            String snSetDescription = change.getSNSetDescription(target);

            /* We wouldn't expect a brand new shard to host old RNs. */
            if (change.getRelocatedRNs().size() > 0) {
                throw new IllegalStateException
                    ("New shard " + candidateShard + " to be deployed on " +
                     snSetDescription + ", should not host existing RNs " +
                     change.getRelocatedRNs());
            }

            /* Make the shard. */
            plan.addTask(new DeployShard(plan,
                                         planShardIdx,
                                         snSetDescription));

            /* Make all the new RNs that will go on this new shard */

            /*
             * Create the first RN in a primary datacenter first, so it can be
             * the self-electing node and can act as the helper for the
             * remaining nodes, including any non-electable ones
             */
            final List<RepNodeId> newRnIds =
                new ArrayList<RepNodeId>(change.getNewRNs());
            for (final Iterator<RepNodeId> i = newRnIds.iterator();
                 i.hasNext(); ) {
                final RepNodeId rnId = i.next();
                final Datacenter dc = target.getDatacenter(rnId);
                if (dc.getDatacenterType().isPrimary()) {
                    i.remove();
                    newRnIds.add(0, rnId);
                    break;
                }
            }

            for (final RepNodeId proposedRNId : newRnIds) {
                RepNode rn = target.get(proposedRNId);
                String newMountPoint = diff.getMountPoint(proposedRNId);
                plan.addTask(new DeployNewRN(plan,
                                             rn.getStorageNodeId(),
                                             planShardIdx,
                                             newMountPoint));
            }

            /*
             * After the RNs have been created and stored in the topology
             * update their helper hosts.
             */
            for (int i = 0; i < change.getNewRNs().size(); i++) {
                plan.addTask(new UpdateNthRNHelperHost(plan, planShardIdx, i));
                plan.addTask(new NewNthRNParameters(plan, planShardIdx, i));
            }
        }

        /* These are the shards that existed before, but have new RNs */
        for (Map.Entry<RepGroupId, ShardChange> change :
             diff.getChangedShards().entrySet()) {

            RepGroupId rgId = change.getKey();
            if (newShards.contains(rgId)) {
                continue;
            }

            /* Make all the new RNs that will go on this new shard */
            for (RepNodeId proposedRNId : change.getValue().getNewRNs()) {
                RepNode rn = target.get(proposedRNId);
                String newMountPoint = diff.getMountPoint(proposedRNId);
                plan.addTask(new DeployNewRN(plan,
                                             rn.getStorageNodeId(),
                                             rgId,
                                             newMountPoint));
            }

            /*
             * After the new RNs have been created and stored in the topology
             * update the helper hosts for all the RNs in the shard, including
             * the ones that existed before.
             */
            for(RepNode member : target.get(rgId).getRepNodes()) {
                RepNodeId rnId = member.getResourceId();
                plan.addTask(new UpdateHelperHost(plan, rnId, rgId));
                plan.addTask(new NewRepNodeParameters(plan, rnId));
            }
        }
    }

    /**
     * Create tasks to move an RN from one SN to another.
     * The relocation requires three actions that must seen atomic:
     *  1. updating kvstore metadata (topology, params(disable bit, helper
     *   hosts for the target RN and all other members of the HA group) and
     *   broadcast it to all members of the shard. This requires an Admin rep
     *   group quorum
     *  2. updating JE HA rep group metadata (groupDB) and share this with all
     *   members of the JE HA group. This requires a shard master and
     *   quorum. Since we are in the business of actively shutting down one of
     *   the members of the shard, this is clearly a moment of vulnerability
     *  3. Moving the environment data to the new SN
     *  4. Delete the old environment from the old SN
     *
     * Once (1) and (2) are done, the change is logically committed. (1) and
     * (2) can fail due to lack of write availability. The RN is unavailable
     * from (1) to the end of (3). There are a number of options that can short
     * these periods of unavailability, but none that can remove it entirely.
     *
     * One option for making the time from 1->3 shorter is to reach into the JE
     * network backup layer that is the foundation of NetworkRestore, in order
     * to reduce the amount of time used by (3) to transfer data to the new SN.
     * Another option that would make the period from 1-> 2 less
     * vulnerable to lack of quorum is to be able to do writes with a less than
     * quorum number of acks, which is a pending JE HA feature.
     *
     * Both options lessen but do not remove the unavailability periods and the
     * possibility that the changes must be reverse. RelocateRN embodies step
     * 1 and 2 and attempt to clean up if either step fails.
     */
    private void makeRelocatedRNTasks() {

        Set<StorageNodeId> sourceSNs = new HashSet<StorageNodeId>();
        for (Map.Entry<RepGroupId, ShardChange> change :
             diff.getChangedShards().entrySet()) {

            for (RelocatedRN reloc : change.getValue().getRelocatedRNs()) {
                RepNodeId rnId = reloc.getRnId();
                StorageNodeId oldSNId = reloc.getOldSNId();
                StorageNodeId newSNId = reloc.getNewSNId();

                /*
                 * Stop the RN, update its params and topo, update the helper
                 * host param for all members of the group, update its HA group
                 * address, redeploy it on the new SN, and delete the RN from
                 * the original SN once the new RN has come up, and is
                 * consistent with the master. Also ask the original SN to
                 * delete the files from the environment.
                 */
                plan.addTask(new RelocateRN(plan, oldSNId, newSNId, rnId,
                                            diff.getMountPoint(rnId)));
                sourceSNs.add(oldSNId);
            }

            /*
             * SNs that were previously over capacity and have lost an RN may
             * now be able to increase the per-RN memory settings. Check
             * all the source SNs.
             */
            for (StorageNodeId snId : sourceSNs) {
                plan.addTask(new CheckRNMemorySettings(plan, snId));
            }
        }
    }

    /**
     * Partition related tasks. For a brand new deployment, add all the
     * partitions. For redistributions, generate one task per migrated
     * partition.
     */
    private void makePartitionTasks() {

        /* Brand new deployment -- only create new partitions, no migrations. */
        if (diff.getNumCreatedPartitions() > 0) {
            List<RepGroupId> newShards = diff.getNewShards();
            List<Integer> partitionCount =
                new ArrayList<Integer>(newShards.size());
            for (int i = 0; i < newShards.size(); i++) {
                ShardChange change = diff.getShardChange(newShards.get(i));
                int newParts = change.getNumNewPartitions();
                partitionCount.add(newParts);
            }

            plan.addTask(new AddPartitions(plan, partitionCount,
                                           diff.getNumCreatedPartitions()));
            return;
        }

        if (diff.getChangedShards().isEmpty()) {
            return;
        }

        /* A redistribution. Run all partition migrations in parallel. */
        final ParallelBundle bundle = new ParallelBundle();
        for (Map.Entry<RepGroupId,ShardChange> entry :
             diff.getChangedShards().entrySet()){

            RepGroupId targetRGId = entry.getKey();
            List<RelocatedPartition> pChanges =
                entry.getValue().getMigrations();
            for(RelocatedPartition pt : pChanges) {
                Task t = new MigratePartition(plan,
                                              pt.getSourceShard(),
                                              targetRGId,
                                              pt.getPartitionId());
                bundle.addTask(t);
            }
        }

        if (!bundle.isEmpty()) {
            plan.addTask(bundle);
        }
    }
}
