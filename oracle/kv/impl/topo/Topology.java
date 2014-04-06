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

package oracle.kv.impl.topo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import oracle.kv.impl.api.RequestHandler;
import oracle.kv.impl.api.TopologyInfo;
import oracle.kv.impl.fault.UnknownVersionException;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.topo.ResourceId.ResourceType;
import oracle.kv.impl.topo.change.TopologyChange;
import oracle.kv.impl.topo.change.TopologyChangeTracker;

import com.sleepycat.je.LockMode;
import com.sleepycat.je.Transaction;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.model.Persistent;

/**
 * Topology describes the general logical layout of the KVS and its
 * relationship to the physical elements (SNs) on which it has been mapped.
 * <p>
 * The logical layout is defined in terms of the partitions, the assignment of
 * the partitions to replication groups, the RNs that comprise each replication
 * group and the assignment of RNs to SNs.
 * <p>
 * Topology is created and maintained by the Admin; the authoritative instance
 * of the Topology is stored and maintained by the Admin in the AdminDB. Each
 * RN has a copy of the Topology that is stored in its local database and
 * updated lazily in response to changes originating at the Admin. Changes made
 * by the Admin are propagated around the KV Store via two mechanisms:
 * <ol>
 * <li>They are piggy-backed on top of responses to KVS requests.</li>
 * <li>Some commands explicitly broadcast a new topology</li>
 * </ol>
 * <p>
 * KV clients also maintain a copy of the Topology. They acquire an initial
 * copy of the Topology from the RN they contact when the KVS handle is first
 * created. Since they do not participate in the broadcast protocol, they
 * acquire updates primarily via the response mechanism. The updates can then
 * be <code>applied</code> to update the local copy of the Topology.
 * <p>
 * The Topology can be modified via its add/update/remove methods. Each such
 * operation is noted in the list of changes returned by {@link
 * #getChanges(int)}
 */

@Persistent(version=1)
public class Topology implements Metadata<TopologyInfo>, Serializable {

    private static final long serialVersionUID = 1L;

    /* The version associated with this topology instance. */
    private int version;

    /**
     * The compact id that uniquely identifies the Topology as belonging to
     * this store. It's simply a millisecond time stamp that's assigned when a
     * Topology is first created.  It's used to guard against clients that
     * incorrectly communicate with the wrong store.
     *
     * For compatibility with earlier releases, a value of 0 is treated as a
     * wildcard value from pre 2.0 releases.
     *
     * @since 2.0
     */
    private long id = 0;

    private String kvStoreName;

    /* The mapping components comprising the Topology. */
    private PartitionMap partitionMap;
    private RepGroupMap repGroupMap;
    private StorageNodeMap storageNodeMap;
    private DatacenterMap datacenterMap;

    /* Used to dispatch map-specific operations. */
    private Map<ResourceType,
                ComponentMap<? extends ResourceId,
                             ? extends Topology.Component<?>>>
            typeToComponentMaps;

    private TopologyChangeTracker changeTracker;

    /* The Topology id associated with an empty Topology. */
    public static final int EMPTY_TOPOLOGY_ID = -1;

    /*
     * The Topology id that is used to bypass topology checks for r1
     * compatibility.
     */
    public static final int NOCHECK_TOPOLOGY_ID = 0;

    /*
     * The current topology version number. New Topology instances are created
     * with this version number.
     */
    public static final int CURRENT_VERSION = 1;

    /**
     * Creates a new named empty KV Store topology.
     *
     * @param kvStoreName the name associated with the KV Store
     */
    public Topology(String kvStoreName) {
       this(kvStoreName, System.currentTimeMillis());
    }

    /**
     * Creates a named empty KV Store topology, but with a specific topology id.
     */
    public Topology(String kvStoreName, long topoId) {
        this.kvStoreName = kvStoreName;
        partitionMap = new PartitionMap(this);
        repGroupMap = new RepGroupMap(this);
        storageNodeMap = new StorageNodeMap(this);
        datacenterMap = new DatacenterMap(this);
        changeTracker = new TopologyChangeTracker(this);
        typeToComponentMaps =
            new HashMap<ResourceType,
                        ComponentMap<? extends ResourceId,
                                     ? extends Topology.Component<?>>>();

        for (ComponentMap<?,?> m : getAllComponentMaps()) {
            typeToComponentMaps.put(m.getResourceType(), m);
        }

        id = topoId;
        version = CURRENT_VERSION;
    }

    @SuppressWarnings("unused")
    private Topology() {
    }

    @Override
    public MetadataType getType() {
        return MetadataType.TOPOLOGY;
    }

    /**
     * Returns the version associated with the topology. This version number
     * can be used, for example, during java deserialization to check for
     * version compatibility. Note that it's deliberately distinct from the
     * Entity.version() number associated with it. The two version numbers will
     * often be updated together, but there may be Topology changes where we
     * need to update just one version and not the other.
     *
     * @return the version
     */
    public int getVersion() {
        return version;
    }

    /* For unit tests */
    public void setVersion(int version) {
        this.version = version;
    }

    public long getId() {
        return id;
    }

    /**
     * For test use only.
     */
    public void setId(long id) {
        this.id = id;
    }

    /*
     * Returns the top level maps that comprise the KVStore
     */
    ComponentMap<?, ?>[] getAllComponentMaps() {
        return new ComponentMap<?,?>[] {
            partitionMap, repGroupMap, storageNodeMap, datacenterMap};
    }

    /**
     * Returns the name associated with the KV Store.
     */
    public String getKVStoreName() {
        return kvStoreName;
    }

    /**
     * Returns the topology component associated with the resourceId, or null
     * if the component does not exist as part of the Topology.
     */
    public Component<?> get(ResourceId resourceId) {
        return resourceId.getComponent(this);
    }

    /* Overloaded versions of the above get() method. */
    public Datacenter get(DatacenterId datacenterId) {
        return datacenterMap.get(datacenterId);
    }

    public StorageNode get(StorageNodeId storageNodeId) {
        return storageNodeMap.get(storageNodeId);
    }

    public RepGroup get(RepGroupId repGroupId) {
        return repGroupMap.get(repGroupId);
    }

    public Partition get(PartitionId partitionMapId) {
        return partitionMap.get(partitionMapId);
    }

    public RepNode get(RepNodeId repNodeId) {
        RepGroup rg = repGroupMap.get(new RepGroupId(repNodeId.getGroupId()));
        return (rg == null) ? null : rg.get(repNodeId);
    }

    /**
     * Returns the datacenter associated with the SN
     */
    public Datacenter getDatacenter(StorageNodeId storageNodeId) {
        return get(get(storageNodeId).getDatacenterId());
    }

    /**
     * Returns the datacenter associated with the RN
     */
    public Datacenter getDatacenter(RepNodeId repNodeId) {
        return getDatacenter(get(repNodeId).getStorageNodeId());
    }

    /**
     * Returns the partition associated with the key. This is the basis for
     * request dispatching.
     */
    public PartitionId getPartitionId(byte[] keyBytes) {
        if (partitionMap.size() == 0) {
            throw new IllegalArgumentException
             ("Store is not yet configured and deployed, and cannot accept " +
              "data");
        }
        return partitionMap.getPartitionId(keyBytes);
    }

    /**
     * Returns the group associated with the partition. This is the basis for
     * request dispatching.
     */
    public RepGroupId getRepGroupId(PartitionId partitionId) {
        return partitionMap.getRepGroupId(partitionId);
    }

    /**
     * Return the set of all RepGroupIds contained in this topology.
     */
    public Set<RepGroupId> getRepGroupIds() {
        Set<RepGroupId> rgIdSet = new HashSet<RepGroupId>();
        for (RepGroup rg : repGroupMap.getAll()) {
            rgIdSet.add(rg.getResourceId());
        }
        return rgIdSet;
    }

    /**
     * Returns the partition map
     */
    public PartitionMap getPartitionMap() {
        return partitionMap;
    }

    /**
     * Returns the rep group map
     */
    public RepGroupMap getRepGroupMap() {
        return repGroupMap;
    }

    /**
     * Returns the StorageNodeMap
     */
    public StorageNodeMap getStorageNodeMap() {
        return storageNodeMap;
    }

    /**
     * Returns all RepNodes sorted by id.  The sort is by RepGroup id, then by
     * node number, lowest first.
     */
    public List<RepNode> getSortedRepNodes() {
        List<RepNode> srn = new ArrayList<RepNode>();
        for (RepGroup rg: repGroupMap.getAll()) {
            for (RepNode rn: rg.getRepNodes()) {
                srn.add(rn);
            }
        }
        Collections.sort(srn);
        return srn;
    }

    /**
     * Returns all RepNodeIds for a given shard, sorted by id.
     */
    public List<RepNodeId> getSortedRepNodeIds(RepGroupId rgId) {
        List<RepNodeId> srn = new ArrayList<RepNodeId>();
        for (RepNode rn: repGroupMap.get(rgId).getRepNodes()) {
            srn.add(rn.getResourceId());
        }
        Collections.sort(srn);
        return srn;
    }

    /**
     * Returns all Storage Nodes, sorted by id.
     */
    public List<StorageNode> getSortedStorageNodes() {
        List<StorageNode> sns =
            new ArrayList<StorageNode>(storageNodeMap.getAll());
        Collections.sort(sns);
        return sns;
    }

    public List<StorageNodeId> getStorageNodeIds() {
        List<StorageNodeId> snIds = new ArrayList<StorageNodeId>();
        for (StorageNode sn : storageNodeMap.getAll()) {
            snIds.add(sn.getResourceId());
        }
        return snIds;
    }

    /**
     * Returns a set of RepNodeIds for all RepNodes in the Topology that are
     * deployed to the given datacenter; or all RepNodes in the Topology if
     * <code>null</code> is input.
     */
    public Set<RepNodeId> getRepNodeIds(DatacenterId dcid) {
        final Set<RepNodeId> allRNIds = new HashSet<RepNodeId>();
        for (RepGroup rg : repGroupMap.getAll()) {
            for (RepNode rn : rg.getRepNodes()) {
                final RepNodeId rnid = rn.getResourceId();
                if (dcid == null) {
                    /* Return all RepNodeIds from all datacenters. */
                    allRNIds.add(rnid);
                } else {
                    if (dcid.equals(getDatacenter(rnid).getResourceId())) {
                        /* Return RepNodeId only if it belongs to the dc. */
                        allRNIds.add(rnid);
                    }
                }
            }
        }
        return allRNIds;
    }

    /**
     * Returns a set of RepNodeIds for all RepNodes in the Topology.
     */
    public Set<RepNodeId> getRepNodeIds() {
        return getRepNodeIds(null);
    }

    /**
     * Returns the set of RepNodeIds for all RepNodes hosted on this SN
     */
    public Set<RepNodeId> getHostedRepNodeIds(StorageNodeId snId) {
        Set<RepNodeId> snRNIds = new HashSet<RepNodeId>();
        for (RepGroup rg : repGroupMap.getAll()) {
            for (RepNode rn : rg.getRepNodes()) {
                if (rn.getStorageNodeId().equals(snId)) {
                    snRNIds.add(rn.getResourceId());
                }
            }
        }
        return snRNIds;
    }

    /**
     * Returns the DatacenterMap
     */
    public DatacenterMap getDatacenterMap() {
        return datacenterMap;
    }

    /**
     * Returns datacenters sorted by id.
     */
    public List<Datacenter> getSortedDatacenters() {
        List<Datacenter> sdc =
            new ArrayList<Datacenter>(datacenterMap.getAll());
        Collections.sort(sdc, new Comparator<Datacenter>() {
                @Override
                public int compare(Datacenter o1, Datacenter o2) {
                    DatacenterId id1 = o1.getResourceId();
                    DatacenterId id2 = o2.getResourceId();
                    return id1.getDatacenterId() - id2.getDatacenterId();
                }});
        return sdc;
    }

    public TopologyChangeTracker getChangeTracker() {
        return changeTracker;
    }

    /**
     * Returns the current change sequence number associated with Topology.
     * @return the current sequence number
     */
    @Override
    public int getSequenceNumber() {
        return changeTracker.getSeqNum();
    }


    /**
     * Creates a copy of this Topology object.
     *
     * @return the new Topology instance
     */
    public Topology getCopy() {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream() ;
            ObjectOutputStream oos = new ObjectOutputStream(bos) ;
            oos.writeObject(this);
            oos.close();

            ByteArrayInputStream bis =
                new ByteArrayInputStream(bos.toByteArray()) ;
            ObjectInputStream ois = new ObjectInputStream(bis);

            return (Topology) ois.readObject();
        } catch (IOException ioe) {
            throw new IllegalStateException("Unexpected exception", ioe);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Unexpected exception", e);
        }
    }

    /**
     * The sequence of changes to be applied to this instance of the Topology
     * to make it more current. Since changes must be applied in strict
     * sequence, the following condition must hold:
     * <pre>
     * changes.first.getSequenceNumber() <= (Topology.getSequenceNumber() + 1)
     * </pre>
     *
     * Applying the changes effectively move the sequence number forward so
     * that
     * <pre>
     * Topology.getSequenceNumber() == changes.last().getSequenceNumber()
     * </pre>
     * <p>
     * The implementation is prepared to deal with multiple concurrent requests
     * to update the topology, by serializing such requests.
     *
     * @param changes the sequence of changes to be applied to update the
     * Topology in ascending change sequence order
     *
     * @return true if the Topology was in fact updated
     */
    public boolean apply(List<TopologyChange> changes) {
        if (changes == null) {
            return false;
        }

        if (changes.size() == 0) {
            return false;
        }

        /*
         * The change list may overlap the current topology, but there should
         * be no gap. If there is a gap, the changes cannot be safely applied.
         */
        if (changes.get(0).getSequenceNumber() >
            (getSequenceNumber() + 1)) {
            throw new IllegalStateException
                ("Unexpected gap in topology sequence. Current sequence=" +
                 getSequenceNumber() + ", first change =" +
                 changes.get(0).getSequenceNumber());
        }

        int changeCount = 0;
        for (TopologyChange change : changes) {
            if (change.getSequenceNumber() <= getSequenceNumber()) {
                /* Skip the changes we already have. */
                continue;
            }
            changeCount++;
            final ResourceType rtype = change.getResourceId().getType();
            if (rtype == ResourceType.REP_NODE) {
                RepNodeId rnId = (RepNodeId)change.getResourceId();
                RepGroup rg =
                    repGroupMap.get(new RepGroupId(rnId.getGroupId()));
                rg.apply(change);
                continue;
            }
            typeToComponentMaps.get(rtype).apply(change);
        }
        return changeCount > 0;
    }

    @Override
    public TopologyInfo getChangeInfo(int startSeqNum) {
        return new TopologyInfo(this, getChanges(startSeqNum));
    }

    /**
     * Returns the dense list of topology changes starting with
     * <code>startSequenceNumber</code>. This method is typically used by the
     * {@link RequestHandler} to return changes as part of a response.
     *
     * @param startSeqNum the inclusive start of the sequence of
     * changes to be returned
     *
     * @return the list of changes starting with startSequenceNumber and ending
     * with getSequenceNumber(). Return null if startSequenceNumber >
     * getSequenceNumber() or if the topology does not contain changes at
     * startSeqNum because they have been discarded.
     *
     * @see #discardChanges
     */
    public List<TopologyChange> getChanges(int startSeqNum) {
        return changeTracker.getChanges(startSeqNum);
    }

    /**
     * Discards the tracked changes associated with this Topology. All changes
     * older than or equal to <code>startSeqNum</code> are discarded. Any
     * subsequent {@link #getChanges} calls to obtain changes
     * <code>startSeqNum</code> will result in IllegalArgumentException being
     * thrown.
     */
    public void discardChanges(int startSeqNum) {
        changeTracker.discardChanges(startSeqNum);
    }

    /**
     * Persist the Topology in the environment. The Topology is persisted in
     * the entity store named: <code>TOPOLOGY_STORE_NAME</code> as the value
     * associated with the key: <code>TOPOLOGY_KEY</code>. If the store does
     * not exist, a new store is created to hold the Topology. If one exists,
     * the Topology object in it is updated.
     * <p>
     * Note that the entire Topology, which can be a large object, is stored as
     * a single value. This matches the needs of the typical user of Topology
     * which need access to the entire object in memory.
     *
     * @param estore the entity store that holds the Topology
     * @param txn the transaction in progress
     */
    public void persist(EntityStore estore, Transaction txn) {
        final PrimaryIndex<String, TopologyHolder> ti =
            estore.getPrimaryIndex(String.class, TopologyHolder.class);
        ti.put(txn, new TopologyHolder(this));
    }

    /**
     * Fetches a previously <code>persisted</code> Topology from the
     * environment. The version number associated with the Topology should
     * be Topology.CURRENT_VERSION, expect when the topology is being
     * upgraded in which case it could be an earlier version. Upgrade code
     * paths need to check the version and tale appropriate action.
     *
     * @param estore the entity store that holds the Topology
     * @param txn the transaction to be used to fetch the Topology. It may be
     * null if the database it's being fetched from is non-transactional as is
     * the case with topology stored at RNs.
     *
     * @see #persist
     */
    public static Topology fetch(EntityStore estore, Transaction txn) {
        final PrimaryIndex<String, TopologyHolder> ti =
            estore.getPrimaryIndex(String.class, TopologyHolder.class);
        final TopologyHolder holder =
            ti.get(txn, TopologyHolder.getKey(), LockMode.READ_UNCOMMITTED);

        return (holder == null) ? null : holder.getTopology();
    }

    public static Topology fetchCommitted(EntityStore estore, Transaction txn) {
        final PrimaryIndex<String, TopologyHolder> ti =
            estore.getPrimaryIndex(String.class, TopologyHolder.class);
        final TopologyHolder holder =
            ti.get(txn, TopologyHolder.getKey(), LockMode.READ_COMMITTED);

        return (holder == null) ? null : holder.getTopology();
    }

    /*
     * Methods used to add a new component to the KV Store.
     */
    public Datacenter add(Datacenter datacenter) {
        return datacenterMap.add(datacenter);
    }

    public StorageNode add(StorageNode storageNode) {
        return storageNodeMap.add(storageNode);
    }

    public RepGroup add(RepGroup repGroup) {
        return repGroupMap.add(repGroup);
    }

    public Partition add(Partition partition) {
        return partitionMap.add(partition);
    }

    /** For use when redistributing partitions. */
    public Partition add(Partition partition, PartitionId partitionId) {
        return partitionMap.add(partition, partitionId);
    }

    /**
     * Methods used to update the component associated with the resourceId with
     * this one. The component previously associated with the resource id
     * is no longer considered to be a part of the topology.
     */
    public Datacenter update(DatacenterId datacenterId,
                             Datacenter datacenter) {
        return datacenterMap.update(datacenterId, datacenter);
    }

    public StorageNode update(StorageNodeId storageNodeId,
                              StorageNode storageNode) {
        return storageNodeMap.update(storageNodeId, storageNode);
    }

    public RepGroup update(RepGroupId repGroupId,
                           RepGroup repGroup) {
        return repGroupMap.update(repGroupId, repGroup);
    }

    public Partition update(PartitionId partitionId,
                            Partition partition) {
        return partitionMap.update(partitionId, partition);
    }

    /**
     * Updates the location of a partition.
     */
    public Partition updatePartition(PartitionId partitionId,
                                     RepGroupId repGroupId)
    {
        return update(partitionId, new Partition(repGroupId));
    }

    /**
     * Methods used to remove an existing component from the Topology. Note
     * that the caller must ensure that removal of the component does not
     * create dangling references. [Sam: could introduce reference counting.
     * worth the effort? ]
     */

    public Datacenter remove(DatacenterId datacenterId) {
        return datacenterMap.remove(datacenterId);
    }

    public StorageNode remove(StorageNodeId storageNodeId) {
        return storageNodeMap.remove(storageNodeId);
    }

    public RepGroup remove(RepGroupId repGroupId) {
        return repGroupMap.remove(repGroupId);
    }

    public Partition remove(PartitionId partitionId) {
        return partitionMap.remove(partitionId);
    }

    public RepNode remove(RepNodeId repNodeId) {
        RepGroup rg = repGroupMap.get(new RepGroupId(repNodeId.getGroupId()));
        if (rg == null) {
            throw new IllegalArgumentException
                ("Rep Group: " + repNodeId.getGroupId() +
                 " is not in the topology");
        }
        return rg.remove(repNodeId);
    }

    private void readObject(ObjectInputStream ois)
        throws IOException, ClassNotFoundException {

        ois.defaultReadObject();
        upgrade();
    }

    /**
     * Upgrades an R1 topology.
     *
     * The upgrade involves just one change: The initialization of the new
     * Datacenter.repFactor field. This field is computed by examining the
     * shards defined by the topology. The algorithm used below picks a random
     * shard to determine the distribution of RNs across DCs and then uses this
     * distribution for determining the RF for each DC. If the distribution is
     * inconsistent across shards, it leaves the Topology unchanged and returns
     * false.
     *
     * @return true if the topology was updated and is now the CURRENT_VERSION.
     * It returns false if the Topology was not modified. This can be because
     * the Topology was already the current version, or because the Topology
     * could not be upgraded because the shard layout did not permit
     * computation of a replication factor. It's the caller's responsibility
     * to check the version in this case and decide to take appropriate action.
     *
     * @throws UnknownVersionException when invoked on a Topology with a
     * version that's later than the CURRENT_VERSION
     */
    public boolean upgrade()
        throws UnknownVersionException {

        if (version == CURRENT_VERSION) {
            return false;
        }

        if (version > CURRENT_VERSION) {
            /* Only accept older topologies for upgrade. */
            throw new UnknownVersionException
                ("Upgrade encountered unknown version",
                 Topology.class.getName(),
                 CURRENT_VERSION,
                 version);
        }

        if (getRepGroupMap().getAll().size() == 0) {
            /* An empty r1 topology */
            version = CURRENT_VERSION;
            return true;
        }

        final RepGroup protoGroup =
            getRepGroupMap().getAll().iterator().next();
        final HashMap<DatacenterId, Integer> protoMap =
            getRFMap(protoGroup);

        /* Verify that shards are all consistent. */
        for (RepGroup rg : getRepGroupMap().getAll()) {
            final HashMap<DatacenterId, Integer> rfMap = getRFMap(rg);

            if (!protoMap.equals(rfMap)) {
                return false;
            }
        }

        /* Shards are isomorphic, can update the topology. */
        for (Entry<DatacenterId, Integer>  dce : protoMap.entrySet()) {
            final Datacenter dc = get(dce.getKey());
            dc.setRepFactor(dce.getValue());
        }
        version = CURRENT_VERSION;
        return true;
    }

    /**
     * Returns a Map associating a RF with each DC used by the RNs in the
     * shard. This method is used solely during upgrades.
     */
    private HashMap<DatacenterId, Integer> getRFMap(RepGroup rgProto) {

        final HashMap<DatacenterId, Integer> rfMap =
            new HashMap<DatacenterId, Integer>();

        /* Establish the prototype distribution */
        for (RepNode rn : rgProto.getRepNodes()) {
            final DatacenterId dcId =
                get(rn.getStorageNodeId()).getDatacenterId();
            Integer rf = rfMap.get(dcId);
            if (rf == null) {
                rf = 0;
            }
            rfMap.put(dcId, rf + 1);
        }
        return rfMap;
    }

    /**
     * All components of the Topology implement this interface
     */
    @Persistent
    public static abstract class Component<T extends ResourceId>
        implements Serializable, Cloneable {

        private static final long serialVersionUID = 1L;

        /*
         * The Topology associated with this component; it's null if the
         * component has not been "added" to the Topology
         */
        private Topology topology;

        /*
         * The unique resource id; it's null if the component has not been
         * "added" to the Topology
         */
        private ResourceId resourceId;

        private int sequenceNumber;

        public Component() {
        }

        @Override
        public abstract Component<?> clone();

        /*
         * The hashCode and equals methods are primarily defined for
         * testing purposes. Note that they exclude the topology field
         * to permit comparison of components across Topologies.
         */
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result +
                ((resourceId == null) ? 0 : resourceId.hashCode());
            result = prime * result + sequenceNumber;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            Component<?>other = (Component<?>)obj;
            if (resourceId == null) {
                if (other.resourceId != null) {
                    return false;
                }
            } else if (!resourceId.equals(other.resourceId)) {
                return false;
            }
            if (sequenceNumber != other.sequenceNumber) {
                return false;
            }
            return true;
        }

        /*
         * The hashCode and equals methods are primarily defined for
         * testing purposes. Note that they exclude the topology field
         * to permit comparison of components across Topologies.
         */

        /**
         * A variation of clone used to ensure that references outside the
         * component are not maintained for Topology log serialization where
         * the component will be copied and applied to a different Topology.
         *
         * @return the standalone copy of the object
         */
        public Component<?> cloneForLog() {
            Component<?> clone = clone();
            clone.topology = null;
            return clone;
        }

        protected Component(Component<?> other) {
            this.topology = other.topology;
            this.resourceId = other.resourceId;
            this.sequenceNumber = other.sequenceNumber;
        }

        /**
         * Returns the topology associated with this component, or null if it's
         * a free-standing component.
         */
        public Topology getTopology() {
            return topology;
        }

        public void setTopology(Topology topology) {
            assert (this.topology == null) || (topology == null);
            this.topology = topology;
        }

        /* Adds the component to the topology */
        public void setResourceId(T resourceId) {
            assert (this.resourceId == null);
            this.resourceId = resourceId;
        }

        /**
         * Returns the unique resourceId associated with the Resource
         */
        @SuppressWarnings("unchecked")
        public T getResourceId() {
            return (T)resourceId;
        }

        /* Returns the ResourceType associated with this component. */
        abstract ResourceType getResourceType();

        /**
         * Returns the sequence number associated with the
         * {@link TopologyChange} that last modified the component. The
         * following invariant must always hold.
         *
         * Component.getSequenceNumber() <= Topology.getSequenceNumber()
         */
        public int getSequenceNumber() {
            return sequenceNumber;
        }

        public void setSequenceNumber(int sequenceNumber) {
            this.sequenceNumber = sequenceNumber;
        }

        /**
         * Returns the StorageNodeId of the SN hosting this component. Only
         * components which reside on a storage node, such as RepNodes,
         * StorageNodes, and AdminInstances will implement this.
         */
        public StorageNodeId getStorageNodeId() {
            throw new UnsupportedOperationException
                ("Not supported for component " + resourceId);
        }

        /**
         * Returns true if this component implements a MonitorAgent protocol
         */
        public boolean isMonitorEnabled() {
            return false;
        }
    }
}
