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

import oracle.kv.impl.map.HashKeyToPartitionMap;
import oracle.kv.impl.topo.ResourceId.ResourceType;

import com.sleepycat.persist.model.Persistent;

@Persistent
public class PartitionMap extends
    ComponentMap<PartitionId, Partition> {

    private static final long serialVersionUID = 1L;

    transient HashKeyToPartitionMap keyToPartitionMap;

    public PartitionMap(Topology topology) {
        super(topology);
    }

    @SuppressWarnings("unused")
    private PartitionMap() {
        super();
    }

    /**
     * Returns the number of partitions in the partition map.
     */
    public int getNPartitions() {
        return cmap.size();
    }

    /**
     * Returns the partition id for the environment that contains the
     * replicated partition database associated with the given key.
     *
     * @param keyBytes the key used to identify the partition.
     *
     * @return the partition id that contains the key.
     */
    PartitionId getPartitionId(byte[] keyBytes) {
        if ((keyToPartitionMap == null) ||
            (keyToPartitionMap.getNPartitions() != size())) {
            /* Initialize transient field on demand. */
            keyToPartitionMap = new HashKeyToPartitionMap(size());
        }
        return keyToPartitionMap.getPartitionId(keyBytes);
    }

    /**
     * Returns the rep group id for the environment that contains the
     * replicated partition database associated with the given partition.
     *
     * <p>
     * Note that the interface does not make any provisions for returning a
     * <i>NULL</i> RepGroupId. This is because a correctly initialized
     * PartitionMap must always have a RepGroupId associated with every
     * partition.  The RepGroupId may be "out of date" due to ongoing partition
     * migrations, but it can not be NULL.
     *
     * @param partitionId the partitionId.
     *
     * @return the id of the RepGroup that contains the partition.
     */
    RepGroupId getRepGroupId(PartitionId partitionId) {
        return cmap.get(partitionId).getRepGroupId();
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.topo.ComponentMap#nextId()
     */
    @Override
    PartitionId nextId() {
        return new PartitionId(nextSequence());
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.topo.ComponentMap#getResourceType()
     */
    @Override
    ResourceType getResourceType() {
        return ResourceType.PARTITION;
    }
}
