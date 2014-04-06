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

import oracle.kv.impl.topo.ResourceId.ResourceType;

import com.sleepycat.persist.model.Persistent;

/**
 * The RepGroupMap describes the replication groups and the replication nodes
 * underlying the KVStore. It's indexed by the groupId to yield a
 * {@link RepGroup}, which in turn can be indexed by a nodeNum to yield a
 * {@link RepNode}.
 * <p>
 * The map is created and maintained by the GAT as part of the overall Topology
 * associated with the KVStore. Note that both group and ids node nums are
 * assigned from sequences to ensure there is no possibility of inadvertent
 * aliasing across the entire KVStore as groups and nodes are repeatedly
 * created and destroyed.
 */
@Persistent
public class RepGroupMap extends ComponentMap<RepGroupId, RepGroup> {

    private static final long serialVersionUID = 1L;

    public RepGroupMap(Topology topology) {
        super(topology);
    }

    @SuppressWarnings("unused")
    private RepGroupMap() {
        super();
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.topo.ComponentMap#nextId()
     */
    @Override
    RepGroupId nextId() {
       return new RepGroupId(nextSequence());
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.topo.ComponentMap#getResourceType()
     */
    @Override
    ResourceType getResourceType() {
       return ResourceType.REP_GROUP;
    }
}

