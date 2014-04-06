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

import java.util.Collection;

import oracle.kv.impl.topo.ResourceId.ResourceType;
import oracle.kv.impl.topo.Topology.Component;
import oracle.kv.impl.topo.change.TopologyChange;

import com.sleepycat.persist.model.Persistent;

/**
 * The RepGroup in a {@link RepGroupMap}. It identifies the RNs within the
 * group.
 * <p>
 * Note that a RepGroup simply serves to group RepNodes. It does not have any
 * attributes associated with it.
 */
@Persistent
public class RepGroup extends Topology.Component<RepGroupId> {

    private static final long serialVersionUID = 1L;
    private final ComponentMap<RepNodeId, RepNode> repNodeMap;

    public RepGroup() {
        repNodeMap = new RepNodeComponentMap(this, null);
    }

    /**
     * Note that this constructor does not copy the component map. It's
     * intended exclusively for use by the cloning operation below to create a
     * RepGroup entry in the topology change list.
     */
    private RepGroup(RepGroup repGroup) {
        super(repGroup);
        repNodeMap = new RepNodeComponentMap(this, null);
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.topo.Topology.Component#getResourceType()
     */
    @Override
    public ResourceType getResourceType() {
        return ResourceType.REP_GROUP;
    }

    public Collection<RepNode> getRepNodes() {
        return repNodeMap.getAll();
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.topo.Topology.Component#clone()
     */
    @Override
    public Component<?> clone() {
        return new RepGroup(this);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result +
            ((repNodeMap == null) ? 0 : repNodeMap.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        RepGroup other = (RepGroup) obj;
        if (repNodeMap == null) {
            if (other.repNodeMap != null) {
                return false;
            }
        } else if (!repNodeMap.equals(other.repNodeMap)) {
            return false;
        } else if (!super.equals(obj)) {
                return false;
        }
        return true;
    }

    public RepNode get(RepNodeId repNodeId) {
       return repNodeMap.get(repNodeId);
    }

    public RepNode add(RepNode repNode) {
        return repNodeMap.add(repNode);
    }

    public RepNode update(RepNodeId resourceId,
                          RepNode repNode) {
        return repNodeMap.update(resourceId, repNode);
    }

    public RepNode remove(RepNodeId repNodeId) {
        return repNodeMap.remove(repNodeId);
    }

    public void apply(TopologyChange change) {
        repNodeMap.apply(change);
    }

    /**
     * Wraps the set method to ensure that the "topology" associated with the
     * repNodeMap is set appropriately as well.
     */
    @Override
    public void setTopology(Topology topology) {
        super.setTopology(topology);
        repNodeMap.setTopology(topology);
    }

    @Override
    public String toString() {
        return "[" + getResourceId() + "]";
    }
}
