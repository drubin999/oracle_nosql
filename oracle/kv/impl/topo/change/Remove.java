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

package oracle.kv.impl.topo.change;

import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.Topology.Component;

import com.sleepycat.persist.model.Persistent;

/**
 * Removes an existing component from the Topology
 */
@Persistent
public class Remove extends TopologyChange {

    private static final long serialVersionUID = 1L;
    private ResourceId resourceId;

    /**
     * @param sequenceNumber
     */
    public Remove(int sequenceNumber,
                  ResourceId resourceId) {
        super(sequenceNumber);

        assert resourceId != null;

        this.resourceId = resourceId;
    }

    @SuppressWarnings("unused")
    private Remove() {
        super();
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.topo.change.TopologyChange#getType()
     */
    @Override
    public Type getType() {
        return Type.REMOVE;
    }

    @Override
    public ResourceId getResourceId() {
        return resourceId;
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.topo.change.TopologyChange#getComponent()
     */
    @Override
    public Component<?> getComponent() {
        return null;
    }

    @Override
    public Remove clone() {
        return new Remove(sequenceNumber, resourceId);
    }
}
