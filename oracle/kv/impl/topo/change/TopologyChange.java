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

import java.io.Serializable;

import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.topo.Topology.Component;

import com.sleepycat.persist.model.Persistent;

/**
 * The Base class for all topology changes. A sequence of changes can be
 * applied to a Topology instance, via {@link Topology#apply} to make it more
 * current.
 * <p>
 * Each TopologyChange represents a logical change entry in a logical log with
 * changes being applied in sequence via {@link Topology#apply} to modify the
 * topology and bring it up to date.
 */
@Persistent
public abstract class TopologyChange implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    public enum Type {ADD, UPDATE, REMOVE}

    int sequenceNumber;

    TopologyChange(int sequenceNumber) {
        super();
        this.sequenceNumber = sequenceNumber;
    }

    protected TopologyChange() {
    }

    public abstract Type getType();

    /**
     * Identifies the resource being changed
     */
    public abstract ResourceId getResourceId();

    /**
     * Returns the impacted component, or null if one is not available.
     */
    public abstract Component<?> getComponent();

    @Override
    public abstract TopologyChange clone();

    /**
     * Returns The sequence number associated with this change.
     */
    public int getSequenceNumber() {
        return sequenceNumber;
    }
    
    @Override
    public String toString() {
        return "seq=" + sequenceNumber + "/" + getType() + " " + 
            getResourceId() + "/" + getComponent();
    }
}
