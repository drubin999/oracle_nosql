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
 * Represents the creation of a new component in the topology.
 */
@Persistent
public class Add extends TopologyChange {

    private static final long serialVersionUID = 1L;

    Component<?> component;

    Add(int sequenceNumber, Component<?> component) {
        super(sequenceNumber);
        assert component.getResourceId() != null;
        this.component = component;
    }

    @SuppressWarnings("unused")
    private Add() { super();}

    /* (non-Javadoc)
     * @see oracle.kv.impl.topo.change.TopologyChange#getType()
     */
    @Override
    public Type getType() {
        return Type.ADD;
    }

    @Override
    public Component<?> getComponent() {
        return component;
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.topo.change.TopologyChange#getResourceId()
     */
    @Override
    public ResourceId getResourceId() {
        return component.getResourceId();
    }

    @Override
    public Add clone() {
        Component<?> comp = component.clone();
        comp.setTopology(null);
        return new Add(sequenceNumber, comp);
    }

    @Override 
    public String toString() {
        return "Add " + component.getResourceId() + " seq=" + 
            sequenceNumber;
    }
}
