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

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;

import oracle.kv.impl.topo.change.TopologyChange;

import com.sleepycat.persist.model.Persistent;

/**
 * The Map implementation underlying the map components that make up the
 * Topology
 */
@Persistent
abstract class ComponentMap<RID extends ResourceId,
                            COMP extends Topology.Component<RID>>
    implements Serializable {

    private static final long serialVersionUID = 1L;

    protected Topology topology;

    /* Components stored in this map are assigned sequence number from here */
    private int idSequence = 0;

    protected HashMap<RID, COMP> cmap;

    public ComponentMap(Topology topology) {
        super();
        this.topology = topology;
        this.cmap = new HashMap<RID, COMP>();
    }

    protected ComponentMap() {
    }

    public void setTopology(Topology topology) {
       this.topology = topology;
    }

    public int nextSequence() {
        return ++idSequence;
    }

    /**
     * Returns the component associated with the resourceId
     *
     * @param resourceId the id used for the lookup
     *
     * @return the component if it exists
     */
    public COMP get(RID resourceId) {
        return cmap.get(resourceId);
    }

    public COMP put(COMP component) {
        return cmap.put(component.getResourceId(), component);
    }

    /**
     * Remove all components from this map and reset the id sequence, in
     * order to create a "blank" map for comparison purposes.
     */
    public void reset() {
        idSequence = 0;
        cmap.clear();
    }

    /*
     * Returns the next id that served as the basis for a new component
     * in the Component type
     */
    abstract RID nextId();

    /* Returns the type of resource id index used by this map. */
    abstract ResourceId.ResourceType getResourceType();

    /**
     * Creates a new Topology component and adds it to the map assigning it
     * with a newly created unique resourceId.
     *
     * @param component to be added
     *
     * @return the component modified with a resource id, that is now part of
     * the topology
     */
    COMP add(COMP component) {
        return add(component, nextId());
    }

    /**
     * Create a new topology component with a preset resource id (for
     * partitions).
     *
     * TODO: confer with Sam.
     */
    COMP add(COMP component, RID resId) {
        if (component.getTopology() != null) {
            throw new IllegalArgumentException("component is already a part " +
                                                "of a Topology");
        }
        component.setTopology(topology);
        component.setResourceId(resId);
        final COMP prev = put(component);
        topology.getChangeTracker().logAdd(component);
        if (prev != null) {
            throw new IllegalStateException(prev +
                                            " was overwritten in topology by " +
                                            component);
        }
        return component;
    }

    /**
     * Updates the component associated with the resource id
     *
     * @param component to be added
     *
     * @return the component modified with a resource id, that is now part of
     * the topology
     */
    COMP update(RID resourceId, COMP component) {
        if ((component.getTopology() != null) ||
            (component.getResourceId() != null)) {
            throw new IllegalArgumentException("component is already a part " +
                                                "of a Topology");
        }

        @SuppressWarnings("unchecked")
        COMP prev = (COMP)topology.get(resourceId);
        if (prev == null) {
            throw new IllegalArgumentException("component: " + resourceId +
                                               " absent from Topology");
        }
        /* Clear just the topology, retain the resource id. */
        prev.setTopology(null);
        component.setTopology(topology);
        component.setResourceId(resourceId);
        prev = put(component);
        assert prev != null;
        topology.getChangeTracker().logUpdate(component);
        return component;
    }

    /**
     * Removes the component associated with the resourceId
     *
     * @param resourceId the id used for the lookup
     *
     * @return the previous component associated with the ResourceId
     */
    public COMP remove(RID resourceId) {
        COMP component = cmap.remove(resourceId);
        if (component == null) {
            throw new IllegalArgumentException("component: " + resourceId +
                                               " absent from Topology");
        }
        /* Clear just the topology, retain the resource id. */
        component.setTopology(null);
        topology.getChangeTracker().logRemove(resourceId);
        return component;
    }

    public int size() {
        return cmap.size();
    }

    /**
     * Returns all the components present in the map
     */
    public Collection<COMP> getAll() {
        return cmap.values();
    }

    /**
     * Return the resource ids for all components present in the map.
     */
    public Collection<RID> getAllIds() {
        return cmap.keySet();
    }

    /**
     * Applies the change to the map. The various sequences need to be dealt
     * with carefully.
     * <p>
     * If the operation is an Add, the generated resourceId must match the
     * one that was passed in.
     * <p>
     * The change sequence numbers must also align up currently.
     */
    public void apply(TopologyChange change) {
         final ResourceId resourceId = change.getResourceId();

         if (change.getType() == TopologyChange.Type.REMOVE) {
            COMP prev = cmap.remove(resourceId);
            if (prev == null) {
                throw new IllegalStateException
                    ("Delete operation detected missing component: " +
                     resourceId);
            }
            topology.getChangeTracker().logRemove(resourceId);

            return;
         }

         @SuppressWarnings("unchecked")
         COMP prev = (COMP)topology.get(resourceId);

         @SuppressWarnings("unchecked")
         COMP curr = (COMP)change.getComponent();
         curr.setTopology(topology);
         int logSeqNumber = curr.getSequenceNumber();

         prev = put(curr);

         switch (change.getType()) {

             case ADD:
                 /* Consume a resource Id */
                 RID consumeId = nextId();
                 if (!consumeId.equals(resourceId)) {
                     throw new IllegalStateException
                         ("resource sequence out of sync; expected: " +
                          consumeId + " replayId: " + resourceId);
                 }
                 if (prev != null) {
                     throw new IllegalStateException
                         ("ADD operation found existing component: " +
                          resourceId);
                 }
                 topology.getChangeTracker().logAdd(curr);
                 break;

             case UPDATE:
                 if (prev == null) {
                     throw new IllegalStateException
                         ("UPDATE operation detected missing component" +
                          resourceId);
                 }
                 /* Clear just the topology, retain the resource id. */
                 prev.setTopology(null);
                 topology.getChangeTracker().logUpdate(curr);
                 break;

             default:
                 throw new IllegalStateException
                     ("unexpected type: " + change.getType());
         }

         if (curr.getSequenceNumber() != logSeqNumber) {
             throw new IllegalStateException
                 ("change sequence mismatch; log #: " + logSeqNumber +
                  " replay #: " + curr.getSequenceNumber());
         }
     }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((cmap == null) ? 0 : cmap.hashCode());
        result = prime * result + idSequence;
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

        if (getClass() != obj.getClass()) {
            return false;
        }

        ComponentMap<?, ?> other = (ComponentMap<?, ?>) obj;
        if (cmap == null) {
            if (other.cmap != null) {
                return false;
            }
        } else if (!cmap.equals(other.cmap)) {
            return false;
        }

        if (idSequence != other.idSequence) {
            return false;
        }

        return true;
    }
}
