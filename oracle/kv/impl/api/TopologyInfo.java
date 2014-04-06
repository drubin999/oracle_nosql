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

package oracle.kv.impl.api;

import java.io.Serializable;
import java.util.List;

import oracle.kv.impl.api.rgstate.RepNodeStateUpdateThread;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.topo.change.TopologyChange;

/**
 * Topology information that's returned as part of a response whenever there's
 * a mismatch between the requester/responder topologies.
 */
public class TopologyInfo implements MetadataInfo, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The topology id associated with the topology that supplied the topo
     * seq number and the changes.
     *
     * @since 2.0
     */
    private final long topoId;

    /**
     * The responder's topology seq number.
     */
    private final int respSeqNum;

    /**
     * The changes to be communicated back, or null if the responder's
     * topology is obsolete relative to that of the requester. The list may
     * also be null if the responder has a newer topology but does not have
     * all the changes that the requester needs to bring its topology up to
     * date. It's then up to the requester to pull a complete copy of the
     * topology from this responding RN.
     */
    private final List<TopologyChange> changes;

    /**
     * Used to denote that a RN has an empty Topology and is waiting for
     * topology to be pushed to it.
     */
    public static TopologyInfo EMPTY_TOPO_INFO =
        new TopologyInfo(Topology.EMPTY_TOPOLOGY_ID,
                         Topology.EMPTY_SEQUENCE_NUMBER, null);

    public TopologyInfo(Topology topo,
                        List<TopologyChange> changes) {
        this(topo.getId(), topo.getSequenceNumber(), changes);
    }

    public TopologyInfo(long topoId,
                        int respSeqNum,
                        List<TopologyChange> changes) {
        this.topoId = topoId;
        this.respSeqNum = respSeqNum;
        this.changes = changes;
    }

    @Override
    public MetadataType getType() {
        return MetadataType.TOPOLOGY;
    }

    @Override
    public int getSourceSeqNum() {
        return respSeqNum;
    }

    @Override
    public boolean isEmpty() {
        return (changes == null) || changes.isEmpty();
    }

    public long getTopoId() {
        return topoId;
    }

    /**
     * Returns the list of topology changes.
     * <p>
     * It's the list of topology changes that were returned because, the
     * responding node had more up to date topology information than the node
     * that initiated the request. That is, {@link Request#getTopoSeqNumber()}
     * &LT {@link Topology#getSequenceNumber} at the RN that actually serviced
     * the request.
     * <p>
     * The requesting RN will take these changes and apply them to its copy of
     * the topology to ensure that it's at least as current as the responding
     * node. Note that it may be possible for the requesting node to get
     * multiple copies of changes lists from the same, or different, RNs until
     * its copy is caught up and the updated sequence number is sent out in
     * subsequent requests. So the topology update must be appropriately
     * synchronized.
     * <p>
     * The list is null if {@link Request#getTopoSeqNumber()} &GT=
     * {@link Topology#getSequenceNumber}, that is, the requesting node is at
     * least as current as the responding node. Or if the responding node has a
     * more current topology, but does not have all the incremental changes
     * needed to bring the requesting node up to date.
     * <p>
     * If the requesting node has a more current topology, the requesting node
     * arranges to push the topology changes over to the responding node via
     * the {@link RepNodeStateUpdateThread}.
     */
    public List<TopologyChange> getChanges() {
        return changes;
    }
}
