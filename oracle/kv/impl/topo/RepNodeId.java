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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.sleepycat.persist.model.Persistent;

/**
 * The KV Store wide unique resource id identifying a REP_NODE in the KV Store.
 */
@Persistent
public class RepNodeId extends ResourceId implements Comparable<RepNodeId> {

    private static final long serialVersionUID = 1L;

    private static final String RN_PREFIX = "rn";

    /* The store-wide unique group id. */
    private int groupId;

    /* The group-wide unique node number. */
    private int nodeNum;

    /**
     * The store-wide unique node id is constructed from the store-wide unique
     * group id and the group-wide unique node id.
     *
     * @param groupId the store-wide unique group id
     * @param nodeNum group-wide unique node number
     */
    public RepNodeId(int groupId, int nodeNum) {
        super();
        this.groupId = groupId;
        this.nodeNum = nodeNum;
    }

    public static String getPrefix() {
        return RN_PREFIX;
    }

    @SuppressWarnings("unused")
    private RepNodeId() {
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    public RepNodeId(ObjectInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        groupId = in.readInt();
        nodeNum = in.readInt();
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to write
     * common elements.
     */
    @Override
    public void writeFastExternal(ObjectOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        out.writeInt(groupId);
        out.writeInt(nodeNum);
    }

    @Override
    public ResourceType getType() {
        return ResourceType.REP_NODE;
    }

    public int getGroupId() {
        return groupId;
    }

    public int getNodeNum() {
        return nodeNum;
    }

    /**
     * Returns a string representation that uniquely identifies this node.
     * The fully qualified name contains both the group ID and the node
     * number.
     *
     * @return the fully qualified name of the RepNode
     */
    @Override
    public String getFullName() {
        return new RepGroupId(getGroupId()).getGroupName() +
               "-" + RN_PREFIX + getNodeNum();
    }

    /**
     * Parses the fullName of a RN into its id.It accepts strings that are
     * in the format of {@link #getFullName()}, and for backward compatibility,
     * also accepts <groupNum,nodeNum>.
     */
    public static RepNodeId parse(String fullName) {
        String idArgs[] = fullName.split("-");
        if (idArgs.length == 2) {
            RepGroupId rgId = RepGroupId.parse(idArgs[0]);
            final int nodeNum = parseForInt(RN_PREFIX, idArgs[1]);
            return new RepNodeId(rgId.getGroupId(), nodeNum);
        }

        /* backward compatibility for older groupNum,nodeNum syntax */
        idArgs = fullName.split(",");
        if (idArgs.length == 2) {
            try {
                return new RepNodeId(Integer.parseInt(idArgs[0]),
                                     Integer.parseInt(idArgs[1]));
            } catch(NumberFormatException e) {
                /* Fall through and throw IllegalArgEx */
            }
        }

        throw new IllegalArgumentException
            (fullName +
             " is not a valid RepNode id. It must follow the format rgX-rnY");
    }

    /**
     * Returns just the name of the group portion of this RepNode.  This
     * name is suitable for use as a BDB/JE HA Group name.
     *
     * @return the group name
     */
    public String getGroupName() {
        return new RepGroupId(getGroupId()).getGroupName();
    }

    @Override
    public String toString() {
        return getFullName();
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.admin.ResourceId#getComponent(oracle.kv.impl.topo.Topology)
     */
    @Override
    public RepNode getComponent(Topology topology) {
        return topology.get(this);
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
        RepNodeId other = (RepNodeId) obj;
        if (groupId != other.groupId) {
            return false;
        }
        if (nodeNum != other.nodeNum) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + groupId;
        result = prime * result + nodeNum;
        return result;
    }

    @Override
    public int compareTo(RepNodeId other) {
        int grp = getGroupId() - other.getGroupId();
        if (grp != 0) {
            return grp;
        }
        return getNodeNum() - other.getNodeNum();
    }
}
