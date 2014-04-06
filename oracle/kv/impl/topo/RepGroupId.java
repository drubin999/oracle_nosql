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
 * The KV Store wide unique resource id identifying a group.
 */
@Persistent
public class RepGroupId extends ResourceId implements Comparable<RepGroupId> {

    public static RepGroupId NULL_ID = new RepGroupId(-1);
    
    /**
     * The prefix used for rep group names
     */
    private static final String REP_GROUP_PREFIX = "rg";

    private static final long serialVersionUID = 1L;

    /* The store-wide unique group id. */
    private int groupId;

    /**
     * The unique group id, as determined by the GAT, that's used as the basis
     * of the resourceId.
     *
     * @param groupId the unique groupId
     */
    public RepGroupId(int groupId) {
        this.groupId = groupId;
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    public RepGroupId(ObjectInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        groupId = in.readInt();
    }

    public boolean isNull() {
        return groupId == NULL_ID.groupId;
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
    }

    public static String getPrefix() {
        return REP_GROUP_PREFIX;
    }

    @SuppressWarnings("unused")
    private RepGroupId() {

    }

    @Override
    public ResourceType getType() {
        return ResourceType.REP_GROUP;
    }

    /**
     * Returns the group-wide unique group id.
     */
    public int getGroupId() {
        return groupId;
    }

    /**
     * Returns the name of the RepGroup. This name is suitable for use as a
     * BDB/JE HA Group name.
     *
     * @return the group name
     */
    public String getGroupName() {
        return REP_GROUP_PREFIX + getGroupId();
    }

    public boolean sameGroup(RepNodeId repNodeId) {
        return (groupId == repNodeId.getGroupId());
    }

    /**
     * Generates a repGroupId from a group name. It's the inverse of
     * {@link #getGroupName}.
     */
    public static RepGroupId parse(String groupName) {
        return new RepGroupId(parseForInt(REP_GROUP_PREFIX, groupName));
    }

    @Override
    public String toString() {
        return getGroupName();
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.admin.ResourceId#getComponent(oracle.kv.impl.topo.Topology)
     */
    @Override
    public RepGroup getComponent(Topology topology) {
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
        RepGroupId other = (RepGroupId) obj;
        if (groupId != other.groupId) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + groupId;
        return result;
    }

    @Override
    public int compareTo(RepGroupId other) {
        return getGroupId() - other.getGroupId();
    }
}
