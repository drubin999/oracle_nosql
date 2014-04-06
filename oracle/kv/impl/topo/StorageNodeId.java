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

@Persistent
public class StorageNodeId extends ResourceId
	implements Comparable<StorageNodeId> {

    private static final long serialVersionUID = 1L;
    private static final String SN_PREFIX = "sn";

    private int storageNodeId;

    public StorageNodeId(int storageNodeId) {
        super();
        this.storageNodeId = storageNodeId;
    }

    @SuppressWarnings("unused")
    private StorageNodeId() {
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    public StorageNodeId(ObjectInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        storageNodeId = in.readInt();
    }

    public static String getPrefix() {
        return SN_PREFIX;
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to write
     * common elements.
     */
    @Override
    public void writeFastExternal(ObjectOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        out.writeInt(storageNodeId);
    }

    @Override
    public ResourceType getType() {
        return ResourceType.STORAGE_NODE;
    }

    public int getStorageNodeId() {
        return storageNodeId;
    }

    /**
     * Returns a string representation that uniquely identifies this SNA.
     *
     * @return the fully qualified name of the SNA
     */
    @Override
    public String getFullName() {
        return SN_PREFIX + storageNodeId;
    }

    /**
     * Parse a string that is either an integer or is in snX format and
     * generate a StorageNodeId.
     */
    public static StorageNodeId parse(String s) {
        return new StorageNodeId(parseForInt(SN_PREFIX, s));
    }

    @Override
    public String toString() {
        return getFullName();
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.admin.ResourceId#getComponent(oracle.kv.impl.topo.Topology)
     */
    @Override
    public StorageNode getComponent(Topology topology) {
        return topology.getStorageNodeMap().get(this);
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
        StorageNodeId other = (StorageNodeId) obj;
        if (storageNodeId != other.storageNodeId) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + storageNodeId;
        return result;
    }

    @Override
    public int compareTo(StorageNodeId other) {
        int x = this.getStorageNodeId();
        int y = other.getStorageNodeId();

        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }
}
