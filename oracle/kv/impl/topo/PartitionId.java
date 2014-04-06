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
import java.lang.NumberFormatException;

import com.sleepycat.persist.model.Persistent;

@Persistent
public class PartitionId extends ResourceId {

    public static PartitionId NULL_ID = new PartitionId(-1);

    public PartitionId(int partitionId) {
        super();
        this.partitionId = partitionId;
    }

    private static final long serialVersionUID = 1L;

    private int partitionId;

    @SuppressWarnings("unused")
    private PartitionId() {

    }

    public boolean isNull() {
        return partitionId == NULL_ID.partitionId;
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    public PartitionId(ObjectInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        partitionId = in.readInt();
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to write
     * common elements.
     */
    @Override
    public void writeFastExternal(ObjectOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        out.writeInt(partitionId);
    }

    @Override
    public ResourceType getType() {
        return ResourceType.PARTITION;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public String getPartitionName() {
        return "p" + partitionId;
    }

    public static boolean isPartitionName(String name) {
        if (name.startsWith("p")) {
            try {
                Integer.parseInt(name.substring(1));
                return true;
            } catch (NumberFormatException ignored) {
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return getType() + "-" + partitionId;
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.admin.ResourceId#getComponent(oracle.kv.impl.topo.Topology)
     */
    @Override
    public Partition getComponent(Topology topology) {
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
        PartitionId other = (PartitionId) obj;
        if (partitionId != other.partitionId) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + partitionId;
        return result;
    }
}
