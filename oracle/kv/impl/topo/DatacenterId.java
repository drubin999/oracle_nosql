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
public class DatacenterId extends ResourceId {

    private static final long serialVersionUID = 1L;

    /** The standard prefix for datacenter IDs. */
    public static final String DATACENTER_PREFIX = "zn";

    /**
     * All acceptable prefixes for datacenter IDs, including the standard one
     * ("zn"), introduced in R3, as well as the previous value ("dc"), for
     * compatibility with earlier releases.
     */
    public static final String[] ALL_DATACENTER_PREFIXES = {"zn", "dc"};

    private int datacenterId;

    public DatacenterId(int datacenterId) {
        super();
        this.datacenterId = datacenterId;
    }

    @SuppressWarnings("unused")
    private DatacenterId() {
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    public DatacenterId(ObjectInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        datacenterId = in.readInt();
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to write
     * common elements.
     */
    @Override
    public void writeFastExternal(ObjectOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        out.writeInt(datacenterId);
    }

    @Override
    public ResourceType getType() {
        return ResourceType.DATACENTER;
    }

    public int getDatacenterId() {
        return datacenterId;
    }

    @Override
    public String toString() {
        return DATACENTER_PREFIX + datacenterId;
    }

    /**
     * Parse a string that is either an integer, or in znX or dcX format, and
     * generate datacenter id.
     */
    public static DatacenterId parse(String s) {
        return new DatacenterId(parseForInt(ALL_DATACENTER_PREFIXES, s));
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.admin.ResourceId#getComponent
     *                                          (oracle.kv.impl.topo.Topology)
     */
    @Override
    public Datacenter getComponent(Topology topology) {
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

        final DatacenterId other = (DatacenterId) obj;
        if (datacenterId != other.datacenterId) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + datacenterId;
        return result;
    }
}
