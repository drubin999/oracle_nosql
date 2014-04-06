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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.topo.Topology.Component;

/**
 * A globally unique id, that's used to identify a client making a request to
 * the KV Store.
 */
public class ClientId extends ResourceId {

    /**
     * The prefix used for cleint names when printed
     */
    private static final String CLIENT_PREFIX = "c";

    private static final long serialVersionUID = 1L;
    /* The unique value. */
    final long clientNum;

    public ClientId(long clientNum) {
        super();
        this.clientNum = clientNum;
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    public ClientId(ObjectInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        clientNum = in.readLong();
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to write
     * common elements.
     */
    @Override
    public void writeFastExternal(ObjectOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        out.writeLong(clientNum);
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.admin.ResourceId#getType()
     */
    @Override
    public ResourceType getType() {
       return ResourceType.CLIENT;
    }

    @Override
    public String toString() {
        return CLIENT_PREFIX + clientNum;
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.admin.ResourceId#getComponent(oracle.kv.impl.topo.Topology)
     */
    @Override
    public Component<?> getComponent(Topology topology) {
        throw new UnsupportedOperationException("Method not meaningful for " +
                                                getType());
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
        ClientId other = (ClientId) obj;
        if (clientNum != other.clientNum) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (clientNum ^ (clientNum >>> 32));
        return result;
    }
}
