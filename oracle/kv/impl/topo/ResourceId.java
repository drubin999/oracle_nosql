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
import java.io.Serializable;
import java.util.Arrays;
import java.util.EnumSet;

import oracle.kv.impl.api.ClientId;
import oracle.kv.impl.util.FastExternalizable;

import com.sleepycat.persist.model.Persistent;

/**
 * Uniquely identifies a component in the KV Store. There is a subclass of
 * ResourceId corresponding to each different resource type in the kv store.
 */
@Persistent
public abstract class ResourceId implements Serializable, FastExternalizable {

    private static final long serialVersionUID = 1;

    /**
     * An enumeration identifying the different resource types in the KV Store.
     *
     * WARNING: To avoid breaking serialization compatibility, the order of the
     * values must not be changed and new values must be added at the end.
     */
    public static enum ResourceType {
        DATACENTER() {
            @Override
            ResourceId readResourceId(ObjectInput in, short serialVersion)
                throws IOException {

                return new DatacenterId(in, serialVersion);
            }

            @Override
            public boolean isDatacenter() {
                return true;
            }
        },
        STORAGE_NODE() {
            @Override
            ResourceId readResourceId(ObjectInput in, short serialVersion)
                throws IOException {

                return new StorageNodeId(in, serialVersion);
            }

            @Override
            public boolean isStorageNode() {
                return true;
            }
        },
        REP_GROUP() {
            @Override
            ResourceId readResourceId(ObjectInput in, short serialVersion)
                throws IOException {

                return new RepGroupId(in, serialVersion);
            }

            @Override
            public boolean isRepGroup() {
                return true;
            }
        },
        REP_NODE() {
            @Override
            ResourceId readResourceId(ObjectInput in, short serialVersion)
                throws IOException {

                return new RepNodeId(in, serialVersion);
            }

            @Override
            public boolean isRepNode() {
                return true;
            }
        },
        PARTITION() {
            @Override
            ResourceId readResourceId(ObjectInput in, short serialVersion)
                throws IOException {

                return new PartitionId(in, serialVersion);
            }

            @Override
            public boolean isPartition() {
                return true;
            }
        },
        ADMIN() {
            @Override
            ResourceId readResourceId(ObjectInput in, short serialVersion)
                throws IOException {

                return new AdminId(in, serialVersion);
            }

            @Override
            public boolean isAdmin() {
                return true;
            }
        },
        CLIENT() {
            @Override
            ResourceId readResourceId(ObjectInput in, short serialVersion)
                throws IOException {

                return new ClientId(in, serialVersion);
            }

            @Override
            public boolean isClient() {
                return true;
            }
        };

        abstract ResourceId readResourceId(ObjectInput in,
                                           short serialVersion)
            throws IOException;

        /**
         * Default predicates overridden by the appropriate enumerator.
         */
        public boolean isDatacenter() {
            return false;
        }

        public boolean isStorageNode() {
            return false;
        }

        public boolean isRepGroup() {
            return false;
        }

        public boolean isRepNode() {
            return false;
        }

        public boolean isPartition() {
            return false;
        }

        public boolean isAdmin() {
            return false;
        }

        public boolean isClient() {
            return false;
        }
    }

    private static final ResourceType[] TYPES_BY_ORDINAL;
    static {
        final EnumSet<ResourceType> set = EnumSet.allOf(ResourceType.class);
        TYPES_BY_ORDINAL = new ResourceType[set.size()];
        for (ResourceType op : set) {
            TYPES_BY_ORDINAL[op.ordinal()] = op;
        }
    }

    private static ResourceType getResourceType(int ordinal) {
        if (ordinal < 0 || ordinal >= TYPES_BY_ORDINAL.length) {
            throw new IllegalArgumentException
                ("unknown resource type: " + ordinal);
        }
        return TYPES_BY_ORDINAL[ordinal];
    }

    /**
     * Utility method for ResourceIds to parse string representations.
     * Supports both X, prefixX (i.e. 10, sn10)
     */
    protected static int parseForInt(String idPrefix, String val) {
        try {
            int id = Integer.parseInt(val);
            return id;
        } catch (NumberFormatException e) {

            /*
             * The argument isn't an integer. See if it's prefixed with
             * the resource id's prefix.
             */
            String arg = val.toLowerCase();
            if (arg.startsWith(idPrefix)) {
                try {
                    int id = Integer.parseInt(val.substring(idPrefix.length()));

                    /*
                     * Guard against a negative value, which could happen if
                     * the string had a hyphen, i.e prefix-X
                     */
                    if (id > 0) {
                        return id;
                    }
                } catch (NumberFormatException e2) {
                    /* Fall through and throw an exception. */
                }
            }
            throw new IllegalArgumentException
               (val + " is not a valid id. It must follow the format " +
                idPrefix + "X");
        }
    }

    /**
     * Utility method for ResourceIds to parse string representations.
     * Supports both X, and prefixX for multiple prefixes (i.e. 10, dc10, zn10)
     */
    protected static int parseForInt(final String[] idPrefixes,
                                     final String val) {
        try {
            return Integer.parseInt(val);
        } catch (NumberFormatException e) {
        }

        /* The argument isn't an integer -- see if it has a prefix */
        final String arg = val.toLowerCase();
        for (final String idPrefix : idPrefixes) {
            if (arg.startsWith(idPrefix)) {
                try {
                    final int id =
                        Integer.parseInt(val.substring(idPrefix.length()));

                    /*
                     * Guard against a negative value, which could happen if
                     * the string had a hyphen, i.e prefix-X
                     */
                    if (id > 0) {
                        return id;
                    }
                } catch (NumberFormatException e2) {
                    break;
                }
            }
        }
        throw new IllegalArgumentException(
            val + " is not a valid id." +
            " It must have the form <prefix>X where <prefix> is one of: " +
            Arrays.toString(idPrefixes));
    }

    protected ResourceId() {
    }

    /**
     * FastExternalizable constructor.  Subclasses must call this constructor
     * before reading additional elements.
     *
     * The ResourceType was read by readFastExternal.
     */
    public ResourceId(@SuppressWarnings("unused") ObjectInput in,
                      @SuppressWarnings("unused") short serialVersion) {
    }

    /**
     * FastExternalizable factory for all ResourceId subclasses.
     */
    public static ResourceId readFastExternal(ObjectInput in,
                                              short serialVersion)
        throws IOException {

        final ResourceType type = getResourceType(in.readUnsignedByte());
        return type.readResourceId(in, serialVersion);
    }

    /**
     * FastExternalizable writer.  Subclasses must call this method before
     * writing additional elements.
     */
    @Override
    public void writeFastExternal(ObjectOutput out, short serialVersion)
        throws IOException {

        out.writeByte(getType().ordinal());
    }

    /**
     * Returns the type associated with the resource id.
     */
    public abstract ResourceType getType();

    /**
     * Resolve the resource id to a component in the Topology.
     *
     * @param topology the topology to be used for the resolution
     *
     * @return the topology component
     */
    public abstract Topology.Component<?> getComponent(Topology topology);

    /*
     * Resource ids must provide their own non-default equals and hash code
     * methods.
     */

    @Override
    public abstract boolean equals(Object o);

    @Override
    public abstract int hashCode();

   /**
     * Returns a string representation that uniquely identifies this component,
     * suitable for requesting RMI resources.
     * @return the fully qualified name of the component.
     */
    public String getFullName() {
        throw new UnsupportedOperationException
            ("Not supported for " + getType());
    }
}
