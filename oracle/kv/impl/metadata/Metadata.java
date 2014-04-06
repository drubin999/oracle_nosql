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

package oracle.kv.impl.metadata;

/**
 * Interface implemented by all metadata objects.
 *
 * @param <I> the type of metadata information object return by this metadata
 */
public interface Metadata<I extends MetadataInfo> {

    /** Metadata types */
    public enum MetadataType {
        /* New types must be added to the end of this list */
        TOPOLOGY() {
            @Override
            public String getKey() { return "Topology";}
        },
        TABLE() {
            @Override
            public String getKey() { return "Table";}
        },
        SECURITY() {
            @Override
            public String getKey() { return "Security";}
        };

        /**
         * Gets a unique string for this type. This string may be used as a
         * key into a metadata store.
         *
         * @return a unique string
         */
        abstract public String getKey();
    }

    /* The sequence number of an newly created metadata, empty object */
    public static final int EMPTY_SEQUENCE_NUMBER = 0;

    /**
     * Gets the type of this metadata object.
     *
     * @return the type of this metadata object
     */
    public MetadataType getType();

    /**
     * Gets the highest sequence number of this metadata object.
     * Returns -1 if the metadata has not been initialized.
     *
     * @return the highest sequence number of this metadata object
     */
    public int getSequenceNumber();

    /**
     * Gets an information object for this metadata. The returned object will
     * include the changes between this object and the metadata at the
     * specified sequence number. If the metadata object can not supply
     * information based on the sequence number an empty metadata information
     * object is returned.
     *
     * @param startSeqNum the inclusive start of the sequence of
     * changes to be included
     * @return a metadata info object
     */
    public I getChangeInfo(int startSeqNum);
}
