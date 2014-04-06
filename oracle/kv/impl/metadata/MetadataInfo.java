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

import oracle.kv.impl.metadata.Metadata.MetadataType;

/**
 * Interface implemented by all metadata information objects. Objects
 * implementing this interface are used to transmit metadata information in
 * response to a metadata request. The information may be in the form of changes
 * in metadata or a subset of metadata and is implementation dependent. The
 * responder may not have the requested information in which case the object
 * will be empty. Objects implementing MetadataInfo should also implement
 * <code>Serializable</code>.
 */
public interface MetadataInfo {

    /**
     * Gets the type of metadata this information object contains.
     *
     * @return the type of metadata
     */
    public MetadataType getType();

    /**
     * Returns the highest sequence number associated with the metadata that
     * is known to the source of this object.
     *
     * @return the source's metadata sequence number
     */
    public int getSourceSeqNum();

    /**
     * Returns true if this object does not include any metadata information
     * beyond the type and sequence number.
     *
     * @return true if this object does not include any metadata information
     */
    public boolean isEmpty();
}
