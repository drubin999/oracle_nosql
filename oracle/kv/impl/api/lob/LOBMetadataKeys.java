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

package oracle.kv.impl.api.lob;

/**
 * Defines the keys used to access values in the metadata hash map. Keys are
 * present by default in the hash map. Some keys may not be present when the
 * LOB is in a partial state, or when it is being deleted; the doc associated
 * with such keys details the lifetime of these keys.
 */
public interface LOBMetadataKeys {

    /**
     * The set of keys was augmented (in Version 2, KVS 2.1.55) to support
     * append metadata operations. It has two changes:
     *
     * 1) A new key APPEND_LOB_SIZE to save the LOB_SIZE when an append
     * operation is in progress.
     *
     * 2) NUM_CHUNKS is now stored as a Long
     *
     * 3) The key format has been changed to:
     *     o Use a more efficient unpadded representation for super chunk and
     *       chunk id components.
     *     o To eliminate a bug in the padded representation.
     */
    static final int CURRENT_VERSION = 2;

    /**
     * The metadata version number. To help deal with LOB representation
     * changes.
     */
    static final String METADATA_VERSION = "metadataVersion";

    /**
     * The application defined key (serialized in its string format) associated
     * with this LOB. It effectively establishes a bi-directional relationship.
     * It can be used to reclaim the storage associated with orphaned LOBs, by
     * scanning the LOB keyspace and ensuring that an app key does not point to
     * it.
     */
    static final String APP_KEY = "appKey";

    /**
     * The total number of chunks that make up this complete blob. It's only
     * present in non-partial LOBs. Note that there is a degree of redundancy
     * between lobSize and numChunks, since numChunks could be computed from
     * lobSize.The redundancy permits an additional level of internal checking
     * but it could be eliminated.
     */
    static final String NUM_CHUNKS = "numChunks";

    /**
     * The number of chunks per partition for this LOB.
     */
    static final String CHUNKS_PER_PARTITION = "numChunksPerPartition";

    /**
     * The super chunk id to use as the starting point in a search for the
     * last chunk in a partially inserted LOB. This value is updated
     * after the first chunk in a super chunk is written.
     *
     * A LOB with zero bytes is represented with the keys:
     * LAST_SUPER_CHUNK_ID = 1 and NUM_CHUNKS = 0
     */
    static final String LAST_SUPER_CHUNK_ID = "lastSuperChunkId";

    /**
     * The logical (not the storage level) size of the LOB in bytes. This key
     * is absent from partially inserted or partially appended LOBs. It is
     * only present in partially deleted LOBs if the LOB was not partial at the
     * time of the delete. That is, the delete operation does not change the
     * presence of this key while the delete operation is in progress.
     */
    static final String LOB_SIZE = "lobSize";

    /**
     * The chunk size used to store all except the last chunk; the last chunk
     * has a size > 0 and <= chunkSize,
     */
    static final String CHUNK_SIZE = "chunkSize";

    /**
     * The key indicating the LOB is being deleted. It's created as the very
     * first step in the deletion process.
     */
    static final String DELETED = "deleted";

    /**
     * The transient key indicating the LOB is being appended to. It's the
     * value associated with the LOB_SIZE at the start of the append operation.
     * The presence of the key in persistent LOB metadata indicates that this
     * is a partially appended LOB.
     *
     * @since 2.1.55
     */
    static final String APPEND_LOB_SIZE = "appendLobSize";
}
