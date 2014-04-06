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

import com.sleepycat.je.LockMode;
import com.sleepycat.je.Transaction;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;

/**
 * Utility class for storing metadata objects in an entity store.
 */
public class MetadataStore {
    private MetadataStore() {}

    /**
     * Reads a metadata object from the specified store.
     * 
     * @param returnType class of the returned metadata object
     * @param type the type of metadata
     * @param store an entity store
     * @param txn a transaction
     * @return a metadata object
     */
    public static <T extends Metadata<? extends MetadataInfo>> T
                                            read(Class<T> returnType,
                                                 MetadataType type,
                                                 EntityStore store,
                                                 Transaction txn) {
        final MetadataHolder holder =
            openIndex(store).get(txn, type.getKey(), LockMode.READ_UNCOMMITTED);

        return returnType.cast((holder == null) ? null : holder.getMetadata());
    }

    /**
     * Saves a metadata object in the specified store.
     * 
     * @param md a metadata object
     * @param store an entity store
     * @param txn a transaction
     */
    public static void save(Metadata<? extends MetadataInfo> md,
                            EntityStore store,
                            Transaction txn) {
        openIndex(store).put(txn, new MetadataHolder(md));
    }

    private static PrimaryIndex<String, MetadataHolder>
                                                openIndex(EntityStore store) {
        assert store != null;
        return store.getPrimaryIndex(String.class, MetadataHolder.class);
    }
}
