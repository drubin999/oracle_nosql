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

package oracle.kv.table;

/**
 * A wrapper class for return values from
 * {@link TableAPI#tableKeysIterator(IndexKey, MultiRowOptions,
 *  oracle.kv.table.TableIteratorOptions)}
 * This classes allows the iterator to return all field value information that
 * can be obtained directly from the index without an additional fetch.
 *
 * Note: this class has a natural ordering that is inconsistent with
 * equals. Ordering is based on the indexKey only.
 *
 * @since 3.0
 */
public class KeyPair implements Comparable<KeyPair> {
    private final PrimaryKey primaryKey;
    private final IndexKey indexKey;

    /**
     * @hidden
     * For internal use only.
     */
    public KeyPair(PrimaryKey primaryKey, IndexKey indexKey) {
        this.primaryKey = primaryKey;
        this.indexKey = indexKey;
    }

    /**
     * Returns the PrimaryKey from the pair.
     *
     * @return the PrimaryKey
     */
    public PrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    /**
     * Returns the IndexKey from the pair.
     *
     * @return the IndexKey
     */
    public IndexKey getIndexKey() {
        return indexKey;
    }

    /**
     * Compares the indexKey of this object with the indexKey of the specified
     * object for order.
     */
    @Override
    public int compareTo(KeyPair other) {
        return indexKey.compareTo((other).getIndexKey());
    }
}

