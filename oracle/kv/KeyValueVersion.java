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

package oracle.kv;

/**
 * Represents a key/value pair along with its version.
 *
 * <p>A KeyValueVersion instance is returned by methods such as {@link
 * KVStore#storeIterator(Direction, int)} and {@link
 * KVStore#multiGetIterator(Direction, int, Key, KeyRange, Depth)}.  The key,
 * version and value properties will always be non-null.</p>
 */
public class KeyValueVersion {

    private final Key key;
    private final Value value;
    private final Version version;

    /**
     * Creates a KeyValueVersion with non-null properties.
     */
    public KeyValueVersion(final Key key,
                           final Value value,
                           final Version version) {
        assert key != null;
        assert value != null;
        assert version != null;
        this.key = key;
        this.value = value;
        this.version = version;
    }

    /**
     * Returns the Key part of the KV pair.
     */
    public Key getKey() {
        return key;
    }

    /**
     * Returns the Value part of the KV pair.
     */
    public Value getValue() {
        return value;
    }

    /**
     * Returns the Version of the KV pair.
     */
    public Version getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return key.toString() + ' ' + value + ' ' + version;
    }
}
