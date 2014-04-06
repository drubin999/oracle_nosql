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
 * Holds a Value and Version that are associated with a given Key.
 *
 * <p>A ValueVersion instance is returned by methods such as {@link KVStore#get
 * get} and {@link KVStore#multiGet multiGet} as the current value and version
 * associated with a given key.  The version and value properties will always
 * be non-null.</p>
 */
public class ValueVersion {

    private Value value;
    private Version version;

    /**
     * Used internally to create an object with null value and version.
     */
    public ValueVersion() {
    }

    /**
     * Used internally to create an object with a value and version.
     */
    public ValueVersion(Value value, Version version) {
        this.value = value;
        this.version = version;
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

    /**
     * Used internally to initialize the Value part of the KV pair.
     */
    public ValueVersion setValue(Value value) {
        this.value = value;
        return this;
    }

    /**
     * Used internally to initialize the Version of the KV pair.
     */
    public ValueVersion setVersion(Version version) {
        this.version = version;
        return this;
    }

    @Override
    public String toString() {
        return "<ValueVersion " + value + ' ' + version + '>';
    }
}
