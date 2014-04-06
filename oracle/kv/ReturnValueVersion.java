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

import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

/**
 * Used with put and delete operations to return the previous value and
 * version.
 * <p>
 * A ReturnValueVersion instance may be created and passed as the {@code
 * prevValue} parameter to methods such as {@link KVStore#put(Key, Value,
 * ReturnValueVersion, Durability, long, TimeUnit)}.
 * </p>
 * <p>
 * For best performance, it is important to choose only the properties that are
 * required.  The KV Store is optimized to avoid I/O when the requested
 * properties are in cache.
 * </p>
 * <p>Note that because both properties are optional, the version property,
 * value property, or both properties may be null.</p>
 */
public class ReturnValueVersion extends ValueVersion {

    /**
     * Specifies whether to return the value, version, both or neither.
     * <p>
     * For best performance, it is important to choose only the properties that
     * are required.  The KV Store is optimized to avoid I/O when the requested
     * properties are in cache.  </p>
     */
    public enum Choice {

        /**
         * Return the value only.
         */
        VALUE(true, false),

        /**
         * Return the version only.
         */
        VERSION(false, true),

        /**
         * Return both the value and the version.
         */
        ALL(true, true),

        /**
         * Do not return the value or the version.
         */
        NONE(false, false);

        private boolean needValue;
        private boolean needVersion;

        private Choice(boolean needValue, boolean needVersion) {
            this.needValue = needValue;
            this.needVersion = needVersion;
        }

        /**
         * For internal use only.
         * @hidden
         */
        public boolean needValue() {
            return needValue;
        }

        /**
         * For internal use only.
         * @hidden
         */
        public boolean needVersion() {
            return needVersion;
        }

        /**
         * For internal use only.
         * @hidden
         */
        public boolean needValueOrVersion() {
            return needValue || needVersion;
        }
    }

    private final static Choice[] CHOICES_BY_ORDINAL;
    static {
        final EnumSet<Choice> set = EnumSet.allOf(Choice.class);
        CHOICES_BY_ORDINAL = new Choice[set.size()];
        for (Choice op : set) {
            CHOICES_BY_ORDINAL[op.ordinal()] = op;
        }
    }

    /**
     * For internal use only.
     * @hidden
     */
    public static Choice getChoice(int ordinal) {
        if (ordinal < 0 || ordinal >= CHOICES_BY_ORDINAL.length) {
            throw new RuntimeException("unknown Choice: " + ordinal);
        }
        return CHOICES_BY_ORDINAL[ordinal];
    }

    private final Choice returnChoice;

    /**
     * Creates an object for returning the value, version or both.
     *
     * @param returnChoice determines whether the value, version, both or
     * none are returned.
     */
    public ReturnValueVersion(Choice returnChoice) {
        this.returnChoice = returnChoice;
    }

    /**
     * Returns the ReturnValueVersion.Choice used to create this object.
     */
    public Choice getReturnChoice() {
        return returnChoice;
    }
}
