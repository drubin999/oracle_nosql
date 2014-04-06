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
 * ReturnRow is used with put and delete operations to return the previous row
 * value and version.
 * <p>
 * A ReturnRow instance may be used as the {@code
 * prevRecord} parameter to methods such as {@link TableAPI#put(Row,
 * ReturnRow, WriteOptions)}.
 * </p>
 * <p>
 * For best performance, it is important to choose only the properties that are
 * required.  The store is optimized to avoid I/O when the requested
 * properties are in cache.
 * </p>
 * <p>Note that because both properties are optional, the version property,
 * value property, or both properties may be null.</p>
 *
 * @since 3.0
 */
public interface ReturnRow extends Row {

    /**
     * Returns the Choice of what information is returned.
     */
    Choice getReturnChoice();

    /**
     * Specifies whether to return the row value, version, both or neither.
     * <p>
     * For best performance, it is important to choose only the properties that
     * are required.  The store is optimized to avoid I/O when the requested
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

        private final boolean needValue;
        private final boolean needVersion;

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
}



