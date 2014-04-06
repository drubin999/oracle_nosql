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
 * StringDef is an extension of {@link FieldDef} to encapsulate a String.
 * It adds a minimum and maximum value range and type.
 * <p>
 * Comparisons to minimum and maxium values are done using
 * {@link String#compareTo}.
 *
 * @since 3.0
 */
public interface StringDef extends FieldDef {

    /**
     * Get the minimum value for the string.
     *
     * @return the minimum value for the instance if defined, otherwise null
     */
    String getMin();

    /**
     * Get the maximum value for the string.
     *
     * @return the maximum value for the instance if defined, otherwise null
     */
    String getMax();

    /**
     * @return true if the minimum is inclusive.  This value is only relevant
     * if {@link #getMin} returns a non-null value.
     */
    boolean isMinInclusive();

    /**
     * @return true if the maximum is inclusive.  This value is only relevant
     * if {@link #getMax} returns a non-null value.
     */
    boolean isMaxInclusive();

    /**
     * @return a deep copy of this object
     */
    @Override
    public StringDef clone();
}
