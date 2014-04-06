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
 * EnumValue extends {@link FieldValue} to represent a single value in an
 * enumeration.  Enumeration values are represented as strings.
 *
 * @since 3.0
 */
public interface EnumValue extends FieldValue {

    /**
     * Returns the {@link EnumDef} instance that defines this value.
     *
     * @return the EnumDef
     */
    EnumDef getDefinition();

    /**
     * Gets the string value of the enumeration.
     *
     * @return the string value of the EnumValue
     */
    String get();

    /**
     * Returns the index of the value in the enumeration definition.  This is
     * used for sort order when used in keys and index keys.
     *
     * @return the index of the value of this object in the enumeration
     * definition returned by {@link #getDefinition}
     */
    int getIndex();

    /**
     * Returns a deep copy of this object.
     *
     * @return a deep copy of this object
     */
    @Override
    public EnumValue clone();
}



