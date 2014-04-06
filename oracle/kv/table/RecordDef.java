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

import java.util.List;

/**
 * RecordDef represents a simple record containing a number of
 * field objects where the objects are named by a String name and need
 * not be of the same type.
 *
 * @since 3.0
 */
public interface RecordDef extends FieldDef {

    /**
     * Get the field names for the record in declaration order.
     *
     * @return the list of field names declaration order
     */
    List<String> getFields();

    /**
     * Get the named field.
     *
     * @param name the name of the field to return
     *
     * @return the FieldDef for the name, or null if there is no such
     * named field
     */
    FieldDef getField(String name);

    /**
     * Get the name of the record.  Records require names even if they are
     * nested or used as an array or map element.
     *
     * @return the name of the record
     */
    String getName();

    /**
     * Returns true if the named field is nullable.
     *
     * @param name the name of the field
     *
     * @return true if the named field is nullable
     */
    boolean isNullable(String name);

    /**
     * Creates an instance using the default value for the named field.
     * The return value is {@link FieldValue} and not a more specific type
     * because in the case of nullable fields the default will be a null value,
     * which is a special value that returns true for {@link #isNullable}.
     *
     * @param name the name of the field
     *
     * @return a default value
     */
    FieldValue getDefaultValue(String name);

    /**
     * @return a deep copy of this object
     */
    @Override
    public RecordDef clone();
}

