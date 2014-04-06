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
 * RecordValue extends {@link FieldValue} to represent a multi-valued object
 * that contains a map of string names to fields.  The field values may be
 * simple or complex and allowed fields are defined by the FieldDef definition
 * of the record.
 *
 * @since 3.0
 */
public interface RecordValue extends FieldValue {

    /**
     * Returns the RecordDef that defines the content of this record.
     *
     * @return the RecordDef
     */
    RecordDef getDefinition();

    /**
     * Returns the value of the named field.
     *
     * @param fieldName the name of the desired field
     *
     * @return the field value if it is available, null if it has not
     * been set
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object
     */
    FieldValue get(String fieldName);

    /**
     * Set the named field, silently overwriting existing values.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the type of the field does not match the
     * input type
     */
    RecordValue put(String fieldName, int value);

    /**
     * Set the named field, silently overwriting existing values.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the type of the field does not match the
     * input type
     */
    RecordValue put(String fieldName, long value);

    /**
     * Set the named field, silently overwriting existing values.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the type of the field does not match the
     * input type
     */
    RecordValue put(String fieldName, String value);

    /**
     * Set the named field, silently overwriting existing values.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the type of the field does not match the
     * input type
     */
    RecordValue put(String fieldName, double value);

    /**
     * Set the named field, silently overwriting existing values.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the type of the field does not match the
     * input type
     */
    RecordValue put(String fieldName, float value);

    /**
     * Set the named field, silently overwriting existing values.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the type of the field does not match the
     * input type
     */
    RecordValue put(String fieldName, boolean value);

    /**
     * Set the named field, silently overwriting existing values.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the type of the field does not match the
     * input type
     */
    RecordValue put(String fieldName, byte[] value);

    /**
     * Set the named field, silently overwriting existing values.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the type of the field does not match the
     * input type
     */
    RecordValue putFixed(String fieldName, byte[] value);

    /**
     * Set the named field, silently overwriting existing values.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the type of the field does not match the
     * input type
     */
    RecordValue putEnum(String fieldName, String value);

    /**
     * Put a null value in the named field, silently overwriting
     * existing values.
     *
     * @param fieldName name of the desired field
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the type of the field does not match the
     * input type
     */
    RecordValue putNull(String fieldName);

    /**
     * Set the named field, silently overwriting existing values.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the type of the field does not match the
     * input type
     */
    RecordValue put(String fieldName, FieldValue value);

    /**
     * Set a RecordValue field, silently overwriting existing values.
     * The returned object is empty of fields and must be further set by the
     * caller.
     *
     * @param fieldName name of the desired field
     *
     * @return an empty instance of RecordValue
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the definition of the field does not
     * match the input definition
     */
    RecordValue putRecord(String fieldName);

    /**
     * Set an ArrayValue field, silently overwriting existing values.
     * The returned object is empty of fields and must be further set by the
     * caller.
     *
     * @param fieldName name of the desired field
     *
     * @return an empty instance of RecordValue
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the definition of the field does not
     * match the input definition
     */
    ArrayValue putArray(String fieldName);

    /**
     * Set a MapValue field, silently overwriting existing values.
     * The returned object is empty of fields and must be further set by the
     * caller.
     *
     * @param fieldName name of the desired field
     *
     * @return an empty instance of RecordValue
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the definition of the field does not
     * match the input definition
     */
    MapValue putMap(String fieldName);

    /**
     * Returns the number of fields in the record.  Only top-level fields are
     * counted.
     *
     * @return the number of fields
     */
    int size();

    /**
     * Returns true if there are no fields in the record, false otherwise.
     *
     * @return true if there are no fields in the record, false otherwise
     */
    boolean isEmpty();

    /**
     * Remove the named field if it exists.
     *
     * @param fieldName the name of the field to remove
     *
     * @return the FieldValue if it existed, otherwise null
     */
    FieldValue remove(String fieldName);

    /**
     * Copies the fields from another RecordValue instance, overwriting
     * fields in this object with the same name.
     *
     * @param source the source RecordValue from which to copy
     *
     * @throws IllegalArgumentException if the {@link RecordDef} of source
     * does not match that of this instance.
     */
    void copyFrom(RecordValue source);

    /**
     * Returns true if the record contains the named field.
     *
     * @param fieldName the name of the field
     *
     * @return true if the field exists in the record, otherwise null
     */
    boolean contains(String fieldName);

    /**
     * Returns a deep copy of this object.
     *
     * @return a deep copy of this object
     */
    @Override
    public RecordValue clone();
}



