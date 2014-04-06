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
 * ArrayValue extends {@link FieldValue} to add methods appropriate for array
 * values.
 *
 * @since 3.0
 */
public interface ArrayValue extends FieldValue {

    /**
     * Returns the ArrayDef that defines the content of this array.
     *
     * @return an ArrayDef
     */
    ArrayDef getDefinition();

    /**
     * Gets the value at the specified index.
     *
     * @param index the index to use for the get
     *
     * @return the value at the index or null if none exists
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index < 0 || index >= size())
     */
    FieldValue get(int index);

    /**
     * Returns the size of the array.
     *
     * @return the size of the array
     */
    int size();

    /**
     * Returns the array values as an unmodifiable list.
     *
     * @return the list of values
     */
    List<FieldValue> toList();

    /**
     * Adds a new value at the end of the array.
     *
     * @param value the value to add
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     */
    ArrayValue add(FieldValue value);

    /**
     * Inserts a new value at the specified index.  This does not replace an
     * existing value, all values at or above the index are shifted to the
     * right.
     *
     * @param index the index for the entry
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index < 0 || index >= size())
     *
     * @return this
     */
    ArrayValue add(int index, FieldValue value);

    /**
     * Set the value at the specified index.  This method replaces any
     * existing value at that index.
     *
     * @param index the index for the entry
     *
     * @param value the value to set
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index < 0 || index >= size())
     *
     * @return this
     */
    ArrayValue set(int index, FieldValue value);

    /**
     * Adds a new value at the end of the array.
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index < 0 || index >= size())
     *
     * @return this
     */
    ArrayValue add(int value);

    /**
     * Adds an array of new values at the end of the array.
     *
     * @param values the array of values to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     */
    ArrayValue add(int[] values);

    /**
     * Inserts a new value at the specified index.  This does not replace an
     * existing value, all values at or above the index are shifted to the
     * right.
     *
     * @param index the index for the entry
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index < 0 || index >= size())
     *
     * @return this
     */
    ArrayValue add(int index, int value);

    /**
     * Set the value at the specified index.  This method replaces any
     * existing value at that index.
     *
     * @param index the index for the entry
     *
     * @param value the value to set
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index < 0 || index >= size())
     *
     * @return this
     */
    ArrayValue set(int index, int value);

    /**
     * Adds a new value at the end of the array.
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index < 0 || index >= size())
     *
     * @return this
     */
    ArrayValue add(long value);

    /**
     * Adds an array of new values at the end of the array.
     *
     * @param values the array of values to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     */
    ArrayValue add(long[] values);

    /**
     * Inserts a new value at the specified index.  This does not replace an
     * existing value, all values at or above the index are shifted to the
     * right.
     *
     * @param index the index for the entry
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index < 0 || index >= size())
     *
     * @return this
     */
    ArrayValue add(int index, long value);

    /**
     * Set the value at the specified index.  This method replaces any
     * existing value at that index.
     *
     * @param index the index for the entry
     *
     * @param value the value to set
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index < 0 || index >= size())
     *
     * @return this
     */
    ArrayValue set(int index, long value);

    /**
     * Adds a new value at the end of the array.
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index < 0 || index >= size())
     *
     * @return this
     */
    ArrayValue add(String value);

    /**
     * Adds an array of new values at the end of the array.
     *
     * @param values the array of values to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     */
    ArrayValue add(String[] values);

    /**
     * Inserts a new value at the specified index.  This does not replace an
     * existing value, all values at or above the index are shifted to the
     * right.
     *
     * @param index the index for the entry
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index < 0 || index >= size())
     *
     * @return this
     */
    ArrayValue add(int index, String value);

    /**
     * Set the value at the specified index.  This method replaces any
     * existing value at that index.
     *
     * @param index the index for the entry
     *
     * @param value the value to set
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index < 0 || index >= size())
     *
     * @return this
     */
    ArrayValue set(int index, String value);

    /**
     * Adds a new value at the end of the array.
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index < 0 || index >= size())
     *
     * @return this
     */
    ArrayValue add(double value);

    /**
     * Adds an array of new values at the end of the array.
     *
     * @param values the array of values to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     */
    ArrayValue add(double[] values);

    /**
     * Inserts a new value at the specified index.  This does not replace an
     * existing value, all values at or above the index are shifted to the
     * right.
     *
     * @param index the index for the entry
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index < 0 || index >= size())
     *
     * @return this
     */
    ArrayValue add(int index, double value);

    /**
     * Set the value at the specified index.  This method replaces any
     * existing value at that index.
     *
     * @param index the index for the entry
     *
     * @param value the value to set
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index < 0 || index >= size())
     *
     * @return this
     */
    ArrayValue set(int index, double value);

    /**
     * Adds a new value at the end of the array.
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index < 0 || index >= size())
     *
     * @return this
     */
    ArrayValue add(float value);

    /**
     * Adds an array of new values at the end of the array.
     *
     * @param values the array of values to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     */
    ArrayValue add(float[] values);

    /**
     * Inserts a new value at the specified index.  This does not replace an
     * existing value, all values at or above the index are shifted to the
     * right.
     *
     * @param index the index for the entry
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index < 0 || index >= size())
     *
     * @return this
     */
    ArrayValue add(int index, float value);

    /**
     * Set the value at the specified index.  This method replaces any
     * existing value at that index.
     *
     * @param index the index for the entry
     *
     * @param value the value to set
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index < 0 || index >= size())
     *
     * @return this
     */
    ArrayValue set(int index, float value);

    /**
     * Adds a new value at the end of the array.
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index < 0 || index >= size())
     *
     * @return this
     */
    ArrayValue add(boolean value);

    /**
     * Adds an array of new values at the end of the array.
     *
     * @param values the array of values to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     */
    ArrayValue add(boolean[] values);

    /**
     * Inserts a new value at the specified index.  This does not replace an
     * existing value, all values at or above the index are shifted to the
     * right.
     *
     * @param index the index for the entry
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index < 0 || index >= size())
     *
     * @return this
     */
    ArrayValue add(int index, boolean value);

    /**
     * Set the value at the specified index.  This method replaces any
     * existing value at that index.
     *
     * @param index the index for the entry
     *
     * @param value the value to set
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index < 0 || index >= size())
     *
     * @return this
     */
    ArrayValue set(int index, boolean value);

    /**
     * Adds a new value at the end of the array.
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index < 0 || index >= size())
     *
     * @return this
     */
    ArrayValue add(byte[] value);

    /**
     * Adds an array of new values at the end of the array.
     *
     * @param values the array of values to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     */
    ArrayValue add(byte[][] values);

    /**
     * Inserts a new value at the specified index.  This does not replace an
     * existing value, all values at or above the index are shifted to the
     * right.
     *
     * @param index the index for the entry
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index < 0 || index >= size())
     *
     * @return this
     */
    ArrayValue add(int index, byte[] value);

    /**
     * Set the value at the specified index.  This method replaces any
     * existing value at that index.
     *
     * @param index the index for the entry
     *
     * @param value the value to set
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index < 0 || index >= size())
     *
     * @return this
     */
    ArrayValue set(int index, byte[] value);

    /**
     * Adds a new value at the end of the array.
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index < 0 || index >= size())
     *
     * @return this
     */
    ArrayValue addFixed(byte[] value);

    /**
     * Adds an array of new values at the end of the array.
     *
     * @param values the array of values to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     */
    ArrayValue addFixed(byte[][] values);

    /**
     * Inserts a new value at the specified index.  This does not replace an
     * existing value, all values at or above the index are shifted to the
     * right.
     *
     * @param index the index for the entry
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index < 0 || index >= size())
     *
     * @return this
     */
    ArrayValue addFixed(int index, byte[] value);

    /**
     * Set the value at the specified index.  This method replaces any
     * existing value at that index.
     *
     * @param index the index for the entry
     *
     * @param value the value to set
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index < 0 || index >= size())
     *
     * @return this
     */
    ArrayValue setFixed(int index, byte[] value);

    /**
     * Adds a new value at the end of the array.
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     */
    ArrayValue addEnum(String value);

    /**
     * Adds an array of new values at the end of the array.
     *
     * @param values the array of values to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return this
     */
    ArrayValue addEnum(String[] values);

    /**
     * Inserts a new value at the specified index.  This does not replace an
     * existing value, all values at or above the index are shifted to the
     * right.
     *
     * @param index the index for the entry
     *
     * @param value the value to insert
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index < 0 || index >= size())
     *
     * @return this
     */
    ArrayValue addEnum(int index, String value);

    /**
     * Set the value at the specified index.  This method replaces any
     * existing value at that index.
     *
     * @param index the index for the entry
     *
     * @param value the value to set
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index < 0 || index >= size())
     *
     * @return this
     */
    ArrayValue setEnum(int index, String value);

    /**
     * Sets the value at the specified index with an empty RecordValue,
     * replacing any existing value at that index. The returned object
     * is empty and must be further initialized based on the definition of the
     * field.
     *
     * @param index the index of the entry to set
     *
     * @return an empty instance of RecordValue
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     */
    RecordValue setRecord(int index);

    /**
     * Adds a new RecordValue to the end of the array.  The returned
     * object is empty and must be further initialized based on the definition
     * of the field.
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return an empty RecordValue
     */
    RecordValue addRecord();

    /**
     * Inserts a new RecordValue at the specified index.  This does not
     * replace an existing value, all values at or above the index are shifted
     * to the right.  The returned object is empty and must be further
     * initialized based on the definition of the field.
     *
     * @param index the index for the entry
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index < 0 || index >= size())
     *
     * @return an empty RecordValue
     */
    RecordValue addRecord(int index);

    /**
     * Sets the value at the specified index with an empty MapValue,
     * replacing any existing value at that index.  The returned object
     * is empty and must be further initialized based on the definition of the
     * field.
     *
     * @param index the index of the entry to set
     *
     * @return an empty MapValue
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index < 0 || index >= size())
     *
     */
    MapValue setMap(int index);

    /**
     * Adds a new MapValue to the end of the array.  The returned
     * object is empty and must be further initialized based on the definition
     * of the field.
     *
     * @return an empty MapValue
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     */
    MapValue addMap();

    /**
     * Inserts a new MapValue at the specified index.  This does not
     * replace an existing value, all values at or above the index are shifted
     * to the right. The returned object is empty and must be further
     * initialized based on the definition of the field.
     *
     * @param index the index for the entry
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index < 0 || index >= size())
     *
     * @return an empty MapValue
     */
    MapValue addMap(int index);

    /**
     * Sets the value at the specified index with an empty ArrayValue,
     * replacing any existing value at that index.  The returned object
     * is empty and must be further initialized based on the definition of the
     * field.
     *
     * @param index the index of the entry to set
     *
     * @return an empty ArrayValue
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     */
    ArrayValue setArray(int index);

    /**
     * Adds a new ArrayValue to the end of the array. The returned
     * object is empty and must be further initialized based on the definition
     * of the field.
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @return an empty ArrayValue
     */
    ArrayValue addArray();

    /**
     * Inserts a new ArrayValue at the specified index.  This does not
     * replace an existing value, all values at or above the index are shifted
     * to the right.  The returned object is empty and must be further
     * initialized based on the definition of the field.
     *
     * @param index the index for the entry
     *
     * @throws IllegalArgumentException if the definition of the value does not
     * match that of the array
     *
     * @throws IndexOutOfBoundsException if the index is out of range for the
     * array (index < 0 || index >= size())
     *
     * @return an empty ArrayValue
     */
    ArrayValue addArray(int index);

    /**
     * @return a deep copy of this object.
     */
    @Override
    public ArrayValue clone();
}
