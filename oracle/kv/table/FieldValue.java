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
 * FieldValue represents a value of a single field.  A value may be simple or
 * complex (single-valued vs multi-valued).  FieldValue is the building block
 * of row values in a table.
 *<p>
 * The FieldValue interface defines casting and interrogation methods common to
 * all implementing classes.  Each implementing type has its own interface
 * which is an extension of FieldValue.  In most cases interfaces that extend
 * FieldValue have corresponding classes that extend {@link FieldDef}.  The
 * exceptions are {@link Row}, {@link PrimaryKey} and {@link IndexKey}.  These
 * all extend {@link RecordValue} as they are specialized instances of records.
 *<p>
 * By default fields can be nullable, which means that a special null value can
 * be assigned to them.  A null value is a FieldValue instance that returns true
 * for {@link #isNull} and will throw exceptions for most other operations that
 * require an actual instance of a type.  This means that callers who might
 * reasonably expect a null value should first check to see if the value is null
 * before using the value.
 *<p>
 * FieldValue instances are not thread safe.
 *
 * @since 3.0
 */
public interface FieldValue extends Comparable<FieldValue> {

    /**
     * Get the type of the value.
     *
     * @return the type of the instance
     */
    FieldDef.Type getType();

    /**
     * Create a JSON representation of the value.
     *
     * @param prettyPrint set to true for a nicely formatted JSON string,
     * with indentation and carriage returns, otherwise the string will be a
     * single line
     *
     * @return a JSON representation of the value
     */
    String toJsonString(boolean prettyPrint);

    /**
     * Returns true if this is a {@link BooleanValue}.
     *
     * @return true if this is a BooleanValue, false otherwise
     */
    boolean isBoolean();

    /**
     * Returns true if this is a {@link BinaryValue}.
     *
     * @return true if this is a BinaryValue, false otherwise
     */
    boolean isBinary();

    /**
     * Returns true if this is a {@link DoubleValue}.
     *
     * @return true if this is a DoubleValue, false otherwise
     */
    boolean isDouble();

    /**
     * Returns true if this is an {@link EnumValue}.
     *
     * @return true if this is an EnumValue, false otherwise
     */
    boolean isEnum();

    /**
     * Returns true if this is a {@link FixedBinaryValue}.
     *
     * @return true if this is a FixedBinaryValue, false otherwise
     */
    boolean isFixedBinary();

    /**
     * Returns true if this is a {@link FloatValue}.
     *
     * @return true if this is a FloatValue, false otherwise
     */
    boolean isFloat();

    /**
     * Returns true if this is an {@link IntegerValue}.
     *
     * @return true if this is an IntegerValue, false otherwise
     */
    boolean isInteger();

    /**
     * Returns true if this is a {@link LongValue}.
     *
     * @return true if this is a LongValue, false otherwise
     */
    boolean isLong();

    /**
     * Returns true if this is a {@link StringValue}.
     *
     * @return true if this is a StringValue, false otherwise
     */
    boolean isString();

    /**
     * Returns true if this is an {@link ArrayValue}.
     *
     * @return true if this is an ArrayValue, false otherwise
     */
    boolean isArray();

    /**
     * Returns true if this is a {@link MapValue}.
     *
     * @return true if this is a MapValue, false otherwise
     */
    boolean isMap();

    /**
     * Returns true if this is a {@link RecordValue}.
     *
     * @return true if this is a RecordValue, false otherwise
     */
    boolean isRecord();

    /**
     * Returns true if this is a {@link Row}.  Row also
     * returns true for {@link #isRecord}.
     *
     * @return true if this is a Row}, false otherwise
     */
    boolean isRow();

    /**
     * Returns true if this is a {@link PrimaryKey}.  PrimaryKey also
     * returns true for {@link #isRecord} and {@link #isRow}.
     *
     * @return true if this is a PrimaryKey}, false otherwise
     */
    boolean isPrimaryKey();

    /**
     * Returns true if this is an {@link IndexKey}.  IndexKey also
     * returns true for {@link #isRecord}.
     *
     * @return true if this is an IndexKey}, false otherwise
     */
    boolean isIndexKey();

    /**
     * Returns true if this is a null value instance.
     *
     * @return true if this is a null value, false otherwise.
     */
    boolean isNull();

    /**
     * Create a deep copy of this object.
     *
     * @return a new copy
     */
    FieldValue clone();

    /**
     * Casts to BinaryValue.
     *
     * @return a BinaryValue
     *
     * @throws ClassCastException if this is not a BinaryValue
     */
    BinaryValue asBinary();

    /**
     * Casts to BooleanValue.
     *
     * @return a BooleanValue
     *
     * @throws ClassCastException if this is not a BooleanValue
     */
    BooleanValue asBoolean();

    /**
     * Casts to DoubleValue.
     *
     * @return a DoubleValue
     *
     * @throws ClassCastException if this is not a DoubleValue
     */
    DoubleValue asDouble();

    /**
     * Casts to EnumValue.
     *
     * @return an EnumValue
     *
     * @throws ClassCastException if this is not an EnumValue
     */
    EnumValue asEnum();

    /**
     * Casts to FixedBinaryValue.
     *
     * @return a FixedBinaryValue
     *
     * @throws ClassCastException if this is not a FixedBinaryValue
     */
    FixedBinaryValue asFixedBinary();

    /**
     * Casts to FloatValue.
     *
     * @return a FloatValue
     *
     * @throws ClassCastException if this is not a FloatValue
     */
    FloatValue asFloat();

    /**
     * Casts to IntegerValue.
     *
     * @return an IntegerValue
     *
     * @throws ClassCastException if this is not an IntegerValue
     */
    IntegerValue asInteger();

    /**
     * Casts to LongValue.
     *
     * @return a LongValue
     *
     * @throws ClassCastException if this is not a LongValue
     */
    LongValue asLong();

    /**
     * Casts to StringValue.
     *
     * @return a StringValue
     *
     * @throws ClassCastException if this is not a StringValue
     */
    StringValue asString();

    /**
     * Casts to ArrayValue.
     *
     * @return an  ArrayValue
     *
     * @throws ClassCastException if this is not an ArrayValue
     */
    ArrayValue asArray();

    /**
     * Casts to MapValue.
     *
     * @return a MapValue
     *
     * @throws ClassCastException if this is not a MapValue
     */
    MapValue asMap();

    /**
     * Casts to RecordValue.
     *
     * @return a RecordValue
     *
     * @throws ClassCastException if this is not a RecordValue
     */
    RecordValue asRecord();

    /**
     * Casts to Row.
     *
     * @return a Row
     *
     * @throws ClassCastException if this is not a Row.
     */
    Row asRow();

    /**
     * Casts to PrimaryKey.
     *
     * @return a PrimaryKey
     *
     * @throws ClassCastException if this is not a PrimaryKey
     */
    PrimaryKey asPrimaryKey();

    /**
     * Casts to IndexKey.
     *
     * @return an IndexKey
     *
     * @throws ClassCastException if this is not an IndexKey
     */
    IndexKey asIndexKey();
}



