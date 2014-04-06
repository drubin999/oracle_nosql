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
 * FieldRange defines a range of values to be used in a table or index
 * iteration or multiGet operation.  A FieldRange is used as the least
 * significant component in a partially specified {@link PrimaryKey} or
 * {@link IndexKey} in order to create a value range for an operation that
 * returns multiple rows or keys.  The data types supported by FieldRange
 * are limited to those which are valid for primary keys and/or index keys,
 * as indicated by {@link FieldDef#isValidKeyField} and
 * {@link FieldDef#isValidIndexField}.
 * <p>
 * This object is used to scope a table or index operation and is constructed
 * by {@link Table#createFieldRange} and {@link Index#createFieldRange}.  If
 * used on a table the field referenced must be part of the table's primary key
 * and be in proper order relative to the parent value.  If used on an index
 * the field must be part of the index definition and in proper order relative
 * to the index definition.
 * <p>
 * Indexes can include array fields as long as the array element type is valid
 * for use in an index.  For example, an array of LONG can be indexed but an
 * array of MAP cannot.  An array of ARRAY cannot be indexed because of the
 * explosion of index entries that would result.  There can be only one array
 * in an index, also because of the potential explosion of entries.  When
 * querying an index using an array only a single value can be specified.  That
 * is, it is not possible to provide a set of possible values for the index.
 * When an array field is the target of a FieldRange the caller must use the
 * methods associated with the type of the array element.  For example, if it
 * is an array of LONG then methods such as setStart(long, boolean) should be
 * used.
 *
 * @since 3.0
 */
public class FieldRange {

    private final String fieldName;
    private final FieldDef field;
    private FieldValue start;
    private boolean startInclusive;
    private FieldValue end;
    private boolean endInclusive;

    /**
     * @hidden
     * Internal use.  Used by factory methods on Table and Index.
     * The fields referenced by startValue and endValue must be the same field
     * within the field definition for the table on which this object is used.
     *
     * @param fieldName is the name of the field on which this range is
     * defined.
     *
     * @param field is the definition of the field on which this range is
     * defined.
     */
    public FieldRange(String fieldName, FieldDef field) {
        this.fieldName = fieldName;
        this.field = field;
    }

    /**
     * A convenience factory method to create a MultiRowOptions
     * instance using this FieldRange.
     *
     * @return a new MultiRowOptions
     */
    public MultiRowOptions createMultiRowOptions() {
        return new MultiRowOptions(this);
    }

    /**
     * Returns the FieldValue that defines lower bound of the range,
     * or null if no lower bound is enforced.
     *
     * @return the start FieldValue
     */
    public FieldValue getStart() {
        return start;
    }

    /**
     * Returns whether start is included in the range, i.e., start is less than
     * or equal to the first FieldValue in the range.  This value is valid only
     * if the start value is not null.
     *
     * @return true if the start value is inclusive
     */
    public boolean getStartInclusive() {
        return startInclusive;
    }

    /**
     * Returns the FieldValue that defines upper bound of the range,
     * or null if no upper bound is enforced.
     *
     * @return the end FieldValue
     */
    public FieldValue getEnd() {
        return end;
    }

    /**
     * Returns whether end is included in the range, i.e., end is greater than
     * or equal to the last FieldValue in the range.  This value is valid only
     * if the end value is not null.
     *
     * @return true if the end value is inclusive
     */
    public boolean getEndInclusive() {
        return endInclusive;
    }

    /**
     * Returns the FieldDef for the field used in the range.
     *
     * @return the FieldDef
     */
    public FieldDef getDefinition() {
        return field;
    }

    /**
     * Returns the name for the field used in the range.
     *
     * @return the name of the field
     */
    public String getFieldName() {
        return fieldName;
    }

    /**
     * Sets the start value of the range to the specified integer value.
     *
     * @param value the value to set
     *
     * @param isInclusive set to true if the range is inclusive of the value,
     * false if it is exclusive.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the value is not valid for the
     * field in the range.
     */
    public FieldRange setStart(int value, boolean isInclusive) {
        final FieldDef fieldToUse = getFieldDef();
        final FieldValue val = fieldToUse.createInteger(value);
        return setStartValue(val, isInclusive);
    }

    /**
     * Sets the start value of the range to the specified double value.
     *
     * @param value the value to set
     *
     * @param isInclusive set to true if the range is inclusive of the value,
     * false if it is exclusive.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the value is not valid for the
     * field in the range.
     */
    public FieldRange setStart(double value, boolean isInclusive) {
        final FieldDef fieldToUse = getFieldDef();
        final FieldValue val = fieldToUse.createDouble(value);
        return setStartValue(val, isInclusive);
    }

    /**
     * Sets the start value of the range to the specified float value.
     *
     * @param value the value to set
     *
     * @param isInclusive set to true if the range is inclusive of the value,
     * false if it is exclusive.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the value is not valid for the
     * field in the range.
     */
    public FieldRange setStart(float value, boolean isInclusive) {
        final FieldDef fieldToUse = getFieldDef();
        final FieldValue val = fieldToUse.createFloat(value);
        return setStartValue(val, isInclusive);
    }

    /**
     * Sets the start value of the range to the specified long value.
     *
     * @param value the value to set
     *
     * @param isInclusive set to true if the range is inclusive of the value,
     * false if it is exclusive.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the value is not valid for the
     * field in the range.
     */
    public FieldRange setStart(long value, boolean isInclusive) {
        final FieldDef fieldToUse = getFieldDef();
        final FieldValue val = fieldToUse.createLong(value);
        return setStartValue(val, isInclusive);
    }

    /**
     * Sets the start value of the range to the specified string value.
     *
     * @param value the value to set
     *
     * @param isInclusive set to true if the range is inclusive of the value,
     * false if it is exclusive.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the value is not valid for the
     * field in the range.
     */
    public FieldRange setStart(String value, boolean isInclusive) {
        final FieldDef fieldToUse = getFieldDef();
        final FieldValue val = fieldToUse.createString(value);
        return setStartValue(val, isInclusive);
    }

    /**
     * Sets the start value of the range to the specified enumeration value.
     *
     * @param value the value to set
     *
     * @param isInclusive set to true if the range is inclusive of the value,
     * false if it is exclusive.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the value is not valid for the
     * field in the range.
     */
    public FieldRange setStartEnum(String value,
                                   boolean isInclusive) {
        final FieldDef fieldToUse = getFieldDef();
        final FieldValue val = fieldToUse.createEnum(value);
        return setStartValue(val, isInclusive);
    }

    /**
     * Sets the end value of the range to the specified integer value.
     *
     * @param value the value to set
     *
     * @param isInclusive set to true if the range is inclusive of the value,
     * false if it is exclusive.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the value is not valid for the
     * field in the range.
     */
    public FieldRange setEnd(int value, boolean isInclusive) {
        final FieldDef fieldToUse = getFieldDef();
        final FieldValue val = fieldToUse.createInteger(value);
        return setEndValue(val, isInclusive);
    }

    /**
     * Sets the end value of the range to the specified double value.
     *
     * @param value the value to set
     *
     * @param isInclusive set to true if the range is inclusive of the value,
     * false if it is exclusive.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the value is not valid for the
     * field in the range.
     */
    public FieldRange setEnd(double value, boolean isInclusive) {
        final FieldDef fieldToUse = getFieldDef();
        final FieldValue val = fieldToUse.createDouble(value);
        return setEndValue(val, isInclusive);
    }

    /**
     * Sets the end value of the range to the specified float value.
     *
     * @param value the value to set
     *
     * @param isInclusive set to true if the range is inclusive of the value,
     * false if it is exclusive.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the value is not valid for the
     * field in the range.
     */
    public FieldRange setEnd(float value, boolean isInclusive) {
        final FieldDef fieldToUse = getFieldDef();
        final FieldValue val = fieldToUse.createFloat(value);
        return setEndValue(val, isInclusive);
    }

    /**
     * Sets the end value of the range to the specified long value.
     *
     * @param value the value to set
     *
     * @param isInclusive set to true if the range is inclusive of the value,
     * false if it is exclusive.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the value is not valid for the
     * field in the range.
     */
    public FieldRange setEnd(long value, boolean isInclusive) {
        final FieldDef fieldToUse = getFieldDef();
        final FieldValue val = fieldToUse.createLong(value);
        return setEndValue(val, isInclusive);
    }

    /**
     * Sets the end value of the range to the specified date value.
     *
     * @param value the value to set
     *
     * @param isInclusive set to true if the range is inclusive of the value,
     * false if it is exclusive.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the value is not valid for the
     * field in the range.
     */
    public FieldRange setEndDate(long value, boolean isInclusive) {
        final FieldDef fieldToUse = getFieldDef();
        final FieldValue val = fieldToUse.createLong(value);
        return setEndValue(val, isInclusive);
    }

    /**
     * Sets the end value of the range to the specified string value.
     *
     * @param value the value to set
     *
     * @param isInclusive set to true if the range is inclusive of the value,
     * false if it is exclusive.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the value is not valid for the
     * field in the range.
     */
    public FieldRange setEnd(String value, boolean isInclusive) {
        final FieldDef fieldToUse = getFieldDef();
        final FieldValue val = fieldToUse.createString(value);
        return setEndValue(val, isInclusive);
    }

    /**
     * Sets the end value of the range to the specified enumeration value.
     *
     * @param value the value to set
     *
     * @param isInclusive set to true if the range is inclusive of the value,
     * false if it is exclusive.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the value is not valid for the
     * field in the range.
     */
    public FieldRange setEndEnum(String value, boolean isInclusive) {
        final FieldDef fieldToUse = getFieldDef();
        final FieldValue val = fieldToUse.createEnum(value);
        return setEndValue(val, isInclusive);
    }

    /**
     * Sets the start value of the range to the specified value.
     *
     * @param value the value to set
     *
     * @param isInclusive set to true if the range is inclusive of the value,
     * false if it is exclusive.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the value is not valid for the
     * field in the range.
     */
    public FieldRange setStart(FieldValue value, boolean isInclusive) {
        final FieldDef fieldToUse = getFieldDef();
        if (!fieldToUse.isType(value.getType())) {
            throw new IllegalArgumentException
                ("Value is not of correct type: " + value.getType());
        }
        return setStartValue(value, isInclusive);
    }

    /**
     * Sets the end value of the range to the specified value.
     *
     * @param value the value to set
     *
     * @param isInclusive set to true if the range is inclusive of the value,
     * false if it is exclusive.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the value is not valid for the
     * field in the range.
     */
    public FieldRange setEnd(FieldValue value, boolean isInclusive) {
        final FieldDef fieldToUse = getFieldDef();
        if (!fieldToUse.isType(value.getType())) {
            throw new IllegalArgumentException
                ("Value is not of correct type: " + value.getType());
        }
        return setEndValue(value, isInclusive);
    }

    /*
     * Internal use
     */
    private FieldRange setStartValue(FieldValue value, boolean isInclusive) {
        if (field.isArray()) {
            final ArrayValue array = field.createArray();
            array.add(value);
            value = array;
        }
        validate(value, end);
        start = value;
        startInclusive = isInclusive;
        return this;
    }

    private FieldRange setEndValue(FieldValue value, boolean isInclusive) {
        if (field.isArray()) {
            final ArrayValue array = field.createArray();
            array.add(value);
            value = array;
        }
        validate(start, value);
        end = value;
        endInclusive = isInclusive;
        return this;
    }

    private void validate(FieldValue startVal, FieldValue endVal) {
        if (startVal != null && endVal != null) {
            if (startVal.compareTo(endVal) > 0) {
                throw new IllegalArgumentException
                    ("FieldRange: start value must be less than " +
                     "the end value");
            }
        }
    }

    /**
     * This indirection allows array indexes to interpose themselves and
     * return an appropriate FieldDef for the array element type.
     */
    private FieldDef getFieldDef() {
        if (field.isArray()) {
            return field.asArray().getElement();
        }
        return field;
    }
}
