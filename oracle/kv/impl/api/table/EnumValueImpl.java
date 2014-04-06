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

package oracle.kv.impl.api.table;

import oracle.kv.impl.util.SortableString;
import oracle.kv.table.EnumDef;
import oracle.kv.table.EnumValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.TextNode;

import com.sleepycat.persist.model.Persistent;

/**
 * A single value in an enumeration is represented as a string.  Only strings
 * that are part of the enumeration are allowed to be set in this object.
 * Validation is performed on construction and setting of the value.
 * Construction requires the {@link EnumDef} that defines the valid strings for
 * the enumeration.
 */
@Persistent(version=1)
class EnumValueImpl extends FieldValueImpl implements EnumValue {
    private static final long serialVersionUID = 1L;
    private final EnumDefImpl field;
    private final String value;


    EnumValueImpl(EnumDef field, String value) {
        this.field = (EnumDefImpl) field;
        this.value = value;
        validate();
    }

    /* DPL */
    @SuppressWarnings("unused")
    private EnumValueImpl() {
        field = null;
        value = null;
    }

    @Override
    public EnumDef getDefinition() {
        return field;
    }

    @Override
    public String get() {
        return value;
    }

    @Override
    public FieldDef.Type getType() {
        return FieldDef.Type.ENUM;
    }

    @Override
    public EnumValueImpl clone() {
        return new EnumValueImpl(field, value);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof EnumValueImpl) {
            EnumValueImpl otherVal = (EnumValueImpl) other;
            EnumDefImpl def = field;
            EnumDefImpl otherDef = (EnumDefImpl) otherVal.getDefinition();
            /*
             * Avoid calling EnumDefImpl.equals() because it will
             * result in a recursive calling circle.
             */
            return (def.valuesEqual(otherDef) &&
                    value.equals(otherVal.get()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    /**
     * compareTo compares based on order of the specified enumeration values
     * in the enum and not the string values.
     */
    @Override
    public int compareTo(FieldValue other) {
        if (other instanceof EnumValueImpl) {
            int thisIndex = indexOf(value);
            int otherIndex = indexOf(((EnumValueImpl)other).value);
            return ((Integer)thisIndex).compareTo(otherIndex);
        }
        throw new ClassCastException
            ("Object is not an IntegerValue");
    }

    @Override
    public EnumValue asEnum() {
        return this;
    }

    @Override
    public boolean isEnum() {
        return false;
    }

    /**
     * In order to sort correctly keys from an enumeration value must be the
     * value's index in the declaration.
     */
    @Override
    public String formatForKey(FieldDef field1) {
        return SortableString.toSortable
            (getIndex(), (field).getEncodingLen());
    }

    /**
     * Add one to the index of this value in the enum and return a new
     * EnumValueImpl based on that index.  If there is no value with the
     * next index, return null.
     */
    @Override
    public EnumValueImpl getNextValue() {
        int index = getIndex();
        EnumDefImpl def = field;
        if (def.isValidIndex(index + 1)) {
            return def.createEnum(index + 1);
        }
        return null;
    }

    /**
     * Minimum value for any enum is 0
     */
    @Override
    public FieldValueImpl getMinimumValue() {
        return (field).createEnum(0);
    }

    @Override
    public JsonNode toJsonNode() {
        return new TextNode(value);
    }

    @Override
    public int getIndex() {
        return (field).indexOf(value);
    }

    /**
     * Overrides the FieldValueImpl method to make sure that the correct schema
     * is used when constructing the GenericEnumSymbol instance.
     */
    @Override
    Object toAvroValue(Schema schema) {
        Schema toUse = getUnionSchema(schema, Schema.Type.ENUM);
        return new GenericData.EnumSymbol(toUse, get());
    }

    int indexOf(String enumValue) {
        return (field).indexOf(enumValue);
    }

    static EnumValueImpl createFromKey(EnumDef field, String indexString) {
        EnumDefImpl def = (EnumDefImpl)field;
        int index = SortableString.intFromSortable(indexString);
        return def.createEnum(index);
    }


    private void validate() {
        if (field != null && value != null) {
            (field).validateValue(value);
            return;
        }
        throw new IllegalArgumentException
            ("Value not valid for enumeration: " + value);
    }
}
