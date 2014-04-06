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

import static oracle.kv.impl.api.table.JsonUtils.BOOLEAN;
import static oracle.kv.impl.api.table.JsonUtils.BYTES;
import static oracle.kv.impl.api.table.JsonUtils.DESC;
import static oracle.kv.impl.api.table.JsonUtils.DOUBLE;
import static oracle.kv.impl.api.table.JsonUtils.FLOAT;
import static oracle.kv.impl.api.table.JsonUtils.INT;
import static oracle.kv.impl.api.table.JsonUtils.LONG;
import static oracle.kv.impl.api.table.JsonUtils.STRING;
import static oracle.kv.impl.api.table.JsonUtils.TYPE;

import java.io.Serializable;

import oracle.kv.table.ArrayDef;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.BinaryDef;
import oracle.kv.table.BinaryValue;
import oracle.kv.table.BooleanDef;
import oracle.kv.table.BooleanValue;
import oracle.kv.table.DoubleDef;
import oracle.kv.table.DoubleValue;
import oracle.kv.table.EnumDef;
import oracle.kv.table.EnumValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FixedBinaryDef;
import oracle.kv.table.FixedBinaryValue;
import oracle.kv.table.FloatDef;
import oracle.kv.table.FloatValue;
import oracle.kv.table.IntegerDef;
import oracle.kv.table.IntegerValue;
import oracle.kv.table.LongDef;
import oracle.kv.table.LongValue;
import oracle.kv.table.MapDef;
import oracle.kv.table.MapValue;
import oracle.kv.table.RecordDef;
import oracle.kv.table.RecordValue;
import oracle.kv.table.StringDef;
import oracle.kv.table.StringValue;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.TextNode;

import com.sleepycat.persist.model.Persistent;

/**
 * Implements FieldDef
 */
@Persistent(version=1)
abstract class FieldDefImpl
    implements FieldDef, Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    /*
     * Immutable properties.
     */
    final private Type type;
    final private String description;

    /**
     * Convenience constructor.
     */
    FieldDefImpl(Type type) {
        this(type, null);
    }

    FieldDefImpl(Type type,
                 String description) {
        this.type = type;
        this.description = description;
    }

    FieldDefImpl(FieldDefImpl impl) {
        type = impl.type;
        description = impl.description;
    }

    @SuppressWarnings("unused")
    FieldDefImpl() {
        type = null;
        description = null;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public Type getType() {
        return type;
    }

    @Override
    public boolean isType(FieldDef.Type type1) {
        return this.type == type1;
    }

    /**
     * Return true if this type can participate in a primary key.
     * Only simple fields can be part of a key.  Boolean type is not
     * allowed in keys (TODO: is there a valid case for this?).
     */
    @Override
    public boolean isValidKeyField() {
        return false;
    }

    @Override
    public boolean isValidIndexField() {
        return false;
    }

    @Override
    public boolean isString() {
        return false;
    }

    @Override
    public boolean isInteger() {
        return false;
    }

    @Override
    public boolean isLong() {
        return false;
    }

    @Override
    public boolean isDouble() {
        return false;
    }

    @Override
    public boolean isFloat() {
        return false;
    }

    @Override
    public boolean isBoolean() {
        return false;
    }

    @Override
    public boolean isBinary() {
        return false;
    }

    @Override
    public boolean isFixedBinary() {
        return false;
    }

    @Override
    public boolean isArray() {
        return false;
    }

    @Override
    public boolean isMap() {
        return false;
    }

    @Override
    public boolean isRecord() {
        return false;
    }

    @Override
    public boolean isEnum() {
        return false;
    }

    @Override
    public BinaryDef asBinary() {
        throw new ClassCastException
            ("Field is not a Binary: " + getClass());
    }

    @Override
    public FixedBinaryDef asFixedBinary() {
        throw new ClassCastException
            ("Field is not a FixedBinary: " + getClass());
    }

    @Override
    public BooleanDef asBoolean() {
        throw new ClassCastException
            ("Field is not a Boolean: " + getClass());
    }

    @Override
    public DoubleDef asDouble() {
        throw new ClassCastException
            ("Field is not a Double: " + getClass());
    }

    @Override
    public FloatDef asFloat() {
        throw new ClassCastException
            ("Field is not a Float: " + getClass());
    }

    @Override
    public IntegerDef asInteger() {
        throw new ClassCastException
            ("Field is not an Integer: " + getClass());
    }

    @Override
    public LongDef asLong() {
        throw new ClassCastException
            ("Field is not a Long: " + getClass());
    }

    @Override
    public StringDef asString() {
        throw new ClassCastException
            ("Field is not a String: " + getClass());
    }

    @Override
    public EnumDef asEnum() {
        throw new ClassCastException
            ("Field is not an Enum: " + getClass());
    }

    @Override
    public ArrayDef asArray() {
        throw new ClassCastException
            ("Field is not an Array: " + getClass());
    }

    @Override
    public MapDef asMap() {
        throw new ClassCastException
            ("Field is not a Map: " + getClass());
    }

    @Override
    public RecordDef asRecord() {
        throw new ClassCastException
            ("Field is not a Record: " + getClass());
    }

    @Override
    public FieldDefImpl clone() {
        try {
            return (FieldDefImpl) super.clone();
        } catch (CloneNotSupportedException ignore) {
        }
        return null;
    }

    @Override
    public ArrayValue createArray() {
        throw new ClassCastException
            ("Field is not an Array: " + getClass());
    }

    @Override
    public BinaryValue createBinary(byte[] value) {
        throw new ClassCastException
            ("Field is not a Binary: " + getClass());
    }

    @Override
    public FixedBinaryValue createFixedBinary(byte[] value) {
        throw new ClassCastException
            ("Field is not a FixedBinary: " + getClass());
    }

    @Override
    public BooleanValue createBoolean(boolean value) {
        throw new ClassCastException
            ("Field is not a Boolean: " + getClass());
    }

    @Override
    public DoubleValue createDouble(double value) {
        throw new ClassCastException
            ("Field is not a Double: " + getClass());
    }

    @Override
    public FloatValue createFloat(float value) {
        throw new ClassCastException
            ("Field is not a Float: " + getClass());
    }

    @Override
    public EnumValue createEnum(String value) {
        throw new ClassCastException
            ("Field is not an Enum: " + getClass());
    }

    @Override
    public IntegerValue createInteger(int value) {
        throw new ClassCastException
            ("Field is not an Integer: " + getClass());
    }

    @Override
    public LongValue createLong(long value) {
        throw new ClassCastException
            ("Field is not a Long: " + getClass());
    }

    @Override
    public MapValue createMap() {
        throw new ClassCastException
            ("Field is not a Map: " + getClass());
    }

    @Override
    public RecordValue createRecord() {
        throw new ClassCastException
            ("Field is not a Record: " + getClass());
    }

    @Override
    public StringValue createString(String value) {
        throw new ClassCastException
            ("Field is not a String: " + getClass());
    }

    /**
     * Creates a value instance for the type based on JsonNode input.
     * This is used when constructing a table definition from
     * JSON input or from an Avro schema.
     */
    @SuppressWarnings("unused")
    FieldValueImpl createValue(JsonNode node) {
        return null;
    }

    /**
     * Implementing classes must override equals
     */
    @Override
    @SuppressWarnings("unused")
    public boolean equals(Object other) {
        throw new IllegalStateException
            ("Classes that implement FieldDefImpl must override equals");
    }

    @Override
    public int hashCode() {
        return type.hashCode();
    }

    /**
     * For internal use only.
     *
     * Add this object into Jackson ObjectNode for serialization to
     * a string format.  This implementation works for the common
     * members of FieldDef objects.  Overrides must add state specific
     * to that type.
     * <p>
     * Type is the only state that is absolutely required.  When used in a
     * top-level table or RecordDef the simple types will have names, but when
     * used as the type for an ArrayDef or MapDef only the type is interesting.
     * In those cases the other state is ignored.
     */
    void toJson(ObjectNode node) {
        if (description != null) {
            node.put(DESC, description);
        }
        node.put(TYPE, getType().toString());
    }

    /**
     * An internal interface for those fields which have a special encoding
     * length.  By default an invalid value is returned.  This is mostly useful
     * for testing.  It is only used by Integer and Long.
     */
    int getEncodingLength() {
        return -1;
    }

    /**
     * Record type must override this in order to return their full definition.
     * This method is used to help generate an Avro schema for a table.
     */
    @SuppressWarnings("unused")
    JsonNode mapTypeToAvro(ObjectNode node) {
        throw new IllegalArgumentException("Type must override mapTypeToAvro: " +
                                           getType());
    }

    /**
     * This method returns the JsonNode representing the Avro schema type
     * for the field.  For simple types it's just a string (TextNode) with
     * the required syntax for Avro.  Complex types and Enumeration override
     * the mapTypeToAvro function to perform the appropriate mapping.
     */
    final JsonNode mapTypeToAvroJsonNode() {
        String textValue = null;
        switch (type) {
        case INTEGER:
            textValue = INT;
            break;
        case LONG:
            textValue = LONG;
            break;
        case STRING:
            textValue = STRING;
            break;
        case BOOLEAN:
            textValue = BOOLEAN;
            break;
        case FLOAT:
            textValue = FLOAT;
            break;
        case DOUBLE:
            textValue = DOUBLE;
            break;
        case BINARY:
            textValue = BYTES;
            break;
        case FIXED_BINARY:
        case ENUM:
        case MAP:
        case RECORD:
        case ARRAY:
            /*
             * The complex types are prepared for a null value in this path.
             * If null, they will allocate the new node.
             */
            return mapTypeToAvro(null);
        default:
            throw new IllegalStateException
                ("Unknown type in mapTypeToAvroJsonNode: " + type);
        }
        return new TextNode(textValue);
    }

    /*
     * Common utility to compare objects for equals() overrides.  It handles
     * the fact that one or both objects may be null.
     */
    boolean compare(Object o, Object other) {
        if (o != null) {
            return o.equals(other);
        }
        return (other == null);
    }
}
