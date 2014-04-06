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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import oracle.kv.table.ArrayValue;
import oracle.kv.table.EnumDef;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.MapValue;
import oracle.kv.table.RecordDef;
import oracle.kv.table.RecordValue;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import com.sleepycat.persist.model.Persistent;

/**
 * RecordValueImpl implements RecordValue and is a multi-valued object that
 * contains a map of string names to fields.  The field values may be simple or
 * complex and allowed fields are defined by the FieldDef definition of the
 * record.
 */
@Persistent(version=1)
class RecordValueImpl extends ComplexValueImpl
    implements RecordValue {
    private static final long serialVersionUID = 1L;
    protected final Map<String, FieldValue> valueMap;

    RecordValueImpl(RecordDef field) {
        super(field);
        valueMap = new TreeMap<String, FieldValue>(FieldComparator.instance);
    }

    RecordValueImpl(RecordDef field, Map<String, FieldValue> valueMap) {
        super(field);
        if (valueMap == null) {
            throw new IllegalArgumentException
                ("Null valueMap passed to RecordValueImpl");
        }
        this.valueMap = valueMap;
    }

    RecordValueImpl(RecordValueImpl other) {
        super(other.field);
        valueMap = new TreeMap<String, FieldValue>(FieldComparator.instance);
        copyFields(other);
    }

    /* DPL */
    @SuppressWarnings("unused")
    private RecordValueImpl() {
        super(null);
        valueMap = null;
    }

    @Override
    public FieldValue get(String fieldName) {
        return valueMap.get(fieldName);
    }

    /**
     * Put methods.  All of these silently overwrite any existing state by
     * creating/using new FieldValue objects.  These methods return "this"
     * in order to support chaining of operations.
     */

    @Override
    public RecordValue put(String name, int value) {
        FieldDef def = validateNameAndType(name, FieldDef.Type.INTEGER);
        valueMap.put(name, def.createInteger(value));
        return this;
    }

    @Override
    public RecordValue put(String name, long value) {
        FieldDef def = validateNameAndType(name, FieldDef.Type.LONG);
        valueMap.put(name, def.createLong(value));
        return this;
    }

    @Override
    public RecordValue put(String name, String value) {
        FieldDef def = validateNameAndType(name, FieldDef.Type.STRING);
        valueMap.put(name, def.createString(value));
        return this;
    }

    @Override
    public RecordValue put(String name, double value) {
        FieldDef def = validateNameAndType(name, FieldDef.Type.DOUBLE);
        valueMap.put(name, def.createDouble(value));
        return this;
    }

    @Override
    public RecordValue put(String name, float value) {
        FieldDef def = validateNameAndType(name, FieldDef.Type.FLOAT);
        valueMap.put(name, def.createFloat(value));
        return this;
    }

    @Override
    public RecordValue put(String name, boolean value) {
        FieldDef def = validateNameAndType(name, FieldDef.Type.BOOLEAN);
        valueMap.put(name, def.createBoolean(value));
        return this;
    }

    @Override
    public RecordValue put(String name, byte[] value) {
        FieldDef def = validateNameAndType(name, FieldDef.Type.BINARY);
        valueMap.put(name, def.createBinary(value));
        return this;
    }

    @Override
    public RecordValue putFixed(String name, byte[] value) {
        FieldDef def = validateNameAndType(name, FieldDef.Type.FIXED_BINARY);
        valueMap.put(name, def.createFixedBinary(value));
        return this;
    }

    @Override
    public RecordValue putEnum(String name, String value) {
        EnumDefImpl enumField =
            (EnumDefImpl) validateNameAndType(name, FieldDef.Type.ENUM);
        valueMap.put(name, enumField.createEnum(value));
        return this;
    }

    @Override
    public RecordValue putNull(String name) {
        FieldDef ft = getDefinition(name);
        if (ft == null) {
            throw new IllegalArgumentException("No such field in record " +
                                               getDefinition().getName() + ": " +
                                               name);
        }
        if (!getDefinition().isNullable(name)) {
            throw new IllegalArgumentException
                ("Named field is not nullable: " + name);
        }
        valueMap.put(name, NullValueImpl.getInstance());
        return this;
    }

    @Override
    public RecordValue put(String name, FieldValue value) {
        if (value.isNull()) {
            return putNull(name);
        }
        validateNameAndType(name, value.getType());
        valueMap.put(name, value);
        return this;
    }

    @Override
    public RecordValueImpl putRecord(String name) {
        FieldDef def = validateNameAndType(name, FieldDef.Type.RECORD);
        RecordValue val = def.createRecord();
        valueMap.put(name, val);
        return (RecordValueImpl)val;
    }

    @Override
    public ArrayValueImpl putArray(String name) {
        FieldDef def = validateNameAndType(name, FieldDef.Type.ARRAY);
        ArrayValue val = def.createArray();
        valueMap.put(name, val);
        return (ArrayValueImpl)val;
    }

    @Override
    public MapValueImpl putMap(String name) {
        FieldDef def = validateNameAndType(name, FieldDef.Type.MAP);
        MapValue val = def.createMap();
        valueMap.put(name, val);
        return (MapValueImpl)val;
    }

    @Override
    public int size() {
        return valueMap.size();
    }

    @Override
    public boolean isEmpty() {
        return valueMap.isEmpty();
    }

    @Override
    public RecordDefImpl getDefinition() {
        return (RecordDefImpl) super.getDefinition();
    }

    @Override
    public FieldDef.Type getType() {
        return FieldDef.Type.RECORD;
    }

    @Override
    public boolean isRecord() {
        return true;
    }

    @Override
    public RecordValue asRecord() {
        return this;
    }

    /**
     * Deep copy
     */
    @Override
    public RecordValueImpl clone() {
        return new RecordValueImpl(this);
    }

    @Override
    public boolean equals(Object other) {
        /* maybe avoid some work */
        if (this == other) {
            return true;
        }
        if (!(other instanceof RecordValueImpl)) {
            return false;
        }
        RecordValueImpl otherValue = (RecordValueImpl) other;

        /*
         * field-by-field comparison
         */
        if (size() == otherValue.size() &&
            field.equals(otherValue.getDefinition())) {
            for (Map.Entry<String, FieldValue> entry : valueMap.entrySet()) {
                if (!entry.getValue().equals(otherValue.get(entry.getKey()))) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int code = size();
        for (Map.Entry<String, FieldValue> entry : valueMap.entrySet()) {
            code += entry.getKey().hashCode() + entry.getValue().hashCode();
        }
        return code;
    }

    /**
     * FieldDef must match for both objects.
     */
    @Override
    public int compareTo(FieldValue other) {
        if (other instanceof RecordValueImpl) {
            RecordValueImpl otherImpl = (RecordValueImpl) other;
            if (!field.equals(otherImpl.field)) {
                throw new IllegalArgumentException
                    ("Cannot compare RecordValues with different definitions");
            }
            return compare(otherImpl, getFields());
        }
        throw new ClassCastException
            ("Object is not an RecordValue");
    }

    /**
     * This is a standalone method with semantics similar to compareTo from
     * the Comparable interface but it takes a restricted list of fields to
     * compare, in order.  The compareTo interface calls this method.
     *
     * Compare field values in order.  Return as soon as there is a difference.
     * If this object has a field the other does not, return > 0.  If this
     * object is missing a field the other has, return < 0.
     */
    public int compare(RecordValueImpl other, List<String> fieldList) {
        for (String fieldName : fieldList) {
            FieldValueImpl val = (FieldValueImpl) get(fieldName);
            FieldValueImpl otherVal = (FieldValueImpl) other.get(fieldName);
            if (val != null) {
                if (otherVal == null) {
                    return 1;
                }
                int comp = val.compareTo(otherVal);
                if (comp != 0) {
                    return comp;
                }
            } else if (otherVal != null) {
                return -1;
            }
        }
        /* they must be equal */
        return 0;
    }

    @Override
    public JsonNode toJsonNode() {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        /*
         * Add fields in field declaration order.  A little slower but it's
         * what the user expects.  Fields may be missing.  That is allowed.
         */
        for (String fieldName : getFields()) {
            FieldValueImpl val = (FieldValueImpl) get(fieldName);
            if (val != null) {
                node.put(fieldName, val.toJsonNode());
            }
        }
        return node;
    }

    @Override
    public FieldValue remove(String name) {
        return valueMap.remove(name);
    }

    /**
     * The use of getFields() in this method ensures that only fields
     * appropriate for the implementing type (Row, PrimaryKey, IndexKey, etc)
     * are copied. Use of putField() bypasses unnecessary checking of field
     * name and type which is done implicitly by the definition check.
     */
    @Override
    public void copyFrom(RecordValue source) {
        copyFrom(source, false);
    }

    void copyFrom(RecordValue source, boolean ignoreDefinition) {
        if (!ignoreDefinition &&
            !getDefinition().equals(source.getDefinition())) {
            throw new IllegalArgumentException
                ("Definition of source record does not match this object");
        }
        for (String fieldName : getFields()) {
            FieldValue val = source.get(fieldName);
            if (val != null) {
                putField(fieldName, val);
            }
        }
    }

    @Override
    public boolean contains(String fieldName) {
        return valueMap.containsKey(fieldName);
    }

    /**
     * Clear the fields.
     * TODO: should this be part of the interface?
     */
    public void clear() {
        valueMap.clear();
    }

    /**
     * Enum is serialized in indexes as an integer representing the value's
     * index in the enumeration declaration.  Deserialize here.
     */
    RecordValue putEnum(String name, int value) {
        EnumDefImpl enumField =
            (EnumDefImpl) validateNameAndType(name, FieldDef.Type.ENUM);
        valueMap.put(name, enumField.createEnum(value));
        return this;
    }

    /**
     * Create and set the named field based on its String representation.  The
     * String representation is that returned by
     * {@link Object#toString toString}.
     *
     * @param name name of the desired field
     *
     * @param value the value to set
     *
     * @param type the type to use for coercion from String
     *
     * @return an instance of FieldValue representing the value.
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the definition of the field does not
     * match the input definition.
     */
    public FieldValue put(String name, String value, FieldDef.Type type) {
        FieldDef newField = validateNameAndType(name, type);
        FieldValue val = create(value, newField);
        valueMap.put(name, val);
        return val;
    }

    void copyFields(RecordValueImpl from) {
        for (Map.Entry<String, FieldValue> entry : from.valueMap.entrySet()) {
            putField(entry.getKey(), entry.getValue().clone());
        }
    }

    /**
     * Return the FieldDef for the named field in the record.
     */
    FieldDef getDefinition(String name) {
        return getDefinition().getField(name);
    }

    /**
     * Return the number of fields in this record.  This method will be
     * overridden by classes that restrict the number (PrimaryKey, IndexKey).
     */
    int getNumFields() {
        return getDefinition().getNumFields();
    }

    /**
     * Return the field definition for the named field.  Null if it does not
     * exist or is not available in this instance (e.g. PrimaryKey, IndexKey).
     */
    FieldDef getField(String fieldName) {
        return getDefinition().getField(fieldName);
    }

    FieldMapEntry getFieldMapEntry(String fieldName) {
        return getDefinition().getFieldMapEntry(fieldName, false);
    }

    /**
     * Internal method to get an ordered list of fields for this object.  By
     * default it is the field order list from the associated RecordDefImpl
     * object.  Key objects will override this to restrict it to the list they
     * require.
     */
    List<String> getFields() {
        return getDefinition().getFieldsInternal();
    }

    /**
     * Validate the value of the object.  By default there is not validation.
     * Subclasses may implement this.
     */
    void validate() {
    }

    /*
     * Record maps to GenericRecord
     */
    @Override
    Object toAvroValue(Schema schema) {
        Schema recordSchema = getRecordSchema(schema);
        GenericRecord record = new GenericData.Record(recordSchema);
        RecordDefImpl def = getDefinition();
        for (Map.Entry<String, FieldMapEntry> entry :
                 def.getFieldMap().getFields().entrySet()) {
            String fieldName = entry.getKey();
            FieldValueImpl fv = (FieldValueImpl) get(fieldName);
            if (fv == null) {
                fv = entry.getValue().getDefaultValue();
            }
            if (fv.isNull()) {
                record.put(fieldName, null);
            } else {
                Schema s = recordSchema.getField(fieldName).schema();
                record.put(fieldName, fv.toAvroValue(s));
            }
        }
        return record;
    }

    /**
     * Add matching JSON fields to the record.  For each named field do this:
     * 1.  find it in the record definition
     * 2.  if present, add it to the value
     * 3.  if not present ignore, unless exact is true, in which case throw.
     */
    @Override
    void addJsonFields(JsonParser jp, boolean exact) {
        int numFields = 0;
        try {
            while (jp.nextToken() != JsonToken.END_OBJECT) {
                String fieldname = jp.getCurrentName();
                if (fieldname != null) {

                    /*
                     * getFieldMapEntry() will filter out fields
                     * not relevant to this type (e.g. index key).
                     */
                    FieldMapEntry fme = getFieldMapEntry(fieldname);
                    if (fme == null) {
                        if (exact) {
                            throw new IllegalArgumentException
                                ("Unexpected field in JSON input: " +
                                 fieldname);
                        }
                        /*
                         * An exact match is not required.  Consume the token.
                         * If it is a nested JSON Object or Array, skip it
                         * entirely.
                         */
                        JsonToken token = jp.nextToken();
                        if (token == JsonToken.START_OBJECT) {
                            skipToJsonToken(jp, JsonToken.END_OBJECT);
                        } else if (token == JsonToken.START_ARRAY) {
                            skipToJsonToken(jp, JsonToken.END_ARRAY);
                        }
                        continue;
                    }
                    JsonToken token = jp.nextToken();
                    /*
                     * Handle null.
                     */
                    if (token == JsonToken.VALUE_NULL) {
                        if (fme.isNullable()) {
                            putNull(fieldname);
                            ++numFields;
                            continue;
                        }
                        throw new IllegalArgumentException
                            ("Invalid null value in JSON input for field "
                             + fieldname);
                    }
                    switch (fme.getField().getType()) {
                    case INTEGER:
                        put(fieldname, jp.getIntValue());
                        break;
                    case LONG:
                        put(fieldname, jp.getLongValue());
                        break;
                    case DOUBLE:
                        put(fieldname, jp.getDoubleValue());
                        break;
                    case FLOAT:
                        put(fieldname, jp.getFloatValue());
                        break;
                    case STRING:
                        put(fieldname, jp.getText());
                        break;
                    case BINARY:
                        put(fieldname, jp.getBinaryValue());
                        break;
                    case FIXED_BINARY:
                        putFixed(fieldname, jp.getBinaryValue());
                        break;
                    case BOOLEAN:
                        put(fieldname, jp.getBooleanValue());
                        break;
                    case ARRAY:
                        ArrayValueImpl array = putArray(fieldname);
                        array.addJsonFields(jp, exact);
                        break;
                    case MAP:
                        MapValueImpl map = putMap(fieldname);
                        map.addJsonFields(jp, exact);
                        break;
                    case RECORD:
                        RecordValueImpl record = putRecord(fieldname);
                        record.addJsonFields(jp, exact);
                        break;
                    case ENUM:
                        putEnum(fieldname, jp.getText());
                        break;
                    }
                    ++numFields;
                }
            }
        } catch (IOException ioe) {
            throw new IllegalArgumentException
                (("Failed to parse JSON input: " + ioe.getMessage()), ioe);
        }
        if (exact && (getNumFields() != numFields)) {
            throw new IllegalArgumentException
                ("Not enough fields for value in JSON input." +
                 "Found " + numFields + ", expected " + getNumFields());
        }
    }

    static RecordValueImpl fromAvroValue(FieldDef definition,
                                         Object obj,
                                         Schema schema) {
        Schema recordSchema = getRecordSchema(schema);
        GenericRecord r = (GenericRecord) obj;
        RecordValueImpl record = new RecordValueImpl((RecordDef)definition);
        RecordDefImpl defImpl = (RecordDefImpl)definition;
        for (Map.Entry<String, FieldMapEntry> entry :
                 defImpl.getFieldMap().getFields().entrySet()) {
            FieldMapEntry fme = entry.getValue();
            String fieldName = entry.getKey();
            Object o = r.get(fieldName);
            if (o != null) {
                Schema fieldSchema = recordSchema.getField(fieldName).schema();
                record.put(fieldName, FieldValueImpl.
                           fromAvroValue(fme.getField(), o, fieldSchema));
            } else if (fme.isNullable()) {
                record.putNull(fieldName);
            } else {
                record.put(fieldName, fme.getDefaultValue());
            }
        }
        return record;
    }

    /*
     * Handle the fact that this field may be nullable and therefore have a
     * Union schema.
     */
    private static Schema getRecordSchema(Schema schema) {
        return getUnionSchema(schema, Schema.Type.RECORD);
    }

    /**
     * Internal use
     */

    private void putField(String name, FieldValue value) {
        valueMap.put(name, value);
    }

    /**
     * TODO: more validation based on definition.
     */
    FieldDef validateNameAndType(String name,
                                 FieldDef.Type type) {
        FieldDef ft = getDefinition(name);
        if (ft == null) {
            throw new IllegalArgumentException("No such field in record " +
                                               getDefinition().getName() + ": " +
                                               name);
        }
        if (ft.getType() != type) {
            throw new IllegalArgumentException
                ("Incorrect type for field " +
                 name + ", type is " + type +
                 ", expected " + ft.getType());
        }
        return ft;
    }

    /**
     * Create FieldValue instances from Strings that are stored "naturally"
     * for the data type.  This is opposed to the String encoding used for
     * key components.
     */
    private FieldValue create(String value,
                              final FieldDef field1) {
        switch (field1.getType()) {
        case INTEGER:
            return new IntegerValueImpl(Integer.parseInt(value));
        case LONG:
            return new LongValueImpl(Long.parseLong(value));
        case STRING:
            return new StringValueImpl(value);
        case DOUBLE:
            return new DoubleValueImpl(Double.parseDouble(value));
        case FLOAT:
            return new FloatValueImpl(Float.parseFloat(value));
        case BOOLEAN:
            return new BooleanValueImpl(value);
        case ENUM:
            return new EnumValueImpl((EnumDef) field1, value);
        default:
            throw new IllegalArgumentException("Type not yet implemented: " +
                                               field1.getType());
        }
    }
}



