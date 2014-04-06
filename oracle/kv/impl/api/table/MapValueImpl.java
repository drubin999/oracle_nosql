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
import java.util.Map;
import java.util.TreeMap;

import oracle.kv.table.ArrayValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.MapDef;
import oracle.kv.table.MapValue;
import oracle.kv.table.RecordValue;

import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import com.sleepycat.persist.model.Persistent;

/**
 * MapValueImpl implements the MapValue interface and is a container object
 * that holds a map of FieldValue objects all of the same type.  The getters
 * and setters use the same semantics as Java Map.
 */
@Persistent(version=1)
class MapValueImpl extends ComplexValueImpl implements MapValue {
    private static final long serialVersionUID = 1L;
    private final Map<String, FieldValue> fields;

    MapValueImpl(MapDef field) {
        super(field);
        fields = new TreeMap<String, FieldValue>(FieldComparator.instance);
    }

    /* DPL */
    @SuppressWarnings("unused")
    private MapValueImpl() {
        super(null);
        fields = null;
    }

    @Override
    public int size() {
        return fields.size();
    }

    @Override
    public FieldValue remove(String fieldName) {
        return fields.remove(fieldName);
    }

    @Override
    public FieldValue get(String fieldName) {
        return fields.get(fieldName);
    }

    @Override
    public MapValue put(String name, int value) {
        validate(FieldDef.Type.INTEGER);
        fields.put(name, getElement().createInteger(value));
        return this;
    }

    @Override
    public MapValue put(String name, long value) {
        validate(FieldDef.Type.LONG);
        fields.put(name, getElement().createLong(value));
        return this;
    }

    @Override
    public MapValue put(String name, String value) {
        validate(FieldDef.Type.STRING);
        fields.put(name, getElement().createString(value));
        return this;
    }

    @Override
    public MapValue put(String name, double value) {
        validate(FieldDef.Type.DOUBLE);
        fields.put(name, getElement().createDouble(value));
        return this;
    }

    @Override
    public MapValue put(String name, float value) {
        validate(FieldDef.Type.FLOAT);
        fields.put(name, getElement().createFloat(value));
        return this;
    }

    @Override
    public MapValue put(String name, boolean value) {
        validate(FieldDef.Type.BOOLEAN);
        fields.put(name, getElement().createBoolean(value));
        return this;
    }

    @Override
    public MapValue put(String name, byte[] value) {
        validate(FieldDef.Type.BINARY);
        fields.put(name, getElement().createBinary(value));
        return this;
    }

    @Override
    public MapValue putFixed(String name, byte[] value) {
        validate(FieldDef.Type.FIXED_BINARY);
        fields.put(name, getElement().createFixedBinary(value));
        return this;
    }

    @Override
    public MapValue putEnum(String name, String value) {
        validate(FieldDef.Type.ENUM);
        fields.put(name, getElement().createEnum(value));
        return this;
    }

    @Override
    public MapValue put(String fieldName, FieldValue value) {
        /*
         * TODO: this validation needs to be expanded and shared with
         * ArrayValue at least
         */
        if (!getElement().isType(value.getType())) {
            throw new IllegalArgumentException
                ("Incorrect type for map");
        }
        fields.put(fieldName, value);
        return this;
    }

    @Override
    public RecordValueImpl putRecord(String fieldName) {
        RecordValue val = getElement().createRecord();
        fields.put(fieldName, val);
        return (RecordValueImpl) val;
    }

    @Override
    public MapValueImpl putMap(String fieldName) {
        MapValue val = getElement().createMap();
        fields.put(fieldName, val);
        return (MapValueImpl) val;
    }

    @Override
    public ArrayValueImpl putArray(String fieldName) {
        ArrayValue val = getElement().createArray();
        fields.put(fieldName, val);
        return (ArrayValueImpl) val;
    }

    /**
     * Override ComplexValue.getDefinition() to return MapDef
     */
    @Override
    public MapDefImpl getDefinition() {
        return (MapDefImpl) super.getDefinition();
    }

    @Override
    public FieldDef.Type getType() {
        return FieldDef.Type.MAP;
    }

    @Override
    public MapValueImpl clone() {
        MapValueImpl map = new MapValueImpl((MapDef) field);
        for (Map.Entry<String, FieldValue> entry : fields.entrySet()) {
            map.put(entry.getKey(), entry.getValue().clone());
        }
        return map;
    }

    @Override
    public boolean isMap() {
        return true;
    }

    @Override
    public MapValue asMap() {
        return this;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof MapValueImpl) {
            MapValueImpl otherValue = (MapValueImpl) other;
            /* maybe avoid some work */
            if (this == otherValue) {
                return true;
            }
            /*
             * detailed comparison
             */
            if (size() == otherValue.size() &&
                getElement().equals(otherValue.getElement()) &&
                field.equals(otherValue.getDefinition())) {
                for (Map.Entry<String, FieldValue> entry : fields.entrySet()) {
                    if (!entry.getValue().
                        equals(otherValue.get(entry.getKey()))) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        int code = size();
        for (Map.Entry<String, FieldValue> entry : fields.entrySet()) {
            code += entry.getKey().hashCode() + entry.getValue().hashCode();
        }
        return code;
    }

    /**
     * FieldDef must match.
     *
     * Compare field values in array order.  Return as soon as there is a
     * difference. If this object has a field the other does not, return > 0.
     * If this object is missing a field the other has, return < 0.
     */
    @Override
    public int compareTo(FieldValue other) {
        if (other instanceof MapValueImpl) {
            MapValueImpl otherImpl = (MapValueImpl) other;
            if (!field.equals(otherImpl.field)) {
                throw new IllegalArgumentException
                    ("Cannot compare MapValues with different definitions");
            }
            for (Map.Entry<String, FieldValue> entry : fields.entrySet()) {
                FieldValueImpl val = (FieldValueImpl) entry.getValue();
                FieldValueImpl otherVal =
                    (FieldValueImpl) otherImpl.get(entry.getKey());
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
        throw new ClassCastException
            ("Object is not a MapValue");
    }

    /**
     * Map is represented as ObjectNode.  Jackson does not have a MapNode
     */
    @Override
    public JsonNode toJsonNode() {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        for (Map.Entry<String, FieldValue> entry : fields.entrySet()) {
            node.put(entry.getKey(),
                     ((FieldValueImpl)entry.getValue()).toJsonNode());
        }
        return node;
    }

    Map<String, FieldValue> getFields() {
        return fields;
    }

    /**
     * The Avro Generic interface for maps takes a Java Map<String, Object> but
     * the Object must cast to the appropriate type based on Avro's mappings
     * of Java objects to Avro values.
     */
    @Override
    Object toAvroValue(Schema schema) {
        Schema valueSchema = getValueSchema(schema);
        Map<String, Object> newMap =
            new TreeMap<String, Object>(FieldComparator.instance);
        for (Map.Entry<String, FieldValue> entry : getFields().entrySet()) {
            newMap.put(entry.getKey(),
                       ((FieldValueImpl)entry.getValue()).
                       toAvroValue(valueSchema));
        }
        return newMap;
    }

    /*
     * Handle the fact that this field may be nullable and therefore have a
     * Union schema.
     */
    private static Schema getValueSchema(Schema schema) {
        return getUnionSchema(schema, Schema.Type.MAP).getValueType();
    }

    @SuppressWarnings("unchecked")
    static MapValueImpl fromAvroValue(FieldDef def,
                                      Object o,
                                      Schema schema) {
        Map<Utf8, Object> avroMap = (Map<Utf8, Object>) o;
        MapValueImpl map = new MapValueImpl((MapDef)def);
        for (Map.Entry<Utf8, Object> entry : avroMap.entrySet()) {
            String key = entry.getKey().toString();
            map.put(key, FieldValueImpl.
                    fromAvroValue(((MapDef)def).getElement(),
                                  entry.getValue(),
                                  getValueSchema(schema)));
        }
        return map;
    }

    /**
     * Add JSON fields to the map.
     */
    @Override
    void addJsonFields(JsonParser jp, boolean exact) {
        try {
            FieldDef element = getElement();
            while (jp.nextToken() != JsonToken.END_OBJECT) {
                String fieldname = jp.getCurrentName();
                JsonToken token = jp.nextToken();
                /*
                 * Handle null.
                 */
                if (token == JsonToken.VALUE_NULL) {
                    throw new IllegalArgumentException
                        ("Invalid null value in JSON input for field "
                         + fieldname);
                }

                switch (element.getType()) {
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
                    /*
                     * current token is '[', then array elements
                     * TODO: need to have a full-on switch for adding
                     * array elements of the right type.
                     */
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
            }
        } catch (IOException ioe) {
            throw new IllegalArgumentException
                (("Failed to parse JSON input: " + ioe.getMessage()), ioe);
        }
    }

    /*
     * internals
     */
    private FieldDef getElement() {
        return getDefinition().getElement();
    }

    private void validate(FieldDef.Type type) {
        if (!getElement().isType(type)) {
            throw new IllegalArgumentException
                ("Incorrect type for map");
        }
    }
}



