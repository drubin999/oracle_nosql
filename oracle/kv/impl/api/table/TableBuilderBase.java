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

import static oracle.kv.impl.api.table.JsonUtils.DEFAULT;
import static oracle.kv.impl.api.table.JsonUtils.NULLABLE;

import java.util.List;

import oracle.kv.table.FieldDef;

import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

/**
 * TableBuilderBase is a base class for TableBuilder and TableEvolver that
 * has shared code to add/construct instances of FieldDef.  It has several
 * table-specific methods which are used as interfaces to allow consistent
 * return values (TableBuilderBase) on usage.
 */
public class TableBuilderBase {
    protected FieldMap fields;

    /**
     * A constructor for a new empty object.
     */
    TableBuilderBase() {
        fields = new FieldMap();
    }

    TableBuilderBase(FieldMap map) {
        fields = map;
    }

    public FieldMap getFieldMap() {
        return fields;
    }

    public List<String> getFieldOrder() {
        return fields.getFieldOrder();
    }

    public FieldDef getField(String name) {
        return fields.get(name);
    }

    /**
     * Returns the string form of the type of this builder.  This can be
     * useful for error messages to avoid instanceof.  This defaults to
     * "Table"
     */
    public String getBuilderType() {
        return "Table";
    }

    /**
     * Returns true if this builder is a collection builder, such as
     * ArrayBuild or MapBuilder.
     */
    public boolean isCollectionBuilder() {
        return false;
    }

    /**
     * Validate the object by building it.  This may be overridden if
     * necessary.
     */
    public TableBuilderBase validate() {
        build();
        return this;
    }

    /**
     * These must be overridden by TableBuilder
     */
    @SuppressWarnings("unused")
    public TableBuilderBase primaryKey(String ... key) {
        throw new IllegalArgumentException("primaryKey not supported");
    }

    @SuppressWarnings("unused")
    public TableBuilderBase shardKey(String ... key) {
        throw new IllegalArgumentException("shardKey not supported");
    }

    @SuppressWarnings("unused")
    public TableBuilderBase primaryKey(List<String> key) {
        throw new IllegalArgumentException("primaryKey not supported");
    }

    @SuppressWarnings("unused")
    public TableBuilderBase shardKey(List<String> key) {
        throw new IllegalArgumentException("shardKey not supported");
    }

    @SuppressWarnings("unused")
    public TableBuilderBase setR2compat(boolean r2compat) {
        throw new IllegalArgumentException("setR2compat not supported");
    }

    @SuppressWarnings("unused")
    public TableBuilderBase setSchemaId(int id) {
        throw new IllegalArgumentException("setSchemaId not supported");
    }

    @SuppressWarnings("unused")
    public TableBuilderBase addSchema(String avroSchema) {
        throw new IllegalArgumentException("addSchema not supported");
    }

    @SuppressWarnings("unused")
    public TableBuilderBase setDescription(String description) {
        throw new IllegalArgumentException("setDescription not supported");
    }

    public TableImpl buildTable() {
        throw new IllegalArgumentException("buildTable must be overridden");
    }

    public FieldDef build() {
        throw new IllegalArgumentException("build must be overridden");
    }

    /*
     * Integer
     */
    public TableBuilderBase addInteger(String name) {
        return addInteger(name, null, null, null, null, null);
    }

    public TableBuilderBase addInteger(String name, String description,
                                       Boolean nullable,
                                       Integer defaultValue,
                                       Integer min, Integer max) {
        IntegerDefImpl def = new IntegerDefImpl(description, min, max);
        if (isCollectionBuilder()) {
            return addField(def);
        }
        IntegerValueImpl value = (defaultValue != null ?
                                  def.createInteger(defaultValue) : null);
        return addField(name, makeFieldMapEntry(def, nullable, value));
    }

    /*
     * Adds to collection (map, array), no name, not nullable, no default.
     */
    public TableBuilderBase addInteger() {
        return addInteger(null, null, null, null, null, null);
    }

    public TableBuilderBase addInteger(String description,
                                       Integer min, Integer max) {
        return addInteger(null, description, null, null, min, max);
    }

    /*
     * Long
     */
    public TableBuilderBase addLong(String name) {
        return addLong(name, null, null, null, null, null);
    }

    public TableBuilderBase addLong(String name, String description,
                                    Boolean nullable,
                                    Long defaultValue,
                                    Long min, Long max) {
        LongDefImpl def = new LongDefImpl(description, min, max);
        if (isCollectionBuilder()) {
            return addField(def);
        }
        LongValueImpl value = (defaultValue != null ?
                               def.createLong(defaultValue) : null);
        return addField(name, makeFieldMapEntry(def, nullable, value));
    }

    /*
     * Adds to collection (map, array), no name, not nullable, no default.
     */
    public TableBuilderBase addLong() {
        return addLong(null, null, null, null, null, null);
    }

    public TableBuilderBase addLong(String description,
                                    Long min, Long max) {
        return addLong(null, description, null, null, min, max);
    }

    /*
     * Double
     */
    public TableBuilderBase addDouble(String name) {
        return addDouble(name, null, null, null, null, null);
    }

    public TableBuilderBase addDouble(String name, String description,
                                      Boolean nullable,
                                      Double defaultValue,
                                      Double min, Double max) {
        DoubleDefImpl def = new DoubleDefImpl(description, min, max);
        if (isCollectionBuilder()) {
            return addField(def);
        }
        DoubleValueImpl value = (defaultValue != null ?
                                 def.createDouble(defaultValue) : null);
        return addField(name, makeFieldMapEntry(def, nullable, value));
    }

    /*
     * Adds to collection (map, array), no name, not nullable, no default.
     */
    public TableBuilderBase addDouble() {
        return addDouble(null, null, null, null, null, null);
    }

    public TableBuilderBase addDouble(String description,
                                      Double min, Double max) {
        return addDouble(null, description, null, null, min, max);
    }

    /*
     * Float
     */
    public TableBuilderBase addFloat(String name) {
        return addFloat(name, null, null, null, null, null);
    }

    public TableBuilderBase addFloat(String name, String description,
                                      Boolean nullable,
                                      Float defaultValue,
                                      Float min, Float max) {
        FloatDefImpl def = new FloatDefImpl(description, min, max);
        if (isCollectionBuilder()) {
            return addField(def);
        }
        FloatValueImpl value = (defaultValue != null ?
                                def.createFloat(defaultValue) : null);
        return addField(name, makeFieldMapEntry(def, nullable, value));
    }

    /*
     * Adds to collection (map, array), no name, not nullable, no default.
     */
    public TableBuilderBase addFloat() {
        return addFloat(null, null, null, null, null, null);
    }

    public TableBuilderBase addFloat(String description,
                                     Float min, Float max) {
        return addFloat(null, description, null, null, min, max);
    }

    /*
     * Boolean
     */

    public TableBuilderBase addBoolean(String name) {
        return addBoolean(name, null, null, null);
    }

    public TableBuilderBase addBoolean(String name, String description) {
        return addBoolean(name, description, null, null);
    }

    public TableBuilderBase addBoolean(String name, String description,
                                       Boolean nullable,
                                       Boolean defaultValue) {
        BooleanDefImpl def = new BooleanDefImpl(description);
        if (isCollectionBuilder()) {
            return addField(def);
        }
        BooleanValueImpl value = (defaultValue != null ?
                                  def.createBoolean(defaultValue) : null);
        return addField(name, makeFieldMapEntry(def, nullable, value));
    }

    /*
     * Adds to collection (map, array), no name, not nullable, no default.
     */
    public TableBuilderBase addBoolean() {
        return addBoolean(null, null, null, null);
    }

    /*
     * String
     */
    public TableBuilderBase addString(String name) {
        return addString(name, null, null, null, null, null, null, null);
    }

    public TableBuilderBase addString(String name, String description,
                                      Boolean nullable,
                                      String defaultValue,
                                      String min, String max,
                                      Boolean minInclusive,
                                      Boolean maxInclusive) {
        StringDefImpl def = new StringDefImpl(description, min, max,
                                              minInclusive, maxInclusive);
        if (isCollectionBuilder()) {
            return addField(def);
        }
        StringValueImpl value = (defaultValue != null ?
                                 def.createString(defaultValue) : null);
        return addField(name, makeFieldMapEntry(def, nullable, value));
    }

    /*
     * Adds to collection (map, array), no name, not nullable, no default.
     */
    public TableBuilderBase addString() {
        return addString(null, null, null, null, null);
    }

    public TableBuilderBase addString(String description,
                                      String min, String max,
                                      Boolean minInclusive,
                                      Boolean maxInclusive) {
        return addString(null, description, null, null,
                         min, max, minInclusive, maxInclusive);
    }

    /*
     * Enum
     */
    public TableBuilderBase addEnum(String name, String[] values,
                                    String description,
                                    Boolean nullable, String defaultValue) {
        EnumDefImpl def = new EnumDefImpl(name, values, description);
        if (isCollectionBuilder()) {
            return addField(def);
        }
        EnumValueImpl value = (defaultValue != null ?
                               def.createEnum(defaultValue) : null);
        return addField(name, makeFieldMapEntry(def, nullable, value));
    }

    /*
     * Adds to collection (map, array), no name, not nullable, no default.
     */
    public TableBuilderBase addEnum(String name, String[] values,
                                    String description) {
        return addEnum(name, values, description, null, null);
    }

    /*
     * Binary
     */
    public TableBuilderBase addBinary() {
        return addBinary(null, null, null);
    }

    public TableBuilderBase addBinary(String name,
                                      String description) {
        return addBinary(name, description, null);
    }

    public TableBuilderBase addBinary(String name) {
        return addBinary(name, null, null);
    }

    /*
     * This is a special case for where there may be a union and null
     * default value coming from a schema.  It should never happen
     * when creating a table from an R2 schema.  It is useful for testing.
     */
    public TableBuilderBase addBinary(String name,
                                      String description,
                                      Boolean nullable) {
        BinaryDefImpl def = new BinaryDefImpl(description);
        if (isCollectionBuilder()) {
            return addField(def);
        }
        return addField(name, makeFieldMapEntry(def, nullable, null));
    }

    /*
     * FixedBinary
     */
    public TableBuilderBase addFixedBinary(String name, int size) {
        return addFixedBinary(name, size, null);
    }

    /*
     * FixedBinary requires a name whether it's in a record or being
     * added to a collection.  When being added to a record, pass
     * true as the isRecord parameter.
     */
    public TableBuilderBase addFixedBinary(String name, int size,
                                           String description) {
        FixedBinaryDefImpl def =
            new FixedBinaryDefImpl(name, size, description);
        if (isCollectionBuilder()) {
            return addField(def);
        }
        return addField(name, makeFieldMapEntry(def, null, null));
    }

    /*
     * This is a special case for where there may be a union and null
     * default value coming from a schema.  It should never happen
     * when creating a table from an R2 schema.  It is useful for testing.
     */
    public TableBuilderBase addFixedBinary(String name, int size,
                                           String description,
                                           Boolean nullable) {
        FixedBinaryDefImpl def =
            new FixedBinaryDefImpl(name, size, description);
        if (isCollectionBuilder()) {
            return addField(def);
        }
        return addField(name, makeFieldMapEntry(def, nullable, null));
    }


    /**
     * Remove a field
     */
    public void removeField(String fieldName) {
        FieldDef toBeRemoved = getField(fieldName);
        if (toBeRemoved == null) {
            throw new IllegalArgumentException
                ("Field does not exist: " + fieldName);
        }
        validateFieldRemoval(fieldName);
        fields.remove(fieldName);
    }

    /**
     * Default implementation of field removal validation.  This is
     * overridden by classes that need to perform actual validation.
     */
    @SuppressWarnings("unused")
    void validateFieldRemoval(String fieldName) {}

    /**
     * Returns the number of fields defined
     */
    public int size() {
        return fields.size();
    }

    /**
     * Add field to map or array.  These fields do not have names.
     */
    @SuppressWarnings("unused")
    public TableBuilderBase addField(FieldDef field) {
        throw new IllegalArgumentException
            ("addField(FieldDef) can only be used for maps and arrays");
    }

    /**
     * Adds to a record using default values.
     */
    public TableBuilderBase addField(String name, FieldDef field) {
        return addField
            (name, makeFieldMapEntry((FieldDefImpl)field, null, null));
    }

    public TableBuilderBase addField(String name, FieldMapEntry field) {
        validateFieldAddition(name);
        assert name != null;
        if (fields.exists(name)) {
            throw new IllegalArgumentException
                ("Named field already exists: " + name);
        }
        fields.put(name, field);
        return this;
    }

    /**
     * Validate the addition of a field to the map.  At this time it ensures
     * that the field name uses allowed characters.  Sub-classes involved with
     * schema evolution will override it.
     */
    void validateFieldAddition(String name) {
        if (name != null) {
            TableImpl.validateComponent(name, false);
        }
    }

    TableBuilderBase generateAvroSchemaFields(Schema schema,
                                              String name,
                                              JsonNode defaultValue,
                                              String desc) {
        return generateAvroSchemaFields(schema, name, defaultValue,
                                        desc, false);
    }

    private TableBuilderBase generateAvroSchemaFields(Schema schema,
                                                      String name,
                                                      JsonNode defaultValue,
                                                      String desc,
                                                      boolean isUnion) {
        Schema.Type ftype = schema.getType();

        switch (ftype) {
        case BOOLEAN:
            if (isCollectionBuilder()) {
                addBoolean();
            } else {
                addBoolean(name, desc,
                           isUnion, /* nullable */
                           (defaultValue != null && !defaultValue.isNull() ?
                            defaultValue.getBooleanValue() : null));
            }
            break;
        case BYTES:
            if (isCollectionBuilder()) {
                addBinary();
            } else {
                addBinary(name, desc,
                          isUnion);
            }
            break;
        case FIXED:
            if (isCollectionBuilder()) {
                addFixedBinary(name, schema.getFixedSize(), desc);
            } else {
                addFixedBinary(name, schema.getFixedSize(), desc,
                               isUnion);
            }
            break;
        case DOUBLE:
            if (isCollectionBuilder()) {
                addDouble();
            } else {
                addDouble(name, desc,
                          isUnion, /* nullable */
                          (defaultValue != null && !defaultValue.isNull() ?
                           defaultValue.getValueAsDouble() :
                           null),
                          null, null);
            }
            break;
        case FLOAT:
            if (isCollectionBuilder()) {
                addFloat();
            } else {
                addFloat(name, desc,
                         isUnion, /* nullable */
                         (defaultValue != null && !defaultValue.isNull() ?
                          (float) defaultValue.getValueAsDouble() :
                          null),
                         null, null);
            }
            break;
        case ENUM:
            List<String> symbols = schema.getEnumSymbols();
            String[] enumValues = new String[symbols.size()];
            for (int i = 0; i < enumValues.length; i++) {
                enumValues[i] = symbols.get(i);
            }
            if (isCollectionBuilder()) {
                addEnum(name, enumValues, null);
            } else {
                addEnum(name, enumValues, desc,
                        isUnion, /* nullable */
                        (defaultValue != null && !defaultValue.isNull() ?
                         defaultValue.getValueAsText() : null));
            }
            break;
        case INT:
            if (isCollectionBuilder()) {
                addInteger();
            } else {
                addInteger(name, desc,
                           isUnion, /* nullable */
                           (defaultValue != null && !defaultValue.isNull() ?
                            defaultValue.getIntValue() : null),
                           null, null);
            }
            break;
        case LONG:
            if (isCollectionBuilder()) {
                addLong();
            } else {
                addLong(name, desc,
                        isUnion, /* nullable */
                        (defaultValue != null && !defaultValue.isNull() ?
                         defaultValue.getLongValue() : null),
                        null, null);
            }
            break;
        case ARRAY:
            ArrayDefImpl arrayDef = (ArrayDefImpl)
                TableBuilder.createArrayBuilder(desc)
                .generateAvroSchemaFields(schema,
                                          null, /* name */
                                          null, /* default */
                                          desc).build();
            if (isCollectionBuilder()) {
                addField(arrayDef);
            } else {
                FieldValueImpl defaultVal = arrayDef.createValue(defaultValue);
                addField(name, makeFieldMapEntry(arrayDef, isUnion, defaultVal));
            }
            break;
        case MAP:
            MapDefImpl mapDef = (MapDefImpl)
                TableBuilder.createMapBuilder(desc)
                .generateAvroSchemaFields(schema,
                                          null, /* name */
                                          null, /* default */
                                          desc).build();
            if (isCollectionBuilder()) {
                addField(mapDef);
            } else {
                FieldValueImpl defaultVal = mapDef.createValue(defaultValue);
                addField(name, makeFieldMapEntry(mapDef, isUnion, defaultVal));
            }
            break;
        case RECORD:
            RecordDefImpl recordDef = (RecordDefImpl)
                TableBuilder.createRecordBuilder(schema.getName(),
                                                 desc)
                .generateAvroSchemaFields(schema,
                                          null, /* name */
                                          null, /* default */
                                          desc).build();
            if (isCollectionBuilder()) {
                addField(recordDef);
            } else {
                FieldValueImpl defaultVal =
                    recordDef.createValue(defaultValue);
                addField(name, makeFieldMapEntry(recordDef, isUnion,
                                                 defaultVal));
            }
            break;
        case STRING:
            if (isCollectionBuilder()) {
                addString();
            } else {
                addString(name, desc,
                          isUnion, /* nullable */
                          (defaultValue != null && !defaultValue.isNull() ?
                           defaultValue.getTextValue() : null),
                          null, null, null, null);
            }
            break;
        case UNION:
            unionNotAllowed(isUnion, Schema.Type.UNION);
            handleUnion(schema, name, defaultValue, desc);
            break;
        case NULL:
            throw new IllegalArgumentException
                ("Unsupported Avro type: " + ftype);
        default:
            throw new IllegalStateException
                ("Unknown type: " + ftype);
        }
        return this;
    }

    /*
     * Unions are handled under constrained conditions:
     * 1.  there may be only 2 schemas in the union
     * 2.  one of the schemas must be "null"
     * 3.  the non-null schema must be a simple type such as integer or string
     * Under these conditions they become a single nullable field in the table.
     *
     * Avro is a bit finicky about unions and default values.  The default value
     * must be the first member of the union (see comments in FieldDefImpl).
     */
    private void handleUnion(Schema schema,
                             String name,
                             JsonNode defaultValue,
                             String desc) {
        List<Schema> unionSchemas = schema.getTypes();
        if (unionSchemas.size() != 2) {
            throw new IllegalArgumentException
                ("Avro unions must contain only 2 members");
        }
        boolean foundNull = false;
        Schema nonNullSchema = null;
        for (Schema s : unionSchemas) {
            if (s.getType() == Schema.Type.NULL) {
                foundNull = true;
            } else {
                nonNullSchema = s;
            }
        }
        if (!foundNull) {
            throw new IllegalArgumentException
                ("Avro union must include null");
        }
        generateAvroSchemaFields(nonNullSchema,
                                 name,
                                 defaultValue,
                                 desc,
                                 true);
    }

    private void unionNotAllowed(boolean isUnion, Schema.Type type) {
        if (isUnion) {
            throw new IllegalArgumentException
                ("Avro union with type is not supported: " + type);
        }
    }

    private FieldMapEntry makeFieldMapEntry(FieldDefImpl def,
                                            Boolean nullable,
                                            FieldValueImpl defaultValue) {
        return new FieldMapEntry(def, (nullable != null ? nullable : true),
                                 defaultValue);
    }

    void fromJson(String fieldName, ObjectNode node) {
        JsonNode defaultNode = node.get(DEFAULT);
        Boolean nullable = JsonUtils.getBooleanFromJson(node, NULLABLE);

        FieldDefImpl def = JsonUtils.fromJson(node);

        /*
         * Default node of "null" and no default are equivalent.
         */
        FieldValueImpl value = (defaultNode == null || defaultNode.isNull()) ?
            null : def.createValue(defaultNode);
        addField(fieldName, makeFieldMapEntry(def, nullable, value));
    }
}
