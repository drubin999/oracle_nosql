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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import oracle.kv.table.FieldDef;

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

/**
 * This is a utility class to aid in interaction with Jackson JSON
 * processing libraries as well as helpful JSON operations.
 */
public class JsonUtils {

    /*
     * There are a number of string constants used for JSON input/output
     * as well as for generating proper Avro schema names.  These are all
     * defined here for simplicity.
     */

    /*
     * Tables JSON input/output format labels
     */
    final static String DESC = "description";
    final static String NULLABLE = "nullable";
    final static String MIN = "min";
    final static String MAX = "max";
    final static String MIN_INCL = "min_inclusive";
    final static String MAX_INCL = "max_inclusive";
    final static String COLLECTION = "collection";
    final static String ENUM_NAME = "enum_name";

    /*
     * These are used for construction of JSON nodes representing FieldDef
     * instances.  Some are used for both tables and Avro schemas.
     *
     * Avro and Tables
     */
    final static String NAME = "name";
    final static String TYPE = "type";
    final static String DEFAULT = "default";
    final static String ENUM_VALS = "symbols";
    final static String FIELDS = "fields";
    final static String NULL = "null";

    /*
     * Avro type strings
     */
    final static String RECORD = "record";
    final static String ENUM = "enum";
    final static String ARRAY = "array";
    final static String MAP = "map";
    final static String INT = "int";
    final static String LONG = "long";
    final static String STRING = "string";
    final static String BOOLEAN = "boolean";
    final static String DOUBLE = "double";
    final static String FLOAT = "float";
    final static String BYTES = "bytes";
    final static String FIXED = "fixed";
    final static String FIXED_SIZE = "size";

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final DecoderFactory decoderFactory =
        DecoderFactory.get();
    private static final EncoderFactory encoderFactory =
        EncoderFactory.get();

    public static ObjectMapper getObjectMapper() {
        return mapper;
    }

    static ObjectNode createObjectNode() {
        return mapper.createObjectNode();
    }

    public static ObjectWriter createWriter(boolean pretty) {
        /* 1.9  om.writerWithDefaultPrettyPrinter() : */
        return (pretty ? mapper.defaultPrettyPrintingWriter()
                : mapper.writer());
    }

    static JsonFactory getJsonFactory() {
        return mapper.getJsonFactory();
    }

    static JsonParser createJsonParser(InputStream in)
        throws IOException {

        return mapper.getJsonFactory().createJsonParser(in);
    }

    static DecoderFactory getDecoderFactory() {
        return decoderFactory;
    }

    static EncoderFactory getEncoderFactory() {
        return encoderFactory;
    }

    /**
     * Translate the specified Base64 string into a byte array.
     */
    public static String encodeBase64(byte[] buf) {
        return mapper.convertValue(buf, String.class);
    }

    /**
     * Decode the specified Base64 string into a byte array.
     */
    public static byte[] decodeBase64(String str) {
        return mapper.convertValue(str, byte[].class);
    }

    /*
     * From here down are utility methods used to help construct tables
     * and fields from JSON.
     */

    /**
     * This is a generic method used to construct FieldDef objects from
     * the JSON representation of a table.  This code could be spread out
     * across the various classes in per-class fromJson() methods but there
     * is no particular advantage in doing so.  Code is better shared in one
     */
    static FieldDefImpl fromJson(ObjectNode node) {
        String nameString = getStringFromNode(node, NAME, false);
        String descString = getStringFromNode(node, DESC, false);
        String minString = getStringFromNode(node, MIN, false);
        String maxString = getStringFromNode(node, MAX, false);
        String sizeString = getStringFromNode(node, FIXED_SIZE, false);
        String typeString = getStringFromNode(node, TYPE, true);

        FieldDef.Type type = FieldDef.Type.valueOf(typeString);
        switch (type) {
        case INTEGER:
            return new IntegerDefImpl
                (descString,
                 minString != null ? Integer.valueOf(minString) : null,
                 maxString != null ? Integer.valueOf(maxString) : null);
        case LONG:
            return new LongDefImpl
                (descString,
                 minString != null ? Long.valueOf(minString) : null,
                 maxString != null ? Long.valueOf(maxString) : null);
        case DOUBLE:
            return new DoubleDefImpl
                (descString,
                 minString != null ? Double.valueOf(minString) : null,
                 maxString != null ? Double.valueOf(maxString) : null);
        case FLOAT:
            return new FloatDefImpl
                (descString,
                 minString != null ? Float.valueOf(minString) : null,
                 maxString != null ? Float.valueOf(maxString) : null);
        case STRING:
            Boolean minInclusive = getBooleanFromJson(node, MIN_INCL);
            Boolean maxInclusive = getBooleanFromJson(node, MAX_INCL);
            return new StringDefImpl
                (descString, minString, maxString, minInclusive, maxInclusive);
        case BINARY:
            return new BinaryDefImpl(descString);
        case FIXED_BINARY:
            int size = (sizeString == null ? 0 : Integer.valueOf(sizeString));
            return new FixedBinaryDefImpl(nameString, size, descString);
        case BOOLEAN:
            return new BooleanDefImpl(descString);
        case ARRAY:
        case MAP:
            JsonNode jnode = node.get(COLLECTION);
            if (jnode == null) {
                throw new IllegalArgumentException
                    ("Map and Array require a collection object");
            }
            FieldDefImpl elementDef = fromJson((ObjectNode) jnode);
            if (type == FieldDef.Type.ARRAY) {
                return new ArrayDefImpl(elementDef, descString);
            }
            return new MapDefImpl(elementDef, descString);
        case RECORD:
            JsonNode fieldsNode = node.get(FIELDS);
            if (fieldsNode == null) {
                throw new IllegalArgumentException
                    ("Record is missing fields object");
            }
            final RecordBuilder builder =
                TableBuilder.createRecordBuilder(nameString, descString);
            ArrayNode arrayNode = (ArrayNode) fieldsNode;
            for (int i = 0; i < arrayNode.size(); i++) {
                ObjectNode o = (ObjectNode) arrayNode.get(i);
                String fieldName = getStringFromNode(o, NAME, true);
                builder.fromJson(fieldName, o);
            }
            try {
                return (FieldDefImpl) builder.build();
            } catch (Exception e) {
                throw new IllegalArgumentException
                ("Failed to build record from JSON, field name: " + nameString );
            }
        case ENUM: {
            JsonNode valuesNode = node.get(ENUM_VALS);
            if (valuesNode == null) {
                throw new IllegalArgumentException
                    ("Enumeration is missing values");
            }
            arrayNode = (ArrayNode) valuesNode;
            String enumName = getStringFromNode(node, ENUM_NAME, true);
            String values[] = new String[arrayNode.size()];
            for (int i = 0; i < arrayNode.size(); i++) {
                values[i] = arrayNode.get(i).getValueAsText();
            }
            return new EnumDefImpl(enumName, values, descString);
        }
        }
        throw new IllegalArgumentException
            ("Cannot construct FieldDef type from JSON: " + type);
    }

    /**
     * Adds an index definition from the ObjectNode (JSON) to the table.
     */
    static void indexFromJsonNode(ObjectNode node, TableImpl table) {
        ArrayNode fields = (ArrayNode) node.get("fields");
        ArrayList<String> fieldStrings = new ArrayList<String>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            fieldStrings.add(fields.get(i).getValueAsText());
        }
        String name = getStringFromNode(node, "name", true);
        String desc = getStringFromNode(node, "description", false);
        table.addIndex(new IndexImpl(name, table, fieldStrings, desc));
    }

    /**
     * Build Table from JSON string
     *
     * NOTE: as of R3 this is used only for test purposes, along with the
     * methods it calls.  In the future there may be public methods to
     * construct tables from JSON.
     */
    protected static TableImpl fromJsonString(String jsonString,
                                              TableImpl parent) {
        JsonNode rootNode = null;
        try {
            rootNode = JsonUtils.getObjectMapper().readTree(jsonString);
        } catch (IOException ioe) {
            throw new IllegalArgumentException("IOException parsing Json: " +
                                               ioe);
        }

        /*
         * Create a TableBuilder for the table.
         */
        TableBuilder tb =
            TableBuilder.createTableBuilder
            (rootNode.get("name").getValueAsText(),
             rootNode.get("description").getValueAsText(),
             parent, true);

        /*
         * Create the primary key and shard key lists
         */
        tb.primaryKey(makeListFromArray(rootNode, "primaryKey"));
        if (parent == null) {
            tb.shardKey(makeListFromArray(rootNode, "shardKey"));
        }

        if (rootNode.get("r2compat") != null) {
            tb.setR2compat(true);
        }

        /*
         * Add fields.
         */
        ArrayNode arrayNode = (ArrayNode) rootNode.get("fields");
        for (int i = 0; i < arrayNode.size(); i++) {
            ObjectNode node = (ObjectNode) arrayNode.get(i);
            String fieldName =
                getStringFromNode(node, "name", true);
            if (parent == null ||
                !(parent).isKeyComponent(fieldName)) {
                tb.fromJson(fieldName, node);
            }
        }

        TableImpl newTable = tb.buildTable();
        /*
         * Add indexes if present
         */
        if (rootNode.get("indexes") != null) {
            arrayNode = (ArrayNode) rootNode.get("indexes");
            for (int i = 0; i < arrayNode.size(); i++) {
                ObjectNode node = (ObjectNode) arrayNode.get(i);
                indexFromJsonNode(node, newTable);
            }
        }
        return newTable;
    }

    private static List<String> makeListFromArray(JsonNode node,
                                                  String fieldName) {
        ArrayNode arrayNode = (ArrayNode) node.get(fieldName);
        ArrayList<String> keyList = new ArrayList<String>(arrayNode.size());
        for (int i = 0; i < arrayNode.size(); i++) {
            keyList.add(i, arrayNode.get(i).getValueAsText());
        }
        return keyList;
    }

    /**
     * Returns the string value of the named field in the ObjectNode
     * if it exists, otherwise null.
     * @param node the containing node
     * @param name the name of the field in the node
     * @param required true if the field must exist
     * @return the string value of the field, or null
     * @throws IllegalArgumentException if the named field does not
     * exist in the node and required is true
     */
    private static String getStringFromNode(ObjectNode node,
                                            String name,
                                            boolean required) {
        JsonNode jnode = node.get(name);
        if (jnode != null) {
            return jnode.getValueAsText();
        } else if (required) {
            throw new IllegalArgumentException
                ("Missing required node in JSON table representation: " + name);
        }
        return null;
    }

    /**
     * Returns the Boolean value of the named field in the ObjectNode
     * if it exists, otherwise null.  The field must be a boolean
     * field.
     *
     * @param node the containing node
     * @param name the name of the field in the node
     * @return the Boolean value of the field, or null
     */
    static Boolean getBooleanFromJson(ObjectNode node, String name) {
        JsonNode jnode = node.get(name);
        if (jnode != null && jnode.isBoolean()) {
            return jnode.getBooleanValue();
        }
        return null;
    }
}
