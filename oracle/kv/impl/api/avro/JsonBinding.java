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

package oracle.kv.impl.api.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import oracle.kv.Value;
import oracle.kv.avro.JsonAvroBinding;
import oracle.kv.avro.JsonRecord;
import oracle.kv.avro.RawAvroBinding;
import oracle.kv.avro.RawRecord;
import oracle.kv.avro.SchemaNotAllowedException;
import oracle.kv.avro.UndefinedSchemaException;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.UnresolvedUnionException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.ResolvingDecoder;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

/**
 * Implements our JSON binding API by subclassing the built-in Avro generic
 * classes to translate JsonNode objects to Avro format and back again.
 * Although it may appear that Avro includes built-in support for JSON and the
 * Jackson API (JsonNode), in fact it only provides the following:
 * <ul>
 *   <li>
 *   The org.apache.avro.data.Json class provides a serialization mechanism for
 *   JSON via the Jackson API (JsonNode), but it is not a standard Avro
 *   serialization.  It is a self-describing format, isn't compatible with the
 *   standard format used for generic and specific bindings, and does not
 *   support schema evolution.  For all three reasons, it is not appropriate
 *   for our API.
 *   </li>
 *   <li>
 *   The org.apache.avro.io.JsonEncoder allows serializing Avro data as JSON
 *   text, and the JsonDecoder deserializes JSON text back to Avro data.  The
 *   JSON text format represents Avro data types according to the rules in the
 *   Avro spec; for example, 'bytes' and 'fixed' types are represented as
 *   Strings with Unicode escape syntax and 'union' has a special format
 *   defined in the spec.
 *   </li>
 *   <li>
 *   GenericRecord.toString returns JSON text for debugging purposes, as if it
 *   were written by a JsonEncoder.
 * </ul>
 * <p>
 * In our JsonBinding, we map the standard Avro serialization format to
 * JsonNode, and we follow the same rules as defined above for the JSON text
 * encoding.  Schema evolution is also supported.
 * <p>
 * Our JsonBinding.toValue method is the equivalent of serializing a JSON
 * object as JSON text, then deserializing it using a generic binding and a
 * JsonDecoder, then serializing it again as Avro binary data.  Of course, this
 * implementation was not used because it would be very slow compared to the
 * approach we've taken.
 * <p>
 * WARNING:  To implement the mapping to JsonNode we subclass GenericData,
 * GenericDatumReader and GenericDatumWriter.  Although these Avro classes are
 * intended to be subclassed in this way, the subclassing can be fragile if the
 * overriden methods are changed in Avro from version to version, and we don't
 * have control over that.
 * <p>
 * NOTE: Because the Avro javadoc and comments are extremely sparse, it is
 * important to document the implementation of this class in more detail than
 * would otherwise be necessary.
 * <p>
 * <strong>Avro Schema Validation</strong>
 * <p>
 * One might be led to believe that because Avro has schemas, it has a full
 * featured schema validation facility that would, for example, validate a JSON
 * object and give meaningful error messages.  The reality is different.
 * <p>
 * Avro schemas do not have constraints that some might expect, for example,
 * there are no numeric range constraints.  The purpose of the schema is really
 * for serialization, not validation, so any validation features are simply a
 * side effect of the need for proper serialization.
 * <p>
 * Avro does guarantee that serialized data conforms to its associated schema.
 * However, it does not always output meaningful error messages and does not
 * have the schema validation features that some might expect.
 * <p>
 * On error messages, when Avro rejects an object during serializtion it is
 * possible to figure out from the exception messages what is wrong with the
 * object.  However, this is not as easy as some might like it to be.  In at
 * least some cases, the field name is not included in the error message.
 * <p>
 * In addition, Avro sometimes simply coerces the data to conform to the schema
 * rather than give an error message.  A 'fixed' binary array that is longer
 * than allowed by the schema is simply truncated to the required length; in
 * this case we have subclassed the datum writer in order to report a
 * meaningful error message.  With Avro 1.6, when using the 'int' type, a
 * number that has more information than can be contained in an int (a float,
 * double or long) is simply truncated to an int using Number.intValue; in this
 * case we have also subclassed the datum writer to report an error.
 * <p>
 * This section applies to the generic binding as well as the JSON binding.  It
 * also applies to the specific binding; however, with a specific binding some
 * constraints are enforced by Java itself, since the generated classes have
 * setters with specific data types.  For example, an 'int' field may not
 * contain a float, double or long value, because the setter param is Java type
 * 'int'.
 * <p>
 * In our implementation of the JSON binding, for sake of consistency we do not
 * perform any more validation than is performed by the built-in generic
 * generic binding.  If we were to add more validation in the future (by
 * subclassing) we should do so for all types of bindings.
 * <p>
 * Note that Avro does include some addition validation facilities that we are
 * not currently using:
 * <ul>
 *   <li>
 *   The ValidatingEncoder and ValidatingDecoder classes check for two things:
 *   they disallow byte arrays of the wrong length for the 'fixed' type, and
 *   they disallow illegal enum indices.  For the 'fixed' type, we have
 *   implemented validation in our subclasses. For enum indices, this check
 *   seems redundant since it is also checked during serialization.  Because
 *   these classes have additional overhead -- they traverse the schema in
 *   parallel with the data -- and have very little benefit, they're not
 *   currently used.
 *   </li>
 *   <li>
 *   GenericData.validate does validation without doing serialization.  This
 *   method returns a boolean rather than throwing an exception, so when it
 *   fails there is no information returned about what is wrong.  Therefore,
 *   this method isn't currently used here and probably won't be useful in the
 *   future.  Even if it were used, it would only work for the generic binding
 *   and would need to be rewritten for the JSON binding.
 *   </li>
 * </ul>
 */
class JsonBinding implements JsonAvroBinding {

    /**
     * A raw binding is used for packaging and unpackaging the Avro raw data
     * bytes and their associated writer schema.
     */
    private final RawAvroBinding rawBinding;

    /**
     * The allowed schemas as used to throw SchemaNotAllowedException when an
     * attempt is made to use this binding with a different schema, and is also
     * used to determine the reader schema.
     */
    private final Map<String, Schema> allowedSchemas;

    JsonBinding(AvroCatalogImpl catalog, Map<String, Schema> allowedSchemas)
        throws UndefinedSchemaException {

        this.rawBinding = catalog.getRawBinding();
        this.allowedSchemas = new HashMap<String, Schema>(allowedSchemas);
        /* May throw UndefinedSchemaException. */
        catalog.checkDefinedSchemas(allowedSchemas);
    }

    /**
     * Straightforward deserialization to JsonRecord using RawBinding,
     * BinaryDecoder and JsonDatumReader (our subclass of GenericDatumReader).
     */
    @Override
    public JsonRecord toObject(Value value)
        throws SchemaNotAllowedException, IllegalArgumentException {

        final RawRecord raw = rawBinding.toObject(value);
        final Schema writerSchema = raw.getSchema();
        /* May throw SchemaNotAllowedException. */
        final Schema readerSchema =
            AvroCatalogImpl.checkToObjectSchema(writerSchema, allowedSchemas);

        final JsonDatumReader reader =
            new JsonDatumReader(writerSchema, readerSchema);
        final Decoder decoder =
            DecoderFactory.get().binaryDecoder(raw.getRawData(), null);

        final JsonNode node;
        try {
            node = reader.read(null, decoder);
        } catch (Exception e) {
            throw new IllegalArgumentException
                ("Unable to deserialize JsonNode", e);
        }

        return new JsonRecord(node, readerSchema);
    }

    /**
     * Straightforward serialization of JsonRecord using RawBinding,
     * BinaryEncoder and JsonDatumWriter (our subclass of GenericDatumWriter).
     */
    @Override
    public Value toValue(JsonRecord object)
        throws SchemaNotAllowedException, UndefinedSchemaException,
               IllegalArgumentException {

        final Schema writerSchema = object.getSchema();
        /* May throw SchemaNotAllowedException. */
        AvroCatalogImpl.checkToValueSchema(writerSchema, allowedSchemas);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final JsonDatumWriter writer = new JsonDatumWriter(writerSchema);
        final Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);

        try {
            writer.write(object.getJsonNode(), encoder);
            encoder.flush();
        } catch (Exception e) {
            throw new IllegalArgumentException
                ("Unable to serialize JsonNode", e);
        }

        final RawRecord raw = new RawRecord(out.toByteArray(), writerSchema);
        /* May throw UndefinedSchemaException. */
        return rawBinding.toValue(raw);
    }

    /**
     * Subclass of GenericData, a singleton object which is used by
     * GenericDatumReader and GenericDatumWriter in certain cases to access the
     * deserialized representation.  By default (in GenericData) the
     * deserialized representation is GenericRecord, of course.  We override
     * such methods to use JsonNode instead.
     * <p>
     * Note that some methods that access the deserialized representation are
     * in GenericData, but others are in GenericDatumReader and
     * GenericDatumWriter.  There doesn't seem to be a rule about which access
     * methods are in GenericData versus the others.
     */
    private static class JsonData extends GenericData {

        /** Singleton. */
        static final JsonData INSTANCE = new JsonData();

        /** Only the singleton is allowed. */
        private JsonData() {
        }

        /**
         * Not called by GenericDatumReader or GenericDatumWriter, but we've
         * implemented it anyway.  Might be used by Avro tools.
         * <p>
         * This method was added in Avro 1.6.x.
         */
        @Override
        public JsonDatumReader createDatumReader(Schema schema) {
            return new JsonDatumReader(schema, schema);
        }

        /**
         * Called by GenericDatumReader to create deserialized form of Avro
         * 'record' type.  We map the 'record' type to the Jackson ObjectNode.
         * <p>
         * This method was added in Avro 1.6.x.
         */
        @Override
        public Object newRecord(Object old, Schema schema) {
            return JsonNodeFactory.instance.objectNode();
        }

        /**
         * Called by GenericDatumReader during deserialization of Avro fields
         * for the 'record' type.  Converts the data format from what was
         * returned by GenericDatumWriter.read to JsonNode.
         * <p>
         * There is another setField method with an extra state param.  We
         * could use the state param if necessary to hold schema information,
         * but so far that hasn't been necessary.  The default version of that
         * method simply calls this one.  To use the state param in the future,
         * also override getRecordState.
         */
        @Override
        public void setField(Object r, String name, int pos, Object value) {
            final ObjectNode parent = (ObjectNode) r;
            final JsonNode child = genericToJson(value);
            parent.put(name, child);
        }

        /**
         * Called by GenericDatumWriter during serialization of Avro fields for
         * the 'record' type.
         * <p>
         * Called by GenericDatumReader to get field values for reuse during
         * deserialization, but this never happens because we don't reuse
         * JsonNode objects, i.e., we pass null for the 'old' param of
         * JsonDatumReader.read.  Called by GenericData in a couple other cases
         * (validate and hashCode) that don't apply here.
         * <p>
         * Like setField this method has a signature with an extra state param.
         * See setField comments.
         *
         * @return null if the field is not present in the JSON object.
         */
        @Override
        public Object getField(Object r, String name, int pos) {
            return ((JsonNode) r).get(name);
        }

        /**
         * Never called, because our reader doesn't call it.
         * <p>
         * This method was added in Avro 1.6.x.
         */
        @Override
        public Object createFixed(Object old, Schema schema) {
            throw new UnsupportedOperationException();
        }

        /**
         * Called by our JsonDatumReader.readFixed during deserialization of
         * the Avro 'fixed' type.  Uses the JSON encoding for this type defined
         * by the Avro spec.
         * <p>
         * Called by GenericData in a couple other cases (induce and deepCopy)
         * that don't apply here.
         * <p>
         * This method doesn't necessarily have to be overridden here (it could
         * be implemented by JsonDatumReader), but we done it this way for
         * consistency with the Avro implementation.
         * <p>
         * This method was added in Avro 1.6.x.
         */
        @Override
        public Object createFixed(Object old, byte[] bytes, Schema schema) {
            return bytesToString(bytes, 0, schema.getFixedSize());
        }

        /**
         * Called by our JsonDatumWriter.write during serialization of the
         * Avro 'union' type.  Uses the JSON encoding for this type defined by
         * the Avro spec.
         * <p>
         * Called by GenericData in a few other cases that don't apply here
         * (hashCode, compare and deepCopy).
         * <p>
         * This method doesn't necessarily have to be overridden here (it could
         * be implemented by JsonDatumWriter), but we done it this way for
         * consistency with the Avro implementation.
         */
        @Override
        public int resolveUnion(Schema unionSchema, Object datum) {
            final JsonNode node = (JsonNode) datum;

            /*
             * Compute the implied Avro schema name for the datum.  According
             * to the Avro spec, the null type is represented simply as a JSON
             * null.  And for all other types a JSON object with a single
             * property is used, where the propery name is the Avro type and
             * the property value is the datum.
             */
            final String schemaName;
            if (node.isNull()) {
                schemaName = "null";
            } else {
                if (!(node.isObject())) {
                    throw new UnresolvedUnionException(unionSchema, datum);
                }
                final Iterator<String> names = node.getFieldNames();
                if (!names.hasNext()) {
                    throw new UnresolvedUnionException(unionSchema, datum);
                }
                schemaName = names.next();
                if (names.hasNext()) {
                    throw new UnresolvedUnionException(unionSchema, datum);
                }
            }

            /*
             * With the datum schema name we can use the union schema to find
             * the index of the datum's type.
             */
            final Integer index = unionSchema.getIndexNamed(schemaName);
            if (index == null) {
                throw new UnresolvedUnionException(unionSchema, datum);
            }
            return index;
        }
    }

    /**
     * Subclass of GenericDatumWriter for overriding the deserialized
     * representation: use JsonNode rather than GenericRecord.  See JsonData
     * for general information.
     */
    static class JsonDatumWriter extends GenericDatumWriter<JsonNode> {

        private final boolean applyDefaultValues;

        JsonDatumWriter(Schema schema) {
            this(schema, false);
        }
      
        /**
         * @param applyDefaultValues if true, default field values are used for
         * missing fields during serialization, and the first type in a union
         * is used.  This is used by SchemaChekcer to validate default values
         * in the schema. It is NOT used for serialization of user data, since
         * that would violate the Avro spec.
         */
        JsonDatumWriter(Schema schema, boolean applyDefaultValues) {
            /** Always use the JsonData singleton. */
            super(schema, JsonData.INSTANCE); 
            this.applyDefaultValues = applyDefaultValues;
        }

        /**
         * Called to serialize the Avro 'fixed' type.  Uses the JSON encoding
         * for this type defined by the Avro spec.  Uses the
         * GenericBinding.writeFixed utility method to perform validation.
         */
        @Override
        protected void writeFixed(Schema schema, Object datum, Encoder out)
            throws IOException {

            final byte[] bytes = stringToBytes(schema, (String) datum,
                                               true /*isFixedType*/);
            GenericBinding.writeFixed(schema, bytes, out);
        }

        /**
         * Called as part of serializing the Avro 'array' type.
         */
        @Override
        protected long getArraySize(Object array) {
            return ((ArrayNode) array).size();
        }

        /**
         * Called as part of serializing the Avro 'array' type.
         */
        @Override
        protected Iterator<? extends Object> getArrayElements(Object array) {
            return ((ArrayNode) array).iterator();
        }

        /**
         * Called as part of serializing the Avro 'map' type.
         */
        @Override
        protected int getMapSize(Object map) {
            return ((ObjectNode) map).size();
        }

        /**
         * Called as part of serializing the Avro 'map' type.
         */
        @Override
        protected Iterable<Map.Entry<Object, Object>>
            getMapEntries(Object map) {

            final ObjectNode objectNode = (ObjectNode) map;
            return new Iterable<Map.Entry<Object, Object>>() {

                @Override
                public Iterator<Map.Entry<Object, Object>> iterator() {

                    /*
                     * Although getFields returns Iterator<Map.Entry<String,
                     * JsonNode>> we must implement the Iterator ourselves
                     * because Java doesn't allow using a subtype of a generic
                     * type unless the required type is <?> or <? extends X>,
                     * and the Avro method isn't declared that way.  This
                     * wouldn't be necessary if the Avro getMapEntries method
                     * was declared to return type Iterable<Map.Entry<?, ?>>.
                     */
                    final Iterator<Map.Entry<String, JsonNode>> iter =
                        objectNode.getFields();

                    return new Iterator<Map.Entry<Object, Object>>() {

                        @Override
                        public boolean hasNext() {
                            return iter.hasNext();
                        }

                        @Override
                        public Map.Entry<Object, Object> next() {
                            final Map.Entry<String, JsonNode> entry =
                                iter.next();
                            return new SimpleEntry<Object, Object>
                                (entry.getKey(), entry.getValue());
                        }

                        @Override
                        public void remove() {
                            throw new UnsupportedOperationException();
                        }
                    };
                }
            };
        }

        /**
         * We override the primary write method to handle certain data types
         * specially.
         * <p>
         * The pattern in the Avro implementation is that, for types that are
         * treated specially in subclasses, specific methods are defined that
         * can be overridden: readFixed/writeFixed, readBytes/writeBytes, the
         * methods for enums, arrays and maps, etc.  Avro does not do this for
         * the union type (it must not have come up as an issue in subclasses
         * yet), so we are forced to override the primary write method instead.
         * The same thing is true of the 'int' type.  And although there is a
         * writeBytes method, it isn't passed the schema.
         */
        @Override
        protected void write(Schema schema, Object datum, Encoder out)
            throws IOException {

            final JsonNode node = (JsonNode) datum;
            final Schema.Type type = schema.getType();

            switch (type) {
            case RECORD:
                if (!node.isObject()) {
                    throw jsonTypeError(type, node);
                }
                writeRecord(schema, node, out);
                break;
            case ENUM:
                if (!node.isTextual()) {
                    throw jsonTypeError(type, node);
                }
                writeEnum(schema, node.getTextValue(), out);
                break;
            case ARRAY:
                if (!node.isArray()) {
                    throw jsonTypeError(type, node);
                }
                writeArray(schema, node, out);
                break;
            case MAP:
                if (!node.isObject()) {
                    throw jsonTypeError(type, node);
                }
                writeMap(schema, node, out);
                break;
            case UNION:

                /* When applying default values, use first type in union. */
                if (applyDefaultValues) {
                    write(schema.getTypes().get(0), datum, out);
                    break;
                }

                /*
                 * In a standard union, the null type is represented simply as
                 * a JSON null; for all other types a JSON object with a single
                 * property is used, where the propery name is the Avro type
                 * and the property value is the datum.
                 */
                final int index = resolveUnion(schema, node);
                final Schema datumSchema = schema.getTypes().get(index);
                if (!node.isNull()) {
                    datum = getData().getField
                        (node, datumSchema.getFullName(), 0);
                    if (datum == null) {
                        throw new RuntimeException
                            ("Unexpected missing union field: " +
                             datumSchema.getFullName() + " union: " +
                             schema.getFullName() + " value: " + node);
                    }
                }
                out.writeIndex(index);
                write(datumSchema, datum, out);
                break;
            case FIXED:
                if (!node.isTextual()) {
                    throw jsonTypeError(type, node);
                }
                writeFixed(schema, node.getTextValue(), out);
                break;
            case STRING:
                if (!node.isTextual()) {
                    throw jsonTypeError(type, node);
                }
                writeString(schema, node.getTextValue(), out);
                break;
            case BYTES:
                if (!node.isTextual()) {
                    throw jsonTypeError(type, node);
                }
                /* Use JSON encoding for 'bytes' defined by Avro spec. */
                out.writeBytes(stringToBytes(schema, node.getTextValue(),
                               false /*isFixedType*/));
                break;
            case INT:
                if (!node.isInt() && !node.isLong()) {
                    throw jsonTypeError(type, node);
                }
                final int intVal = node.getIntValue();
                if (intVal != node.getLongValue()) {
                    throw jsonTypeError(type, node);
                }
                out.writeInt(intVal);
                break;
            case LONG:
                if (!node.isInt() && !node.isLong()) {
                    throw jsonTypeError(type, node);
                }
                out.writeLong(node.getLongValue());
                break;
            case FLOAT:
                if (!node.isNumber()) {
                    throw jsonTypeError(type, node);
                }
                out.writeFloat((float) node.getDoubleValue());
                break;
            case DOUBLE:
                if (!node.isNumber()) {
                    throw jsonTypeError(type, node);
                }
                out.writeDouble(node.getDoubleValue());
                break;
            case BOOLEAN:
                if (!node.isBoolean()) {
                    throw jsonTypeError(type, node);
                }
                out.writeBoolean(node.getBooleanValue());
                break;
            case NULL:
                if (!node.isNull()) {
                    throw jsonTypeError(type, node);
                }
                out.writeNull();
                break;
            default:
                /* Should never happen. */
                throw new RuntimeException("Unknown type: " + type);
            }
        }

        @Override
        protected void writeRecord(Schema schema, Object datum, Encoder out)
            throws IOException {

            for (final Schema.Field f : schema.getFields()) {
                Object value = getData().getField(datum, f.name(), f.pos());
                /* Apply default value to a missing field. */
                if (value == null && applyDefaultValues) {
                    value = f.defaultValue();
                }
                /* Use NullNode for missing fields as a convenience. */
                if (value == null) {
                    value = JsonNodeFactory.instance.nullNode();
                }
                try {
                    write(f.schema(), value, out);
                } catch (RuntimeException e) {
                    throw wrapTypeError(e, " in field " + f.name() +
                                           " of " + schema.getFullName());
                }
            }
        }
    }

    private static AvroTypeException wrapTypeError(RuntimeException cause,
                                                   String addMsg) {
        return new AvroTypeException
            (cause.getMessage() + addMsg,
             (cause.getCause() != null) ? cause.getCause() : cause);
    }

    private static AvroTypeException jsonTypeError(Schema.Type type,
                                                   JsonNode node) {
        return new AvroTypeException("Expected Avro type " + type +
                                     " but got JSON value: " + node);
    }

    /**
     * Subclass of GenericDatumReader for overriding the deserialized
     * representation: use JsonNode rather than GenericRecord.  See JsonData
     * for general information.
     */
    private static class JsonDatumReader extends GenericDatumReader<JsonNode> {

        /** Always use the JsonData singleton. */
        JsonDatumReader(Schema writer, Schema reader) {
            super(writer, reader, JsonData.INSTANCE);
        }

        /**
         * Called to deserialize the Avro 'bytes' type.  Uses the JSON encoding
         * for this type defined by the Avro spec.
         */
        @Override
        protected Object readBytes(Object old, Decoder in)
            throws IOException {

            final ByteBuffer buf = in.readBytes(null);
            return bytesToString(buf.array(), buf.arrayOffset(),
                                 buf.remaining());
        }

        /**
         * Never called by the base class or elsewhere, but we've implemented
         * it anyway.
         */
        @Override
        protected Object createBytes(byte[] value) {
            return bytesToString(value, 0, value.length);
        }

        /**
         * Called to deserialize the Avro 'fixed' type.  Uses the JSON encoding
         * for this type defined by the Avro spec.
         */
        @Override
        protected Object readFixed(Object old, Schema expected, Decoder in)
            throws IOException {

            final byte[] b = new byte[expected.getFixedSize()];
            in.readFixed(b, 0, b.length);
            return JsonData.INSTANCE.createFixed(old, b, expected);
        }

        /**
         * Called during deserialization of the Avro 'enum' type.  Since the
         * JSON encoding for this type defined by the Avro spec is the symbol
         * String, we can simply return it here.  There is no need to override
         * readEnum or writeEnum.
         */
        @Override
        protected Object createEnum(String symbol, Schema schema) {
            return symbol;
        }

        /**
         * Called as part of deserializing the Avro 'array' type.
         */
        @Override
        protected Object newArray(Object old, int size, Schema schema) {
            return JsonNodeFactory.instance.arrayNode();
        }

        /**
         * Called as part of deserializing the Avro 'array' type.  Before
         * adding it to the JSON ArrayNode, the value is converted to JsonNode
         * from what was returned by GenericDatumReader.read.
         */
        @Override
        protected void addToArray(Object array, long pos, Object value) {
            ((ArrayNode) array).add(genericToJson(value));
        }

        /**
         * Called as part of deserializing the Avro 'map' type.
         */
        @Override
        protected Object newMap(Object old, int size) {
            return JsonNodeFactory.instance.objectNode();
        }

        /**
         * Called as part of deserializing the Avro 'map' type.  Before adding
         * it to the JSON ObjectNode, the value is converted to JsonNode from
         * what was returned by GenericDatumReader.read.
         */
        @Override
        protected void addToMap(Object map, Object key, Object value) {
            ((ObjectNode) map).put(key.toString(), genericToJson(value));
        }

        /**
         * We override the primary read method just in order to use the JSON
         * encoding for the 'union' type as defined by the Avro spec.  See
         * JsonDatumWriter.write for more info.
         */
        @Override
        protected Object read(Object old, Schema expected, ResolvingDecoder in)
            throws IOException {

            if (expected.getType() != Schema.Type.UNION) {
                return super.read(old, expected, in);
            }

            final int index = in.readIndex();
            final Schema datumSchema = expected.getTypes().get(index);
            final Object datum = read(old, datumSchema, in);
            if (datum == null) {
                return null;
            }
            final Object record = JsonData.INSTANCE.newRecord(old, expected);
            JsonData.INSTANCE.setField(record, datumSchema.getFullName(), 0,
                                       datum);
            return record;
        }
    }

    /**
     * Translates a byte array to its 'bytes' or 'fixed' string representation.
     * The Avro spec defines the JSON representation for bytes/fixed as a
     * String where the lower byte of each char is mapped directly to a byte in
     * the the byte array.
     */
    private static String bytesToString(byte[] buf, int offset, int len) {
        final StringBuilder builder = new StringBuilder(len);
        for (int i = 0; i < len; i += 1) {
            builder.append((char) (buf[offset + i] & 0xFF));
        }
        return builder.toString();
    }

    /**
     * Translates a 'bytes' or 'fixed' string representation to a byte array.
     * The Avro spec defines the JSON representation for bytes/fixed as a
     * String where the lower byte of each char is mapped directly to a byte in
     * the the byte array.
     */
    private static byte[] stringToBytes(Schema schema,
                                        String str,
                                        boolean isFixedType) {
        final byte[] buf = new byte[str.length()];
        for (int i = 0; i < buf.length; i += 1) {
            final char c = str.charAt(i);
            if (c > 0xFF) {
                final String fieldDesc = isFixedType ?
                    ("A 'fixed' field of type: " + schema.getName()) :
                    "A 'bytes' field";
                throw new AvroTypeException
                    (fieldDesc + " contains illegal char: 0x" +
                     Integer.toHexString(c) +
                     "; all char values must be <= 0xFF");
            }
            buf[i] = (byte) c;
        }
        return buf;
    }

    /**
     * Translates from what was returned by GenericDatumReader.read, which is
     * the generic object representation, to JsonNode.
     * <p>
     * ObjectNode and ArrayNode are returned without translation, because they
     * are handled separately by specific methods for those types.
     * <p>
     * Type 'bytes' and 'fixed' are translated from strings to TextNodes, just
     * like ordinary strings, and are then handled separately by specific
     * methods for those types.
     * <p>
     * An alternative approach would be to call the ObjectNode.put and
     * ArrayNode.add method signatures for specific data types (int, boolean,
     * etc).  But this would have to be done redundantly in three places for
     * records, maps and arrays.  Translating to a JsonNode simplifies the
     * implementation because ObjectNode.put(String, JsonNode) and
     * ArrayNode.add(JsonNode) can always be used.
     */
    private static JsonNode genericToJson(Object value) {
        if (value == null) {
            return JsonNodeFactory.instance.nullNode();
        }
        if (value instanceof JsonNode) {
            return (JsonNode) value;
        }
        if (value instanceof CharSequence) {
            return JsonNodeFactory.instance.textNode(value.toString());
        }
        if (value instanceof Integer) {
            return JsonNodeFactory.instance.numberNode((Integer) value);
        }
        if (value instanceof Long) {
            return JsonNodeFactory.instance.numberNode((Long) value);
        }
        if (value instanceof Float) {
            return JsonNodeFactory.instance.numberNode((Float) value);
        }
        if (value instanceof Double) {
            return JsonNodeFactory.instance.numberNode((Double) value);
        }
        if (value instanceof Boolean) {
            return JsonNodeFactory.instance.booleanNode((Boolean) value);
        }
        /* Should never happen. */
        throw new IllegalStateException("Unknown generic datum class: " +
                                        value.getClass().getName());
    }
}
