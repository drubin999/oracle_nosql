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

package oracle.kv.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.codehaus.jackson.JsonNode;

import oracle.kv.Value;

/**
 * The {@code JsonAvroBinding} interface has the same methods as {@link
 * AvroBinding}, but represents values as instances of {@link JsonRecord}.
 * A single schema binding is created using {@link
 * AvroCatalog#getJsonBinding}, and a multiple schema binding is created
 * using {@link AvroCatalog#getJsonMultiBinding}.
 * <p>
 * The trade-offs in using a {@code JsonAvroBinding}, compared to other types
 * of bindings, are:
 *   <ul>
 *   <li>Probably the most important reason for using a JSON binding is for
 *   interoperability with other components or external systems that use JSON
 *   objects.
 *   <p></li>
 *   <li>Like generic bindings, an advantage of using a JSON binding is that
 *   values may be treated generically.  Also, if schemas are treated
 *   dynamically, then the set of schemas used in the application need not be
 *   fixed at build time.
 *   <p></li>
 *   <li>Unlike {@link GenericRecord}, certain Avro data types are not
 *   represented conveniently using JSON syntax, namely:
 *     <ul>
 *     <li>Avro {@code int} and {@code long} are both represented as a JSON
 *     {@code integer}, and Avro {@code float} and {@code double} are both
 *     represented as a JSON {@code number}.  WARNING: To avoid type conversion
 *     errors, consider defining integer fields in the Avro schema using type
 *     {@code long}, and floating point fields using type {@code double}.</li>
 *     <li>Avro {@code bytes} and {@code fixed} are both represented as a
 *     JSON {@code string}, using Unicode escape syntax.  WARNING: Characters
 *     greater than {@code 0xFF} are invalid because they cannot be translated
 *     to bytes without loss of information.  Also note that the Jackson
 *     {@link org.codehaus.jackson.node.BinaryNode} class is <em>not</em>
 *     used with these Avro types.</li>
 *     <li>Avro unions have a <a
 * href="http://avro.apache.org/docs/current/spec.html#json_encoding">special
 *      JSON representation</a>.</li>
 *     </ul>
 *   Therefore, applications using JSON should limit the data types used in
 *   their Avro schemas, and should treat the above data types carefully.
 *   <p>
 *   Also note the following type mappings:
 *     <ul>
 *     <li>Avro {@code record} and {@code map} are both represented as a JSON
 *     {@code object}.</li>
 *     <li>An Avro {@code enum} is represented as a JSON {@code string}.</li>
 *     </ul>
 *   <p></li>
 *   <li>Like a {@link GenericRecord}, a JSON object does not provide type
 *   safety.  It can also be error prone, because fields are accessed by
 *   string name.
 *   <p></li>
 *   <li>To support class evolution, with JSON bindings (like generic
 *   bindings, but unlike specific bindings) the application must supply the
 *   Avro {@link Schema} objects at runtime.  In other words, the application
 *   must maintain a set of known schemas for use at runtime.
 *   <p></li>
 *   </ul>
 * <p>
 * See {@link AvroCatalog} for general information on Avro bindings and
 * schemas.  The schemas used in the examples below are described in the {@link
 * AvroCatalog} javadoc.
 * <p>
 * When using a {@code JsonAvroBinding}, a {@link JsonRecord} is used to
 * represent values.  A {@link JsonRecord} represents an Avro object as an
 * {@link JsonNode} in the Jackson API.
 * <p>
 * As with a generic binding, a {@link JsonNode} represents an Avro object
 * roughly as a map of string field names to field values.  However, the
 * Jackson API is very rich and fully supports the JSON format.  For those
 * familiar with XML, the Jackson API is for JSON what the DOM API is for XML.
 * JSON text may also be easily parsed and formatted using the Jackson API.
 * <p>
 * The following code fragment demonstrates writing and reading a value using a
 * JSON single schema binding.
 *
 * <pre class="code">
 * Schema.Parser parser = new Schema.Parser();
 * Schema nameSchema = parser.parse(nameSchemaText);
 * Schema memberSchema = parser.parse(memberSchemaText);
 *
 * JsonAvroBinding binding = avroCatalog.getJsonBinding(memberSchema);
 *
 * // Create object
 * ObjectNode member = JsonNodeFactory.instance.objectNode();
 * JsonRecord object = new JsonRecord(member, memberSchema)
 * ObjectNode name = member.putObject("name");
 * name.put("first", ...);
 * name.put("last", ...);
 * member.put("age", ...);
 *
 * // Serialize and store
 * Value value = binding.toValue(object);
 * kvStore.put(key, value);
 *
 * // Sometime later, retrieve and deserialize
 * ValueVersion vv = kvStore.get(key);
 * JsonRecord object = binding.toObject(vv.getValue());
 *
 * // Use object
 * ObjectNode member = (ObjectNode) object.getNode();
 * ObjectNode name = (ObjectNode) member.get("name");
 * int age = member.get("age").getIntValue();
 * ...</pre>
 * <p>
 * The following code fragment demonstrates reading values with different
 * schemas using a JSON multiple schema binding.
 *
 * <pre class="code">
 * Schema.Parser parser = new Schema.Parser();
 * Schema nameSchema = parser.parse(nameSchemaText);
 * Schema memberSchema = parser.parse(memberSchemaText);
 * Schema anotherSchema = parser.parse(anotherSchemaText);
 *
 * Map&lt;String, Schema&gt; schemas = new HashMap&lt;String, Schema&gt;()
 * schemas.put(memberSchema.getFullName(), memberSchema);
 * schemas.put(anotherSchema.getFullName(), anotherSchema);
 *
 * JsonAvroBinding binding = avroCatalog.getJsonMultiBinding(schemas);
 *
 * Iterator&lt;KeyValueVersion&gt; iter = kvStore.multiGetIterator(...);
 * for (KeyValueVersion kvv : iter) {
 *     JsonRecord object = binding.toObject(kvv.getValue());
 *     JsonNode jsonNode = object.getJsonNode();
 *     String schemaName = object.getSchema().getFullName();
 *     if (schemaName.equals(memberSchema.getFullName())) {
 *         ...
 *     } else if (schemaName.equals(anotherSchema.getFullName())) {
 *         ...
 *     } else {
 *         ...
 *     }
 * }</pre>
 * <p>
 * A special use case for a JSON multiple schema binding is when the
 * application treats values dynamically based on their schema, rather than
 * using a fixed set of known schemas.  The {@link
 * AvroCatalog#getCurrentSchemas} method can be used to obtain a map of the
 * most current schemas,  which can be passed to {@link
 * AvroCatalog#getJsonMultiBinding}.
 * <p>
 * For example, the following code fragment demonstrates reading values with
 * different schemas using a JSON multiple schema binding.  Note that in a long
 * running application, it is possible that a schema may be added and used to
 * store a key-value pair, after the binding has been created.  The application
 * may handle this possibility by catching {@link SchemaNotAllowedException}.
 *
 * <pre class="code">
 * JsonAvroBinding binding =
 *     avroCatalog.getJsonMultiBinding(avroCatalog.getCurrentSchemas());
 *
 * Iterator&lt;KeyValueVersion&gt; iter = kvStore.storeIterator(...);
 * for (KeyValueVersion kvv : iter) {
 *     JsonRecord object;
 *     try {
 *         object = binding.toObject(kvv.getValue());
 *     } catch (SchemaNotAllowedException e) {
 *         // In this example, ignore values with a schema that was not
 *         // known at the time the binding was created.
 *         continue;
 *     }
 *     JsonNode jsonNode = object.getJsonNode();
 *     String schemaName = object.getSchema().getFullName();
 *     if (schemaName.equals(memberSchema.getFullName())) {
 *         ...
 *     } else if (schemaName.equals(anotherSchema.getFullName())) {
 *         ...
 *     } else {
 *         ...
 *     }
 * }</pre>
 *
 * @since 2.0
 */
public interface JsonAvroBinding extends AvroBinding<JsonRecord> {

    /**
     * {@inheritDoc}
     * <p>
     * If necessary, this method automatically performs schema evolution, as
     * described in {@link AvroCatalog}. In the context of schema evolution,
     * the writer schema is the one associated internally with the {@code
     * value} parameter (this association was normally made earlier when the
     * value was stored), and the reader schema is the one associated with this
     * binding (and was specified when the binding was created).
     * <p>
     * In other words, this method transforms the serialized data in the {@code
     * value} parameter to conform to the schema of the {@link JsonRecord} that
     * is returned.
     *
     * @return the deserialized {@link JsonRecord} instance.  The {@link
     * JsonRecord#getSchema} method will return the reader schema, which is the
     * schema that was specified when this binding was created.
     */
    @Override
    public JsonRecord toObject(Value value)
        throws SchemaNotAllowedException, IllegalArgumentException;

    /**
     * {@inheritDoc}
     * <p>
     * In the context of schema evolution, as described in {@link AvroCatalog},
     * the returned value is serialized according to the writer schema.  The
     * writer schema is the one associated with the {@link JsonRecord}
     * {@code object} parameter; it is returned by {@link JsonRecord#getSchema}
     * and specified when creating a {@link JsonRecord} object.  The writer
     * schema must be one of the schemas specified when this binding was
     * created.
     * <p>
     * In other words, this method returns serialized data that conforms to the
     * schema of the given {@link JsonRecord}.
     *
     * @param object the {@link JsonRecord} instance the user wishes to
     * store, or at least serialize.
     */
    @Override
    public Value toValue(JsonRecord object)
        throws SchemaNotAllowedException, UndefinedSchemaException,
               IllegalArgumentException;
}
