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

import oracle.kv.Value;

/**
 * The {@code GenericAvroBinding} interface has the same methods as {@link
 * AvroBinding}, but represents values as instances of {@link GenericRecord}.
 * A single schema binding is created using {@link
 * AvroCatalog#getGenericBinding}, and a multiple schema binding is created
 * using {@link AvroCatalog#getGenericMultiBinding}.
 * <p>
 * The trade-offs in using a {@code GenericAvroBinding}, compared to other
 * types of bindings, are:
 *   <ul>
 *   <li>The obvious advantage of using a generic binding is that values may be
 *   treated generically.  Also, if schemas are treated dynamically, then the
 *   set of schemas used in an application need not be fixed at build time.
 *   <p></li>
 *   <li>Because {@link GenericRecord} is part of the Avro API, it conveniently
 *   represents all data types defined by Avro.  This is in contrast to a
 *   {@link JsonAvroBinding}.
 *   <p></li>
 *   <li>{@link GenericRecord} does not provide type safety because field
 *   values must be cast to the type defined in the schema.  It can also be
 *   error prone, because fields are accessed by string name.
 *   <p></li>
 *   <li>To support class evolution, with generic bindings (unlike Avro
 *   specific bindings) the application must supply the Avro {@link Schema}
 *   objects at runtime.  In other words, the application must maintain a set
 *   of known schemas for use at runtime.
 *   <p></li>
 *   </ul>
 * <p>
 * See {@link AvroCatalog} for general information on Avro bindings and
 * schemas.  The schemas used in the examples below are described in the {@link
 * AvroCatalog} javadoc.
 * <p>
 * When using a {@code GenericAvroBinding}, a {@link GenericRecord} is used to
 * represent values.  A {@link GenericRecord} represents an Avro object roughly
 * as a map of string field names to field values, as follows:
 * <p>
 * <pre class="code">
 *  public interface GenericRecord {
 *    public Schema getSchema();
 *    public Object get(String key);
 *    public void put(String key, Object value);
 *  }</pre>
 * Note that field values in this API are type Object and must be cast to the
 * appropriate type, as defined by the schema. The mapping from Avro schema
 * data types to Java data types is described at the bottom of the {@link
 * org.apache.avro.generic} package description.
 * <p>
 * The following code fragment demonstrates writing and reading a value using a
 * generic single schema binding.
 *
 * <pre class="code">
 * Schema.Parser parser = new Schema.Parser();
 * Schema nameSchema = parser.parse(nameSchemaText);
 * Schema memberSchema = parser.parse(memberSchemaText);
 *
 * GenericAvroBinding binding = avroCatalog.getGenericBinding(memberSchema);
 *
 * // Create object
 * GenericRecord name = new GenericData.Record(nameSchema)
 * name.put("first", ...);
 * name.put("last", ...);
 * GenericRecord object = new GenericData.Record(memberSchema)
 * object.put("name", name);
 * object.put("age", new Integer(...));
 *
 * // Serialize and store
 * Value value = binding.toValue(object);
 * kvStore.put(key, value);
 *
 * // Sometime later, retrieve and deserialize
 * ValueVersion vv = kvStore.get(key);
 * GenericRecord object = binding.toObject(vv.getValue());
 *
 * // Use object
 * GenericRecord name = (GenericRecord) object.get("name");
 * Integer age = (Integer) object.get("age");
 * ...</pre>
 * <p>
 * The following code fragment demonstrates reading values with different
 * schemas using a generic multiple schema binding.
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
 * GenericAvroBinding binding = avroCatalog.getGenericMultiBinding(schemas);
 *
 * Iterator&lt;KeyValueVersion&gt; iter = kvStore.multiGetIterator(...);
 * for (KeyValueVersion kvv : iter) {
 *     GenericRecord object = binding.toObject(kvv.getValue());
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
 * A special use case for a generic multiple schema binding is when the
 * application treats values dynamically based on their schema, rather than
 * using a fixed set of known schemas.  The {@link
 * AvroCatalog#getCurrentSchemas} method can be used to obtain a map of the
 * most current schemas,  which can be passed to {@link
 * AvroCatalog#getGenericMultiBinding}.
 * <p>
 * For example, the following code fragment demonstrates reading values with
 * different schemas using a generic multiple schema binding.  Note that in a
 * long running application, it is possible that a schema may be added and used
 * to store a key-value pair, after the binding has been created.  The
 * application may handle this possibility by catching {@link
 * SchemaNotAllowedException}.
 *
 * <pre class="code">
 * GenericAvroBinding binding =
 *     avroCatalog.getGenericMultiBinding(avroCatalog.getCurrentSchemas());
 *
 * Iterator&lt;KeyValueVersion&gt; iter = kvStore.storeIterator(...);
 * while (iter.hasNext()) {
 *     KeyValueVersion kvv = iter.next();
 *     GenericRecord object;
 *     try {
 *         object = binding.toObject(kvv.getValue());
 *     } catch (SchemaNotAllowedException e) {
 *         // In this example, ignore values with a schema that was not
 *         // known at the time the binding was created.
 *         continue;
 *     }
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
public interface GenericAvroBinding extends AvroBinding<GenericRecord> {

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
     * value} parameter to conform to the schema of the {@link GenericRecord}
     * that is returned.
     *
     * @return the deserialized {@link GenericRecord} instance.  The {@link
     * GenericRecord#getSchema} method will return the reader schema, which is
     * the schema that was specified when this binding was created.
     */
    @Override
    public GenericRecord toObject(Value value)
        throws SchemaNotAllowedException, IllegalArgumentException;

    /**
     * {@inheritDoc}
     * <p>
     * In the context of schema evolution, as described in {@link AvroCatalog},
     * the returned value is serialized according to the writer schema.  The
     * writer schema is the one associated with the {@link GenericRecord}
     * {@code object} parameter; it is returned by {@link
     * GenericRecord#getSchema} and normally specified when creating a {@link
     * org.apache.avro.generic.GenericData.Record} object.  The writer schema
     * must be one of the schemas specified when this binding was created.
     * <p>
     * In other words, this method returns serialized data that conforms to the
     * schema of the given {@link GenericRecord}.
     *
     * @param object the {@link GenericRecord} instance the user wishes to
     * store, or at least serialize.
     */
    @Override
    public Value toValue(GenericRecord object)
        throws SchemaNotAllowedException, UndefinedSchemaException,
               IllegalArgumentException;
}
