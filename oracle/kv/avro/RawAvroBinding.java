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

import oracle.kv.Value;

import org.apache.avro.generic.GenericRecord;

/**
 * The {@code RawAvroBinding} interface has the same methods as {@link
 * AvroBinding}, but represents values as instances of {@link RawRecord}.  A
 * raw binding is created using {@link AvroCatalog#getRawBinding}.
 * <p>
 * The trade-offs in using a {@code RawAvroBinding}, compared to other types
 * of bindings, are:
 *   <ul>
 *   <li>The advantage of a raw binding is that it allows handling the Avro
 *   serialized byte array directly in the application.  In other words, it
 *   allows "escaping" from the built-in serialization provided by the other
 *   bindings in this package, when necessary. For example:
 *     <ul>
 *     <li><p>The Avro serialization and deserialization APIs may be used
 *     directly, when none of the built-in bindings in this package are
 *     appropriate.
 *     </li>
 *     <li><p>The serialized byte array may be copied to or from another
 *     component or system, without deserializing it.
 *     </li>
 *     <li><p>An application may wish to examine the schema of a value before
 *     deciding on which binding to use, or whether or not to deserialize the
 *     value.
 *     </li>
 *     </ul>
 *   <p></li>
 *   <li>Because it does not perform serialization, deserialization or class
 *   evolution, a raw binding is lower level and more difficult to use than the
 *   other built-in buildings.  WARNING: When using a raw binding, it is the
 *   user's responsibility to ensure that a {@link Value} contains valid
 *   serialized Avro data, before writing it to the store.
 *   </li>
 *   </ul>
 * <p>
 * See {@link AvroCatalog} for general information on Avro bindings and
 * schemas.  The schemas used in the examples below are described in the {@link
 * AvroCatalog} javadoc.
 * <p>
 * When using a {@code RawAvroBinding}, a {@link RawRecord} is used to
 * represent values.  A {@link RawRecord} contains the raw Avro serialized byte
 * array and its associated schema.
 * <p>
 * The only purpose of a raw binding is to package and unpackage the Avro
 * serialized data and the internal schema identifier.  These are stored
 * together in the byte array of the {@link Value} object, using an internal
 * format known to the binding.
 * <p>
 * No schema is specified when calling {@link AvroCatalog#getRawBinding}, no
 * class evolution is performed by a raw binding's {@link #toObject toObject}
 * method, and the binding may be used for values with any schema known to the
 * store.
 * <p>
 * The following code fragment demonstrates writing and reading a value using a
 * raw binding.  The Avro APIs are used directly to perform serialization and
 * deserialization.  In the example a {@link GenericRecord} is used, but other
 * Avro APIs could be used as well.
 * <p>
 * The example performs the same function as the built-in generic binding
 * provided by this class, i.e., the serialization and deserialization sections
 * below are equivalent what the built-in generic binding already provides.  An
 * application would normally use a raw binding along with custom serialization
 * done differently than shown below.
 *
 * <pre class="code">
 * Schema.Parser parser = new Schema.Parser();
 * Schema nameSchema = parser.parse(nameSchemaText);
 * Schema memberSchema = parser.parse(memberSchemaText);
 *
 * RawAvroBinding binding = avroCatalog.getRawBinding();
 *
 * // Create object
 * GenericRecord name = new GenericData.Record(nameSchema)
 * name.put("first", ...);
 * name.put("last", ...);
 * GenericRecord object = new GenericData.Record(memberSchema)
 * object.put("name", name);
 * object.put("age", new Integer(...));
 *
 * // Serialize using Avro APIs directly
 * ByteArrayOutputStream out = new ByteArrayOutputStream();
 * GenericDatumWriter&lt;GenericRecord&gt; writer =
 *     new GenericDatumWriter&lt;GenericRecord&gt;(object.getSchema());
 * Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
 * writer.write(object, encoder);
 * encoder.flush();
 *
 * // Package and store
 * RawRecord raw = new RawRecord(out.toByteArray(), object.getSchema());
 * kvStore.put(key, binding.toValue(raw));
 *
 * // Sometime later, retrieve and unpackage
 * ValueVersion vv = kvStore.get(key);
 * RawRecord raw = binding.toObject(vv.getValue());
 *
 * // Deserialize using Avro APIs directly
 * Decoder decoder =
 *     DecoderFactory.get().binaryDecoder(raw.getRawData(), null);
 * GenericDatumReader&lt;GenericRecord&gt; reader =
 *     new GenericDatumReader&lt;GenericRecord&gt;(raw.getSchema());
 * GenericRecord object = reader.read(null, decoder);
 *
 * // Use object
 * GenericRecord name = (GenericRecord) object.get("name");
 * Integer age = (Integer) object.get("age");
 * ...</pre>
 * <p>
 * The following code fragment demonstrates reading values and examining
 * their schema using a raw binding, without performing deserialization.
 * Another binding could be used to deserialize the value, if desired,
 * after examining the schema.  Or, the serialized byte array could be
 * copied to or from another component or system, without deserializing it.
 *
 * <pre class="code">
 * RawAvroBinding binding = avroCatalog.getRawBinding();
 *
 * Iterator&lt;KeyValueVersion&gt; iter = kvStore.multiGetIterator(...);
 * for (KeyValueVersion kvv : iter) {
 *     RawRecord object = binding.toObject(kvv.getValue());
 *     Schema schema = object.getSchema();
 *
 *     // Use schema to decide how to process the object...
 * }</pre>
 *
 * @since 2.0
 */
public interface RawAvroBinding extends AvroBinding<RawRecord> {

    /**
     * {@inheritDoc}
     * <p>
     * This method does not perform deserialization or class evolution. It only
     * unpackages the Avro serialized data and the internal schema identifier.
     * These are stored together in the byte array of the {@link Value} object,
     * using an internal format known to the binding.
     * <p>
     * WARNING: When using a raw binding, it is the user's responsibility to
     * ensure that a {@link Value} contains valid serialized Avro data, before
     * writing it to the store.
     *
     * @return the {@link RawRecord} instance.  The {@link RawRecord#getSchema}
     * method will return the writer schema, which is the schema that was
     * specified when the value was stored.
     */
    @Override
    public RawRecord toObject(Value value)
        throws IllegalArgumentException;

    /**
     * {@inheritDoc}
     * <p>
     * This method does not perform serialization. It only packages the Avro
     * serialized data and the internal schema identifier.  These are stored
     * together in the byte array of the {@link Value} object, using an
     * internal format known to the binding.
     * <p>
     * WARNING: When using a raw binding, it is the user's responsibility to
     * ensure that a {@link Value} contains valid serialized Avro data, before
     * writing it to the store.
     *
     * @param object the {@link RawRecord} instance that the user wishes to
     * package as a {@link Value}.
     */
    @Override
    public Value toValue(RawRecord object)
        throws UndefinedSchemaException;
}
