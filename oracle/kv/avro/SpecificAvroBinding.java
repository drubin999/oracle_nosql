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

import org.apache.avro.specific.SpecificRecord;

import oracle.kv.Value;

/**
 * The {@code SpecificAvroBinding} interface has the same methods as {@link
 * AvroBinding}, but represents values as instances of a generated Avro
 * specific class which implements {@link SpecificRecord}.  A single schema
 * binding is created using {@link AvroCatalog#getSpecificBinding}, and a
 * multiple schema binding is created using {@link
 * AvroCatalog#getSpecificMultiBinding}.
 * <p>
 * The trade-offs in using a {@code SpecificAvroBinding}, compared to other
 * types of bindings, are:
 *   <ul>
 *   <li>Avro specific classes provide type safety and ease of use.  Properties
 *   are accessed through getter/setter methods with parameters and return
 *   values of the correct type, as defined in the schema.
 *   <p></li>
 *   <li>When using Avro specific classes, the client application does not need
 *   to be aware of the Avro schema at runtime.  The generated class has an
 *   internal reference to its associated schema, and this schema is used by
 *   the binding, so the application does not need to explicitly supply a
 *   schema.  Schemas are supplied at build time, when the source code for the
 *   specific class is generated.
 *   <p></li>
 *   <li>A potential disadvantage to using Avro specific classes is that the
 *   set of specific classes used in the application is fixed at build time.
 *   Applications that wish to treat values generically, or dynamically based
 *   on the schema, should use a generic or JSON binding instead.
 *   </li>
 *   </ul>
 * <p>
 * See {@link AvroCatalog} for general information on Avro bindings and
 * schemas.  The schemas used in the examples below are described in the {@link
 * AvroCatalog} javadoc.
 * <p>
 * When using a {@code SpecificAvroBinding}, an Avro specific Java class, which
 * implements the Avro {@link SpecificRecord} interface, is used to represent
 * values.  An Avro specific class is a POJO (Plain Old Java Object) having
 * properties (getter and setter methods) for each field in its associated Avro
 * schema.  The source code for an Avro specific class is generated from an
 * Avro schema using the Avro compiler tools; see the Avro example in {@code
 * KVHOME/examples/avro} for details.
 * <p>
 * For example, the generated Avro specific classes for the example schemas
 * are:
 * <pre class="code">
 *  package com.example;  
 *
 *  import org.apache.avro.Schema;
 *  import org.apache.avro.specific.SpecificRecord;
 *
 *  public class FullName implements SpecificRecord {
 *    public FullName() {}
 *    public Schema getSchema() { ... }
 *    public String getFirst() { ... }
 *    public void setFirst(String value) { ... }
 *    public String getLast() { ... }
 *    public void setLast(String value) { ... }
 *  }
 *
 *  public class MemberInfo implements SpecificRecord {
 *    public MemberInfo() {}
 *    public Schema getSchema() { ... }
 *    public FullName getName() { ... }
 *    public void setName(FullName value) { ... }
 *    public Integer getAge() { ... }
 *    public void setAge(Integer value) { ... }
 *  }</pre>
 * In addition to the methods shown, useful {@link Object#toString}, {@link
 * Object#equals} and {@link Object#hashCode} methods are generated.  Avro also
 * generates internal methods for performing serialization and deserialization,
 * and an internal static field that holds the schema used to compile the
 * class.  Note that the full Avro schema name and full Java class name are
 * always equal.
 * <p>
 * The following code fragment demonstrates writing and reading a value using a
 * specific single schema binding.  Note that no casting is needed when a
 * single schema binding is used.
 *
 * <pre class="code">
 * AvroBinding&lt;MemberInfo&gt; binding =
 *     avroCatalog.getSpecificBinding(MemberInfo.class);
 *
 * // Create object
 * FullName name = new FullName();
 * name.setFirst(...);
 * name.setLast(...);
 * MemberInfo object = new MemberInfo();
 * object.setName(name);
 * object.setAge(...);
 *
 * // Serialize and store
 * Value value = binding.toValue(object);
 * kvStore.put(key, value);
 *
 * // Sometime later, retrieve and deserialize
 * ValueVersion vv = kvStore.get(key);
 * MemberInfo object = binding.toObject(vv.getValue());
 *
 * // Use object
 * FullName name = object.getName();
 * int age = object.getAge();
 * ...</pre>
 *
 * The following code fragment demonstrates reading values with different
 * schemas (different specific classes) using a multiple schema binding.  Note
 * that schema name could also be used to distinguish between classes, instead
 * of using {@code instanceof}.
 *
 * <pre class="code">
 * SpecificAvroBinding&lt;SpecificRecord&gt; binding =
 *     avroCatalog.getSpecificMultiBinding();
 *
 * Iterator&lt;KeyValueVersion&gt; iter = kvStore.multiGetIterator(...);
 * for (KeyValueVersion kvv : iter) {
 *     SpecificRecord object = binding.toObject(kvv.getValue());
 *     if (object instanceof MemberInfo) {
 *         MemberInfo member = (MemberInfo) object;
 *         ...
 *     } else if (object instanceof OtherSpecificClass) {
 *         ...
 *     } else {
 *         ...
 *     }
 * }</pre>
 *
 * @param <T> is the {@link SpecificRecord} type of the deserialized object
 * that is passed to {@link #toValue toValue} and returned by {@link #toObject
 * toObject}.  When using a single schema binding, {@code T} is the generated
 * Avro specific class, which implements {@link SpecificRecord}.  When using a
 * multiple schema binding, {@code T} is the {@link SpecificRecord} interface
 * itself.
 *
 * @since 2.0
 */
public interface SpecificAvroBinding<T extends SpecificRecord>
    extends AvroBinding<T> {

    /**
     * {@inheritDoc}
     * <p>
     * If necessary, this method automatically performs schema evolution, as
     * described in {@link AvroCatalog}. In the context of schema evolution,
     * the writer schema is the one associated internally with the {@code
     * value} parameter (this association was normally made earlier when the
     * value was stored), and the reader schema is the one associated with the
     * Avro specific class of the caller.  The schema of an Avro specific class
     * is the one from which the class was generated using the Avro compiler
     * tools.
     * <p>
     * In other words, this method transforms the serialized data in the {@code
     * value} parameter to conform to the schema of the Avro specific class
     * that is returned.
     *
     * @return the deserialized {@link SpecificRecord} instance.
     */
    @Override
    public T toObject(Value value)
        throws SchemaNotAllowedException, IllegalArgumentException;

    /**
     * {@inheritDoc}
     * <p>
     * In the context of schema evolution, as described in {@link AvroCatalog},
     * the returned value is serialized according to the writer schema.  The
     * writer schema is the one associated with the Avro specific class of the
     * {@code object} parameter.  The schema of an Avro specific class is the
     * one from which the class was generated using the Avro compiler tools.
     * <p>
     * In other words, this method returns serialized data that conforms to the
     * schema of the Avro specific class.
     *
     * @param object the {@link SpecificRecord} instance the user wishes to
     * store, or at least serialize.
     *
     * @throws SchemaNotAllowedException if the {@code object} parameter's
     * class is not the class passed to {@link AvroCatalog#getSpecificBinding};
     * this can only happen if generic type warnings are ignored or suppressed.
     * This exception is never thrown by a {@link
     * AvroCatalog#getSpecificBinding specific multi-binding}.
     */
    @Override
    public Value toValue(T object)
        throws SchemaNotAllowedException, UndefinedSchemaException,
               IllegalArgumentException;
}
