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

import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;

import oracle.kv.Consistency;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.Value;

/**
 * A catalog of Avro schemas and bindings for a store.
 * <p>
 * Manages schemas and provides {@link AvroBinding}s for use with the Avro
 * data format.  The bindings are used along with {@link KVStore} APIs for
 * storing and retrieving key-value pairs.  The bindings are used to serialize
 * Avro values before writing them, and deserialize Avro values after reading
 * them.  An AvroCatalog is obtained by calling {@link KVStore#getAvroCatalog}.
 * <p>
 * <em>WARNING:</em> We strongly recommend using an {@link AvroBinding}.  NoSQL
 * Database will leverage Avro in the future to provide additional features and
 * capabilities.
 * <p>
 * <em>WARNING:</em> To take advantage of the Avro data format, the bindings in
 * this class must be used.  The {@link Value} byte array is constructed by the
 * binding to include an internal reference to the schema used for
 * serialization.  The {@link Value} byte array may not be manipulated directly
 * by the application.
 *
 * <h3>Avro Schemas</h3>
 *
 * When the Avro data format is used, each stored value must be associated with
 * an Avro schema.  The Avro schema describes the fields allowed in the value,
 * along with their data types.  An Avro schema is created by the application
 * developer, added to the store using the NoSQL Database administration
 * interface, and used in the client API via the {@code AvroCatalog} class.
 * <p>
 * An Avro schema is created in JSON format, typically using a text editor and
 * initially saved in a text file.  Of course, to create an Avro schema the
 * developer must understand the Avro schema syntax.  For more information see
 * Avro Schemas in the Getting Started Guide and the <a
 * href="http://avro.apache.org/docs/current/spec.html">Avro schema
 * specification</a>.
 * <p>
 * Once created and saved in a text file, the schema is added to the store
 * using the {@code ddl add-schema} administrative command, using the text file
 * as input; see Adding Schema in the Getting Started Guide.  Until a schema is
 * added, it may not be used in the client API to store values.  The use of the
 * schema in the client API is described further below.
 * <p>
 * Note that the use of Avro schemas allows serialized values to be stored in a
 * very space-efficient binary format.  Each value is stored without any
 * metadata other than a small internal schema identifier, between 1 and 4
 * bytes in size.  One such reference is stored per key-value pair.  In this
 * way, the serialized Avro data format is always associated with the schema
 * used to serialize it, with minimal overhead.  This association is made
 * transparently to the application, and the internal schema identifier is
 * managed by the bindings supplied by the {@code AvroCatalog} class.  The
 * application never sees or uses the internal identifier directly.
 * <p>
 * Two example schemas are shown below along with the administrative commands
 * for adding them to the store.  These schemas are used further below in other
 * examples.
 * <p>
 * The schemas might be stored in a simple text file, {@code schema1.txt}:
 * <pre class="code">
 *  { 
 *  "type": "record",
 *  "name": "MemberInfo",
 *  "namespace": "avro",
 *  "fields": [
 *      {"name": "name", "type": {
 *          "type": "record",
 *          "name": "FullName",
 *          "fields": [
 *              {"name": "first", "type": "string", "default": ""},
 *              {"name": "last", "type": "string", "default": ""}
 *          ]
 *      }, "default": {}},
 *      {"name": "age", "type": "int", "default": 0}
 *   ]
 * }</pre>
 *
 * The administrative command for adding the above schemas is:
 * <pre class="code">
 *  &gt; ddl add-schema -file schema1.txt</pre>
 *
 * <h3>Schema Evolution</h3>
 *
 * A schema may be changed, even after data values are stored using that
 * schema, using the {@code ddl add-schema} administrative command with the
 * {@code -evolve} option; see Changing Schema in the Getting Started Guide.
 * The modified schema is saved in a text file, which is passed to this command
 * as input.  For example, fields may be added, removed or renamed.
 * <p>
 * For example, if a middle name property is added in the future to the
 * schema, it might be stored in  {@code schema2.txt}.  Note that a new field
 * must be given a default value.
 * <pre class="code">
 *  { 
 *  "type": "record",
 *  "name": "MemberInfo",
 *  "namespace": "avro",
 *  "fields": [
 *      {"name": "name", "type": {
 *          "type": "record",
 *          "name": "FullName",
 *          "fields": [
 *              {"name": "first", "type": "string", "default": ""},
 *              <strong>{ "name": "middle", "type": "string", "default": "" },</strong>
 *              {"name": "last", "type": "string", "default": ""}
 *          ]
 *      }, "default": {}},
 *      {"name": "age", "type": "int", "default": 0}
 *   ]
 * }</pre>
 *
 * The administrative command for adding the new version of the schema is:
 * <pre class="code">
 *  &gt; ddl add-schema -file schema2.txt -evolve</pre>
 * <p>
 * When a schema is changed, multiple versions of the schema will exist and be
 * maintained by the store.  The version of the schema used to serialize a
 * value, before writing it to the store, is called the <em>writer
 * schema</em>.  The writer schema is specified by the application when
 * creating a binding.  It is associated with the value when calling the
 * binding's {@link AvroBinding#toValue} method to serialize the data.  As
 * mentioned above, the writer schema is associated internally with every
 * stored value.
 * <p>
 * The <em>reader schema</em> is used to deserialize a value after reading it
 * from the store. Like the writer schema, the reader schema is specified by
 * the client application when creating a binding.  It is used to deserialize
 * the data when calling the binding's {@link AvroBinding#toObject} method,
 * after reading a value from the store.
 * <p>
 * When the reader and writer schemas are different, schema evolution is
 * applied during deserialization.  Schema evolution is applied by transforming
 * the data during deserialization, so that data stored according to the writer
 * schema is transformed to conform to the reader schema.  When the reader and
 * writer schemas are the same, no data transformation is necessary.  Also note
 * that no data transformation takes place during serialization; i.e., data is
 * always written according to the writer schema.
 * <p>
 * Reader and writer schemas can be different when a client is changed to use a
 * new version of the schema, and then reads data that was written using the
 * old version.  Schema versions can also be different when two clients are
 * operating concurrently using two different versions of a schema.  In a
 * distributed system such as NoSQL Database, it is normally not possible or
 * desirable to upgrade all clients simultaneously, since this would require
 * downtime.  Therefore, for some period of time there will be a mix of clients
 * operating concurrently using different versions of a schema.  Fortunately,
 * this situation is handled gracefully by virtue of schema evolution.
 * <p>
 * For example, imagine that a new field is added to a schema and there are two
 * versions of the schema.  The new field is only present in the new version of
 * the schema.  The new field must be assigned a default value in the new
 * schema.  There are three possible cases.
 *   <ol>
 *   <li>The writer schema and reader schema are the same.  Schema evolution is
 *   not necessary and no data transformation is applied.
 *   <p></li>
 *   <li>The writer schema is the old version and the reader schema is the new
 *   version.  Because the writer schema is the old version, the new field is
 *   not present in the stored data.  When a client uses the new version as a
 *   reader schema, the new field will appear to the client as having the
 *   default value.
 *   <p></li>
 *   <li>The writer schema is the new version and the reader schema is the old
 *   version.  Because the writer schema is the new version, the new field is
 *   present in the stored data.  When a client uses the old version as a
 *   reader schema, the new field will not appear at all to the client.
 *   </li>
 *   </ol>
 * If instead a field were deleted from a schema, the same rules would apply
 * but with the roles reversed.  Renaming a field is also possible by adding a
 * field alias to the schema; in this case the field is accessible by both the
 * old and new name.  For more information see Schema Evolution in the Getting
 * Started Guide and the detailed rules for schema evolution in the <a
 * href="http://avro.apache.org/docs/current/spec.html">Avro schema
 * specification</a>.
 * <p>
 * To support schema evolution, be sure never to change a schema's name or
 * namespace.  A schema is uniquely identified by its Avro full name, which is
 * similar to a full Java class name and consists of a combination of the Avro
 * schema namespace and the schema name.
 *
 * <h3>Avro schema restrictions</h3>
 *
 * The Avro type of a top-level schema, that is to be stored as the value in a
 * key-value pair, must be the Avro type <em>record</em>.
 *
 * <h3>Choosing a Binding</h3>
 *
 * The {@code AvroCatalog} provides a variety of {@link AvroBinding}s that
 * serialize and deserialize the Avro data format.  A summary of each binding
 * is below.
 *   <ul>
 *   <li>{@link SpecificAvroBinding} is recommended when the schema(s) of the
 *   object(s) in the database are known when the application is being written.
 *   The names of the fields, and how to access them, are known at build time.
 *   A POJO (Plain Old Java Object) class for each schema is generated using
 *   the Avro compiler tools.  The POJO classes have property getters and
 *   setters that provide type safety.  This makes the {@code
 *   SpecificAvroBinding} the easiest of the bindings to use.
 *   <p></li>
 *   <li>{@link GenericAvroBinding} is recommended when the schema(s) of the
 *   object(s) in the database are not known at build-time. Rather than access
 *   the objects using predefined getters and setters, a program using {@code
 *   GenericAvroBinding} passes in the names of the fields to a generalized
 *   getter to retrieve data from an Avro object. For example, a generalized
 *   NoSQL Database record browser would require this capability. 
 *   <p></li>
 *   <li>{@link JsonAvroBinding} is recommended when interoperability with
 *   other components or external systems that use JSON objects is needed.
 *   With the {@code JsonAvroBinding}, the Jackson API is used to manipulate
 *   JSON data objects.  Note that certain Avro data types are not conveniently
 *   represented as JSON values; see {@code JsonAvroBinding} for details.
 *   <p></li>
 *   <li>{@link RawAvroBinding} is recommended when an "escape" from the
 *   built-in serialization provided by the other bindings is needed.  The
 *   {@code RawAvroBinding} does not perform serialization, but instead allows
 *   specifying the Avro binary data as a byte array.  Serialization can be
 *   performed in any way desired, or not at all in the case where Avro binary
 *   data is exchanged with other components or external systems. Because it is
 *   low level and provides complete flexibility, the {@code RawAvroBinding}
 *   provides the least safety and is the most difficult of the bindings to
 *   use. 
 *   </ul>
 * <p>
 * The detailed trade-offs for using each type of binding are described in
 * their javadoc: {@link SpecificAvroBinding}, {@link GenericAvroBinding},
 * {@link JsonAvroBinding}, and {@link RawAvroBinding}.
 *
 * <h3>Single schema and multiple schema bindings</h3>
 *
 * Specific, generic and JSON bindings have a single schema variant ({@link
 * #getSpecificBinding getSpecificBinding}, {@link
 * #getGenericBinding getGenericBinding} and {@link #getJsonBinding
 * getJsonBinding}) and a multiple schema variant ({@link
 * #getSpecificMultiBinding getSpecificMultiBinding}, {@link
 * #getGenericMultiBinding getGenericMultiBinding} and {@link
 * #getJsonMultiBinding getJsonMultiBinding}).
 * <p>
 * A single schema binding provides type checking.  Only values with the given
 * schema (or class, in the case of a specific class binding) can be used with
 * the binding.  A single schema <em>specific</em> class binding provides
 * compile-time type checking, while a a single schema generic or JSON binding
 * provides run-time type checking.
 * <p>
 * A single schema binding is safer than a multiple schema binding and often
 * preferable for that reason.  However, a multiple schema binding may be more
 * useful when retrieving key-value pairs of different types.  A {@link
 * KVStore} method may return values of different types if the application
 * stores multiple types for a single key, or if a method is called that
 * returns multiple key-value pairs such as {@link KVStore#multiGet multiGet},
 * {@link KVStore#multiGetIterator multiGetIterator}, or {@link
 * KVStore#storeIterator storeIterator}.  There are several ways of determining
 * which type is returned in these cases.
 *   <ul>
 *   <li>The key in the key-value pair may indicate the value type according
 *   to application specific knowledge of the key structure.  In this case,
 *   using a single schema binding may be appropriate.  The application can
 *   choose which binding to use based on the key structure.
 *   <p></li>
 *   <li>The schema name or a common property of the object may be used to
 *   determine the value type.  In this case a multiple schema binding can be
 *   used to return a {@link SpecificRecord}, {@link GenericRecord} or {@link
 *   JsonRecord}, and then the schema name or a property of the object can be
 *   examined.
 *   <p></li>
 *   <li>For a specific binding, the class may be used to determine the value
 *   type.  In this case a multiple schema binding can be used to return the
 *   {@link SpecificRecord}, and then {@code instanceof} can be used to
 *   determine the concrete class.
 *   </li>
 *   </ul>
 * <p>
 * Note that both single and multiple schema bindings perform class evolution
 * when deserializing a value.  The deserialized value will conform to the
 * schema specified as an argument of the getXxxBinding or getXxxMultiBinding
 * method.
 * <p>
 * A special use case for a generic or JSON multiple schema binding is when the
 * application treats values dynamically based on their schema, rather than
 * using a fixed set of schemas that is known in advance to the client
 * application.  In this case the {@link #getCurrentSchemas getCurrentSchemas}
 * method can be used to obtain a map of the most current schemas,  which can
 * be passed to {@link #getGenericMultiBinding getGenericMultiBinding} or
 * {@link #getJsonMultiBinding getJsonMultiBinding}.
 *
 * <h3>Using Schemas with Bindings</h3>
 *
 * A client application normally embeds a copy of the schemas it uses, rather
 * than getting the current schemas from the store.  The client's schemas are
 * specified when a binding is created by one of the getXxxBinding methods.
 * This supports schema evolution (as described above), in that the {@link
 * AvroBinding#toObject toObject} method will transform the serialized data
 * such that the returned object conforms to the schema known to the
 * application.
 * <p>
 * The application specifies its known, embedded schemas in different ways,
 * depending on the type of binding used.
 *   <ul>
 *   <li>If an Avro specific binding is used, the schema is specified when the
 *   specific class is generated using the Avro compiler tools.  The schema
 *   text (in JSON format) is included in the generated code as a static String
 *   field, and is internally available to the binding.
 *   <p></li>
 *   <li>If a generic binding or JSON binding is used, the application's
 *   schemas must be explicitly embedded in the application.  For example, the
 *   application might maintain the text (in JSON format) of its schemas in the
 *   application source code (in static String fields) or in a resource file
 *   included in the application jar.  To create {@link Schema} objects from
 *   the schema text, the {@link org.apache.avro.Schema.Parser Schema.Parser}
 *   class may be used by the application.  After creating it, the schema
 *   object is passed to the getXxxBinding method.  A schema object is also
 *   passed to the constructor of {@link GenericRecord}, {@link JsonRecord} and
 *   {@link RawRecord}.
 *   </li>
 *   </ul>
 * <p>
 * As described further above, all schemas used by an application must be
 * defined using the NoSQL Database administrative interface.  If a schema
 * specified by the application via the client API has not been defined in the
 * store, an {@link UndefinedSchemaException} will be thrown by the
 * getXxxBinding method (if the schema is passed to this method), or by one of
 * the methods of the returned binding.  Matching of the application specified
 * schemas with schemas in the store is performed using the {@link
 * Schema#equals} method.
 * <p>
 * One exception to the above is that an application may choose to use the
 * current version of schemas in the store that are returned by {@link
 * #getCurrentSchemas getCurrentSchemas}; in this case the set of schemas used
 * in the application need not be fixed at build time.  A second exception is
 * when the application chooses to use a raw binding and does not serialize or
 * deserialize the data, for example, when the serialized byte array is copied
 * to or from another component or system.
 * <p>
 * WARNING: The application should not create new {@code Schema} objects
 * unnecessarily, since schema creation is an expensive operation.  The
 * expected approach is to create each distinct {@code Schema} only once, and
 * reuse that object whenever it is needed.  Also note that all {@code Schema}
 * objects created by the application and passed to an API method in this
 * package are cached.  This cache is associated with the {@code AvroCatalog}
 * instance, which is associated with the {@code KVStore} instance.  The cached
 * references to the {@code Schema} objects are not discarded until the {@code
 * KVStore} instance is closed and discarded.  For example, a very undesirable
 * approach would be for the application to create a new {@code Schema} object
 * for each serialization or deserialization operation; in this case,
 * performance would suffer greatly and the cached schemas would eventually
 * fill the JVM heap.
 *
 * @since 2.0
 */
public interface AvroCatalog {

    /**
     * Returns a binding for representing values as instances of a generated
     * Avro specific class, for a single given class.
     *
     * @param cls an Avro specific class that was previously generated using
     * the Avro code generation tools.
     *
     * @return the AvroBinding that can be used for serialization and
     * deserialization.
     *
     * @throws UndefinedSchemaException if the schema associated with the given
     * class parameter has not been defined using the NoSQL Database
     * administration interface.
     *
     * @see SpecificAvroBinding
     */
    public <T extends SpecificRecord> SpecificAvroBinding<T>
        getSpecificBinding(Class<T> cls);

    /**
     * Returns a binding for representing values as instances of generated Avro
     * specific classes, for any Avro specific class.
     *
     * @return the AvroBinding that can be used for serialization and
     * deserialization.
     *
     * @see SpecificAvroBinding
     */
    public SpecificAvroBinding<SpecificRecord> getSpecificMultiBinding();

    /**
     * Returns a binding for representing a value as an Avro {@link
     * GenericRecord}, for values that conform to a single given expected
     * schema.
     *
     * @param schema the Avro schema expected for all values and {@link
     * GenericRecord}s used with this binding.
     *
     * @return the AvroBinding that can be used for serialization and
     * deserialization.
     *
     * @throws UndefinedSchemaException if the given schema has not been
     * defined using the NoSQL Database administration interface.
     */
    public GenericAvroBinding getGenericBinding(Schema schema);

    /**
     * Returns a binding for representing a value as an Avro {@link
     * GenericRecord}, for values that conform to multiple given expected
     * schemas.
     *
     * @param schemas the Avro schemas expected for all values and {@link
     * GenericRecord}s used with this binding.  The key in the map is the full
     * name of the schema.
     *
     * @return the AvroBinding that can be used for serialization and
     * deserialization.
     *
     * @throws UndefinedSchemaException if any of the given schemas has not
     * been defined using the NoSQL Database administration interface.
     */
    public GenericAvroBinding
        getGenericMultiBinding(Map<String, Schema> schemas);

    /**
     * Returns a binding for representing a value as a {@link JsonRecord}, for
     * values that conform to a single given expected schema.
     *
     * @param schema the Avro schema expected for all values and {@link
     * JsonRecord}s used with this binding.
     *
     * @return the AvroBinding that can be used for serialization and
     * deserialization.
     *
     * @throws UndefinedSchemaException if the given schema has not been
     * defined using the NoSQL Database administration interface.
     */
    public JsonAvroBinding getJsonBinding(Schema schema);

    /**
     * Returns a binding for representing a value as a {@link JsonRecord}, for
     * values that conform to multiple given expected schemas.
     *
     * @param schemas the Avro schemas expected for all values and {@link
     * JsonRecord}s used with this binding.  The key in the map is the full
     * name of the schema.
     *
     * @return the AvroBinding that can be used for serialization and
     * deserialization.
     *
     * @throws UndefinedSchemaException if any of the given schemas has not
     * been defined using the NoSQL Database administration interface.
     */
    public JsonAvroBinding getJsonMultiBinding(Map<String, Schema> schemas);

    /**
     * Returns a binding for representing a value as a {@link RawRecord}
     * containing the raw Avro serialized byte array and its associated schema.
     *
     * @return the AvroBinding that can be used for packaging and unpackaging
     * the serialized value.
     */
    public RawAvroBinding getRawBinding();

    /**
     * Returns an immutable Map containing the most current version of all
     * schemas from the {@link KVStore} client schema cache.  The Map key is
     * the full name of the schema.
     * <p>
     * A special use case for a generic or JSON multiple schema binding is when
     * the application treats values dynamically based on their schema, rather
     * than using a fixed set of known schemas.  The {@link #getCurrentSchemas
     * getCurrentSchemas} method can be used to obtain a map of the most
     * current schemas,  which can be passed to {@link #getGenericMultiBinding
     * getGenericMultiBinding} or {@link #getJsonMultiBinding
     * getJsonMultiBinding}.  See {@link GenericAvroBinding} and {@link
     * JsonAvroBinding} for an example of this use case.
     *
     * @return an immutable Map of full schema name to schema object.
     */
    public Map<String, Schema> getCurrentSchemas();

    /**
     * Refreshes the cache of stored schemas, adding any new schemas or new
     * versions of schemas to the cache that have been stored via the
     * administration interface since the cache was last refreshed.
     * <p>
     * Calling this method is normally not necessary, since the schema cache is
     * automatically refreshed whenever a schema is specified via any of the
     * Avro binding APIs, and that schema is not already present in the cache.
     * <p>
     * Calling this method periodically may be necessary when the {@link
     * KVStore} handle is long lived, the {@link #getCurrentSchemas} method is
     * used to obtain current schemas, and the application wishes to obtain
     * schemas that were recently added using the administration interface.
     * <p>
     * WARNING: Calling this method often from multiple threads may cause
     * blocking during the query for schema changes.  Also note calling this
     * method often could impact the performance of other operations, since it
     * queries kv pairs in the store.
     *
     * @param consistency determines the consistency associated with the read
     * used to query for new schemas.  If null, the {@link
     * KVStoreConfig#getConsistency default consistency} is used.
     */
    public void refreshSchemaCache(Consistency consistency);
}
