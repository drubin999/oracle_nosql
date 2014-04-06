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

import org.apache.avro.Schema;

import oracle.kv.Value;
import oracle.kv.avro.UndefinedSchemaException;

/**
 * Provides caching of the C schema and other utilities needed by the C API.
 */
public interface CBindingBridge {

    /**
     * Returns the Java schema associated with the given C schema, or null if
     * the C schema is not present in the cache (which might also mean the
     * schema is not present in the store).  This method is used during Avro C
     * serialization, to obtain the Java schema for creating a RawRecord.
     * <p>
     * If null is returned, the C schema should be converted to JSON text and
     * putSchema(String,long) should be called.
     */
    public Schema getJavaSchema(long cSchema);

    /**
     * Adds the given C schema to the cache and associates it with the Java
     * schema having the given schema text.  This method is called when there
     * is a cache miss, i.e., getJavaSchema returns null.
     * <p>
     * The cached Java schema is returned.  If another thread added the Java
     * schema first, then the cSchema passed here and the cSchema passed by the
     * other thread will both be cached and associated with the Java schema.
     *
     * @throws UndefinedSchemaException if the given schema is not present in
     * the store.
     *
     * @throws IllegalArgumentException if the given schema text cannot be
     * parsed.
     */
    public Schema putSchema(String schemaText, long cSchema)
        throws UndefinedSchemaException, IllegalArgumentException;

    /**
     * Returns the C schema associated with the given Java schema, or zero if
     * the C schema is not present in the cache.  This method is used during
     * Avro C deserialization, to obtain the C schema from the Java schema
     * returned by RawAvroBinding.toObject().getSchema().
     * <p>
     * If zero is returned, a C schema should be created from the schema JSON
     * text (which can be obtained from Schema.toString) and
     * putSchema(Schema,long) should be called.
     *
     * @throws UndefinedSchemaException if the given Java schema is not present
     * in the store.  If the Java schema was obtained from
     * RawAvroBinding.toObject().getSchema(), this should never happen.
     */
    public long getCSchema(Schema javaSchema)
        throws UndefinedSchemaException;

    /**
     * Adds the given C schema to the cache and associates it with the given
     * Java schema.  This method is called when there is a cache miss, i.e.,
     * getCSchema returns zero.
     * <p>
     * The cached C schema is returned.  If another thread added the C schema
     * first, then the return value will not be equal to the cSchema argument
     * and the cSchema argument will NOT be cached.  The returned C schema,
     * which is cached, should be the one ultimately returned to the user.
     *
     * @throws UndefinedSchemaException if the given Java schema is not present
     * in the store.  If the Java schema was obtained from
     * RawAvroBinding.toObject().getSchema(), this should never happen.
     */
    public long putSchema(Schema javaSchema, long cSchema)
        throws UndefinedSchemaException;

    /**
     * Returns all C schemas in the cache.  Can be used to deallocate them.
     */
    public long[] getCachedCSchemas();

    /**
     * Returns the offset of the raw value within the Value's byte array, in
     * other words, the byte length of the schema ID at the front of the array.
     * <p>
     * Allows copying the raw data (using the offset) directly between the
     * Value's byte array and a C array, using JNI.
     */
    public int getValueRawDataOffset(Value value);

    /**
     * Returns the Schema for the given Value, as if
     * RawAvroBinding.toObject().getSchema() were called.  Used during
     * deserialization in the C API, instead of using RawAvroBinding.toObject.
     * <p>
     * This is efficient for use via JNI, mainly due to the separate use of
     * getValueRawDataOffset and also because there is no intermediate
     * RawRecord object.
     */
    public Schema getValueSchema(Value value)
        throws IllegalArgumentException;

    /**
     * Allocates a Value object with a byte array sized to hold the schema ID
     * and raw data of the given size.  Used during serialization in the C API,
     * instead of using RawAvroBinding.toValue.
     * <p>
     * The schema ID is copied into the front of the byte array.  After calling
     * this method, getValueRawDataOffset can be called to get the offset for
     * copying in the raw data.
     * <p>
     * This is efficient for use via JNI, mainly due to the separate use of
     * getValueRawDataOffset and also because there is no intermediate
     * RawRecord object.
     */
    public Value allocateValue(Schema schema, int dataSize)
        throws UndefinedSchemaException;
}
