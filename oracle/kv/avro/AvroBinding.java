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

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;

import oracle.kv.Value;
import oracle.kv.Value.Format;
import oracle.kv.ValueBinding;

/**
 * The {@code AvroBinding} interface has the same methods as {@link
 * ValueBinding}, but adds semantics and exceptions that are specific to the
 * Avro data format.
 * <p>
 * All {@code AvroBinding} subtypes -- {@link SpecificAvroBinding}, {@link
 * GenericAvroBinding}, {@link JsonAvroBinding} and {@link RawAvroBinding} --
 * operate on the built-in Avro data format.  The serialized data used and
 * produced by the binding -- the byte array of the {@link Value} -- is
 * serialized Avro data, packaged in an internal format that includes a
 * reference to the Avro schema.  The {@link #toValue toValue} method of each
 * Avro binding returns this serialized format, and the {@link #toObject
 * toObject} method is passed this serialized format.
 * <p>
 * See {@link AvroCatalog} for a comparison of the different types of Avro
 * bindings and the trade-offs in using them.
 *
 * @param <T> is the type of the deserialized object that is passed to {@link
 * #toValue toValue} and returned by {@link #toObject toObject}.  The specific
 * type depends on the particular binding that is used.  It may be an
 * Avro-generated specific class that implements {@link SpecificRecord}, a
 * {@link GenericRecord}, a {@link JsonRecord}, or a {@link RawRecord}.
 *
 * @since 2.0
 */
public interface AvroBinding<T> extends ValueBinding<T> {

    /**
     * {@inheritDoc}
     *
     * @param value {@inheritDoc}.  The byte array of the {@link Value} is
     * serialized Avro data, packaged in an internal format that includes a
     * reference to the Avro schema
     *
     * @throws SchemaNotAllowedException if the schema associated with the
     * {@code value} parameter is not allowed with this binding.
     *
     * @throws IllegalArgumentException if the value format is not {@link
     * Format#AVRO}, the schema identifier embedded in the {@code value}
     * parameter is invalid, or the serialized data cannot be parsed.
     */
    @Override
    public T toObject(Value value)
        throws SchemaNotAllowedException, IllegalArgumentException;

    /**
     * {@inheritDoc}
     *
     * @return {@inheritDoc}.  The byte array of the {@link Value} is
     * serialized Avro data, packaged in an internal format that includes a
     * reference to the Avro schema
     *
     * @throws SchemaNotAllowedException if the schema associated with the
     * {@code object} parameter is not allowed with this binding.
     *
     * @throws UndefinedSchemaException if the schema associated with the
     * {@code object} parameter has not been defined using the NoSQL Database
     * administration interface.  Note that when the allowed schemas for a
     * binding are specified (and validate) at the time the binding is created,
     * this exception is extremely unlikely and is only possible if a schema is
     * mistakenly disabled after the binding is created.
     *
     * @throws IllegalArgumentException if the {@code object} parameter is
     * invalid according to its schema, and cannot be serialized.
     */
    @Override
    public Value toValue(T object)
        throws SchemaNotAllowedException, UndefinedSchemaException,
               IllegalArgumentException;
}
