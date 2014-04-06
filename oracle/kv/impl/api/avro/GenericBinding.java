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
import java.util.HashMap;
import java.util.Map;

import oracle.kv.Value;
import oracle.kv.avro.GenericAvroBinding;
import oracle.kv.avro.RawAvroBinding;
import oracle.kv.avro.RawRecord;
import oracle.kv.avro.SchemaNotAllowedException;
import oracle.kv.avro.UndefinedSchemaException;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

/**
 * Provides a straightforward mapping from the built-in Avro generic classes to
 * our generic binding API.
 */
class GenericBinding implements GenericAvroBinding {

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

    GenericBinding(AvroCatalogImpl catalog, Map<String, Schema> allowedSchemas)
        throws UndefinedSchemaException {

        this.rawBinding = catalog.getRawBinding();
        this.allowedSchemas = new HashMap<String, Schema>(allowedSchemas);
        /* May throw UndefinedSchemaException. */
        catalog.checkDefinedSchemas(allowedSchemas);
    }

    /**
     * Straightforward deserialization to GenericRecord using RawBinding,
     * BinaryDecoder and GenericDatumReader.
     */
    @Override
    public GenericRecord toObject(Value value)
        throws SchemaNotAllowedException, IllegalArgumentException {

        final RawRecord raw = rawBinding.toObject(value);
        final Schema writerSchema = raw.getSchema();
        /* May throw SchemaNotAllowedException. */
        final Schema readerSchema =
            AvroCatalogImpl.checkToObjectSchema(writerSchema, allowedSchemas);

        final GenericDatumReader<GenericRecord> reader =
            new GenericDatumReader<GenericRecord>(writerSchema, readerSchema);
        final Decoder decoder =
            DecoderFactory.get().binaryDecoder(raw.getRawData(), null);

        try {
            return reader.read(null, decoder);
        } catch (Exception e) {
            throw new IllegalArgumentException
                ("Unable to deserialize GenericRecord", e);
        }
    }

    /**
     * Straightforward serialization of GenericRecord using RawBinding,
     * BinaryEncoder and GenericWriter (our subclass of GenericDatumWriter).
     */
    @Override
    public Value toValue(GenericRecord object)
        throws SchemaNotAllowedException, UndefinedSchemaException,
               IllegalArgumentException {

        final Schema writerSchema = object.getSchema();
        /* May throw SchemaNotAllowedException. */
        AvroCatalogImpl.checkToValueSchema(writerSchema, allowedSchemas);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final GenericWriter writer = new GenericWriter(writerSchema);
        final Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);

        try {
            writer.write(object, encoder);
            encoder.flush();
        } catch (Exception e) {
            throw new IllegalArgumentException
                ("Unable to serialize GenericRecord", e);
        }

        final RawRecord raw = new RawRecord(out.toByteArray(), writerSchema);
        /* May throw UndefinedSchemaException. */
        return rawBinding.toValue(raw);
    }

    /**
     * Subclass of GenericDatumWriter to implement special rules for certain
     * data types.
     */
    static class GenericWriter extends GenericDatumWriter<GenericRecord> {

        GenericWriter(Schema writerSchema) {
            super(writerSchema);
        }

        /**
         * Called to serialize the Avro 'fixed' type.  Uses the
         * GenericBinding.writeFixed utility method to perform validation.
         */
        @Override
        protected void writeFixed(Schema schema, Object datum, Encoder out)
            throws IOException {

            final byte[] bytes = ((GenericFixed) datum).bytes();
            GenericBinding.writeFixed(schema, bytes, out);
        }

        /**
         * We override the primary write method to validate 'int' values.
         */
        @Override
        protected void write(Schema schema, Object datum, Encoder out)
            throws IOException {

            switch (schema.getType()) {
            case INT:
                GenericBinding.writeInt(datum, out);
                break;
            default:
                super.write(schema, datum, out);
                break;
            }
        }
    }

    /**
     * Utility method for writing the 'int' data type, for all bindings.
     * <p>
     * By default in the superclass, an 'int' datum may be any Number subclass
     * and the value is silently truncated if it exceeds 32-bits.  Here we cast
     * the datum to Integer, which throws an exception for other types.  This
     * behavior is consistent with other scalar types (long, float, etc) and is
     * the same as the behavior for the 'int' type in Avro 1.5.
     */
    static void writeInt(Object datum, Encoder out)
        throws IOException, AvroRuntimeException {

        out.writeInt((Integer)datum);
    }

    /**
     * Utility method for writing the 'fixed' data type, for all bindings.
     *
     * @throws AvroRuntimeException if the schema-defined size for a 'fixed'
     * field is not equal to the size of the byte array in the object field
     * being deserialized.
     */
    static void writeFixed(Schema schema, byte[] bytes, Encoder out)
        throws IOException, AvroRuntimeException {

        final int sizeInSchema = schema.getFixedSize();
        if (sizeInSchema != bytes.length) {
            throw new AvroRuntimeException
                ("Schema for 'fixed' type: " + schema.getName() +
                 " has length: " + sizeInSchema +
                 " but byte array has length: " + bytes.length);
        }
        out.writeFixed(bytes, 0, sizeInSchema);
    }
}
