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
import java.util.Collections;
import java.util.Map;

import oracle.kv.Value;
import oracle.kv.avro.RawAvroBinding;
import oracle.kv.avro.RawRecord;
import oracle.kv.avro.SchemaNotAllowedException;
import oracle.kv.avro.SpecificAvroBinding;
import oracle.kv.avro.UndefinedSchemaException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

/**
 * Provides a straightforward mapping from the built-in Avro specific classes
 * to our specific binding API.
 */
class SpecificBinding<T extends SpecificRecord>
    implements SpecificAvroBinding<T> {

    /**
     * A raw binding is used for packaging and unpackaging the Avro raw data
     * bytes and their associated writer schema.
     */
    private final RawAvroBinding rawBinding;

    private final Map<String, Schema> allowedSchemas;

    SpecificBinding(AvroCatalogImpl catalog, Class<T> allowedCls)
        throws UndefinedSchemaException {

        this.rawBinding = catalog.getRawBinding();

        if (allowedCls != null) {
            final Schema schema = SpecificData.get().getSchema(allowedCls);
            this.allowedSchemas =
                Collections.singletonMap(schema.getFullName(), schema);
            /* May throw UndefinedSchemaException. */
            catalog.checkDefinedSchemas(allowedSchemas);
        } else {
            this.allowedSchemas = null;
        }
    }

    /**
     * Straightforward deserialization to SpecificRecord using RawBinding,
     * BinaryDecoder and SpecificDatumReader.
     */
    @Override
    public T toObject(Value value)
        throws SchemaNotAllowedException, IllegalArgumentException {

        final RawRecord raw = rawBinding.toObject(value);
        final Schema writerSchema = raw.getSchema();
        final Schema readerSchema;
        if (allowedSchemas != null) {
            /* May throw SchemaNotAllowedException. */
            readerSchema = AvroCatalogImpl.checkToObjectSchema(writerSchema,
                                                               allowedSchemas);
        } else {
            final SpecificData specificData = SpecificData.get();
            final Class<?> cls = specificData.getClass(writerSchema);
            readerSchema = specificData.getSchema(cls);
        }

        final SpecificDatumReader<T> reader =
            new SpecificDatumReader<T>(writerSchema, readerSchema);
        final Decoder decoder =
            DecoderFactory.get().binaryDecoder(raw.getRawData(), null);

        try {
            return reader.read(null, decoder);
        } catch (Exception e) {
            throw new IllegalArgumentException
                ("Unable to deserialize SpecificRecord", e);
        }
    }

    /**
     * Straightforward serialization of SpecificRecord using RawBinding,
     * BinaryEncoder and SpecificWriter (our subclass of SpecificDatumWriter).
     */
    @Override
    public Value toValue(T object)
        throws SchemaNotAllowedException, UndefinedSchemaException,
               IllegalArgumentException {

        final Schema writerSchema = object.getSchema();
        if (allowedSchemas != null) {
            /* May throw SchemaNotAllowedException. */
            AvroCatalogImpl.checkToValueSchema(writerSchema, allowedSchemas);
        }

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final SpecificWriter<T> writer = new SpecificWriter<T>(writerSchema);
        final Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);

        try {
            writer.write(object, encoder);
            encoder.flush();
        } catch (Exception e) {
            throw new IllegalArgumentException
                ("Unable to serialize SpecificRecord", e);
        }

        final RawRecord raw = new RawRecord(out.toByteArray(), writerSchema);
        /* May throw UndefinedSchemaException. */
        return rawBinding.toValue(raw);
    }

    /**
     * Subclass of SpecificDatumWriter to implement special rules for certain
     * data types.
     */
    private static class SpecificWriter<T> extends SpecificDatumWriter<T> {

        SpecificWriter(Schema writerSchema) {
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
    }
}
