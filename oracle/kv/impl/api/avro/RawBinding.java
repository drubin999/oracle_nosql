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
import oracle.kv.avro.RawAvroBinding;
import oracle.kv.avro.RawRecord;
import oracle.kv.avro.UndefinedSchemaException;
import oracle.kv.impl.api.avro.SchemaCache;
import oracle.kv.impl.api.avro.SchemaInfo;

import com.sleepycat.util.PackedInteger;

/**
 * This class forms the basis for other binding implementations in that it is
 * responsible for packaging a schema ID and Avro binary data into a Value byte
 * array, and unpackaging it.  It also ensures that the toObject method throws
 * IllegalArgumentException when the schema ID is invalid, and that the toValue
 * method throws UndefinedSchemaException when the schema is unknown.
 */
class RawBinding implements RawAvroBinding {

    private final SchemaCache schemaCache;
    
    RawBinding(SchemaCache schemaCache) {
        this.schemaCache = schemaCache;
    }

    @Override
    public RawRecord toObject(Value value)
        throws IllegalArgumentException {

        /* Derive schema from schema ID in value byte array. */
        final Schema schema = getValueSchema(value, schemaCache);

        /* Copy raw data out of value byte array. */
        final byte[] buf = value.getValue();
        final int dataOffset = getValueRawDataOffset(value);
        final byte[] rawData = new byte[buf.length - dataOffset];
        System.arraycopy(buf, dataOffset, rawData, 0, rawData.length);

        return new RawRecord(rawData, schema);
    }

    /**
     * Shared by toObject and SharedCache.CBindingBridgeImpl.
     * See CBindingBridge.getValueRawDataOffset.
     */
    static int getValueRawDataOffset(Value value) {
        return PackedInteger.getReadSortedIntLength(value.getValue(), 0);
    }

    /**
     * Shared by toObject and SharedCache.CBindingBridgeImpl.
     * See CBindingBridge.getValueSchema.
     */
    static Schema getValueSchema(Value value, SchemaCache schemaCache)
        throws IllegalArgumentException {

        /* Check value format. */
        if (value.getFormat() != Value.Format.AVRO) {
            throw new IllegalArgumentException("Value.Format is not AVRO");
        }

        /* Unpackage schema ID and raw data (Avro binary data). */
        final byte[] buf = value.getValue();
        final int schemaId;
        try {
            schemaId = PackedInteger.readSortedInt(buf, 0);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException
                ("Internal schema ID in Value is invalid, possibly due to" +
                 " incorrect binding usage", e);
        }

        /* Get schema and check for invalid schema ID. */
        final SchemaInfo info = schemaCache.getSchemaInfoById(schemaId);
        if (info == null) {
            throw new IllegalArgumentException
                ("Internal schema ID in Value is unknown, possibly due to" +
                 " incorrect binding usage; schemaId: " + schemaId);
        }

        return info.getSchema();
    }

    @Override
    public Value toValue(RawRecord object)
        throws UndefinedSchemaException {

        final Schema schema = object.getSchema();
        final byte[] rawData = object.getRawData();
        final int rawDataSize = rawData.length;

        /* Allocate value byte array and copy in schema ID. */
        final Value value = allocateValue(schema, rawDataSize, schemaCache);

        /* Copy in raw data. */
        final int dataOffset = getValueRawDataOffset(value);
        final byte[] buf = value.getValue();
        System.arraycopy(rawData, 0, buf, dataOffset, rawDataSize);

        return value;
    }

    /**
     * Shared by toValue and SharedCache.CBindingBridgeImpl.
     * See CBindingBridge.allocateValue.
     */
    static Value allocateValue(Schema schema,
                               int rawDataSize,
                               SchemaCache schemaCache)
        throws UndefinedSchemaException {

        /* Get schema ID and check for undefined schema. */
        final SchemaInfo info = schemaCache.getSchemaInfoByValue(schema);
        if (info == null) {
            throw AvroCatalogImpl.newUndefinedSchemaException(schema);
        }
        final int schemaId = info.getId();

        /* Allocate room for schema ID and raw data (Avro binary data). */
        final int dataOffset = PackedInteger.getWriteSortedIntLength(schemaId);
        final byte[] buf = new byte[dataOffset + rawDataSize];

        /* Copy in the schema ID. */
        PackedInteger.writeSortedInt(buf, 0, schemaId);

        return Value.internalCreateValue(buf, Value.Format.AVRO);
    }
}
