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

import java.util.Arrays;

import org.apache.avro.Schema;

/**
 * A RawRecord represents an Avro object as a Schema along with the raw Avro
 * serialized data. It is used with a {@link RawAvroBinding}.
 *
 * @see RawAvroBinding
 * @see AvroCatalog#getRawBinding getRawBinding
 *
 * @since 2.0
 */
public class RawRecord {
    private final byte[] rawData;
    private final Schema schema;

    /**
     * Creates a RawRecord from a Schema and Avro serialized data.
     */
    public RawRecord(byte[] rawData, Schema schema) {
        this.rawData = rawData;
        this.schema = schema;
    }

    /**
     * Returns the Avro serialized data for this RawRecord.
     */
    public byte[] getRawData() {
        return rawData;
    }

    /**
     * Returns the Avro Schema for this RawRecord.
     */
    public Schema getSchema() {
        return schema;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof RawRecord)) {
            return false;
        }
        final RawRecord o = (RawRecord) other;
        return Arrays.equals(rawData, o.rawData) && schema.equals(o.schema);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(rawData);
    }

    @Override
    public String toString() {
        return Arrays.toString(rawData) + "\nSchema: " + schema.toString();
    }
}
