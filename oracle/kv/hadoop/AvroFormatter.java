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

package oracle.kv.hadoop;

import oracle.kv.KeyValueVersion;
import oracle.kv.KVStore;

import org.apache.avro.generic.IndexedRecord;

/**
 * Avro Formatter is an interface implemented by user-specified classes that
 * can format NoSQL Database records into AvroRecords to be returned by a
 * KVAvroRecordReader. {@link #toAvroRecord} is called once for each record
 * retrieved by a KVAvroInputFormat.
 *
 * @since 2.0
 */
public interface AvroFormatter {

    /**
     * Convert a KeyValueVersion into a String.
     *
     * @param kvv the Key and Value to be formatted.
     *
     * @param kvstore the KV Store object related to this record so that the
     * Formatter may retrieve (e.g.) Avro bindings.
     *
     * @return The Avro record suitable for interpretation by the caller
     * of the KVAvroInputFormat.
     */
    public IndexedRecord toAvroRecord(KeyValueVersion kvv, KVStore kvstore);
}
