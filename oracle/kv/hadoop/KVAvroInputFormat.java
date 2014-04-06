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

import java.io.IOException;

import oracle.kv.Key;

import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * A Hadoop InputFormat class for reading data from Oracle NoSQL Database and
 * returning Avro IndexedRecords as the map-reduce value and the record's Key
 * as the map-reduce key.
 * <p>
 * One use for this class is to read NoSQL Database records into Oracle Loader
 * for Hadoop.
 * <p>
 * Refer to the javadoc for {@link KVInputFormatBase} for information on the
 * parameters that may be passed to this class.
 *
 * @since 2.0
 */
public class KVAvroInputFormat
    extends KVInputFormatBase<Key, IndexedRecord> {

    /**
     * @hidden
     */
    @Override
    public RecordReader<Key, IndexedRecord>
        createRecordReader(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {

        KVAvroRecordReader ret = new KVAvroRecordReader();
        ret.initialize(split, context);
        return ret;
    }
}
