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
import oracle.kv.Value;
import oracle.kv.avro.AvroCatalog;
import oracle.kv.avro.GenericAvroBinding;

import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * @hidden
 */
public class KVAvroRecordReader
    extends KVRecordReaderBase<Key, IndexedRecord> {

    private Class<?> formatterClass;
    private AvroFormatter formatter = null;
    private GenericAvroBinding binding;

    /**
     * Called once at initialization.
     * @param split the split that defines the range of records to read
     * @param context the information about the task
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {

        KVInputSplit kvInputSplit = (KVInputSplit) split;
        super.initialize(split, context);

        String formatterClassName = kvInputSplit.getFormatterClassName();
        if (formatterClassName != null &&
            !"".equals(formatterClassName)) {
            try {
                formatterClass = Class.forName(formatterClassName);
                formatter = (AvroFormatter) formatterClass.newInstance();
            } catch (Exception E) {
                IllegalArgumentException iae = new IllegalArgumentException
                    ("Couldn't find formatter class: " +
                     formatterClassName);
                iae.initCause(E);
                throw iae;
            }
        }
        AvroCatalog catalog = kvstore.getAvroCatalog();
        binding = catalog.getGenericMultiBinding(catalog.getCurrentSchemas());
    }

    /**
     * Get the current value
     * @return the object that was read
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public IndexedRecord getCurrentValue()
        throws IOException, InterruptedException {

        if (current == null) {
            return null;
        }

        if (formatter != null) {
            return formatter.toAvroRecord(current, kvstore);
        }

        /* Key unusedKey = current.getKey(); */
        Value value = current.getValue();

        IndexedRecord record = null;
        if (value != null) {
            record = binding.toObject(value);
        }

        return record;
    }

    /**
     * Get the current key.
     * @return the current key or null if there is no current key
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public Key getCurrentKey()
        throws IOException, InterruptedException {

        if (current == null) {
            return null;
        }

        return current.getKey();
    }
}
