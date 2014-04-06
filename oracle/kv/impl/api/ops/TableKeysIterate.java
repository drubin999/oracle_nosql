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

package oracle.kv.impl.api.ops;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.ArrayList;
import java.util.List;

import oracle.kv.impl.api.StoreIteratorParams;
import oracle.kv.impl.api.table.TargetTables;
import oracle.kv.impl.topo.PartitionId;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

/**
 * Iterate over table keys where the records may or may not reside on
 * the same partition.  PrimaryKey objects are returned which means that
 * matching records are not fetched.
 */
public class TableKeysIterate extends TableIterateOperation {

    public TableKeysIterate(StoreIteratorParams sip,
                            TargetTables targetTables,
                            boolean majorComplete,
                            byte[] resumeKey) {
        super(OpCode.TABLE_KEYS_ITERATE, sip, targetTables,
              majorComplete, resumeKey);
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    TableKeysIterate(ObjectInput in, short serialVersion)
        throws IOException {

        super(OpCode.TABLE_KEYS_ITERATE, in, serialVersion);
    }

    @Override
    public Result execute(Transaction txn,
                          PartitionId partitionId,
                          final OperationHandler operationHandler) {

        verifyTableAccess();

        final List<byte[]> results = new ArrayList<byte[]>();

        final boolean moreElements = iterateTable
            (operationHandler,
             txn,
             partitionId,
             getMajorComplete(),
             getDirection(),
             getBatchSize(),
             getResumeKey(),
             CursorConfig.READ_COMMITTED,
             new OperationHandler.ScanVisitor() {

                 @Override
                 public int visit(Cursor cursor,
                                  DatabaseEntry keyEntry,
                                  DatabaseEntry dataEntry) {

                     /*
                      * 1.  check to see if key is part of table
                      * 2.  if so:
                      *  - call getCurrent to lock the record, using
                      *  the original (partial) data entry.
                      *  - add to results
                      */
                     int match = keyInTargetTable(operationHandler,
                                                  keyEntry,
                                                  dataEntry,
                                                  cursor);
                     if (match > 0) {

                         /*
                          * The iteration was done using READ_UNCOMMITTED so
                          * it's necessary to call getCurrent() here to lock
                          * the record.  The original DatabaseEntry is used
                          * to avoid fetching data.  It had setPartial() called
                          * on it.
                          */
                         assert dataEntry.getPartial();
                         if (cursor.getCurrent
                             (keyEntry, dataEntry,
                              LockMode.DEFAULT) == OperationStatus.SUCCESS) {

                             /*
                              * Add ancestor table results.  These appear
                              * before targets, even for reverse iteration.
                              */
                             match += addAncestorKeys(cursor,
                                                      results,
                                                      keyEntry);
                             addKeyResult(results, keyEntry.getData());
                         }
                     }
                     return match;
                 }
             });
        /*
         * Table iteration filters results on the server side so some records
         * may be skipped.  This voids the moreElements logic in
         * OperationHandler.scan() so if moreElements is true but there are no
         * actual results in the current set, reset moreElements to false.
         */
        boolean more = (moreElements && results.size() == 0) ? false :
            moreElements;
        return new Result.KeysIterateResult(getOpCode(), results, more);
    }
}
