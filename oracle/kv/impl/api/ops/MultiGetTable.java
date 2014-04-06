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

import oracle.kv.Direction;
import oracle.kv.KeyRange;
import oracle.kv.impl.api.table.TargetTables;
import oracle.kv.impl.topo.PartitionId;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

/**
 * A multi-get table operation over a set of records in the same partition.
 * Entire Row records are returned which means that the data is fetched
 * from matching keys.
 */
public class MultiGetTable extends MultiTableOperation {

    /**
     * Construct a multi-get operation.
     */
    public MultiGetTable(byte[] parentKey,
                         TargetTables targetTables,
                         KeyRange subRange) {
        super(OpCode.MULTI_GET_TABLE, parentKey, targetTables, subRange);
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    MultiGetTable(ObjectInput in, short serialVersion)
        throws IOException {

        super(OpCode.MULTI_GET_TABLE, in, serialVersion);
    }

    @Override
    public Result execute(Transaction txn,
                          PartitionId partitionId,
                          final OperationHandler operationHandler) {

        verifyTableAccess();

        final List<ResultKeyValueVersion> results =
            new ArrayList<ResultKeyValueVersion>();

        final boolean moreElements = iterateTable
            (operationHandler,
             txn,
             partitionId,
             true /*majorPathComplete*/,
             Direction.FORWARD,
             0 /*batchSize*/,
             null /*resumeKey*/,
             CursorConfig.READ_COMMITTED,
             new OperationHandler.ScanVisitor() {

                 @Override
                 public int visit(Cursor cursor,
                                  DatabaseEntry keyEntry,
                                  DatabaseEntry dataEntry) {

                     /*
                      * 1.  check to see if key is part of table
                      * 2.  if so:
                      *    - fetch data
                      *    - add to results
                      */
                     int match = keyInTargetTable(operationHandler,
                                                  keyEntry,
                                                  dataEntry,
                                                  cursor);
                     if (match > 0) {

                         final DatabaseEntry dentry = new DatabaseEntry();
                         if (cursor.getCurrent
                             (keyEntry, dentry,
                              LockMode.DEFAULT) == OperationStatus.SUCCESS) {

                             /*
                              * Filter out non-table data.
                              */
                             if (!isTableData(dentry.getData(), null)) {
                                 return 0;
                             }

                             /*
                              * Add ancestor table results.  These appear
                              * before targets, even for reverse iteration.
                              */
                             match += addAncestorValues(cursor,
                                                        results,
                                                        keyEntry);

                             addValueResult(operationHandler, results,
                                            cursor, keyEntry, dentry);
                         }
                     }
                     return match;
                 }
             });

        assert (!moreElements);
        return new Result.IterateResult(getOpCode(), results, moreElements);
    }
}
