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

import oracle.kv.Direction;
import oracle.kv.KeyRange;
import oracle.kv.impl.api.table.TargetTables;
import oracle.kv.impl.rep.migration.MigrationStreamHandle;
import oracle.kv.impl.topo.PartitionId;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

/**
 * A multi-delete table operation over table(s) in the same partition.
 * This code is shared between normal client multiDelete operations and
 * table data removal code which is internal to RepNodes.  In the latter
 * case there are options that don't apply to client operations:
 * 1.  batch size is allowed
 * 2.  resume key used for batching (returned by a single iteration instance)
 * 3.  major key may be incomplete which is not possible in the client operation
 * This state is added to the object but is only set and used by a separate
 * direct object constructor.
 */
public class MultiDeleteTable extends MultiTableOperation {

    private final byte[] resumeKey;
    private final boolean majorPathComplete;
    private final int batchSize;

    /*
     * This is only used by table data removal to track the last key
     * deleted in order to use it as the resumeKey for batch deletes.
     */
    private byte[] lastDeleted;

    /**
     * Construct a multi-get operation, used by client.
     */
    public MultiDeleteTable(byte[] parentKey,
                            TargetTables targetTables,
                            KeyRange subRange) {
        super(OpCode.MULTI_DELETE_TABLE, parentKey, targetTables, subRange);
        this.majorPathComplete = true;
        this.batchSize = 0;
        this.resumeKey = null;
    }

    /**
     * FastExternalizable constructor.
     */
    MultiDeleteTable(ObjectInput in, short serialVersion)
        throws IOException {

        super(OpCode.MULTI_DELETE_TABLE, in, serialVersion);
        this.majorPathComplete = true;
        this.batchSize = 0;
        this.resumeKey = null;
    }

    /**
     * Construct a MultiDeleteTable operation for internal use by table data
     * removal.  This constructor requires only a single key and target
     * table but also requires batchSize, resumeKey, and majorPathComplete
     * state.  KeyRange does not apply.  Note: KeyRange might
     * be used by a more general-purpose delete mechanism if ever exposed.
     */
    public MultiDeleteTable(byte[] parentKey,
                            long targetTableId,
                            boolean majorPathComplete,
                            int batchSize,
                            byte[] resumeKey) {
        super(OpCode.MULTI_DELETE_TABLE, parentKey,
              new TargetTables(targetTableId), null);
        this.majorPathComplete = majorPathComplete;
        this.batchSize = batchSize;
        this.resumeKey = resumeKey;
    }

    @Override
    public Result execute(Transaction txn,
                          PartitionId partitionId,
                          final OperationHandler operationHandler) {

        verifyTableAccess();

        /*
         * This should really be an int but it needs to be accessible from
         * the inner class below which mean it just be final.  A single-element
         * array is used to make this possible.
         */
        final int[] nDeletions = new int[1];

        final boolean moreElements = iterateTable
            (operationHandler,
             txn,
             partitionId,
             majorPathComplete,
             Direction.FORWARD,
             batchSize,
             resumeKey,
             CursorConfig.READ_COMMITTED,
             new OperationHandler.ScanVisitor() {

                 @Override
                 public int visit(Cursor cursor,
                                  DatabaseEntry keyEntry,
                                  DatabaseEntry dataEntry) {

                     /*
                      * 1.  check to see if key is part of table
                      * 2.  if so, delete the record.
                      */
                     int match = keyInTargetTable(operationHandler,
                                                  keyEntry,
                                                  dataEntry,
                                                  cursor);
                     if (match > 0) {
                         lastDeleted = keyEntry.getData();
                         if (cursor.delete() == OperationStatus.SUCCESS) {
                             int num = 1;
                             /*
                              * If this is a client-driven operation the
                              * migration stream needs to be considered.
                              */
                             if (isClientOperation()) {
                                 MigrationStreamHandle.get().
                                     addDelete(keyEntry);
                                 num += deleteAncestorKeys(cursor, keyEntry);
                             }
                             nDeletions[0] += num;
                         }
                     }
                     return match;
                 }
             });

        assert (!moreElements || batchSize > 0);
        return new Result.MultiDeleteResult(getOpCode(), nDeletions[0]);
    }

    public byte[] getLastDeleted() {
        return lastDeleted;
    }

    /*
     * The internal table removal code will set a non-zero batchSize.
     */
    private boolean isClientOperation() {
        return batchSize == 0;
    }
}
