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

import oracle.kv.FaultException;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.IndexKeyImpl;
import oracle.kv.impl.api.table.IndexRange;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.table.Table;

import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.Transaction;

/**
 * All table scan operations (primary key, index key) are implemented by this
 * class.
 */
public class TableOperationHandler {
    static int STOP = -1;
    static int CONTINUE = -2;

    /**
     * Called by indexScan for each selected record.
     * @returns the number of records added to the result set.  If return
     * is STOP then the scan is complete and should terminate.
     */
    interface IndexScanVisitor {
        int visit(SecondaryCursor cursor,
                  DatabaseEntry indexKeyEntry,
                  DatabaseEntry primaryKeyEntry,
                  DatabaseEntry dataEntry);

        /**
         * Abstract the iteration to allow the caller to handle
         * duplicates vs full scans.
         */
        OperationStatus getNext(SecondaryCursor cursor,
                                DatabaseEntry indexKeyEntry,
                                DatabaseEntry primaryKeyEntry,
                                DatabaseEntry dataEntry);

        OperationStatus getPrev(SecondaryCursor cursor,
                                DatabaseEntry indexKeyEntry,
                                DatabaseEntry primaryKeyEntry,
                                DatabaseEntry dataEntry);
    }

    /**
     * Perform an index scan.
     *
     * Does child/ancestor table logic really make sense for indexes?  Yes, it
     * does. It might be used for "return all rows from a non-target table
     * where an index match exists for an index on a parent/ancestor table."  A
     * couple of example use cases are:
     *   1.  return all email addresses (nested table) for users in city
     *      "foo", where  city is a field in User table and indexed.
     *   2.  return all users (User parent table) whose email provider is
     *      "yahoo" where email provider is an indexed field in a nested Email
     *      table.
     */
    static boolean indexScan(Transaction txn,
                             SecondaryDatabase db,
                             IndexImpl index,
                             IndexRange range,
                             byte[] resumeSecondaryKey,
                             byte[] resumePrimaryKey,
                             boolean noData,
                             int batchSize,
                             IndexScanVisitor visitor) {
        /*
         * Always use READ_COMMITTED for CursorConfig and set non-cloning.
         * These are necessary to avoid JE deadlocks.
         */
        int nRecords = 0;
        final SecondaryCursor cursor =
            db.openCursor(txn, CursorConfig.READ_COMMITTED);
        DbInternal.setNonCloning(cursor, true);
        try {
            if (range.isReverse()) {
                return reverseScan(cursor, index, range, resumeSecondaryKey,
                                   resumePrimaryKey, noData,
                                   batchSize, visitor);
            }

            /*
             * FORWARD and UNORDERED scans are always forward.
             */
            byte[] startKey = range.getStartKey();
            DatabaseEntry keyEntry;
            DatabaseEntry pkeyEntry;
            final DatabaseEntry dataEntry = new DatabaseEntry();
            OperationStatus status;
            if (noData) {
                dataEntry.setPartial(0, 0, true);
            }

            /*
             * If resuming an iteration, start there.  It overrides the
             * startKey.  A resumeKey doesn't mean that it isn't an exact
             * match query.  It's possible that batch size is < number of
             * duplicates.
             */
            if (resumeSecondaryKey != null) {
                keyEntry = new DatabaseEntry(resumeSecondaryKey);
                pkeyEntry = new DatabaseEntry(resumePrimaryKey);
                status = resume(cursor, range, keyEntry,
                                pkeyEntry, dataEntry, visitor);
            } else if (startKey != null) {
                keyEntry = new DatabaseEntry(startKey);
                pkeyEntry = new DatabaseEntry();
                if (range.getExactMatch()) {
                    status = cursor.getSearchKey(keyEntry,
                                                 pkeyEntry,
                                                 dataEntry,
                                                 LockMode.DEFAULT);
                } else {
                    status = cursor.getSearchKeyRange(keyEntry,
                                                      pkeyEntry,
                                                      dataEntry,
                                                      LockMode.DEFAULT);
                }
            } else {
                assert !range.getExactMatch();
                keyEntry = new DatabaseEntry();
                pkeyEntry = new DatabaseEntry();
                status = cursor.getFirst(keyEntry,
                                         pkeyEntry,
                                         dataEntry,
                                         LockMode.DEFAULT);
            }

            while (status == OperationStatus.SUCCESS) {
                int num = visitor.visit(cursor, keyEntry, pkeyEntry, dataEntry);
                if (num == CONTINUE) {
                    continue;
                }
                if (num == STOP) {
                    break;
                }
                nRecords += num;
                if (batchSize > 0 && nRecords >= batchSize) {
                    return true;
                }
                status = visitor.getNext(cursor,
                                         keyEntry,
                                         pkeyEntry,
                                         dataEntry);
            }
        } finally {
            TxnUtil.close(cursor);
        }
        return false;
    }

    /**
     * Do a reverse index scan.  Positioning of the cursor is tricky
     * for this case.  "start" is the end of the scan and "end" is the start.
     * A resumeKey is equivalent to the end (which is inclusive).
     * 1.  No range contraints.  Start at the end and move backwards.  This
     * happens when there is no end key and no prefix.
     * 2.  Start only.  Start at the end and move backwards to start.  This
     * happens when start is set and there is no prefix
     * 3.  End only.  Find end, move backwards.  This happens when there is
     * an end key and no prefix.
     * 4.  Prefix serves and both start and/or end.  When specified with a
     * start key only it is used to find the end of the range.  When specified
     * with an end key it is used to terminate the iteration.  When specified
     * alone it is both the start and the end.
     */
    private static boolean reverseScan(final SecondaryCursor cursor,
                                       IndexImpl index,
                                       IndexRange range,
                                       byte[] resumeSecondaryKey,
                                       byte[] resumePrimaryKey,
                                       boolean noData,
                                       int batchSize,
                                       IndexScanVisitor visitor) {
        byte[] endKey = range.getEndKey();
        DatabaseEntry keyEntry;
        DatabaseEntry pkeyEntry;
        final DatabaseEntry dataEntry = new DatabaseEntry();
        OperationStatus status;
        int nRecords = 0;
        if (noData) {
            dataEntry.setPartial(0, 0, true);
        }

        if (resumeSecondaryKey != null) {
            keyEntry = new DatabaseEntry(resumeSecondaryKey);
            pkeyEntry = new DatabaseEntry(resumePrimaryKey);
            status = resume(cursor, range, keyEntry,
                            pkeyEntry, dataEntry, visitor);
        } else if (endKey != null) {
            /*
             * End keys are exclusive.  Move to record >= the key and if there
             * is a match, move to the previous record.
             */
            keyEntry = new DatabaseEntry(endKey);
            pkeyEntry = new DatabaseEntry();
            status = cursor.getSearchKeyRange(keyEntry,
                                              pkeyEntry,
                                              dataEntry,
                                              LockMode.DEFAULT);
            if (status == OperationStatus.SUCCESS) {
                status = visitor.getPrev(cursor, keyEntry, pkeyEntry,
                                         dataEntry);
            }
        } else if (range.getPrefixKey() != null) {
            keyEntry = new DatabaseEntry();
            pkeyEntry = new DatabaseEntry();
            status = getEndFromPrefix(cursor, index, range, keyEntry,
                                      pkeyEntry, dataEntry);
        } else {
            /*
             * This is either a complete index iteration or an iteration
             * without a bounded end.  In both cases start at the last
             * record in the index.
             */
            keyEntry = new DatabaseEntry();
            pkeyEntry = new DatabaseEntry();
            status = cursor.getLast(keyEntry,
                                    pkeyEntry,
                                    dataEntry,
                                    LockMode.DEFAULT);
        }
        while (status == OperationStatus.SUCCESS) {
            int num = visitor.visit(cursor, keyEntry, pkeyEntry, dataEntry);
            if (num == CONTINUE) {
                continue;
            }
            if (num == STOP) {
                break;
            }
            nRecords += num;
            if (batchSize > 0 && nRecords >= batchSize) {
                return true;
            }
            status = visitor.getPrev(cursor,
                                     keyEntry,
                                     pkeyEntry,
                                     dataEntry);
        }
        return false;
    }

    /**
     * // get next duplicate >= target
     * status = cursor.getSearchBothRange();
     * if (notfound)
     *   if (exact match) return NOTFOUND
     *   // use only secondary key to find next
     *   status = cursor.getSearchKeyRange();
     * if (found)
     *   if (forward)
     *     return (if eq target, cursor.next(), else cursor points at target)
     *   // reverse
     *     return cursor.prev() (regardless of whether it's pointing at target)
     */
    private static OperationStatus resume(final SecondaryCursor cursor,
                                          final IndexRange range,
                                          DatabaseEntry keyEntry,
                                          DatabaseEntry pkeyEntry,
                                          DatabaseEntry dataEntry,
                                          IndexScanVisitor visitor) {
        /*
         * Copy original entries for later comparison
         */
        final DatabaseEntry origKey = new DatabaseEntry(keyEntry.getData());
        final DatabaseEntry origPkey = new DatabaseEntry(pkeyEntry.getData());

        /*
         * Match next duplicate >= to both keys.
         */
        OperationStatus status =
            cursor.getSearchBothRange(keyEntry, pkeyEntry, dataEntry,
                                      LockMode.DEFAULT);

        if (status == OperationStatus.NOTFOUND) {
            /*
                 * No more duplicates.  If it's an exact match operation we are
                 * done; otherwise fall back to using the secondary key only.
                 */
                if (range.getExactMatch()) {
                    return status;
                }
                status =
                    cursor.getSearchKeyRange(keyEntry, pkeyEntry, dataEntry,
                                             LockMode.DEFAULT);
        }
        if (status == OperationStatus.SUCCESS) {
            /*
             * Use the Visitor methods for resuming navigation.  They
             * handle duplicates (exact match scans) vs not using duplicates.
             *
             * If reverse always return the previous record
             */
            if (range.isReverse()) {
                status = visitor.getPrev(cursor, keyEntry,
                                         pkeyEntry, dataEntry);
            } else {
                /*
                 * If the entry is the original record move to next, otherwise
                 * the cursor is at the desired record.
                 */
                if (pkeyEntry.equals(origPkey) && keyEntry.equals(origKey)) {
                    status =
                        visitor.getNext(cursor, keyEntry, pkeyEntry, dataEntry);
                }
            }
        }
        return status;
    }

    /**
     * Get the last matching record from a prefix.  This is a reverse scan
     * that has a prefix but no end key so the end is implicitly at the
     * end of the prefix.  Index field serialization does not include
     * explicit separators (like primary key serialization) but it does have
     * schema available and prefixes are complete, valid fields.  The algorithm
     * is:
     * 1.  deserialize the prefix to IndexKeyImpl
     * 2.  add "one" to the index key using IndexKeyImpl.incrementLastField().
     * 3.  reserialize and use this as an exclusive key.
     */
    private static OperationStatus getEndFromPrefix
        (final SecondaryCursor cursor,
         final IndexImpl index,
         final IndexRange range,
         DatabaseEntry keyEntry,
         DatabaseEntry pkeyEntry,
         DatabaseEntry dataEntry) {

        assert range.getPrefixKey() != null;

        /*
         * Deserialize
         */
        IndexKeyImpl indexKey =
            index.rowFromIndexKey(range.getPrefixKey(), true);

        OperationStatus status = OperationStatus.NOTFOUND;

        /*
         * Increment the last field with a value in the index key.  If this
         * returns false then there are no further keys so go to the end of
         * the index.
         */
        if (indexKey.incrementIndexKey()) {

            /*
             * Reserialize
             */
            byte[] bytes = index.serializeIndexKey(indexKey);

            /*
             * Look for end.  This is an exclusive value.  Prefixes in this case are,
             * by definition inclusive.  End keys can only be exclusive if explicitly
             * declared and that path does not call this function.
             */
            keyEntry.setData(bytes);

            /*
             * Match record >= exclusive key
             */
            status =
                cursor.getSearchKeyRange(keyEntry, pkeyEntry, dataEntry,
                                         LockMode.DEFAULT);
        }

        if (status == OperationStatus.NOTFOUND) {
            /*
             * Nothing there, go to the end of the index
             */
            return cursor.getLast(keyEntry,
                                  pkeyEntry,
                                  dataEntry,
                                  LockMode.DEFAULT);
        }
        assert status == OperationStatus.SUCCESS;
        /*
         * Don't call the Visitor's getPrev() method.  It will special-case
         * range.getExactMatch() which assumes that the operation is on
         * duplicates.  This path needs to find the previous record, duplicate
         * or otherwise.
         */
        status = cursor.getPrev(keyEntry, pkeyEntry,
                                dataEntry, LockMode.DEFAULT);
        return status;
    }

    /**
     * Verifies that the table exists.  If it does not an exception
     * is thrown.  This method is called by single-key methods such as get
     * and put when they are operating in a table's key space.  It prevents
     * clients who are not yet aware of a table's removal from doing operations
     * on the now-removed table.
     */
    static void checkTable(final OperationHandler operationHandler,
                           final long tableId) {
        if (tableId == 0) {
            return;
        }
        Table table = getTable(operationHandler, tableId);
        if (table == null) {
            throw new FaultException
                ("Cannot access table.  It may not " +
                 "exist, id: " + tableId, true);
        }
    }

    /**
     * Returns the table associated with the table id, or null if the
     * table with the id does not exist.
     */
    static Table getTable(OperationHandler operationHandler,
                          long tableId) {
        return operationHandler.getRepNode().getTable(tableId);
    }
}
