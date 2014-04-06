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

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.FaultException;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.ReturnValueVersion.Choice;
import oracle.kv.Version;
import oracle.kv.impl.api.lob.KVLargeObjectImpl;
import oracle.kv.impl.fault.WrappedClientException;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.RepNodeService;
import oracle.kv.impl.rep.migration.MigrationStreamHandle;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.impl.util.UserDataControl;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.RangeConstraint;
import com.sleepycat.je.dbi.RecordVersion;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.utilint.VLSN;

/**
 * All user level operations, e.g. get/put/delete, etc. are implemented by this
 * class.
 */
public class OperationHandler {

    /** Lowest possible value for a serialized key character. */
    private static final byte MIN_VALUE_BYTE = ((byte) 1);
    /** Lowest possible value for a serialized key character. */
    private static final char MIN_VALUE_CHAR = ((char) 1);

    /** Timeout for getting a valid ReplicatedEnvironment. */
    private static final int ENV_TIMEOUT_MS = 5000;

    /** Minimum possible key. */
    private static final byte[] MIN_KEY = new byte[0];

    /** Used to avoid fetching data for a key-only operation. */
    private static final DatabaseEntry NO_DATA = new DatabaseEntry();
    static {
        NO_DATA.setPartial(0, 0, true);
    }

    /** Used as an empty value DatabaseEntry */
    private static final DatabaseEntry EMPTY_DATA =
        new DatabaseEntry(new byte[0]);

    /** Same key comparator as used for KV keys */
    static final Comparator<byte[]> KEY_BYTES_COMPARATOR =
        new Key.BytesComparator();

    /** This ReplicatedEnvironment node. */
    private final RepNode repNode;

    /** The RepNode's UUID. */
    private UUID repNodeUUID;

    /**
     * boolean to indicate that it is ok to write empty values
     *
     * TODO: post-R3 -- remove this condition because all nodes
     * will have been updated.  This is here in case the necessary
     * code makes it into R3.
     */
    private boolean useEmptyValue;

    /**
     * The logger is available to classes to log notable situations to the
     * server node's (RepNode) log.
     */
    private final Logger logger;

    /**
     * Provides the interface by which key-value entries can be selectively
     * hidden from view.
     */
    interface KVAuthorizer {
        /**
         * Should access to the entry be allowed?
         */
        public boolean allowAccess(DatabaseEntry keyEntry);

        /**
         * Will the authorizer return true for all entries?
         */
        public boolean allowFullAccess();
    }

    public OperationHandler(RepNode repNode, RepNodeService.Params params) {

        this.repNode = repNode;

        /* hard-coded to false until next release (post R3) */
        useEmptyValue = false;

        logger = LoggerUtils.getLogger(this.getClass(), params);
        assert logger != null;
    }

    /**
     * Gets the logger for this instance.
     */
    Logger getLogger() {
        return logger;
    }

    /**
     * Gets the value associated with the key.
     */
    ResultValueVersion get(Transaction txn,
                           PartitionId partitionId,
                           byte[] keyBytes) {

        assert (keyBytes != null);

        final Database db = repNode.getPartitionDB(partitionId);
        final DatabaseEntry dataEntry = new DatabaseEntry();
        final DatabaseEntry keyEntry = new DatabaseEntry(keyBytes);
        final Cursor cursor = db.openCursor(txn, null);
        DbInternal.setNonCloning(cursor, true);
        try {
            final OperationStatus status =
                cursor.getSearchKey(keyEntry, dataEntry, LockMode.DEFAULT);
            if (status != OperationStatus.SUCCESS) {
                return null;
            }
            return makeValueVersion(cursor, dataEntry);
        } finally {
            TxnUtil.close(cursor);
        }
    }

    /**
     * Iterate and return the next batch.
     *
     * @return the key at which to resume the iteration, or null if complete.
     */
    boolean iterate(Transaction txn,
                    PartitionId partitionId,
                    byte[] parentKey,
                    boolean majorPathComplete,
                    KeyRange subRange,
                    Depth depth,
                    Direction direction,
                    int batchSize,
                    byte[] resumeKey,
                    CursorConfig cursorConfig,
                    final List<ResultKeyValueVersion> results,
                    final KVAuthorizer auth) {

        final boolean moreElements = scan
            (txn, partitionId, parentKey, majorPathComplete, subRange, depth,
             false /*noData*/, resumeKey, batchSize, cursorConfig,
             LockMode.DEFAULT, direction, new ScanVisitor() {

            @Override
            public int visit(Cursor cursor,
                             DatabaseEntry keyEntry,
                             DatabaseEntry dataEntry) {
                if (!auth.allowAccess(keyEntry)) {

                    /*
                     * The requestor is not permitted to see this entry.  The
                     * checks done at the start of the operation would have
                     * thrown UnauthorizedException if an explicit reference
                     * to the protected entry had been made, so the caller has
                     * stumbled across an entry that they are not authorized to
                     * see, so we silently skip it.
                     */
                    return 0;
                }

                final ResultValueVersion valVers =
                    makeValueVersion(cursor, dataEntry);
                results.add(new ResultKeyValueVersion(keyEntry.getData(),
                                                      valVers.getValueBytes(),
                                                      valVers.getVersion()));
                return 1;
            }
        });

        return moreElements;
    }

    /**
     * Iterate and return the next batch.
     *
     * @return the key at which to resume the iteration, or null if complete.
     */
    boolean iterateKeys(Transaction txn,
                        PartitionId partitionId,
                        byte[] parentKey,
                        boolean majorPathComplete,
                        KeyRange subRange,
                        Depth depth,
                        Direction direction,
                        int batchSize,
                        byte[] resumeKey,
                        CursorConfig cursorConfig,
                        final List<byte[]> results,
                        final KVAuthorizer auth) {

        final boolean moreElements = scan
            (txn, partitionId, parentKey, majorPathComplete, subRange, depth,
             true /*noData*/, resumeKey, batchSize, cursorConfig,
             LockMode.DEFAULT, direction, new ScanVisitor() {

            @Override
            public int visit(Cursor cursor,
                             DatabaseEntry keyEntry,
                             DatabaseEntry dataEntry) {
                if (!auth.allowAccess(keyEntry)) {

                    /*
                     * The requestor is not permitted to see this entry, so
                     * silently skip it.
                     */
                    return 0;
                }
                results.add(keyEntry.getData());
                return 1;
            }
        });

        return moreElements;
    }

    /**
     * Deletes the keys in a multi-key scan.
     */
    int multiDelete(Transaction txn,
                    PartitionId partitionId,
                    byte[] parentKey,
                    KeyRange subRange,
                    Depth depth,
                    final byte[] lobSuffixBytes,
                    final KVAuthorizer auth) {

        /*
         * This should really be an int but it needs to be accessible from
         * the inner class below which mean it just be final.  A single-element
         * array is used to make this possible.
         */
        final int[] nDeletions = new int[1];

        final boolean moreElements = scan
            (txn, partitionId, parentKey, true /*majorPathComplete*/, subRange,
             depth, true /*noData*/, null /*resumeKey*/, 0 /*batchSize*/,
             CursorConfig.DEFAULT, LockMode.RMW, Direction.FORWARD,
             new ScanVisitor() {

                 @Override
                 public int visit(Cursor cursor,
                                  DatabaseEntry keyEntry,
                                  DatabaseEntry dataEntry) {
                     if (!auth.allowAccess(keyEntry)) {

                         /*
                          * The requestor is not permitted to see this entry, so
                          * silently skip it.
                          */
                         return 0;
                     }

                     if (KVLargeObjectImpl.hasLOBSuffix(keyEntry.getData(),
                                                        lobSuffixBytes)) {
                         final String msg =
                             "Operation: multiDelete" +
                             " Illegal LOB key argument: " +
                             UserDataControl.displayKey(keyEntry.getData()) +
                             ". Use LOB-specific APIs to modify a " +
                             "LOB key/value pair.";
                         throw new WrappedClientException
                             (new IllegalArgumentException(msg));
                     }

                     final OperationStatus status = cursor.delete();
                     assert (status == OperationStatus.SUCCESS);

                     MigrationStreamHandle.get().addDelete(keyEntry);
                     nDeletions[0] += 1;
                     return 1;
                 }
             });

        assert (!moreElements);

        return nDeletions[0];
    }

    /**
     * Called by scan for each selected record.
     */
    interface ScanVisitor {
        static int STOP = -1;
        static int CONTINUE = -2;
        
        /**
         * @returns the number of records added to the result set.  If return
         * is -1 then the scan is complete and should terminate.
         */
        int visit(Cursor cursor,
                  DatabaseEntry keyEntry,
                  DatabaseEntry dataEntry);
    }

    /**
     * Implement a multi-key scan using a parent key, optional KeyRange and
     * Depth parameter, and call the ScanVisitor for each selected record.
     *
     * When batchSize is 1 and the parent is returned, 2 keys instead of 1 will
     * be returned.  The fix for this would add complexity and have very little
     * value.  Normally a user will pass batchSize 1 only when trying to get
     * the first key in a range, and the parent will not be requested.
     *
     * @param parentKey is the byte array of the parent Key parameter, and may
     * be null for a store iteration.
     *
     * @param noData is true if the data should not be fetched, i.e., only the
     * key is needed by the Vistor.
     *
     * @param resumeKey is the key after which to resume the iteration of
     * descendants, or null to start at the parent.  To resume the iteration
     * where it left off, pass the last key returned.
     *
     * @param batchSize is the max number of keys to return in one call, or
     * zero if there is no max and all keys should be returned.
     *
     * @param cursorConfig the CursorConfig to use for the scan.  Should be
     * DEFAULT for a non-iterator operation and READ_COMMITTED for an iterator
     * operation.
     *
     * @param majorPathComplete is true if the parentKey's major path is
     * complete and therefore the subRange applies to the next minor component,
     * or false if the major path is incomplete (there is no minor path) and
     * therefore the subRange applies to the next major component.  Must be
     * true for multiGet iteration and false for store iteration.
     *
     * @return whether more elements may be available.  If batchSize is greater
     * than zero, the scan will stop when that many records have been passed to
     * the visitor, and true will be returned.  False is returned when the
     * iteration is complete.
     */
    boolean scan(Transaction txn,
                 PartitionId partitionId,
                 byte[] parentKey,
                 boolean majorPathComplete,
                 KeyRange subRange,
                 Depth depth,
                 boolean noData,
                 byte[] resumeKey,
                 int batchSize,
                 CursorConfig cursorConfig,
                 LockMode lockMode,
                 Direction direction,
                 ScanVisitor visitor) {

        assert (depth != null);

        if (subRange != null &&
            subRange.isPrefix() &&
            subRange.getStart().length() == 0) {
            subRange = null;
        }

        final boolean includeParent =
            (resumeKey == null) &&
            ((depth == Depth.PARENT_AND_CHILDREN) ||
             (depth == Depth.PARENT_AND_DESCENDANTS));

        final boolean allDescendants =
            (depth == Depth.DESCENDANTS_ONLY) ||
            (depth == Depth.PARENT_AND_DESCENDANTS);

        assert (direction == Direction.FORWARD ||
                direction == Direction.REVERSE);

        final Database db = repNode.getPartitionDB(partitionId);

        if (direction == Direction.FORWARD) {
            return forwardScan(txn, db, parentKey, majorPathComplete, subRange,
                               includeParent, allDescendants, noData,
                               resumeKey, batchSize, cursorConfig, lockMode,
                               visitor);
        }

        return reverseScan(txn, db, parentKey, majorPathComplete, subRange,
                           includeParent, allDescendants, noData,
                           resumeKey, batchSize, cursorConfig, lockMode,
                           visitor);
    }

    /**
     * See scan.
     *
     * Forward scanning may be used for true transactional/atomic operations,
     * and deadlocks will not occur.  It never exceeds the bounds of the query.
     */
    private boolean forwardScan(Transaction txn,
                                Database db,
                                byte[] parentKey,
                                boolean majorPathComplete,
                                KeyRange subRange,
                                boolean includeParent,
                                boolean allDescendants,
                                boolean noData,
                                byte[] resumeKey,
                                int batchSize,
                                CursorConfig cursorConfig,
                                LockMode lockMode,
                                ScanVisitor visitor) {

        /*
         * Determine the search and scan constraints.  The searchInitKey is
         * used to start the scan for descendents, and is used when we do not
         * first position on the parent. The rangeConstraint is used to end the
         * scan and prevents scanning outside the parent key descendents (or
         * sub-range of child keys), which is particularly important to avoid
         * deadlocks with other transactions.
         */
        final byte[] searchInitKey;
        final RangeConstraint rangeConstraint;
        if (subRange == null) {

            /*
             * Case 1: No KeyRange.  Start the scan at first descendant and
             * stop it at a key that doesn't start with the parent key prefix.
             * Scan entire key set when parentKey is null.
             */
            if (parentKey != null) {
                searchInitKey =
                    Key.addComponent(parentKey, majorPathComplete, "");
                rangeConstraint = getPrefixConstraint(searchInitKey);
            } else {
                searchInitKey = MIN_KEY;
                rangeConstraint = null;
            }

        } else if (subRange.isPrefix()) {

            /*
             * Case 2: KeyRange is a prefix.  Start the scan at the first child
             * key with the given KeyRange prefix and stop it at a key that
             * doesn't start with that prefix.
             */
            searchInitKey = Key.addComponent
                (parentKey, majorPathComplete, subRange.getStart());
            rangeConstraint = getPrefixConstraint(searchInitKey);

        } else {

            /*
             * Case 3: KeyRange has different start/end points.  Start the scan
             * at the child key with the inclusive KeyRange start key.
             */
            if (subRange.getStart() != null) {

                final String rangeStart = subRange.getStartInclusive() ?
                    subRange.getStart() :
                    getPathComponentSuccessor(subRange.getStart());

                searchInitKey = Key.addComponent
                    (parentKey, majorPathComplete, rangeStart);

            } else if (parentKey != null) {
                searchInitKey = Key.addComponent(parentKey, majorPathComplete,
                                                 "");
            } else {
                searchInitKey = MIN_KEY;
            }

            /* Stop the scan at the exclusive KeyRange end key. */
            if (subRange.getEnd() != null) {

                final String rangeEnd = subRange.getEndInclusive() ?
                    getPathComponentSuccessor(subRange.getEnd()) :
                    subRange.getEnd();

                rangeConstraint = getRangeEndConstraint
                    (Key.addComponent(parentKey, majorPathComplete, rangeEnd));

            } else if (parentKey != null) {
                rangeConstraint = getPrefixConstraint
                    (Key.addComponent(parentKey, majorPathComplete, ""));
            } else {
                rangeConstraint = null;
            }
        }
        assert (searchInitKey != null);

        final DatabaseEntry keyEntry = new DatabaseEntry();
        final DatabaseEntry dataEntry = new DatabaseEntry();
        if (noData) {
            dataEntry.setPartial(0, 0, true);
        }

        int nRecords = 0;
        final Cursor cursor = db.openCursor(txn, cursorConfig);
        DbInternal.setNonCloning(cursor, true);
        try {
            /* Do separate search for parent. */
            boolean parentFound = false;
            OperationStatus status;
            if (includeParent && parentKey != null) {
                keyEntry.setData(parentKey);
                status = cursor.getSearchKey(keyEntry, dataEntry, lockMode);
                if (status == OperationStatus.SUCCESS) {
                    parentFound = true;
                    nRecords += visitor.visit(cursor, keyEntry, dataEntry);
                }
            }

            /*
             * For a non-prefix range there is a possibility that the range
             * start (or MIN_KEY) exceeds the range end. The rangeConstraint
             * won't detect this if the searchInitKey (range start or MIN_KEY)
             * matches a record exactly.
             */
            if ((subRange != null) &&
                (rangeConstraint != null) &&
                !subRange.isPrefix() &&
                !rangeConstraint.inBounds(searchInitKey)) {
                return false;
            }

            /* Move to first descendant. */
            cursor.setRangeConstraint(rangeConstraint);
            if (parentFound && subRange == null) {
                status = cursor.getNext(keyEntry, dataEntry, lockMode);
            } else if (resumeKey != null) {
                keyEntry.setData(resumeKey);
                status = cursor.getSearchKeyRange(keyEntry, dataEntry,
                                                  lockMode);
                if ((status == OperationStatus.SUCCESS) &&
                    Arrays.equals(resumeKey, keyEntry.getData())) {
                    status = cursor.getNext(keyEntry, dataEntry, lockMode);
                }
            } else {
                keyEntry.setData(searchInitKey);
                status = cursor.getSearchKeyRange(keyEntry, dataEntry,
                                                  lockMode);
            }

            if (status != OperationStatus.SUCCESS) {
                return false;
            }

            /* Simple case: collect all descendants. */
            if (allDescendants) {
                while (status == OperationStatus.SUCCESS) {
                    int nRecs = visitor.visit(cursor, keyEntry, dataEntry);
                    if (nRecs == ScanVisitor.CONTINUE) {
                        /*
                         * The visitor has indicated that we should
                         * loop without doing anything more.  For example,
                         * it may have reset the cursor and performed a new
                         * operation.
                         */
                        continue;
                    }
                    if (nRecs == ScanVisitor.STOP) {
                        /*
                         * The visitor has indicated that the operation is
                         * done, return.
                         */
                        return (batchSize > 0 && nRecords >= batchSize);
                    }
                    nRecords += nRecs;
                    if (batchSize > 0 && nRecords >= batchSize) {
                        return true;
                    }
                    status = cursor.getNext(keyEntry, dataEntry, lockMode);
                }
                return false;
            }

            /*
             * To collect immediate children only, we position at the child's
             * successor key when we encounter a descendant that is not an
             * immediate child.
             */
            final int nChildComponents =
                (parentKey != null) ? (Key.countComponents(parentKey) + 1) : 1;
            while (status == OperationStatus.SUCCESS) {
                final int nComponents =
                    Key.countComponents(keyEntry.getData());
                if (nComponents == nChildComponents) {
                    /* This is an immediate child. */
                    int nRecs = visitor.visit(cursor, keyEntry, dataEntry);
                    if (nRecs == ScanVisitor.STOP) {
                        return (batchSize > 0 && nRecords >= batchSize);
                    }
                    nRecords += nRecs;
                    if (batchSize > 0 && nRecords >= batchSize) {
                        return true;
                    }
                    status = cursor.getNext(keyEntry, dataEntry, lockMode);
                } else {
                    /* Not an immediate child.  Move to child's successor. */
                    assert nComponents > nChildComponents;
                    getChildKeySuccessor(keyEntry, parentKey);
                    status = cursor.getSearchKeyRange(keyEntry, dataEntry,
                                                      lockMode);
                }
            }
            return false;
        } finally {
            TxnUtil.close(cursor);
        }
    }

    /**
     * See scan.
     *
     * Reverse scanning requires passing a CursorConfig with read-committed set
     * to true, and is only used for iteration.  multiGet and multiDelete
     * always use forward scanning.
     *
     * Reverse scanning sometimes exceeds the bounds of the query, including
     * going outside of the key major path.  This is undesirable because it is
     * wasteful, and should be optimized in the future by adding additional key
     * range operations to JE, namely a range search that returns the first
     * record with a key LTE the argument.  The Cursor.getSearchRange method
     * only returns a record with a key GTE the argument.  Additional methods
     * may also be needed.
     *
     * In spite of the fact that we currently go outside the bounds of the key
     * range, this won't cause deadlocks because only one key at a time is
     * locked.
     */
    private boolean reverseScan(Transaction txn,
                                Database db,
                                byte[] parentKey,
                                boolean majorPathComplete,
                                KeyRange subRange,
                                boolean includeParent,
                                boolean allDescendants,
                                boolean noData,
                                byte[] resumeKey,
                                int batchSize,
                                CursorConfig cursorConfig,
                                LockMode lockMode,
                                ScanVisitor visitor) {

        /*
         * For a reverse scan, to avoid deadlocks with transactions that move
         * the cursor forward we must use read-committed (which is always used
         * for an iteration and in this method) and a non-cloning cursor
         * (setNonCloning is called below). Read-committed ensures that we
         * release locks as we move the cursor.  The non-cloning cursor ensures
         * that we release the lock at the current position before getting the
         * lock at the next position, as opposed to overlapping the locks as
         * occurs with a default, cloning cursor.
         */
        assert cursorConfig.getReadCommitted();

        /*
         * Determine the search and scan constraints.  The keyPrefix is used
         * both to create a rangeConstraint and to find the first record
         * following the range as the starting point. The searchInitKey is only
         * used as a starting point when a KeyRange end point is given. The
         * rangeConstraint is used to end the scan.
         */
        final byte[] keyPrefix;
        final byte[] searchInitKey;
        final RangeConstraint rangeConstraint;

        if (subRange == null) {

            /*
             * Case 1: No KeyRange.
             */
            if (parentKey != null) {

                /*
                 * Use the parent key prefix as the range constraint and
                 * starting point.
                 */
                keyPrefix = Key.addComponent(parentKey, majorPathComplete, "");
                searchInitKey = null;
                rangeConstraint = getPrefixConstraint(keyPrefix);
            } else {
                /* Scan all keys, no constraints. */
                keyPrefix = null;
                searchInitKey = null;
                rangeConstraint = null;
            }

        } else if (subRange.isPrefix()) {

            /*
             * Case 2: KeyRange is a prefix.  Same as case 1 but using the
             * KeyRange prefix appended to the parent key.
             */
            keyPrefix = Key.addComponent(parentKey, majorPathComplete,
                                         subRange.getStart());
            searchInitKey = null;
            rangeConstraint = getPrefixConstraint(keyPrefix);

        } else {

            /*
             * Case 3: KeyRange has different start/end points.  The key prefix
             * may be used in several different ways below.
             */
            if (parentKey != null) {
                keyPrefix = Key.addComponent(parentKey, majorPathComplete, "");
            } else {
                keyPrefix = null;
            }

            if (subRange.getEnd() != null) {

                /*
                 * Start the scan at the child key with the exclusive KeyRange
                 * end key.
                 */
                final String rangeEnd = subRange.getEndInclusive() ?
                    getPathComponentSuccessor(subRange.getEnd()) :
                    subRange.getEnd();

                searchInitKey = Key.addComponent
                    (parentKey, majorPathComplete, rangeEnd);
            } else {
                /* Use the keyPrefix, if any, to start the scan. */
                searchInitKey = null;
            }

            if (subRange.getStart() != null) {

                /*
                 * Stop the scan at the inclusive KeyRange start key.
                 */
                final String rangeStart = subRange.getStartInclusive() ?
                    subRange.getStart() :
                    getPathComponentSuccessor(subRange.getStart());

                rangeConstraint = getRangeStartConstraint(Key.addComponent
                                                          (parentKey, majorPathComplete, rangeStart));

            } else if (keyPrefix != null) {
                /* Use the keyPrefix to stop the scan. */
                rangeConstraint = getPrefixConstraint(keyPrefix);
            } else {
                /* Scan all keys, no constraints. */
                rangeConstraint = null;
            }
        }

        final DatabaseEntry noDataEntry = new DatabaseEntry();
        noDataEntry.setPartial(0, 0, true);

        final DatabaseEntry keyEntry = new DatabaseEntry();
        final DatabaseEntry dataEntry =
            noData ? noDataEntry : (new DatabaseEntry());

        int nRecords = 0;
        final Cursor cursor = db.openCursor(txn, cursorConfig);
        DbInternal.setNonCloning(cursor, true);
        try {
            /* Do separate search for parent. */
            OperationStatus status;
            if (includeParent && parentKey != null) {
                keyEntry.setData(parentKey);
                status = cursor.getSearchKey(keyEntry, dataEntry, lockMode);
                if (status == OperationStatus.SUCCESS) {
                    int nRecs = visitor.visit(cursor, keyEntry, dataEntry);
                    nRecords += nRecs;
                }
            }

            /*
             * Move to the successor of the last descendant, then to the
             * previous key to find the last descendant.  Do not set the
             * rangeConstraint at first, because we initially move outside of
             * the range.
             */
            if (resumeKey != null ||
                searchInitKey != null ||
                keyPrefix != null) {

                /*
                 * Move to the successor of the first key we want to return.
                 * When resumeKey is non-null, this is the resumeKey.
                 * Otherwise, use searchInitKey or the keyPrefix.
                 */
                if (resumeKey != null) {
                    keyEntry.setData(resumeKey);
                    status = cursor.getSearchKeyRange
                        (keyEntry, noDataEntry, LockMode.READ_UNCOMMITTED);
                } else if (searchInitKey != null) {
                    keyEntry.setData(searchInitKey);
                    status = cursor.getSearchKeyRange
                        (keyEntry, noDataEntry, LockMode.READ_UNCOMMITTED);
                } else {
                    keyEntry.setData(keyPrefix);
                    status = cursor.getNextAfterPrefix
                        (keyEntry, noDataEntry, LockMode.READ_UNCOMMITTED);
                }

                /*
                 * If we found the successor of the key we want, move to the
                 * previous key to get the key to be returned first.  Otherwise
                 * move to the last key, which may be in or out of the range.
                 */
                if (status == OperationStatus.SUCCESS) {
                    status = cursor.getPrev(keyEntry, dataEntry, lockMode);
                } else {
                    status = cursor.getLast(keyEntry, dataEntry, lockMode);
                }
            } else {
                status = cursor.getLast(keyEntry, dataEntry, lockMode);
            }

            if (status != OperationStatus.SUCCESS) {
                return false;
            }

            /*
             * Because we did not set the rangeConstraint above, we must check
             * it explicitly here for the first record to be returned.
             */
            if (rangeConstraint != null &&
                !rangeConstraint.inBounds(keyEntry.getData())) {
                status = OperationStatus.NOTFOUND;
            }

            /* Now use the rangeConstraint to stop the scan. */
            cursor.setRangeConstraint(rangeConstraint);

            /* Simple case: collect all descendants. */
            if (allDescendants) {
                while (status == OperationStatus.SUCCESS) {
                    int nRecs = visitor.visit(cursor, keyEntry, dataEntry);
                    if (nRecs == ScanVisitor.STOP) {
                        return (batchSize > 0 && nRecords >= batchSize);
                    }
                    nRecords += nRecs;
                    if (batchSize > 0 && nRecords >= batchSize) {
                        return true;
                    }
                    status = cursor.getPrev(keyEntry, dataEntry, lockMode);
                }
                return false;
            }

            /*
             * To collect immediate children only, we position at the child's
             * predecessor key when we encounter a descendant that is not an
             * immediate child.
             */
            final int nChildComponents =
                (parentKey != null) ? (Key.countComponents(parentKey) + 1) : 1;
            while (status == OperationStatus.SUCCESS) {
                final int nComponents =
                    Key.countComponents(keyEntry.getData());
                if (nComponents == nChildComponents) {
                    /* This is an immediate child. */
                    int nRecs = visitor.visit(cursor, keyEntry, dataEntry);
                    if (nRecs == ScanVisitor.STOP) {
                        return (batchSize > 0 && nRecords >= batchSize);
                    }
                    nRecords += nRecs;
                    if (batchSize > 0 && nRecords >= batchSize) {
                        return true;
                    }
                    status = cursor.getPrev(keyEntry, dataEntry, lockMode);
                } else {

                    /*
                     * Not an immediate child.  Move back/up to the immediate
                     * child, which has a key that is a prefix of the key we
                     * found but with nChildComponents.
                     */
                    assert nComponents > nChildComponents;
                    final byte[] nonChildKey = keyEntry.getData();
                    keyEntry.setData
                        (Key.getPrefixKey(nonChildKey, nChildComponents));
                    status = cursor.getSearchKeyRange(keyEntry, dataEntry,
                                                      lockMode);
                    if (status == OperationStatus.SUCCESS &&
                        Arrays.equals(nonChildKey, keyEntry.getData())) {

                        /*
                         * Arrived back at the same record, meaning that there
                         * is no key with nChildComponents for this child and
                         * we are at the first descendent for this child.  Move
                         * to the previous child.
                         */
                        status = cursor.getPrev(keyEntry, dataEntry, lockMode);
                    }
                }
            }
            return false;
        } finally {
            TxnUtil.close(cursor);
        }
    }

    /**
     * Returns the first string that sorts higher than the given string, if
     * the strings were converted to UTF-8.
     *
     * The character with value 1 has the lowest sort value of any character in
     * the "Modified UTF-8" encoding.
     */
    private String getPathComponentSuccessor(String comp) {
        return comp + MIN_VALUE_CHAR;
    }

    /**
     * Returns the first key that sorts higher than a child key, if we were to
     * remove all components following the child key in the given full key.
     *
     * The character with value 1 has the lowest sort value of any character in
     * the "Modified UTF-8" encoding.
     */
    private void getChildKeySuccessor(DatabaseEntry key, byte[] parentKey) {
        final byte[] bytes = key.getData();

        /* Child begins after the parent's delimeter byte. */
        final int childOff = (parentKey != null) ? (parentKey.length + 1) : 0;
        final int childLen = Key.getComponentLength(bytes, childOff);

        /* Successor key is child key plus a byte '1' value. */
        final int newLen = childOff + childLen + 1;
        assert (newLen <= bytes.length);
        bytes[newLen - 1] = MIN_VALUE_BYTE;
        key.setSize(newLen);
    }

    /**
     * Returns a RangeConstraint that prevents the cursor from moving past
     * the descendants of the prefix key.
     */
    private RangeConstraint getPrefixConstraint(final byte[] prefixKey) {

        final int prefixLen = prefixKey.length;

        return new RangeConstraint() {
            @Override
            public boolean inBounds(byte[] checkKey) {
                if (checkKey.length < prefixLen) {
                    return false;
                }
                for (int i = 0; i < prefixLen; i += 1) {
                    if (prefixKey[i] != checkKey[i]) {
                        return false;
                    }
                }
                return true;
            }
        };
    }

    /**
     * Returns a RangeConstraint that prevents the cursor from moving forward
     * past an exclusive end point.
     */
    private RangeConstraint
        getRangeEndConstraint(final byte[] endKeyExclusive) {

        return new RangeConstraint() {
            @Override
            public boolean inBounds(byte[] checkKey) {
                return KEY_BYTES_COMPARATOR.compare(checkKey,
                                                    endKeyExclusive) < 0;
            }
        };
    }

    /**
     * Returns a RangeConstraint that prevents the cursor from moving backward
     * past an inclusive start point.
     */
    private RangeConstraint
        getRangeStartConstraint(final byte[] startKeyInclusive) {

        return new RangeConstraint() {
            @Override
            public boolean inBounds(byte[] checkKey) {
                return KEY_BYTES_COMPARATOR.compare(checkKey,
                                                    startKeyInclusive) >= 0;
            }
        };
    }

    /**
     * Put a key/value pair. If the key exists, the associated value is
     * overwritten.
     */
    Version put(Transaction txn,
                PartitionId partitionId,
                byte[] keyBytes,
                byte[] valueBytes,
                ReturnResultValueVersion prevValue) {

        assert (keyBytes != null) && (valueBytes != null);

        final Database db = repNode.getPartitionDB(partitionId);
        final DatabaseEntry keyEntry = new DatabaseEntry(keyBytes);
        final DatabaseEntry dataEntry = putDatabaseEntry(valueBytes);

        /* Simple case: previous version and value are not returned. */
        if (!prevValue.getReturnChoice().needValueOrVersion()) {

            final Cursor cursor = db.openCursor(txn, null);
            DbInternal.setNonCloning(cursor, true);
            try {
                final OperationStatus status = cursor.put(keyEntry, dataEntry);
                assert (status == OperationStatus.SUCCESS);
                MigrationStreamHandle.get().addPut(keyEntry, dataEntry);
                return getVersion(cursor);
            } finally {
                TxnUtil.close(cursor);
            }
        }

        /*
         * To return previous value/version, we have to either position on the
         * existing record and update it, or insert without overwriting.
         */
        final Cursor cursor = db.openCursor(txn, null);
        DbInternal.setNonCloning(cursor, true);
        try {
            while (true) {
                OperationStatus status =
                    cursor.putNoOverwrite(keyEntry, dataEntry);
                if (status == OperationStatus.SUCCESS) {
                    MigrationStreamHandle.get().addPut(keyEntry, dataEntry);
                    return getVersion(cursor);
                }
                final DatabaseEntry prevData =
                    prevValue.getReturnChoice().needValue() ?
                    new DatabaseEntry() :
                    NO_DATA;
                status = cursor.getSearchKey(keyEntry, prevData, LockMode.RMW);
                if (status == OperationStatus.SUCCESS) {
                    getPrevValueVersion(cursor, prevData, prevValue);
                    cursor.putCurrent(dataEntry);
                    return getVersion(cursor);
                }
                /* Another thread deleted the record.  Continue. */
            }
        } finally {
            TxnUtil.close(cursor);
        }
    }

    /**
     * Insert a key/value pair.
     */
    Version putIfAbsent(Transaction txn,
                        PartitionId partitionId,
                        byte[] keyBytes,
                        byte[] valueBytes,
                        ReturnResultValueVersion prevValue) {

        assert (keyBytes != null) && (valueBytes != null);

        final Database db = repNode.getPartitionDB(partitionId);
        final DatabaseEntry keyEntry = new DatabaseEntry(keyBytes);
        final DatabaseEntry dataEntry = putDatabaseEntry(valueBytes);

        /*
         * To return previous value/version, we have to either position on the
         * existing record and update it, or insert without overwriting.
         */
        final Cursor cursor = db.openCursor(txn, null);
        DbInternal.setNonCloning(cursor, true);
        try {
            while (true) {
                OperationStatus status =
                    cursor.putNoOverwrite(keyEntry, dataEntry);
                if (status == OperationStatus.SUCCESS) {
                    MigrationStreamHandle.get().addPut(keyEntry, dataEntry);
                    return getVersion(cursor);
                }
                /* Simple case: previous version and value are not returned. */
                if (prevValue.getReturnChoice() == Choice.NONE) {
                    return null;
                }
                /* Get and return previous value/version. */
                final DatabaseEntry prevData =
                    prevValue.getReturnChoice().needValue() ?
                    new DatabaseEntry() :
                    NO_DATA;
                status =
                    cursor.getSearchKey(keyEntry, prevData, LockMode.DEFAULT);
                if (status == OperationStatus.SUCCESS) {
                    getPrevValueVersion(cursor, prevData, prevValue);
                    return null;
                }
                /* Another thread deleted the record.  Continue. */
            }
        } finally {
            TxnUtil.close(cursor);
        }
    }

    /**
     * Update a key/value pair.
     */
    Version putIfPresent(Transaction txn,
                         PartitionId partitionId,
                         byte[] keyBytes,
                         byte[] valueBytes,
                         ReturnResultValueVersion prevValue) {

        assert (keyBytes != null) && (valueBytes != null);

        final Database db = repNode.getPartitionDB(partitionId);
        final DatabaseEntry keyEntry = new DatabaseEntry(keyBytes);
        final DatabaseEntry dataEntry = putDatabaseEntry(valueBytes);

        final Cursor cursor = db.openCursor(txn, null);
        DbInternal.setNonCloning(cursor, true);
        try {
            final DatabaseEntry prevData =
                prevValue.getReturnChoice().needValue() ?
                new DatabaseEntry() :
                NO_DATA;
            final OperationStatus status =
                cursor.getSearchKey(keyEntry, prevData, LockMode.RMW);
            if (status != OperationStatus.SUCCESS) {
                return null;
            }
            getPrevValueVersion(cursor, prevData, prevValue);
            cursor.putCurrent(dataEntry);
            MigrationStreamHandle.get().addPut(keyEntry, dataEntry);
            return getVersion(cursor);
        } finally {
            TxnUtil.close(cursor);
        }
    }

    /**
     * Update a key/value pair, if the existing record has the given version.
     */
    Version putIfVersion(Transaction txn,
                         PartitionId partitionId,
                         byte[] keyBytes,
                         byte[] valueBytes,
                         Version matchVersion,
                         ReturnResultValueVersion prevValue) {

        assert (keyBytes != null) && (valueBytes != null) &&
            (matchVersion != null);

        final Database db = repNode.getPartitionDB(partitionId);
        final DatabaseEntry keyEntry = new DatabaseEntry(keyBytes);
        final DatabaseEntry dataEntry = putDatabaseEntry(valueBytes);
        final Cursor cursor = db.openCursor(txn, null);
        DbInternal.setNonCloning(cursor, true);
        try {
            final OperationStatus status =
                cursor.getSearchKey(keyEntry, NO_DATA, LockMode.RMW);
            if (status != OperationStatus.SUCCESS) {
                /* Not present, return null. */
                return null;
            }
            if (versionMatches(cursor, matchVersion)) {
                /* Version matches, update and return new version. */
                cursor.putCurrent(dataEntry);
                MigrationStreamHandle.get().addPut(keyEntry, dataEntry);
                return getVersion(cursor);
            }
            /* No match, get previous value/version and return null. */
            final DatabaseEntry prevData;
            if (prevValue.getReturnChoice().needValue()) {
                prevData = new DatabaseEntry();
                cursor.getCurrent(keyEntry, prevData, LockMode.RMW);
            } else {
                prevData = NO_DATA;
            }
            getPrevValueVersion(cursor, prevData, prevValue);
            return null;
        } finally {
            TxnUtil.close(cursor);
        }
    }

    /**
     * Delete the key/value pair associated with the key.
     */
    boolean delete(Transaction txn,
                   PartitionId partitionId,
                   byte[] keyBytes,
                   ReturnResultValueVersion prevValue) {

        assert (keyBytes != null);

        final Database db = repNode.getPartitionDB(partitionId);
        final DatabaseEntry keyEntry = new DatabaseEntry(keyBytes);

        /* Simple case: previous version and value are not returned. */
        if (!prevValue.getReturnChoice().needValueOrVersion()) {

            final OperationStatus status = db.delete(txn, keyEntry);
            if (status == OperationStatus.SUCCESS) {
                MigrationStreamHandle.get().addDelete(keyEntry);
            }
            return (status == OperationStatus.SUCCESS);
        }

        /*
         * To return previous value/version, we must first position on the
         * existing record and then delete it.
         */
        final Cursor cursor = db.openCursor(txn, null);
        DbInternal.setNonCloning(cursor, true);
        try {
            final DatabaseEntry prevData =
                prevValue.getReturnChoice().needValue() ?
                new DatabaseEntry() :
                NO_DATA;
            final OperationStatus status =
                cursor.getSearchKey(keyEntry, prevData, LockMode.RMW);
            if (status != OperationStatus.SUCCESS) {
                return false;
            }
            getPrevValueVersion(cursor, prevData, prevValue);
            cursor.delete();
            MigrationStreamHandle.get().addDelete(keyEntry);
            return true;
        } finally {
            TxnUtil.close(cursor);
        }
    }

    /**
     * Delete a key/value pair, if the existing record has the given version.
     */
    boolean deleteIfVersion(Transaction txn,
                            PartitionId partitionId,
                            byte[] keyBytes,
                            Version matchVersion,
                            ReturnResultValueVersion prevValue) {

        assert (keyBytes != null) && (matchVersion != null);

        final Database db = repNode.getPartitionDB(partitionId);
        final DatabaseEntry keyEntry = new DatabaseEntry(keyBytes);
        final Cursor cursor = db.openCursor(txn, null);
        DbInternal.setNonCloning(cursor, true);
        try {
            final OperationStatus status =
                cursor.getSearchKey(keyEntry, NO_DATA, LockMode.RMW);
            if (status != OperationStatus.SUCCESS) {
                /* Not present, return false. */
                return false;
            }
            if (versionMatches(cursor, matchVersion)) {
                /* Version matches, delete and return true. */
                cursor.delete();
                MigrationStreamHandle.get().addDelete(keyEntry);
                return true;
            }
            /* No match, get previous value/version and return false. */
            final DatabaseEntry prevData;
            if (prevValue.getReturnChoice().needValue()) {
                prevData = new DatabaseEntry();
                cursor.getCurrent(keyEntry, prevData, LockMode.RMW);
            } else {
                prevData = NO_DATA;
            }
            getPrevValueVersion(cursor, prevData, prevValue);
            return false;
        } finally {
            TxnUtil.close(cursor);
        }
    }

    RepNode getRepNode() {
        return repNode;
    }

    /**
     * Using the cursor at the prior version of the record and the data of the
     * prior version, return the requested previous value and/or version.
     */
    private void getPrevValueVersion(Cursor cursor,
                                     DatabaseEntry prevData,
                                     ReturnResultValueVersion prevValue) {
        switch (prevValue.getReturnChoice()) {
        case VALUE:
            assert !prevData.getPartial();
            prevValue.setValueVersion(prevData.getData(), null);
            break;
        case VERSION:
            prevValue.setValueVersion(null, getVersion(cursor));
            break;
        case ALL:
            assert !prevData.getPartial();
            prevValue.setValueVersion(prevData.getData(), getVersion(cursor));
            break;
        case NONE:
            prevValue.setValueVersion(null, null);
            break;
        default:
            throw new IllegalStateException
                (prevValue.getReturnChoice().toString());
        }
    }

    /**
     * Returns the Version of the record at the given cursor position.
     */
    private Version getVersion(Cursor cursor) {

        final CursorImpl cursorImpl = DbInternal.getCursorImpl(cursor);

        /*
         * Although the LN will normally be resident, since we just wrote it
         * with the cursor, we pass fetchLN=true to handle the rare case that
         * the LN is evicted after being written.
         */
        final RecordVersion recVersion =
            cursorImpl.getCurrentVersion(true /*fetchLN*/);

        return new Version(getRepNodeUUID(), recVersion.getVLSN(),
                           repNode.getRepNodeId(), recVersion.getLSN());
    }

    /**
     * Returns whether the Version of the record at the given cursor position
     * matches the given Version.
     */
    private boolean versionMatches(Cursor cursor, Version matchVersion) {

        final RepNodeId repNodeId = repNode.getRepNodeId();
        final CursorImpl cursorImpl = DbInternal.getCursorImpl(cursor);

        /* First try without forcing an LN fetch. */
        RecordVersion recVersion =
            cursorImpl.getCurrentVersion(false /*fetchLN*/);

        /* The LSN may match, in which case we don't need the VLSN. */
        if (matchVersion.samePhysicalVersion(repNodeId, recVersion.getLSN())) {
            return true;
        }

        /* Try the VLSN if it is resident and available. */
        long vlsn = recVersion.getVLSN();
        if (!VLSN.isNull(vlsn)) {
            return matchVersion.sameLogicalVersion(vlsn);
        }

        /* The VLSN is not resident. Force a fetch and try again. */
        recVersion = cursorImpl.getCurrentVersion(true /*fetchLN*/);
        vlsn = recVersion.getVLSN();
        assert !VLSN.isNull(vlsn);
        return matchVersion.sameLogicalVersion(vlsn);
    }

    /**
     * Get the rep node UUID lazily, since opening the environment is not
     * possible in the constructor.
     */
    private synchronized UUID getRepNodeUUID() {
        if (repNodeUUID != null) {
            return repNodeUUID;
        }
        final RepImpl repImpl = repNode.getEnvImpl(ENV_TIMEOUT_MS);
        if (repImpl == null) {
            throw new FaultException("Unable to get ReplicatedEnvironment " +
                                     "after " + ENV_TIMEOUT_MS + " ms",
                                     true /*isRemote*/);
        }
        repNodeUUID = repImpl.getUUID();
        assert repNodeUUID != null;
        return repNodeUUID;
    }

    /**
     * Returns a ResultValueVersion for the given data and the version at the
     * given cursor position.
     */
    ResultValueVersion makeValueVersion(Cursor c,
                                        DatabaseEntry dataEntry) {
        return new ResultValueVersion(dataEntry.getData(), getVersion(c));
    }

    /**
     * Create a DatabaseEntry for the value bytes.  If the value is
     * empty, as indicated by a single zero byte in the array, use an empty
     * DatabaseEntry to allow JE to optimize the empty value.
     *
     * This use of zero-length values can't be done unconditionally because
     * pre-R3 nodes would be confused by reading truly empty values resulting
     * in an exception.  This code handles empty values but won't write them.
     *
     * TODO: Post-R3, remove the conditional on useEmptyValue.  If zero-length
     * records are written unconditionally it means that R3 is a pre-requisite
     * for upgrading to the next release.  It is possible to make the write
     * conditional to loosen the pre-req constraint but in addition code must
     * be added to partition migration to handle migrating to a node with older
     * code.
     */
    private DatabaseEntry putDatabaseEntry(byte[] value) {

        if (value.length == 1  && value[0] == 0 && useEmptyValue) {
            return EMPTY_DATA;
        }
        return new DatabaseEntry(value);
    }
}
