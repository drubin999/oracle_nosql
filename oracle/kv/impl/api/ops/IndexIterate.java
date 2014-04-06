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
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

import oracle.kv.impl.api.ops.MultiTableOperation.AncestorList;
import oracle.kv.impl.api.table.IndexRange;
import oracle.kv.impl.api.table.TargetTables;
import oracle.kv.impl.topo.PartitionId;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.Transaction;

/**
 * An index iteration that returns row values.  The index is specified by a
 * combination of the indexName and tableName.  The index scan range and
 * additional parameters are specified by the IndexRange.
 *
 * Both primary and secondary keys are required for resumption of batched
 * operations because a single index match may match a large number of
 * primary records. In order to honor batch size constraints a single
 * request/reply operation may need to resume within a set of duplicates.
 *
 * When ancestor tables are requested the operation will fetch the requested
 * ancestor row for any matching rows, and if present, add it to the result
 * list.  Ancestor rows are returned before the corresponding target table row.
 * This is true even if the iteration is in reverse order.  In the event of
 * multiple index entries matching the same primary entry and/or ancestor entry,
 * duplicate rows will be returned.
 *
 *
 * The childTables parameter is a list of child tables to return. These are not
 * supported in R3 and will be null.
 *
 * The ancestorTables parameter, if not null, specifies the ancestor tables to
 * return in addition to the target table, which is the table containing the
 * index.
 *
 * The resumeSecondaryKey parameter is used for batching of results and will be
 * null on the first call
 *
 * The resumePrimaryKey is used for batching of results and will be null
 * on the first call
 *
 * The batchSize parameter is the batch size to to use
 */
public class IndexIterate extends IndexOperation {

    /**
     * Constructs an index operation.
     *
     * For subclasses, allows passing OpCode.
     */
    public IndexIterate(String indexName,
                        TargetTables targetTables,
                        IndexRange range,
                        byte[] resumeSecondaryKey,
                        byte[] resumePrimaryKey,
                        int batchSize) {
        super(OpCode.INDEX_ITERATE, indexName, targetTables,
              range, resumeSecondaryKey, resumePrimaryKey, batchSize);
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     *
     * For subclasses, allows passing OpCode.
     */
    IndexIterate(ObjectInput in, short serialVersion)
        throws IOException {

        super(OpCode.INDEX_ITERATE, in, serialVersion);
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to write
     * common elements.
     */
    @Override
    public void writeFastExternal(ObjectOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
    }

    @Override
    public String toString() {
        return super.toString(); //TODO
    }

    @Override
    public Result execute(Transaction txn,
                          PartitionId partitionId,  /* Not used */
                          final OperationHandler operationHandler) {

        final AncestorList ancestors =
            new AncestorList(operationHandler,
                             txn,
                             getResumePrimaryKey(),
                             targetTables.getAncestorTableIds());

        /*
         * This class returns Row objects which means that a data fetch is
         * required when the table is not key-only.
         */
        final List<ResultKeyValueVersion> results =
            new ArrayList<ResultKeyValueVersion>();
        final boolean moreElements = TableOperationHandler.indexScan
            (txn,
             getSecondaryDatabase(operationHandler),
             getIndex(operationHandler),
             getIndexRange(),
             getResumeSecondaryKey(),
             getResumePrimaryKey(),
             false, /* false means that data is fetched and returned */
             getBatchSize(),
             new TableOperationHandler.IndexScanVisitor() {
                 @Override
                 public int visit(SecondaryCursor cursor,
                                  DatabaseEntry indexKeyEntry,
                                  DatabaseEntry primaryKeyEntry,
                                  DatabaseEntry dataEntry) {

                     if (!inRange(indexKeyEntry.getData())) {
                         return TableOperationHandler.STOP;
                     }

                     /*
                      * Data has been fetched, so there is no need to call
                      * Cursor.getCurrent().
                      */
                     assert !dataEntry.getPartial();
                     final ResultValueVersion valVers =
                         operationHandler.makeValueVersion(cursor,
                                                           dataEntry);
                     int numResults = 1;
                     numResults +=
                         ancestors.addAncestorValues(primaryKeyEntry,
                                                     results);
                     results.add(new ResultKeyValueVersion
                                 (primaryKeyEntry.getData(),
                                  valVers.getValueBytes(),
                                  valVers.getVersion()));
                     return numResults;
                 }

                 @Override
                 public OperationStatus getNext(SecondaryCursor cursor,
                                                DatabaseEntry indexKeyEntry,
                                                DatabaseEntry primaryKeyEntry,
                                                DatabaseEntry dataEntry) {
                     return getNextRecord(cursor, indexKeyEntry,
                                          primaryKeyEntry, dataEntry);
                 }
                 @Override
                 public OperationStatus getPrev(SecondaryCursor cursor,
                                                DatabaseEntry indexKeyEntry,
                                                DatabaseEntry primaryKeyEntry,
                                                DatabaseEntry dataEntry) {
                     return getPreviousRecord(cursor, indexKeyEntry,
                                              primaryKeyEntry, dataEntry);
                 }
             });
        boolean more = (moreElements && results.size() == 0) ? false :
            moreElements;
        return new Result.IterateResult(getOpCode(), results, more);
    }
}
