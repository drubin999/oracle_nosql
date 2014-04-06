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

package oracle.kv.impl.api.table;

import java.util.Arrays;
import java.util.List;

import oracle.kv.Depth;
import oracle.kv.ParallelScanIterator;
import oracle.kv.StoreIteratorConfig;
import oracle.kv.ValueVersion;
import oracle.kv.impl.api.StoreIteratorParams;
import oracle.kv.impl.api.ops.InternalOperation;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.api.ops.ResultKeyValueVersion;
import oracle.kv.impl.api.ops.TableIterate;
import oracle.kv.impl.api.ops.TableKeysIterate;
import oracle.kv.impl.api.parallelscan.ParallelScan.ConvertResultsReturnValue;
import oracle.kv.impl.api.parallelscan.ParallelScan.ParallelScanIteratorImpl;
import oracle.kv.impl.api.parallelscan.ParallelScan.ResultsQueueEntry;
import oracle.kv.stats.DetailedMetrics;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableIteratorOptions;

/**
 * Implementation of the table iterators. These iterators are not sorted
 * and are partition- vs shard-based. They extend the parallel scan
 * code.
 */
class TableScan {

    /* Prevent construction */
    private TableScan() {}

    /**
     * Creates a table iterator returning un-ordered rows.
     *
     * @param store
     * @param key
     * @param getOptions
     * @param iterateOptions
     *
     * @return a table iterator
     */
    static TableIterator<Row>
        createTableIterator(final TableAPIImpl apiImpl,
                            final TableKey key,
                            final MultiRowOptions getOptions,
                            final TableIteratorOptions iterateOptions) {

        final TargetTables targetTables =
            TableAPIImpl.makeTargetTables(key.getTable(), getOptions);

        final StoreIteratorConfig config = new StoreIteratorConfig();
        if (iterateOptions != null) {
            config.setMaxConcurrentRequests(
                            iterateOptions.getMaxConcurrentRequests());
            config.setMaxResultsBatches(iterateOptions.getMaxResultsBatches());
        }

        final StoreIteratorParams params =
            new StoreIteratorParams(TableAPIImpl.getDirection(iterateOptions,
                                                              key),
                                    TableAPIImpl.getBatchSize(iterateOptions),
                                    key.getKeyBytes(),
                                    TableAPIImpl.makeKeyRange(key, getOptions),
                                    Depth.PARENT_AND_DESCENDANTS,
                                    TableAPIImpl.getConsistency(iterateOptions),
                                    TableAPIImpl.getTimeout(iterateOptions),
                                    TableAPIImpl.getTimeoutUnit(iterateOptions));

        return new TableIteratorWrapper<Row>
            (new ParallelScanIteratorImpl<Row>(apiImpl.getStore(),
                                               config, params) {
            @Override
            protected TableIterate generateGetterOp(byte[] resumeKey) {
                return new TableIterate(params,
                                        targetTables,
                                        key.getMajorKeyComplete(),
                                        resumeKey);
            }

            @Override
            protected ConvertResultsReturnValue
                convertResults(Result result) {
                final List<ResultKeyValueVersion> byteKeyResults =
                    result.getKeyValueVersionList();

                final int cnt = byteKeyResults.size();
                if (cnt == 0) {
                    assert (!result.hasMoreElements());
                    return new ConvertResultsReturnValue(0, null);
                }

                final byte[] resumeKey = byteKeyResults.get(cnt - 1).
                    getKeyBytes();

                /*
                 * Convert byte[] keys and values to Row objects.
                 */
                @SuppressWarnings("unchecked")
                ResultsQueueEntry<Row>[] rowResults =
                    new ResultsQueueEntry[cnt];

                int actualCount = 0;
                for (ResultKeyValueVersion entry : byteKeyResults) {
                    TableImpl table = (TableImpl)key.getTable();

                    /*
                     * If there are ancestor tables, start looking at the top
                     * of the hierarchy to catch them.
                     */
                    if (targetTables.hasAncestorTables()) {
                        table = table.getTopLevelTable();
                    }
                    RowImpl fullKey =
                        table.createRowFromKeyBytes(entry.getKeyBytes());
                    if (fullKey != null) {
                        final ValueVersion vv =
                            new ValueVersion(entry.getValue(),
                                             entry.getVersion());
                        final Row row =
                            apiImpl.getRowFromValueVersion(vv, fullKey, false);
                        if (row != null) {
                            rowResults[actualCount++] =
                                new ResultsQueueEntry<Row>(row, null);
                        }
                    }
                }

                /*
                 * If any results were skipped, copy the valid results to
                 * a new array.  This should not be common at all.
                 */
                if (actualCount < cnt) {
                    putResult(Arrays.copyOf(rowResults, actualCount));
                } else {
                    putResult(rowResults);
                }
                return new ConvertResultsReturnValue(actualCount,
                                                     resumeKey);
            }
        });
    }

    /**
     * Creates a table iterator returning un-ordered primary keys.
     *
     * @param store
     * @param key
     * @param getOptions
     * @param iterateOptions
     *
     * @return a table iterator
     */
    static TableIterator<PrimaryKey>
        createTableKeysIterator(final TableAPIImpl apiImpl,
                                final TableKey key,
                                final MultiRowOptions getOptions,
                                final TableIteratorOptions iterateOptions) {
        final TargetTables targetTables =
            TableAPIImpl.makeTargetTables(key.getTable(), getOptions);

        final StoreIteratorConfig config = new StoreIteratorConfig();
        if (iterateOptions != null) {
            config.setMaxConcurrentRequests(
                            iterateOptions.getMaxConcurrentRequests());
            config.setMaxResultsBatches(iterateOptions.getMaxResultsBatches());
        }

        final StoreIteratorParams params =
            new StoreIteratorParams(TableAPIImpl.getDirection(iterateOptions,
                                                              key),
                                    TableAPIImpl.getBatchSize(iterateOptions),
                                    key.getKeyBytes(),
                                    TableAPIImpl.makeKeyRange(key, getOptions),
                                    Depth.PARENT_AND_DESCENDANTS,
                                    TableAPIImpl.getConsistency(iterateOptions),
                                    TableAPIImpl.getTimeout(iterateOptions),
                                    TableAPIImpl.getTimeoutUnit(iterateOptions));

        return new TableIteratorWrapper<PrimaryKey>
            (new ParallelScanIteratorImpl<PrimaryKey>(apiImpl.getStore(),
                                                      config, params) {
            @Override
            protected InternalOperation generateGetterOp(byte[] resumeKey) {
                return new TableKeysIterate(params,
                                            targetTables,
                                            key.getMajorKeyComplete(),
                                            resumeKey);
            }

            @SuppressWarnings("unchecked")
			@Override
            protected ConvertResultsReturnValue
                convertResults(Result result) {
                final List<byte[]> byteKeyResults = result.getKeyList();

                final int cnt = byteKeyResults.size();
                if (cnt == 0) {
                    assert (!result.hasMoreElements());
                    return new ConvertResultsReturnValue(0, null);
                }
                final byte[] resumeKey = byteKeyResults.get(cnt - 1);

                /*
                 * Convert byte[] keys to PrimaryKey objects.
                 */
                ResultsQueueEntry<PrimaryKey>[] keyResults =
                    new ResultsQueueEntry[cnt];

                int actualCount = 0;
                for (byte[] entry : byteKeyResults) {
                    TableImpl table = (TableImpl)key.getTable();

                    /*
                     * If there are ancestor tables, start looking at the top
                     * of the hierarchy to catch them.
                     */
                    if (targetTables.hasAncestorTables()) {
                        table = table.getTopLevelTable();
                    }
                    final PrimaryKey pKey =
                        table.createPrimaryKeyFromKeyBytes(entry);
                    if (pKey != null) {
                        keyResults[actualCount++] =
                            new ResultsQueueEntry<PrimaryKey>(pKey, null);
                    }
                }

                /*
                 * If any results were skipped, copy the valid results to
                 * a new array.  This should not be common at all.
                 */

                if (actualCount < cnt) {
                    putResult(Arrays.copyOf(keyResults, actualCount));
                } else {
                    putResult(keyResults);
                }
                return new ConvertResultsReturnValue(actualCount,
                                                     resumeKey);
            }
        });
    }

    /**
     * Wrapper class for ParallelScanIterator.
     */
    private static class TableIteratorWrapper<K> implements TableIterator<K> {

        private final ParallelScanIterator<K> psi;

        private TableIteratorWrapper(ParallelScanIterator<K> psi) {
            this.psi = psi;
        }

        @Override
        public void close() {
             psi.close();
        }

        @Override
        public List<DetailedMetrics> getPartitionMetrics() {
            return psi.getPartitionMetrics();
        }

        @Override
        public List<DetailedMetrics> getShardMetrics() {
            return psi.getShardMetrics();
        }

        @Override
        public K next() {
            return psi.next();
        }

        @Override
        public boolean hasNext() {
            return psi.hasNext();
        }

        @Override
        public void remove() {
            psi.remove();
        }
    }
}
