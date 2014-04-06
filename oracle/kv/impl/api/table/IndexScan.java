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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static oracle.kv.impl.api.table.TableAPIImpl.getBatchSize;
import static oracle.kv.impl.api.table.TableAPIImpl.getConsistency;
import static oracle.kv.impl.api.table.TableAPIImpl.getTimeout;
import static oracle.kv.impl.api.table.TableAPIImpl.getTimeoutUnit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.Consistency;
import oracle.kv.RequestTimeoutException;
import oracle.kv.StoreIteratorException;
import oracle.kv.ValueVersion;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.Request;
import oracle.kv.impl.api.TopologyManager;
import oracle.kv.impl.api.TopologyManager.PostUpdateListener;
import oracle.kv.impl.api.ops.IndexIterate;
import oracle.kv.impl.api.ops.IndexKeysIterate;
import oracle.kv.impl.api.ops.InternalOperation;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.api.ops.ResultKeyValueVersion;
import oracle.kv.impl.api.ops.ResultTableIndex;
import oracle.kv.impl.api.parallelscan.DetailedMetricsImpl;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.stats.DetailedMetrics;
import oracle.kv.table.KeyPair;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.Row;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableIteratorOptions;

/**
 * Implementation of a scatter-gather iterator for secondary indexes. The
 * iterator will access all the shards in the store, possibly in parallel,
 * and sort the values before returning them through the next() method.
 * <p>
 * Theory of Operation
 * <p>
 * When the iterator is created, one {@code ShardStream} instance is created
 * for each shard. {@code ShardStream} maintains state required for reading
 * from the store, and the returned data. The instances are kept in a
 * {@code TreeSet} and remain until there are no more records available
 * from the shard. How the streams are sorted is described below.
 * <p>
 * When the {@code ShardStream} is first created it is submitted to the
 * thread pool executor. When run, it will attempt to read {@code batchSize}
 * number of records from the store. The returned data is placed on the
 * {@code blocks} queue. If there are more records in the store and there
 * is room on the queue, the stream re-submits itself to eventually read
 * another block. There is locking which will prevent the stream from being
 * submitted more than once. There is more on threads and reading below.
 * <p>
 * Sorting
 * <p>
 * {@code ShardStream} implements {@code Comparable}. (Note that
 * {@code ShardStream} has a natural ordering that is inconsistent with
 * {@code equals}.) When a stream is inserted into the {@code TreeSet} it
 * will sort (using {@code Comparable.compareTo}) on the next element in the
 * stream. If the stream does not have a next element (because the stream has
 * been drained and the read from the store has not yet returned)
 * {@code Comparable.compareTo()} will return -1 causing the stream to sort
 * to the beginning of the set. This means that the first stream in the
 * {@code TreeSet} has the overall next element or is empty waiting on data.
 * See the section below on Direction for an exception to behavior.
 * <p>
 * To get the next overall element the basic steps are as follows:
 * <p>
 * 1. Remove the first stream from the {@code TreeSet} <br>
 * 2. Remove the next element from the stream  <br>
 * 3. Re-insert the stream into the {@code TreeSet}  <br>
 * 4. If the element is not null, return  <br>
 * 5. If the element is null, wait and go back to 1  <br>
 * <p>
 * Removing the element at #2 will cause the stream to update the next
 * element with the element next in line (if any). This will cause the
 * stream to sort based on the new element when re-inserted at #3.
 * <p>
 * There is an optimization at step #5 which will skip the wait if
 * removing an element (#2) resulting in the stream having a non-null
 * next element.
 * <p>
 * Reading
 * <p>
 * Initially, each {@code ShardStream} is submitted to the executor. As
 * mentioned above the {@code ShardStream} will read a block and if
 * there is more data available and space in the queue, will re-submit
 * itself to read more data. This will result in reading {@code QUEUE_SIZE}
 * blocks for each shard. The {@code ShardStream} is not re-submitted once the
 * queue is filled. If reading from the store results in an end-of-data, a
 * flag is set preventing the stream from being submitted.
 * <p>
 * Once elements are removed from the iterator and a block is removed from the
 * queue, an attempt is made to submit a stream for reading. This happens
 * at step #2 above in {@code ShardStream.removeNext()}. The method
 * {@code removeNext()} will first attempt to remove an element from the
 * current block. If that block is empty it removes the next block on the
 * queue, making that the current block. At this point the stream is
 * submitted to the executor.
 * <p>
 * Discussion of inclusive/exclusive iterations
 * <p>
 * Each request sent to the server side needs a start or resume key and an
 * optional end key. By default these are inclusive.  A {@code FieldRange}
 * object may be included to exercise fine control over start/end values for
 * range queries.  {@code FieldRange} indicates whether the values are inclusive
 * or exclusive.  {@code FieldValue} objects are typed so the
 * inclusive/exclusive state is handled here (on the client side) where they
 * can be controlled per-type rather than on the server where they are simple
 * {@code byte[]}. This means that the start/end/resume keys are always
 * inclusive on the server side.
 */
class IndexScan {

    /* Time to wait for data to be returned form the store */
    private static final long WAIT_TIME_MS = 100L;

    /* TODO - configurable? Perhaps based on batch size? */
    /*
     * The size of the queue of blocks in each stream. Note that the maximum
     * number of blocks maintained by the stream is QUEUE_SIZE + 1. (The queue
     * plus the current block).
     */
    private static final int QUEUE_SIZE = 3;

    /*
     * Use to convert, instead of TimeUnit.toMills(), when you don't want
     * a negative result.
     */
    private static final long NANOS_TO_MILLIS = 1000000L;

    /* Prevent construction */
    private IndexScan() {}

    /**
     * Creates a table iterator returning ordered rows.
     *
     * @param store
     * @param getOptions
     * @param iterateOptions
     *
     * @return a table iterator
     */
    static TableIterator<Row> createTableIterator
        (final TableAPIImpl apiImpl,
         final IndexKeyImpl indexKey,
         final MultiRowOptions getOptions,
         final TableIteratorOptions iterateOptions) {

        final TargetTables targetTables =
            TableAPIImpl.makeTargetTables(indexKey.getTable(), getOptions);

        return new IndexScanIterator<Row>(apiImpl.getStore(),
                                          indexKey,
                                          getOptions,
                                          iterateOptions) {
            @Override
            protected InternalOperation createOp(byte[] resumeSecondaryKey,
                                                 byte[] resumePrimaryKey) {
                return new IndexIterate(index.getName(),
                                        targetTables,
                                        range,
                                        resumeSecondaryKey,
                                        resumePrimaryKey,
                                        batchSize);
            }

            @Override
            protected void convertResult(Result result, List<Row> rows) {
                final List<ResultKeyValueVersion> keyValueVersionList =
                                            result.getKeyValueVersionList();
                for (ResultKeyValueVersion keyValue : keyValueVersionList) {
                    Row converted = convert(keyValue);
                    if (converted != null) {
                        rows.add(converted);
                    }
                }
            }

            /**
             * Converts a single key value into a row.
             */
            private Row convert(ResultKeyValueVersion keyValue) {
                if (keyValue == null) {
                    return null;
                }

                /*
                 * If ancestor table returns may be involved, start at the
                 * top level table of this hierarchy.
                 */
                final TableImpl startingTable =
                    targetTables.hasAncestorTables() ?
                    table.getTopLevelTable() : table;
                final RowImpl fullKey =
                    startingTable.createRowFromKeyBytes(keyValue.getKeyBytes());
                if (fullKey == null) {
                    return null;
                }
                final ValueVersion vv =
                    new ValueVersion(keyValue.getValue(),
                                     keyValue.getVersion());
                return apiImpl.getRowFromValueVersion(vv, fullKey, false);

            }

            @Override
            protected byte[] extractResumeSecondaryKey(Row row) {
                /*
                 * The resumeKey is the byte[] value of the IndexKey from
                 * the last Row returned.
                 */
                return index.serializeIndexKey(index.createIndexKey(row));
            }

            @Override
            protected byte[] extractResumePrimaryKey(Row row) {
                TableKey key =
                    TableKey.createKey(((RowImpl) row).getTableImpl(),
                                       row, false);
                return key.getKeyBytes();
            }

            @Override
            protected int compare(Row one, Row two) {
                return ((RecordValueImpl) one).compare((RecordValueImpl)two,
                                                       indexKey.getFields());
            }
        };
    }

    /**
     * Creates a table iterator returning ordered key pairs.
     *
     * @param store
     * @param indexKey
     * @param indexRange
     * @param batchSize
     * @param consistency
     * @param timeout
     * @param timeoutUnit
     *
     * @return a table iterator
     */
    static TableIterator<KeyPair>
        createTableKeysIterator(final TableAPIImpl apiImpl,
                                final IndexKeyImpl indexKey,
                                final MultiRowOptions getOptions,
                                final TableIteratorOptions iterateOptions) {

        final TargetTables targetTables =
            TableAPIImpl.makeTargetTables(indexKey.getTable(), getOptions);

        return new IndexScanIterator<KeyPair>(apiImpl.getStore(),
                                              indexKey,
                                              getOptions,
                                              iterateOptions) {
            @Override
            protected InternalOperation createOp(byte[] resumeSecondaryKey,
                                                 byte[] resumePrimaryKey) {
                return new IndexKeysIterate(index.getName(),
                                            targetTables,
                                            range,
                                            resumeSecondaryKey,
                                            resumePrimaryKey,
                                            batchSize);
            }

            /**
             * Convert the results to KeyPair instances.  Note that in the
             * case where ancestor and/or child table returns are requested
             * the IndexKey returned is based on the the index and the table
             * containing the index, but the PrimaryKey returned may be from
             * a different, ancestor or child table.
             */
            @Override
            protected void convertResult(Result result,
                                         List<KeyPair> elementList) {
                final List<ResultTableIndex> results =
                    result.getTableIndexList();
                for (ResultTableIndex res : results) {
                    final IndexKeyImpl indexKeyImpl =
                                        convertIndexKey(res.getIndexKeyBytes());
                    if (indexKeyImpl != null) {
                        final PrimaryKeyImpl pkey =
                            convertPrimaryKey(res.getPrimaryKeyBytes());
                        if (pkey != null) {
                            elementList.add(new KeyPair(pkey, indexKeyImpl));
                        }
                    }
                }
            }

            @Override
            protected byte[] extractResumeSecondaryKey(KeyPair element) {
                /*
                 * The resumeKey is the byte[] value of the IndexKey from
                 * the last Row returned.
                 */
                return index.serializeIndexKey
                    (((IndexKeyImpl)element.getIndexKey()));
            }

            @Override
            protected byte[] extractResumePrimaryKey(KeyPair element) {
                PrimaryKeyImpl pkey = (PrimaryKeyImpl) element.getPrimaryKey();
                TableKey key =
                    TableKey.createKey(pkey.getTableImpl(), pkey, false);
                return key.getKeyBytes();
            }

            @Override
            protected int compare(KeyPair one, KeyPair two) {
                return one.compareTo(two);
            }

            private IndexKeyImpl convertIndexKey(byte[] bytes) {
                return index.rowFromIndexKey(bytes, false);
            }

            private PrimaryKeyImpl convertPrimaryKey(byte[] bytes) {
                /*
                 * If ancestor table returns may be involved, start at the
                 * top level table of this hierarchy.
                 */
                final TableImpl startingTable =
                    targetTables.hasAncestorTables() ?
                    table.getTopLevelTable() : table;
                return startingTable.createPrimaryKeyFromKeyBytes(bytes);
            }
        };
    }

    /**
     * Base class for building index iterators.
     *
     * @param <K> the type of elements returned by the iterator
     */
    private static abstract class IndexScanIterator<K>
        implements TableIterator<K>,
                   PostUpdateListener {
        private final KVStoreImpl store;
        private final Logger logger;

        private final Consistency consistency;
        private final long timeoutMs;

        protected final IndexImpl index;
        protected final TableImpl table;
        protected final IndexRange range;
        protected final int batchSize;

        /*
         * The number of shards when the itertator was created. If this changes
         * we must abort the operation as data may have been mised during the
         * time the new shard came on-line and when we noticied it. Note that
         * this assumes the number of groups can only increase. Once we support
         * store contraction this may need to be modified;
         */
        private final int nGroups;

        /*
         * The sorted set of streams. Only streams that have elements or are
         * waiting on reads are in the set. Streams that have exhausted the
         * store are discarded.
         */
        private final TreeSet<ShardStream> streams;

        private final ThreadPoolExecutor executor;

        /* True if the iterator has been closed */
        private volatile boolean closed = false;

        /* The exception passed to close(Exception) */
        private Exception closeException = null;

        /*
         * The next element to be returned from this iterator. This may be null
         * if waiting for reads to complete.
         */
        private K next = null;

        /* Per shard metrics provided through ParallelScanIterator */
        private final Map<RepGroupId, DetailedMetricsImpl> shardMetrics =
                                new HashMap<RepGroupId, DetailedMetricsImpl>();

        private IndexScanIterator(KVStoreImpl store,
                                  IndexKeyImpl indexKey,
                                  MultiRowOptions getOptions,
                                  TableIteratorOptions iterateOptions) {
            this.store = store;
            this.range =
                new IndexRange(indexKey, getOptions, iterateOptions);
            this.consistency = getConsistency(iterateOptions);
            final long timeout = getTimeout(iterateOptions);
            timeoutMs = (timeout == 0) ? store.getDefaultRequestTimeoutMs() :
                getTimeoutUnit(iterateOptions).toMillis(timeout);
            if (timeoutMs <= 0) {
                throw new IllegalArgumentException("Timeout must be > 0 ms");
            }
            this.batchSize = getBatchSize(iterateOptions);
            index = indexKey.getIndexImpl();
            table = index.getTableImpl();
            logger = store.getLogger();

            /* Collect group information from the current topology. */
            final TopologyManager topoManager =
                                    store.getDispatcher().getTopologyManager();
            final Topology topology = topoManager.getTopology();
            final Set<RepGroupId> groups = topology.getRepGroupIds();
            nGroups = groups.size();
            if (nGroups == 0) {
                throw new IllegalStateException("Store not yet initialized");
            }

            int nThreads = Math.min(nGroups,
                                    Runtime.getRuntime().availableProcessors());
            if (nThreads == 0) {
                nThreads = 1;
            }
            executor = new IndexScanExecutor(nThreads, logger);

            streams = new TreeSet<ShardStream>();

            /* For each shard, create a stream and start reading */
            for (RepGroupId groupId : groups) {
                final ShardStream stream = new ShardStream(groupId, null, null);
                streams.add(stream);
                stream.submit();
            }
            /* Register a listener to detect changes in the groups (shards). */
            topoManager.addPostUpdateListener(this);
        }

        /**
         * Create an operation using the specified resume key. The resume key
         * parameters may be null.
         *
         * @param resumeSecondaryKey a resume key or null
         * @param resumePrimaryKey a resume key or null
         * @return an operation
         */
        protected abstract InternalOperation createOp
            (byte[] resumeSecondaryKey, byte[] resumePrimaryKey);

        /**
         * Convert the specified result into list of elements.
         * If a RuntimeException is thrown, the iterator will be
         * closed and the exception return to the application.
         *
         * @param result result object
         * @param resultList list to place the converted elements
         */
        protected abstract void convertResult(Result result,
                                              List<K> elementList);

        /**
         * Returns a resume secondary key based on the specified element.
         *
         * @param element
         * @return a resume secondary key
         */
        protected abstract byte[] extractResumeSecondaryKey(K element);

        /**
         * Returns a resume primary key based on the specified element.
         *
         * @param element
         * @return a resume primaryary key
         */
        protected abstract byte[] extractResumePrimaryKey(K element);

        /**
         * Compares the two elements. Returns a negative integer, zero, or a
         * positive integer if object one is less than, equal to, or greater
         * than object two.
         *
         * @return a negative integer, zero, or a positive integer
         */
        protected abstract int compare(K one, K two);

        /* -- From Iterator -- */

        @Override
        public synchronized boolean hasNext() {
            if (closed) {
                /*
                 * The iterator is closed. If there was an exception during
                 * some internal operation, throw it now.
                 */
                if (closeException != null) {
                    throw new StoreIteratorException(closeException, null);
                }
                return false;
            }

            if (next == null) {
                next = getNext();
            }
            return (next != null);
        }

        @Override
        public synchronized K next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final K lastReturned = next;
            next = null;
            return lastReturned;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        /* -- From ParallelScanIterator -- */

        @Override
        public void close() {
            close(null, true);
        }

        /**
         * Close the iterator, recording the specified remote exception. If
         * the reason is not null, the exception is thrown from the hasNext()
         * or next() methods.
         *
         * @param reason the exception causing the close or null
         * @param remove if true remove the topo listener
         */
        private void close(Exception reason, boolean remove) {
            synchronized (this) {
                if (closed) {
                    return;
                }
                /* Mark this Iterator as terminated */
                closed = true;
                closeException = reason;
            }
            
            if (remove) {
                store.getDispatcher().getTopologyManager().
                                            removePostUpdateListener(this);
            }

            try {
                final List<Runnable> unfinishedBusiness =
                                                        executor.shutdownNow();
                if (unfinishedBusiness != null) {
                    logger.log(Level.WARNING,
                               "Index executor didn''t shutdown cleanly. {0} " +
                               "tasks remaining.", unfinishedBusiness.size());
                }
            } finally {
                /*
                 * Clearing the streams and next will GC any records remaining
                 */
                streams.clear();
                next = null;
            }
        }

        /**
         * Gets the next value in sorted order. If no more values remain or the
         * iterator is canceled, null is returned. This method will block
         * waiting for data from the store.
         *
         * @return the next value in sorted order or null
         */
        private K getNext() {
            final long limitNs = System.nanoTime() +
                                 MILLISECONDS.toNanos(timeoutMs);
            while (!closed) {
                /*
                 * The first stream in the set will contain the next
                 * element in order.
                 */
                final ShardStream stream = streams.pollFirst();

                /* If there are no more streams we are done. */
                if (stream == null) {
                    close();
                    return null;
                }
                final K entry = stream.removeNext();

                /*
                 * Return the stream back to the set where it will sort on the
                 * next element.
                 */
                if (!stream.isDone()) {
                    streams.add(stream);
                }

                if ((entry != null) || closed) {
                    return entry;
                }

                /* The stream is empty, if we have time, wait */
                long waitMs =
                       Math.min((limitNs - System.nanoTime()) / NANOS_TO_MILLIS,
                                WAIT_TIME_MS);
                if (waitMs <= 0) {
                    throw new RequestTimeoutException((int)timeoutMs,
                                                      "Operation timed out",
                                                      null, false);
                }
                stream.waitForNext(waitMs);
            }
            return null;    /* Closed */
        }

        /* -- From PostUpdateListener -- */

        @Override
        public boolean postUpdate(Topology topology) {
            if (closed) {
                return true;
            }
            final TopologyManager topoManager =
                                    store.getDispatcher().getTopologyManager();

            final int newGroupSize = topoManager.getTopology().
                                                        getRepGroupIds().size();
            if (nGroups == newGroupSize) {
                return false;
            }

            /*
             * If the number of groups have changed this iterator needs to be
             * closed. The RE will be reported back to the application from
             * hasNext() or next().
             */
            if (nGroups > newGroupSize) {
                close(new UnsupportedOperationException("The number of shards "+
                                         "has decreased during the iteration"),
                      false);
            }

            /*
             * The number of groups has increased.
             */
            if (nGroups < newGroupSize) {
                close(new UnsupportedOperationException("The number of shards "+
                                         "has increased during the iteration"),
                      false);
            }
            return closed;
        }

        /* -- Metrics from ParallelScanIterator -- */

        @Override
        public List<DetailedMetrics> getPartitionMetrics() {
            return null;
        }

        @Override
        public List<DetailedMetrics> getShardMetrics() {
            synchronized (shardMetrics) {
                final ArrayList<DetailedMetrics> ret =
                    new ArrayList<DetailedMetrics>(shardMetrics.size());
                ret.addAll(shardMetrics.values());
                return ret;
            }
        }

        /**
         * Update the group based metrics for this iterator.
         *
         * @param groupId the group to update
         * @param timeInMs the time spent reading
         * @param recordCount the number of records read
         */
        private void updateMetrics(RepGroupId groupId,
                                   long timeInMs, long recordCount) {
            DetailedMetricsImpl dmi;
            synchronized (shardMetrics) {

                dmi = shardMetrics.get(groupId);
                if (dmi == null) {
                    dmi = new DetailedMetricsImpl(groupId.toString(),
                                                  timeInMs, recordCount);
                    shardMetrics.put(groupId, dmi);
                    return;
                }
            }
            dmi.inc(timeInMs, recordCount);
        }

        @Override
        public String toString() {
            return "IndexScanIterator[" + index.getName() +
                ", " + range.getDirection() + "]";
        }

        /**
         * Object that encapsulates the activity around reading index records
         * of a single shard.
         *
         * Note: this class has a natural ordering that is inconsistent with
         * equals.
         */
        private class ShardStream implements Comparable<ShardStream>,
                                             Runnable {
            protected final RepGroupId groupId;

            protected byte[] resumeSecondaryKey;
            protected byte[] resumePrimaryKey;

            /* The queue of blocks. */
            private final BlockingQueue<List<K>> blocks =
                                new LinkedBlockingQueue<List<K>>(QUEUE_SIZE);

            /* The block of values being drained */
            private List<K> currentBlock;

            /* The last element removed, used for sorting */
            private K nextElem = null;

            /* False if nothing left to read */
            private boolean doneReading = false;

            /* True if there are no more values */
            private boolean done = false;

            /* True if this stream is  */
            private boolean active = false;

            ShardStream(RepGroupId groupId, byte[] resumeSecondaryKey,
                        byte[] resumePrimaryKey) {
                this.groupId = groupId;
                this.resumeSecondaryKey = resumeSecondaryKey;
                this.resumePrimaryKey = resumePrimaryKey;
            }

            /**
             * Remove the next element from this stream and return it. If no
             * elements are available null is returned.
             *
             * @return the next element from this stream or null
             */
            K removeNext() {
                assert !done;

                final K ret = nextElem;
                nextElem = ((currentBlock == null) ||
                        currentBlock.isEmpty()) ? null : currentBlock.remove(0);

                /*
                 * If there are no more results in the current block, attempt
                 * to get a new block.
                 */
                if (nextElem == null) {
                    synchronized (this) {
                        currentBlock = blocks.poll();

                        /*
                         * We may have pulled a block off the queue, submit this
                         * stream to get more.
                         */
                        submit();

                        /*
                         * If there are no more blocks and we are done reading
                         * then we are finished.
                         */
                        if (currentBlock == null) {
                            done = doneReading;
                        } else {
                            /* TODO - can this be empty? */
                            nextElem = currentBlock.remove(0);
                        }
                    }
                }
                return ret;
            }

            /**
             * Waits up to waitMs for the next element to be available.
             *
             * @param waitMs the max time in ms to wait
             */
            private void waitForNext(long waitMs) {
                /*
                 * If the stream was previously empty, but it now has a value,
                 * skip the sleep and immediately try again. This can happen
                 * when the stream is initially created (priming the pump) and
                 * when the reads are not keeping up with removal through the
                 * iterator.
                 */
                if (nextElem != null) {
                    return;
                }
                try {
                    synchronized (this) {
                        /* Wait if there is no data left and still reading */
                        if (blocks.isEmpty() && !doneReading) {
                            wait(waitMs);
                        }
                    }
                } catch (InterruptedException ex) {
                    if (!closed) {
                        logger.log(Level.WARNING, "Unexpected interrupt ", ex);
                    }
                }
            }

            /**
             * Returns true if all of the elements have been removed from this
             * stream.
             */
            boolean isDone() {
                return done;
            }

            /**
             * Submit this stream to to request data if there it isn't already
             * submitted, there is more data to read, and there is room for it.
             */
            private synchronized void submit() {
                if (active ||
                    doneReading ||
                    (blocks.remainingCapacity() == 0)) {
                    return;
                }
                active = true;
                executor.submit(this);
            }

            @Override
            public void run() {
                try {
                    assert active;
                    assert !doneReading;
                    assert blocks.remainingCapacity() > 0;

                    final long start = System.nanoTime();
                    final int count = readBlock();

                    final long end = System.nanoTime();
                    final long thisTimeMs = (end - start) / NANOS_TO_MILLIS;

                    updateMetrics(groupId, thisTimeMs, count);
                } catch (RuntimeException re) {
                    active = false;
                    close(re, true);
                }
            }

            /**
             * Read a block of records from the store. Returns the number of
             * records read.
             */
            private int readBlock() {
                final Request req =
                    store.makeReadRequest
                    (createOp(resumeSecondaryKey, resumePrimaryKey),
                     groupId,
                     consistency,
                     timeoutMs,
                     MILLISECONDS);
                final Result result = store.executeRequest(req);
                final boolean hasMore = result.hasMoreElements();
                int nRecords = result.getNumRecords();

                /*
                 * Convert the results into a list of elements ready to be
                 * returned from the iterator.
                 */
                List<K> elementList = null;
                if (nRecords > 0) {
                    elementList = new ArrayList<K>(nRecords);
                    convertResult(result, elementList);

                    /*
                     * convertResult can filter so get the exact number of
                     * converted results.
                     */
                    nRecords = elementList.size();
                    resumeSecondaryKey = (hasMore) ?
                        extractResumeSecondaryKey(elementList.get(nRecords-1)) :
                        null;
                    resumePrimaryKey = (hasMore) ?
                        extractResumePrimaryKey(elementList.get(nRecords-1)) :
                        null;
                }
                synchronized (this) {
                    /* About to exit, active may be reset in submit() */
                    active = false;
                    doneReading = !hasMore;

                    if (nRecords == 0) {
                        assert doneReading;
                        notify();
                        return 0;
                    }
                    assert elementList != null;
                    blocks.add(elementList);

                    /* Wake up the iterator if it is waiting */
                    notify();
                }
                submit();
                return nRecords;
            }

            /**
             * Compares this object with the specified object for order for
             * determining the placement of the stream in the TreeSet. See
             * the IndexScan class doc a detailed description of its operation.
             */
            @Override
            public int compareTo(ShardStream other) {

                /*
                 * If unordered, we skip comparing the streams and always sort
                 * to the top if we have a next element. If no elements, sort
                 * to the bottom. This will keep full streams at the top and
                 * empty streams at the bottom.
                 */
                if (range.isUnordered()) {
                    return (nextElem == null) ? 1 : -1;
                }

                /* Forward or reverse */

                /* If we don't have a next, then sort to the top of the set. */
                if (nextElem == null) {
                    return -1;
                }

                /* If the other next is null then keep them on top. */
                final K otherNext = other.nextElem;
                if (otherNext == null) {
                    return 1;
                }

                /*
                 * Finally, compare the elements, inverting the result
                 * if necessary.
                 */
                final int comp = compare(nextElem, otherNext);
                return (range.isForward()) ? comp : (comp * -1);
            }

            @Override
            public String toString() {
                return "ShardStream[" + groupId + ", " + done + ", " +
                       active + ", " + doneReading + ", " + blocks.size() + "]";
            }
        }
    }

    private static class IndexScanExecutor extends ScheduledThreadPoolExecutor {
        IndexScanExecutor(int nThreads, Logger logger) {
            super(nThreads, new KVThreadFactory(" index scan", logger));
            setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        }
    }
}
