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

package oracle.kv.impl.api.parallelscan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import java.util.logging.Logger;

import com.sleepycat.je.utilint.PropUtil;

import oracle.kv.Consistency;
import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.FaultException;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.KeyValueVersion;
import oracle.kv.ParallelScanIterator;
import oracle.kv.RequestTimeoutException;
import oracle.kv.StoreIteratorConfig;
import oracle.kv.StoreIteratorException;
import oracle.kv.impl.api.KeySerializer;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.Request;
import oracle.kv.impl.api.StoreIteratorParams;
import oracle.kv.impl.api.ops.InternalOperation;
import oracle.kv.impl.api.ops.MultiKeyIterate;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.api.ops.ResultKeyValueVersion;
import oracle.kv.impl.api.ops.StoreIterate;
import oracle.kv.impl.api.ops.StoreKeysIterate;
import oracle.kv.impl.api.parallelscan.ParallelScanHook.HookType;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.stats.DetailedMetrics;

/**
 * ParallelScan implements a multi-threaded storeIterator or storeKeysIterator.
 * Partition iteration "tasks" are created and submitted to a
 * ScheduledThreadPoolExecutor which has either maxConcurrentRequests threads
 * or a number of threads computed by this class (max
 * Runtime.availableProcessors()). Tasks are Runnables that iterate over a
 * partition. Tasks (the Runnables) make RMI StoreIterate (or StoreKeyIterate)
 * calls to the server and then place the results on a "Results Queue"
 * (implemented using a BlockingQueue). The application thread (i.e. the thread
 * which made the actual storeIterator call) reads results from the Results
 * Queue and returns them to the user.
 *
 * Most ParallelScan invocations will have (a) multiple partition iteration
 * threads to send requests to the replication nodes and put the results on the
 * Results Queue, and (b) a single application thread taking the results off
 * the Results Queue and processing them. In order to provide the application
 * with tuning feedback about the proper number of threads, four stats are
 * available. Two of them keep track of the number of times (and time spent) a
 * "put" call on the Results Queue was blocked. The other two stats keep track
 * of the number of times (and time spent) that a "take" call on the Results
 * Queue blocked. The former are indicative of too many threads and the latter
 * indicative of not enough.
 *
 * The Iterator returned is thread safe.
 */
public class ParallelScan {

    private static final long NANOS_TO_MILLIS = 1000000L;

    /*
     * The max # of threads that we'll spawn if user passes 0 for
     * maxConcurrentRequests.
     */
    private static final int MAX_COMPUTED_NTHREADS =
        Runtime.getRuntime().availableProcessors();

    /* Prevent construction */
    private ParallelScan() {}

    /*
     * The entrypoint to ParallelScan from KVStoreImpl.storeKeysIterate.
     */
    public static ParallelScanIterator<Key>
        createParallelKeyScan(final KVStoreImpl storeImpl,
                              final Direction direction,
                              final int batchSize,
                              final Key parentKey,
                              final KeyRange subRange,
                              final Depth depth,
                              final Consistency consistency,
                              final long timeout,
                              final TimeUnit timeoutUnit,
                              final StoreIteratorConfig storeIteratorConfig)
        throws FaultException {

        if (direction != Direction.UNORDERED) {
            throw new IllegalArgumentException
                ("Only Direction.UNORDERED is currently supported, got: " +
                 direction);
        }

        if ((parentKey != null) && (parentKey.getMinorPath().size()) > 0) {
            throw new IllegalArgumentException
                ("Minor path of parentKey must be empty");
        }

        final byte[] parentKeyBytes =
            (parentKey != null) ?
                    storeImpl.getKeySerializer().toByteArray(parentKey) : null;

        /* Prohibit iteration of internal keyspace (//). */
        final KeyRange useRange = storeImpl.getKeySerializer().restrictRange
            (parentKey, subRange);

        final StoreIteratorParams parallelKeyScanSIP =
            new StoreIteratorParams(direction,
                                    batchSize,
                                    parentKeyBytes,
                                    useRange,
                                    depth,
                                    consistency,
                                    timeout,
                                    timeoutUnit);

        return new ParallelScanIteratorImpl<Key>(storeImpl,
                                                 storeIteratorConfig,
                                                 parallelKeyScanSIP) {
            @Override
            protected MultiKeyIterate generateGetterOp(byte[] resumeKey) {
                return new StoreKeysIterate
                    (storeIteratorParams.getParentKeyBytes(),
                     storeIteratorParams.getSubRange(),
                     storeIteratorParams.getDepth(),
                     storeIteratorParams.getDirection(),
                     storeIteratorParams.getBatchSize(),
                     resumeKey);
            }

            @Override
            protected ConvertResultsReturnValue convertResults(Result result) {
                /* Get results and save resume key. */
                final List<byte[]> byteKeyResults = result.getKeyList();

                int cnt = byteKeyResults.size();
                if (cnt == 0) {
                    assert (!result.hasMoreElements());
                    return new ConvertResultsReturnValue(0, null);
                }

                final byte[] resumeKey = byteKeyResults.get(cnt - 1);

                /* Convert byte[] keys to KeyValueVersion objects. */
                @SuppressWarnings("unchecked")
                final ResultsQueueEntry<Key>[] stringKeyResults =
                    new ResultsQueueEntry[cnt];
                for (int i = 0; i < cnt; i += 1) {
                    final byte[] entry = byteKeyResults.get(i);
                    stringKeyResults[i] = new ResultsQueueEntry<Key>
                         (keySerializer.fromByteArray(entry), null);
                }

                putResult(stringKeyResults);
                return new ConvertResultsReturnValue(cnt, resumeKey);
            }
        };
    }

    /*
     * The entrypoint to ParallelScan from KVStoreImpl.storeIterate.
     */
    public static ParallelScanIterator<KeyValueVersion>
        createParallelScan(final KVStoreImpl storeImpl,
                           final Direction direction,
                           final int batchSize,
                           final Key parentKey,
                           final KeyRange subRange,
                           final Depth depth,
                           final Consistency consistency,
                           final long timeout,
                           final TimeUnit timeoutUnit,
                           final StoreIteratorConfig storeIteratorConfig)
        throws FaultException {

        if (direction != Direction.UNORDERED) {
            throw new IllegalArgumentException
                ("Only Direction.UNORDERED is currently supported, got: " +
                 direction);
        }

        if ((parentKey != null) && (parentKey.getMinorPath().size()) > 0) {
            throw new IllegalArgumentException
                ("Minor path of parentKey must be empty");
        }

        final byte[] parentKeyBytes =
            (parentKey != null) ?
            storeImpl.getKeySerializer().toByteArray(parentKey) :
            null;

        /* Prohibit iteration of internal keyspace (//). */
        final KeyRange useRange = storeImpl.getKeySerializer().restrictRange
            (parentKey, subRange);

        final StoreIteratorParams parallelScanSIP =
            new StoreIteratorParams(direction,
                                    batchSize,
                                    parentKeyBytes,
                                    useRange,
                                    depth,
                                    consistency,
                                    timeout,
                                    timeoutUnit);

        return new ParallelScanIteratorImpl<KeyValueVersion>
            (storeImpl, storeIteratorConfig, parallelScanSIP) {
            @Override
            protected MultiKeyIterate generateGetterOp(byte[] resumeKey) {
                return new StoreIterate(storeIteratorParams.getParentKeyBytes(),
                                        storeIteratorParams.getSubRange(),
                                        storeIteratorParams.getDepth(),
                                        storeIteratorParams.getDirection(),
                                        storeIteratorParams.getBatchSize(),
                                        resumeKey);
            }

            @Override
            protected ConvertResultsReturnValue convertResults(Result result) {
                /* Get results and save resume key. */
                final List<ResultKeyValueVersion> byteKeyResults =
                    result.getKeyValueVersionList();

                int cnt = byteKeyResults.size();
                if (cnt == 0) {
                    assert (!result.hasMoreElements());
                    return new ConvertResultsReturnValue(0, null);
                }

                final byte[] resumeKey =
                    byteKeyResults.get(cnt - 1).getKeyBytes();

                /* Convert byte[] keys to KeyValueVersion objects. */
                @SuppressWarnings("unchecked")
                final ResultsQueueEntry<KeyValueVersion>[] stringKeyResults =
                    new ResultsQueueEntry[cnt];
                for (int i = 0; i < cnt; i += 1) {
                    final ResultKeyValueVersion entry = byteKeyResults.get(i);
                    stringKeyResults[i] = new ResultsQueueEntry<KeyValueVersion>
                        (new KeyValueVersion
                         (keySerializer.fromByteArray(entry.getKeyBytes()),
                          entry.getValue(), entry.getVersion()), null);
                }

                putResult(stringKeyResults);
                return new ConvertResultsReturnValue(cnt, resumeKey);
            }
        };
    }

    /**
     * Base class for parallel scan iterators.
     *
     * Both of the parallel scan methods (storeIterator(...,
     * StoreIteratorConfig) and storeKeysIterator(..., StoreIteratorConfig)
     * return an instance of a ParallelScanIterator (as opposed to plain old
     * Iterator) so that we can eventually use them in try-with-resources
     * constructs.
     */
    public static abstract class ParallelScanIteratorImpl<K>
        implements ParallelScanIterator<K> {

        private final KVStoreImpl storeImpl;
        protected final StoreIteratorParams storeIteratorParams;
        private final Logger logger;
        protected final KeySerializer keySerializer;
        private final StoreIteratorMetricsImpl storeIteratorMetrics;

        private int requestTimeoutMs;

        /*
         * Marks the end of all threads submitting results to the Results Queue.
         */
        @SuppressWarnings("unchecked")
        private final ResultsQueueEntry<K>[] poisonPill =
            new ResultsQueueEntry[0];
        private int repFactor;

        /* Indexed by partition id. */
        private final Map<Integer, DetailedMetricsImpl> partitionMetrics;
        private final Map<RepGroupId, DetailedMetricsImpl> shardMetrics;

        private ParallelScanExecutor parallelScanExecutor;

        private ResultsQueueEntry<K>[] elements = null;
        private int nextElement = 0;
        private boolean receivedFirstBatch = false;
        private volatile boolean isCanceled = false;
        private BlockingQueue<ResultsQueueEntry<K>[]> resultsQueue;
        private Set<Future<?>> allTasks;
        private final long timeout;
        private int nShards;

        public ParallelScanIteratorImpl
            (final KVStoreImpl storeImpl,
             final StoreIteratorConfig storeIteratorConfig,
             final StoreIteratorParams storeIteratorParams) {
            this.storeImpl = storeImpl;
            this.storeIteratorParams = storeIteratorParams;
            this.keySerializer = storeImpl.getKeySerializer();
            this.logger = storeImpl.getLogger();
            this.storeIteratorMetrics = storeImpl.getStoreIteratorMetrics();
            this.partitionMetrics = new HashMap<Integer, DetailedMetricsImpl>
                (storeImpl.getNPartitions());
            this.shardMetrics =
                new HashMap<RepGroupId, DetailedMetricsImpl>();

            createAndSubmitTasks(storeIteratorConfig);

            timeout = storeIteratorParams.getTimeout();
            requestTimeoutMs = storeImpl.getDefaultRequestTimeoutMs();
            if (timeout > 0) {
                requestTimeoutMs = PropUtil.durationToMillis
                    (timeout, storeIteratorParams.getTimeoutUnit());
                if (requestTimeoutMs > storeImpl.getReadTimeoutMs()) {
                    String format =
                        "Request timeout parameter: %,d ms exceeds " +
                        "socket read timeout: %,d ms";
                    throw new IllegalArgumentException
                        (String.format(format, requestTimeoutMs,
                                       storeImpl.getReadTimeoutMs()));
                }
            }
        }

        /**
         * Returns the generated op for this iterator.
         */
        protected abstract
            InternalOperation generateGetterOp(byte[] resumeKey);

        /**
         * Converts the results received by server, placing them into the
         * the specified results queue.
         */
        protected abstract ConvertResultsReturnValue
             convertResults(Result result);

        private ResultsQueueEntry<K>[] getMoreElements() {
            if (isCanceled) {
                return null;
            }

            try {
                ResultsQueueEntry<K>[] next = resultsQueue.poll();
                if (next == null) {
                    ParallelScanHook psh = storeImpl.getParallelScanHook();
                    assert !receivedFirstBatch ||
                        psh == null ?
                        true :
                        psh.callback(Thread.currentThread(),
                                     HookType.QUEUE_STALL_GET, null);

                    final long start = System.nanoTime();
                    next = resultsQueue.poll(requestTimeoutMs, MILLISECONDS);
                    final long end = System.nanoTime();

                    if (next == null) {
                        throw new RequestTimeoutException
                            (requestTimeoutMs,
                             "Parallel storeIterator Request Queue take " +
                             "timed out.",
                             null, false);
                    }

                    if (receivedFirstBatch) {
                        final long thisTimeMs = (end - start) / NANOS_TO_MILLIS;
                        storeIteratorMetrics.
                            accBlockedResultsQueueGetTime(thisTimeMs);
                    }
                }

                receivedFirstBatch = true;
                if (next == poisonPill) {
                    close();
                    return null;
                }

                return next;
            } catch (InterruptedException IE) {
                close();
                throw new StoreIteratorException(IE, null);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public synchronized boolean hasNext() {
            if (elements != null &&
                nextElement < elements.length &&
                !isCanceled) {
                return true;
            }

            elements = getMoreElements();
            if (elements == null) {
                return false;
            }

            assert (elements.length > 0);
            nextElement = 0;
            return true;
        }

        @Override
        public synchronized K next() {
            if (!hasNext() || isCanceled) {
                throw new NoSuchElementException();
            }

            final ResultsQueueEntry<K> rqe = elements[nextElement++];
            final StoreIteratorException sie = rqe.getException();
            if (sie != null) {
                throw sie;
            }

            return rqe.getEntry();
        }

        @Override
        public synchronized void close() {
            if (isCanceled) {
                return;
            }

            for (Future<?> f : allTasks) {
                /* Wait for task completion. */
                f.cancel(true);
            }

            final List<Runnable> unfinishedBusiness =
                parallelScanExecutor.shutdownNow();
            if (unfinishedBusiness != null) {
                final int nRemainingTasks = unfinishedBusiness.size();
                logger.warning
                    ("parallelScanExecutor didn't shutdown cleanly. " +
                        nRemainingTasks + " tasks remaining.");
            }

            try {
                final long timeForAwait = 60;
                final boolean ok =
                    parallelScanExecutor.awaitTermination(timeForAwait,
                        TimeUnit.SECONDS);
                if (!ok) {
                    logger.severe("Waiting for termination fail. " +
                        "Time elapsed " + timeForAwait + " secs");
                }
            } catch (InterruptedException IE) {
                logger.info(Thread.currentThread() + " caught " + IE);
                Thread.currentThread().interrupt();
            } finally {
                /* Mark this Iterator as terminated. */
                isCanceled = true;
            }
        }

        @Override
        public List<DetailedMetrics> getPartitionMetrics() {
            synchronized (partitionMetrics) {
                List<DetailedMetrics> l =
                    new ArrayList<DetailedMetrics>(partitionMetrics.size());
                l.addAll(partitionMetrics.values());
                return Collections.unmodifiableList(l);
            }
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

        private void
            createAndSubmitTasks(final StoreIteratorConfig storeIteratorConfig)
            throws FaultException {

            final int maxResultsBatches =
                storeIteratorConfig.getMaxResultsBatches();

            final int nThreads = storeIteratorConfig.getMaxConcurrentRequests();

            final Map<RepGroupId, Set<Integer>> partitionsByShard =
                getPartitionTopology();
            nShards = partitionsByShard.size();
            if (nShards < 1) {
                throw new IllegalStateException
                    ("partitionsByShard has no entries");
            }

            final int useNRepNodesPerShard =
                (storeIteratorParams.getConsistency() == Consistency.ABSOLUTE) ?
                1 :
                repFactor;

            final int useNThreads = (nThreads == 0) ?
                Math.min(MAX_COMPUTED_NTHREADS,
                         nShards * useNRepNodesPerShard) :
                nThreads;

            /*
             * Setting this too low causes stalls on both sides. Review whether
             * 32 * nThreads is the right value.
             */
            final int useMaxResultsBatches =
                maxResultsBatches == 0 ? (useNThreads << 5) : maxResultsBatches;
            resultsQueue = new LinkedBlockingQueue<ResultsQueueEntry<K>[]>
                (useMaxResultsBatches);
            parallelScanExecutor = new ParallelScanExecutor(useNThreads,
                                                            logger);

            /*
             * Submit the partition tasks in round robin order by shard to
             * achieve poor man's balancing across shards.
             */
            final Map<RepGroupId, Set<PartitionIterationTask>> tasksByShard =
                generatePartitionIterationTasks(partitionsByShard,
                                                storeIteratorParams);
            allTasks = new HashSet<Future<?>>(storeImpl.getNPartitions());
            final Collection<Set<PartitionIterationTask>> tasksByShardColl =
                tasksByShard.values();
            @SuppressWarnings("unchecked")
            final Set<PartitionIterationTask>[] tasksByShardArr =
                tasksByShardColl.toArray(new Set[0]);
            boolean didSomething;
            do {
                didSomething = false;
                for (int idx = 0; idx < nShards; idx++) {
                    Set<PartitionIterationTask> tasks = tasksByShardArr[idx];
                    for (PartitionIterationTask task : tasks) {
                        allTasks.add(parallelScanExecutor.submit(task));
                        tasks.remove(task);
                        didSomething = true;
                        break;
                    }
                }
            } while (didSomething);

            parallelScanExecutor.submit(new Runnable() {
                    @Override
                    public void run() {
                        boolean ok = false;
                        try {

                            /*
                             * Make sure all tasks are completed before
                             * dropping the poison pill.
                             */
                            for (Future<?> f : allTasks) {
                                /* Wait for task completion. */
                                f.get();
                            }

                            resultsQueue.put(poisonPill);
                            ok = true;
                        } catch (ExecutionException EE) {
                            logger.severe
                                (Thread.currentThread() + " caught " + EE);
                        } catch (InterruptedException IE) {
                            logger.info
                                (Thread.currentThread() + " caught " + IE);
                        } finally {
                            if (!ok) {
                                close();
                            }
                        }
                    }
                }
                );
        }

        /*
         * Extracts the rep factor of the topology and creates a map of shard to
         * the set of partitions in the shard.
         */
        private Map<RepGroupId, Set<Integer>> getPartitionTopology() {
            final Topology topology =
                storeImpl.getDispatcher().getTopologyManager().getTopology();

            /* Determine Rep Factor. */
            Collection<Datacenter> datacenters =
                topology.getDatacenterMap().getAll();
            if (datacenters.size() < 1) {
                throw new IllegalStateException("No zones in topology?");
            }

            /**
             * TODO Sam says: In the future, each DC(Zone) may have a different
             * RF and you may want to restrict a parallel scan to a subset of
             * the DCs, just like with any read request.
             */
            repFactor =
                (datacenters.toArray(new Datacenter[] {})[0]).getRepFactor();

            final Map<RepGroupId, Set<Integer>> shardPartitions =
                new HashMap<RepGroupId, Set<Integer>>();
            for (int i = 1; i <= storeImpl.getNPartitions(); i++) {
                PartitionId partId = new PartitionId(i);
                RepGroupId rgid = topology.getRepGroupId(partId);
                Set<Integer> parts = shardPartitions.get(rgid);
                if (parts == null) {
                    parts = new HashSet<Integer>();
                    shardPartitions.put(rgid, parts);
                }
                parts.add(i);
            }

            return shardPartitions;
        }

        private Map<RepGroupId, Set<PartitionIterationTask>>
            generatePartitionIterationTasks
                (final Map<RepGroupId, Set<Integer>> partitionsByShard,
                 final StoreIteratorParams sip) {

            logger.info("Generating Partition Iteration Tasks");
            final Map<RepGroupId, Set<PartitionIterationTask>> ret =
                new HashMap<RepGroupId, Set<PartitionIterationTask>>
                (partitionsByShard.size());

            for (Map.Entry<RepGroupId, Set<Integer>> ent :
                     partitionsByShard.entrySet()) {
                final RepGroupId rgid = ent.getKey();
                final Set<Integer> parts = ent.getValue();
                for (Integer part : parts) {
                    final PartitionIterationTask pit =
                        new PartitionIterationTask(sip, rgid, part);
                    Set<PartitionIterationTask> shardTasks = ret.get(rgid);
                    if (shardTasks == null) {
                        shardTasks = new HashSet<PartitionIterationTask>();
                        ret.put(rgid, shardTasks);
                    }

                    shardTasks.add(pit);
                }
            }

            return ret;
        }

        private class PartitionIterationTask implements Runnable {
            private final StoreIteratorParams sip;
            private final RepGroupId rgid;
            private final int part;

            private PartitionIterationTask
                (final StoreIteratorParams sip,
                 final RepGroupId rgid,
                 final int part) {
                this.sip = sip;
                this.rgid = rgid;
                this.part = part;
            }

            @Override
            public void run() {
                try {
                    assert storeImpl.getParallelScanHook() == null ?
                        true :
                        storeImpl.getParallelScanHook().
                        callback(Thread.currentThread(),
                                 HookType.BEFORE_PROCESSING_PARTITION,
                                 "" + rgid);
                    final long start = System.nanoTime();
                    final int cnt =
                        doPartitionIteration(sip, rgid, part);
                    final long end = System.nanoTime();
                    final long thisTimeMs = (end - start) / NANOS_TO_MILLIS;
                    assert storeImpl.getParallelScanHook() == null ?
                        true :
                        storeImpl.getParallelScanHook().
                        callback(Thread.currentThread(),
                                 HookType.AFTER_PROCESSING_PARTITION,
                                 rgid + "/" + cnt);
                    updateDetailedMetrics(rgid, part, thisTimeMs, cnt);
                } catch (Exception E) {
                    logger.severe(Thread.currentThread() + " caught " + E);
                }
            }

            @Override
            public String toString() {
                return "PartitionIterationTask for shard " + rgid +
                    "   partition " + part;
            }
        }

        /* Update the detailed (per-shard and per-partition) metrics. */
        private void updateDetailedMetrics(final RepGroupId rgid,
                                           final int part,
                                           final long timeInMs,
                                           final long recordCount) {

            /* Partition Metrics. */
            final int partIdx = part - 1;
            final String shardName = rgid.toString();
            DetailedMetricsImpl dmi;
            synchronized (partitionMetrics) {
            if (partitionMetrics.get(partIdx) != null) {
                    logger.severe(Thread.currentThread() +
                              "Found existing entry for partition " + part +
                                  " while trying to update detailedMetrics.");
                    return;
                }

                final StringBuilder sb = new StringBuilder();
                sb.append(part).append(" (").append(shardName).append(")");
                dmi = new DetailedMetricsImpl(sb.toString(), timeInMs,
                                              recordCount);
                partitionMetrics.put(partIdx, dmi);
            }

            synchronized (shardMetrics) {
                /* Shard Metrics. */
                dmi = shardMetrics.get(rgid);
                if (dmi == null) {
                    dmi = new DetailedMetricsImpl
                        (shardName, timeInMs, recordCount);
                    shardMetrics.put(rgid, dmi);
                    return;
                }
            }

            dmi.inc(timeInMs, recordCount);
        }

        /* Returns count of records iterated over in partition. */
        @SuppressWarnings("unchecked")
        private int doPartitionIteration
            (final StoreIteratorParams sip,
             final RepGroupId rgid,
             final int partition) {

            boolean moreElements = true;
            byte[] resumeKey = null;
            byte[] lastNonNullResumeKey = null;
            PartitionId partitionId = new PartitionId(partition);

            logger.info(Thread.currentThread() + " iterating over " +
                        partition + " (" + rgid + ")");

            int cnt = 0;
            while (true) {
                /* Avoid round trip when there are no more elements. */
                if (!moreElements) {
                    return cnt;
                }

                Result result = null;

                /* Execute request. */
                final InternalOperation get =
                    generateGetterOp(resumeKey);

                final Request req = storeImpl.makeReadRequest
                    (get, partitionId, sip.getConsistency(), sip.getTimeout(),
                     sip.getTimeoutUnit());
                try {
                    assert storeImpl.getParallelScanHook() == null ?
                        true :
                        storeImpl.getParallelScanHook().
                        callback(Thread.currentThread(),
                                 HookType.BEFORE_EXECUTE_REQUEST, null);
                    result = storeImpl.executeRequest(req);
                } catch (Throwable t) {
                    Key serializedKey = lastNonNullResumeKey == null ?
                        null :
                        keySerializer.fromByteArray(lastNonNullResumeKey);
                    final StoreIteratorException sie =
                        new StoreIteratorException(t, serializedKey);
                    final ResultsQueueEntry<Key> rqe =
                        new ResultsQueueEntry<Key>(null, sie);
                    putResult(new ResultsQueueEntry[] { rqe });
                    return cnt;
                }

                moreElements = result.hasMoreElements();
                final ConvertResultsReturnValue crrv = convertResults(result);
                if (crrv.getResumeKey() != null) {
                    resumeKey = crrv.getResumeKey();
                    if (lastNonNullResumeKey == null) {
                        lastNonNullResumeKey = resumeKey;
                    }
                }
                cnt += crrv.getCnt();
            }
        }

        protected void putResult(final ResultsQueueEntry<K>[] rqe) {

            try {
                if (!resultsQueue.offer(rqe)) {
                    assert storeImpl.getParallelScanHook() == null ?
                        true :
                        storeImpl.getParallelScanHook().
                        callback(Thread.currentThread(),
                                 HookType.QUEUE_STALL_PUT, null);

                    final long start = System.nanoTime();
                    resultsQueue.put(rqe);
                    final long end = System.nanoTime();
                    final long thisTimeMs = (end - start) / NANOS_TO_MILLIS;
                    storeIteratorMetrics.
                        accBlockedResultsQueuePutTime(thisTimeMs);
                }
            } catch (InterruptedException IE) {
                logger.info(Thread.currentThread() + " caught " + IE);
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * A struct to hold entries in the Results Queue.
     */
    public static class ResultsQueueEntry<E> {
        private final E entry;
        private final StoreIteratorException exception;

        public ResultsQueueEntry(E entry, StoreIteratorException exception) {
            this.entry = entry;
            this.exception = exception;
        }

        public StoreIteratorException getException() {
            return exception;
        }

        public E getEntry() {
            return entry;
        }
    }

    /*
     * Struct for multi-value return from convertResults(). See class javadoc
     * above.
     */
    public static class ConvertResultsReturnValue {

        public ConvertResultsReturnValue(int cnt, byte[] resumeKey) {
            this.cnt = cnt;
            this.resumeKey = resumeKey;
        }

        private int cnt;
        private byte[] resumeKey;

        public int getCnt() {
            return cnt;
        }

        public byte[] getResumeKey() {
            return resumeKey;
        }
    }

    private static class ParallelScanExecutor
        extends ScheduledThreadPoolExecutor {

        ParallelScanExecutor(int nThreads, Logger logger) {
            super(nThreads, new KVThreadFactory(" parallel scan", logger));
            setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        }
    }
}
