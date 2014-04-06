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

package oracle.kv;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import oracle.kv.stats.DetailedMetrics;

/**
 * Interface to the specialized Iterator type returned by the {@link
 * KVStore#storeIterator(Direction, int, Key, KeyRange, Depth, Consistency, long,
 * TimeUnit, StoreIteratorConfig) Parallel Scan version} of storeIterator().
 * <p>
 * This Iterator adds the ability to close (terminate) a ParallelScan as well
 * gather per-partition and per-shard statistics about the scan.
 */
public interface ParallelScanIterator<K> extends Iterator<K> {

    /**
     * Close (terminate) a Parallel Scan. This shutdowns down all related
     * threads and tasks, but does not await termination before returning.
     */
    public void close();

    /**
     * Gets the per-partition metrics for this Parallel Scan. This may be
     * called at any time during the iteration in order to obtain metrics to
     * that point or it may be called at the end to obtain metrics for the
     * entire scan. If there are no metrics available yet for a particular
     * partition, then there will not be an entry in the list.
     *
     * @return the per-partition metrics for this Parallel Scan.
     */
    public List<DetailedMetrics> getPartitionMetrics();

    /**
     * Gets the per-shard metrics for this Parallel Scan. This may be called at
     * any time during the iteration in order to obtain metrics to that point
     * or it may be called at the end to obtain metrics for the entire scan.
     * If there are no metrics available yet for a particular shard, then there
     * will not be an entry in the list.
     *
     * @return the per-shard metrics for this Parallel Scan.
     */
    public List<DetailedMetrics> getShardMetrics();

    /**
     * Returns the next element in the iteration.
     *
     * @return the next element in the iteration.
     *
     * @throws NoSuchElementException - iteration has no more elements.
     *
     * @throws StoreIteratorException - an exception occurred during a
     * retrieval as part of a multi-record iteration method. This exception
     * does not necessarily close or invalidate the iterator. Repeated calls to
     * next() may or may not cause an exception to be thrown. It is incumbent
     * on the caller to determine the type of exception and act accordingly.
     */
    @Override
    public K next();
}
