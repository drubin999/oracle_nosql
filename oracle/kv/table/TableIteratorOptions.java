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

package oracle.kv.table;

import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.KVStoreConfig;

/**
 * TableIteratorOptions extends ReadOptions and is passed to read-only store
 * operations that return iterators.  It is used to specify non-default
 * behavior.  Default behavior is configured when a store is opened using
 * {@link KVStoreConfig}.
 *
 * @since 3.0
 */
public class TableIteratorOptions extends ReadOptions {

    private final Direction direction;

    private final int maxConcurrentRequests;
    private final int batchResultsSize;
    private final int maxResultsBatches;

    /**
     * Creates a {@code TableIteratorOptions} with the specified parameters.
     * Equivalent to
     * {@code TableIteratorOptions(direction, consistency, timeout,
     * timeoutUnit, 0, 0, 0)}
     *
     * @param direction a direction
     * @param consistency the read consistency to use or null
     * @param timeout the timeout value to use
     * @param timeoutUnit the {@link TimeUnit} used by the
     * <code>timeout</code> parameter or null
     *
     * @throws IllegalArgumentException if direction is null, the timeout
     * is negative or timeout is > 0 and timeoutUnit is null
     */
    public TableIteratorOptions(Direction direction,
                                Consistency consistency,
                                long timeout,
                                TimeUnit timeoutUnit) {
        this(direction, consistency, timeout, timeoutUnit, 0, 0, 0);
    }

    /**
     * Creates a {@code TableIteratorOptions} with the specified parameters.
     * <p>
     * If {@code consistency} is {@code null}, the
     * {@link KVStoreConfig#getConsistency default consistency}
     * is used. If {@code timeout} is zero the
     * {@link KVStoreConfig#getRequestTimeout default request timeout} is used.
     * <p>
     * {@code maxConcurrentRequests} specifies the maximum degree of parallelism
     * (in effect the maximum number of client-side threads) to be used when
     * running an iteration. Setting {@code maxConcurrentRequests} to 1 causes
     * the iteration to be performed using only the current thread. Setting it
     * to 0 lets the KV Client determine the number of threads based on topology
     * information (up to a maximum of the number of available processors as
     * returned by java.lang.Runtime.availableProcessors()). Values less than 0
     * are reserved for some future use and cause an
     * {@code IllegalArgumentException} to be thrown.
     * <p>
     * {@code maxResultsBatches} specifies the maximum number of results batches
     * that can be held in the NoSQL Database client process before processing
     * on the Replication Node pauses. This ensures that client side memory is
     * not exceeded if the client can't consume results as fast as they are
     * generated by the Rep Nodes.
     *
     * @param direction a direction
     * @param consistency the read consistency to use or null
     * @param timeout the timeout value to use
     * @param timeoutUnit the {@link TimeUnit} used by the
     * <code>timeout</code> parameter or null
     * @param maxConcurrentRequests the maximum number of client-side threads
     * @param batchResultsSize the number of results per request
     * @param maxResultsBatches the maximum number of results sets that can be
     * held on the client side
     *
     * @throws IllegalArgumentException if direction is null, the timeout
     * is negative, timeout is > 0 and timeoutUnit is null, or if
     * maxConcurrentRequests, maxResultsSize, or maxResultsBatches is less
     * than 0.
     */
    public TableIteratorOptions(Direction direction,
                                Consistency consistency,
                                long timeout,
                                TimeUnit timeoutUnit,
                                int maxConcurrentRequests,
                                int batchResultsSize,
                                int maxResultsBatches) {
        super(consistency, timeout, timeoutUnit);
        if (direction == null) {
            throw new IllegalArgumentException("direction must not be null");
        }
        if (maxConcurrentRequests < 0) {
            throw new IllegalArgumentException
                ("maxConcurrentRequests must be >= 0");
        }
        if (batchResultsSize < 0) {
            throw new IllegalArgumentException("batchResultsSize must be >= 0");
        }
        if (maxResultsBatches < 0) {
            throw new IllegalArgumentException
                ("maxResultsBatches must be >= 0");
        }
        this.direction = direction;
        this.maxConcurrentRequests = maxConcurrentRequests;
        this.batchResultsSize = batchResultsSize;
        this.maxResultsBatches = maxResultsBatches;
    }

    /**
     * Returns the direction.
     *
     * @return the direction
     */
    public Direction getDirection() {
        return direction;
    }

    /**
     * Returns the maximum number of concurrent requests.
     *
     * @return the maximum number of concurrent requests
     */
    public int getMaxConcurrentRequests() {
        return maxConcurrentRequests;
    }

    /**
     * Returns the number of results per request.
     *
     * @return the number of results
     */
    public int getResultsBatchSize() {
        return batchResultsSize;
    }

    /**
     * Returns the maximum number of results batches that can be held in the
     * NoSQL Database client process.
     *
     * @return the maximum number of results batches that can be held in the
     * NoSQL Database client process.
     */
    public int getMaxResultsBatches() {
        return maxResultsBatches;
    }
}
