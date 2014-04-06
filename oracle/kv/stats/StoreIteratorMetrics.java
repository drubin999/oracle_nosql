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

package oracle.kv.stats;

/**
 * Interface to the stats and metrics associated with a KVS storeIterator
 * operation.
 */
public interface StoreIteratorMetrics {

    /**
     * Returns the number of put() operations to the Results Queue on the
     * producer side of the queue which have blocked.
     * <p>
     * Blocked Results Queue put operations occur when the client side can't
     * process results as fast as the parallel scan threads receive them from
     * the Replication Nodes.
     */
    public long getBlockedResultsQueuePuts();

    /**
     * Returns the average time (in milliseconds) spent waiting for put()
     * operations to the Results Queue.
     * <p>
     * Blocked Results Queue put operations occur when the client side can't
     * process results as fast as the parallel scan threads receive them from
     * the Replication Nodes.
     */
    public long getAverageBlockedResultsQueuePutTime();

    /**
     * Returns the minimum time (in milliseconds) spent waiting for put()
     * operations to the Results Queue.
     * <p>
     * Blocked Results Queue put operations occur when the client side can't
     * process results as fast as the parallel scan threads receive them from
     * the Replication Nodes.
     */
    public long getMinBlockedResultsQueuePutTime();

    /**
     * Returns the maximum time (in milliseconds) spent waiting for put()
     * operations to the Results Queue.
     * <p>
     * Blocked Results Queue put operations occur when the client side can't
     * process results as fast as the parallel scan threads receive them from
     * the Replication Nodes.
     */
    public long getMaxBlockedResultsQueuePutTime();

    /**
     * Returns the number of Results Queue take() operations on the application
     * side which have blocked. This number does not include the first take()
     * operation on the queue.
     * <p>
     * Blocked Results Queue take operations occur when the Replication Nodes
     * can't produce results as fast as the application is able to process
     * them.
     */
    public long getBlockedResultsQueueGets();

    /**
     * Returns the average time (in milliseconds) spent waiting for Results
     * Queue take() operations, exclusive of the first take() operation.
     * <p>
     * Blocked Results Queue take operations occur when the Replication Nodes
     * can't produce results as fast as the application is able to process
     * them.
     */
    public long getAverageBlockedResultsQueueGetTime();

    /**
     * Returns the minimum time (in milliseconds) spent waiting for Results
     * Queue take() operations, exclusive of the first take() operation.
     * <p>
     * Blocked Results Queue take operations occur when the Replication Nodes
     * can't produce results as fast as the application is able to process
     * them.
     */
    public long getMinBlockedResultsQueueGetTime();

    /**
     * Returns the maximum time (in milliseconds) spent waiting for Results
     * Queue take() operations, exclusive of the first take() operation.
     * <p>
     * Blocked Results Queue take operations occur when the Replication Nodes
     * can't produce results as fast as the application is able to process
     * them.
     */
    public long getMaxBlockedResultsQueueGetTime();
}
