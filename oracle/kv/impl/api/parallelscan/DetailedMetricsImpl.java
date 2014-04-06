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

import oracle.kv.stats.DetailedMetrics;

/**
 * Implementation of the per-partition and per-shard metrics returned by {@link
 * oracle.kv.ParallelScanIterator#getPartitionMetrics and
 * oracle.kv.ParallelScanIterator#getShardMetrics()}.
 */
public class DetailedMetricsImpl implements DetailedMetrics {

    private final String name;
    private long scanTime;
    private long scanRecordCount;

    public DetailedMetricsImpl(final String name,
                        final long scanTime,
                        final long scanRecordCount) {
        this.name = name;
        this.scanTime = scanTime;
        this.scanRecordCount = scanRecordCount;
    }

    /**
     * Return the name of the Shard or a Partition.
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * Return the total time for scanning the Shard or Partition.
     */
    @Override
    public long getScanTime() {
        return scanTime;
    }

    /**
     * Return the record count for the Shard or Partition.
     */
    @Override
    public long getScanRecordCount() {
        return scanRecordCount;
    }

    public synchronized void inc(long time, long count) {
        scanTime += time;
        scanRecordCount += count;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Detailed Metrics: ");
        sb.append(name).append(" records: ").append(scanRecordCount);
        sb.append(" scanTime: ").append(scanTime);
        return sb.toString();
    }
}
