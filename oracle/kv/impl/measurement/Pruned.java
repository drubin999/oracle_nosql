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

package oracle.kv.impl.measurement;

import java.io.Serializable;

import oracle.kv.impl.monitor.Metrics;
import oracle.kv.impl.util.FormatUtils;

/**
 * Tracks measurements which have been pruned from the service side repository
 * buffer.
 */
public class Pruned implements Measurement, Serializable {

    private static final long serialVersionUID = 1L;
    private int numRemoved;

    /* Keeps the time span of measurements that have been deleted.*/
    private long start;
    private long end;

    /**
     * Keep track of an item that has been pruned from the buffer.
     *
     * TODO: Ideally, the bounding of the buffer should be more sophisticated.
     * Currently, pruned items are simply removed and counted. In the future,
     * latency stats should be rolled up and aggregated, and pruning can be done
     * with some measure of the priority of the measurement.
     */
    public void record(Measurement target) {

        numRemoved++;

        /* 
         * Try to get a sense of what time period is spanned by the pruned
         * items.
         */
        long mStart = target.getStart();
        long mEnd = target.getEnd();

        if (end < mEnd) {
            end = mEnd;
        }

        if (mEnd != 0) {
            if (start > mEnd) {
                start = mEnd;
            }
        }

        if (mStart != 0) {
            if (start == 0) {
                start = mStart;
            } else if (start > mStart) {
                start = mStart;
            }
        }

        // TODO: rollup the latency stats instead of just counting them.
    }

    @Override
    public int getId() {
        return Metrics.PRUNED.getId();
    }

    @Override
    public String toString() {

        if (start == 0) {
            return  numRemoved + " measurements dropped.";
        }

        return numRemoved + " measurements from " +
            FormatUtils.formatDateAndTime(start) + 
            FormatUtils.formatDateAndTime(end) + " dropped.";
    }

    @Override
        public long getStart() {
        return start;
    }

    @Override
        public long getEnd() {
        return end;
    }

    public boolean exists() {
        return numRemoved > 0;
    }

    public int getNumRemoved() {
        return numRemoved;
    }
}