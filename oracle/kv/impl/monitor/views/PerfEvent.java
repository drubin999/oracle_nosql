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

package oracle.kv.impl.monitor.views;

import java.io.Serializable;

import oracle.kv.impl.measurement.LatencyInfo;
import oracle.kv.impl.measurement.RepEnvStats;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.FormatUtils;

import com.sleepycat.je.rep.ReplicatedEnvironmentStats;
import com.sleepycat.utilint.Latency;

/**
 * The aggregated interval and cumulative latencies for a rep node, for
 * all operations.
 */
public class PerfEvent implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final String eol = System.getProperty("line.separator");

    private final ResourceId resourceId;

    /*
     * Interval and cumulative stats, aggregated for all user single-operation
     * kvstore operations. Note that these are never null.
     */
    private final LatencyInfo singleInt;
    private final LatencyInfo singleCum;

    /*
     * Interval and cumulative stats, aggregated for all user multi-operation
     * kvstore operations. Note that these are never null.
     */
    private final LatencyInfo multiInt;
    private final LatencyInfo multiCum;

    private final boolean needsAlert;

    private final boolean singleCeilingExceeded;
    private final boolean singleFloorExceeded;
    private final boolean multiCeilingExceeded;
    private final boolean multiFloorExceeded;

    private final long commitLag;
    private final long commitLagThreshold;
    private final boolean lagExceeded;

    public PerfEvent(ResourceId resourceId,
                     LatencyInfo singleInt,
                     LatencyInfo singleCum,
                     int latencyCeiling,
                     int throughputFloor,
                     long commitLagThreshold,
                     LatencyInfo multiInt,
                     LatencyInfo multiCum,
                     final RepEnvStats repEnvStats) {
        this.singleInt = singleInt;
        this.singleCum = singleCum;
        this.multiInt = multiInt;
        this.multiCum = multiCum;
        this.resourceId = resourceId;

        assert singleInt != null;
        assert singleCum != null;
        assert multiInt != null;
        assert multiCum != null;

        this.singleCeilingExceeded =
            latencyCeilingExceeded(latencyCeiling, singleInt);
        this.singleFloorExceeded =
            throughputFloorExceeded(throughputFloor, singleInt);

        this.multiCeilingExceeded =
            latencyCeilingExceeded(latencyCeiling, multiInt);
        this.multiFloorExceeded =
            throughputFloorExceeded(throughputFloor, multiInt);

        this.commitLagThreshold = commitLagThreshold;
        this.commitLag = getCommitLagMs(repEnvStats);
        this.lagExceeded = commitLagThresholdExceeded(
                               commitLag, commitLagThreshold);

        if (singleCeilingExceeded) {
            needsAlert = true;
        } else if (singleFloorExceeded) {
            needsAlert = true;
        } else if (multiCeilingExceeded) {
            needsAlert = true;
        } else if (multiFloorExceeded) {
            needsAlert = true;
        } else if (lagExceeded) {
            needsAlert = true;
        } else {
            needsAlert = false;
        }
    }

    public static boolean latencyCeilingExceeded(int ceiling,
                                                 LatencyInfo stat) {
        return ((ceiling > 0) && (stat.getLatency().getAvg() > ceiling));
    }

    public static boolean throughputFloorExceeded(int floor,
                                                  LatencyInfo stat) {
        /*
         * If there are no multi (or single) ops, then the throughput will
         * default to 0. Because of this, unless a 0 throughput is excluded
         * from the floor test performed below (throughput > 0), an ALERT will
         * ALWAYS be recorded; even when an ALERT is not intended/expected.
         * Thus, only positive throughputs are used in the comparison below.
         */
        return ((floor > 0) && (stat.getThroughputPerSec() > 0) &&
                (stat.getThroughputPerSec() < floor));
    }

    public static long getCommitLagMs(final RepEnvStats repEnvStats) {
        long avgLag = 0L;
        if (repEnvStats != null) {
            final ReplicatedEnvironmentStats jeRepStats =
                                                 repEnvStats.getStats();
            if (jeRepStats != null) {
                final long totalLag = jeRepStats.getReplayTotalCommitLagMs();
                final long nCommits = jeRepStats.getNReplayCommits();
                if (totalLag > 0 && nCommits > 0) {
                    avgLag = totalLag / nCommits;
                }
            }
        }
        return avgLag;
    }

    public static boolean commitLagThresholdExceeded(
                              final RepEnvStats repEnvStats, long threshold) {
        return commitLagThresholdExceeded(getCommitLagMs(repEnvStats),
                                          threshold);
    }

    public static boolean commitLagThresholdExceeded(long lag,
                                                     long threshold) {
        return ((threshold > 0L) && (lag >= threshold));
    }

    public static final String HEADER = eol +
        "                                        --------------------------------- Interval ---------------------------------        --------------------------- Cumulative ---------------------------" +
        eol +
        "Resource   Time yy-mm-dd UTC Op Type    TotalOps PerSec    TotalReq    Min    Max    Avg   95th   99th  ReplicaLagMs        TotalOps PerSec        TotalReq    Min    Max    Avg   95th   99th";
    //   *234567890 yy-mm-dd xx:xx:xx 1234567 12345678901 123456 12345678901 123456 123456 123456 123456 123456   12345678901 123456789012345 123456 123456789012345 123456 123456 123456 123456 123456


    /**
     * Print the single/multi interval/cumulative stats in a way suitable for
     * the .perf file.
     */
    public String getColumnFormatted() {

        final StringBuilder sb = new StringBuilder();
        if (singleInt.getLatency().getTotalOps() > 0) {
            sb.append(getFormatted("single", singleInt, singleCum));

            /* Identify the type of ALERT if applicable */
            if (singleCeilingExceeded) {
                sb.append(" - latency");
                if (singleFloorExceeded) {
                    sb.append(",throughput");
                }
            } else if (singleFloorExceeded) {
                sb.append(" - throughput");
            }
        }

        if (multiInt.getLatency().getTotalOps() > 0) {
            if (sb.length() > 0) {
                sb.append(eol);
            }
            sb.append(getFormatted("multi", multiInt, multiCum));

            /* Identify the type of ALERT if applicable */
            if (multiCeilingExceeded) {
                sb.append(" - latency");
                if (multiFloorExceeded) {
                    sb.append(",throughput");
                }
            } else if (multiFloorExceeded) {
                sb.append(" - throughput");
            }
        }

        /* Always log the lag value in a formatted record, even when the total
         * ops is 0. And identify the ALERT as a lag ALERT where applicable.
         */
        if (sb.length() > 0) {
            if (lagExceeded) {
                if (singleCeilingExceeded || singleFloorExceeded ||
                    multiCeilingExceeded || multiFloorExceeded) {
                    sb.append(",lag");
                } else {
                    sb.append(" - lag");
                }
            }
        } else {
            /* 
             * Total ops are 0 above; for both single and multi. Still report
             * the lag value in a formatted record.
             */
            sb.append(getFormatted("single", singleInt, singleCum));

            /* If it's a lag ALERT, identify it as such. */
            if (lagExceeded) {
                sb.append(" - lag");
            }
        }

        return sb.toString();
    }

    /**
     * Format one pair of interval/cumulative stats in a way suitable for
     * adding to the .perf file.
     */
    private String getFormatted(String label, // single or multi
                                LatencyInfo intInfo,
                                LatencyInfo cumInfo) {
        final Latency intLat = intInfo.getLatency();
        final Latency cumLat = cumInfo.getLatency();

        /*
         * Be sure to use UTC timezone, to match logging output and timestamps
         * in the .stat file.
         */
        String formatted =
            String.format
            ("%-10s %17s %7s %11d %6d %11d %6d %6d %6.1f %6d %6d %13d %15d %6d %15d %6d %6d %6.1f %6d %6d",
             resourceId,
             FormatUtils.formatPerfTime(intInfo.getEnd()),
             label,
             intLat.getTotalOps(),
             intInfo.getThroughputPerSec(),
             intLat.getTotalRequests(),
             intLat.getMin(),
             intLat.getMax(),
             intLat.getAvg(),
             intLat.get95thPercent(),
             intLat.get99thPercent(),
             commitLag,
             cumLat.getTotalOps(),
             cumInfo.getThroughputPerSec(),
             cumLat.getTotalRequests(),
             cumLat.getMin(),
             cumLat.getMax(),
             cumLat.getAvg(),
             cumLat.get95thPercent(),
             cumLat.get99thPercent());

        if (needsAlert) {
            formatted += " ALERT";
        }

        return formatted;
    }

    @Override
    public String toString() {

        /* Single interval, non-cummulative metrics */
        String value = resourceId + " interval=" + singleInt;
        if (singleCeilingExceeded || singleFloorExceeded) {
            value += " ALERT";
        }

        /* Replica commit lag metric */
        value += " replicaLagMs=" + commitLag;
        if (lagExceeded) {
            value += " ALERT";
        }

        /* Single interval, cummulative metrics */
        value += " cumulative=" + singleCum;

        /* Multi interval, non-cummulative metrics */
        value += " multiOpsInterval=" + multiInt;
        if (multiCeilingExceeded || multiFloorExceeded) {
            value += " ALERT";
        }

        /* Multi interval, cummulative metrics */
        value += " multiOpsCumulative=" + multiCum;

        return value;
    }

    public ResourceId getResourceId() {
        return resourceId;
    }

    /** Never returns null. */
    public LatencyInfo getSingleInt() {
        return singleInt;
    }

    /** Never returns null. */
    public LatencyInfo getSingleCum() {
        return singleCum;
    }

    /** Never returns null. */
    public LatencyInfo getMultiInt() {
        return multiInt;
    }

    /** Never returns null. */
    public LatencyInfo getMultiCum() {
        return multiCum;
    }

    public boolean needsAlert() {
        return needsAlert;
    }

    public long getChangeTime() {
        if (singleInt.getLatency().getTotalOps() != 0) {
            return singleInt.getEnd();
        }

        if (multiInt.getLatency().getTotalOps() != 0) {
            return multiInt.getEnd();
        }

        if (lagExceeded) {
            return singleInt.getEnd();
        }
        return 0;
    }

    public long getCommitLagMs() {
        return commitLag;
    }

    public long getCommitLagThreshold() {
        return commitLagThreshold;
    }
}
