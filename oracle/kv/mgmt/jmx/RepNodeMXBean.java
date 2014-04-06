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

package oracle.kv.mgmt.jmx;

import java.util.Date;

/**
 * This MBean represents the RepNode's status, performance metrics, and
 * operational parameters.
 *
 * <p>The performance metrics are organized into five groups, four of
 * which are characterized by whether they refer to single-operation or
 * multi-operation client activities; as well as whether they reference
 * measurements over the most recent collection interval, or measurements
 * accumulated since the RepNode started. The fifth group consists of
 * measurements that don't fall into one of the first four groups.
 * <ol>
 * <li>Single-operation interval metrics.  The accessors for these items begin
 * with the prefix {@code getInterval}.</li>
 * <li>Multi-operation interval metrics.  The accessors for these items begin
 * with the prefix  {@code getMultiInterval}.</li>
 * <li>Single-operation cumulative metrics.  The accessors for these items
 * begin with the prefix {@code getCumulative}.</li>
 * <li>Multi-operation cumulative metrics.  The accessors for these items begin
 * with the prefix  {@code getMultiCumulative}.</li>
 * <li>Metrics that don't fall into one of the above groups.  The accessors
 * for these items begin with the prefix  {@code get}, followed by the
 * metric's name.</li>
 * </ol>
 *
 * <p> Within the first four groups, the accessor's suffix indicates which
 * of the following items are reported.</p>
 * <ol>
 * <li>LatAvg is the average latency recorded during the interval.</li>
 * <li>LatMax is the maximum latency</li>
 * <li>LatMin is the minimum latency</li>
 * <li>Pct99 is the 99th percentile latency</li>
 * <li>Pct95 is the 95th percentile latency</li>
 * <li>TotalOps is the number of operations</li>
 * <li>Throughput is number of operations per second</li>
 * <li>End is the timestamp at the end of the measured interval</li>
 * <li>Start is the timestamp at the beginning of the measured interval</li>
 * </ol>
 *
 * <p> Similarly, within the last group of metrics, the accessor's suffix
 * also indicates which of the following items are reported.
 * <ol>
 * <li>CommitLag for a given replication node is the sum of the differences
 * (in milliseconds) between when each operation performed over a given
 * collection period commits on the node's master and then subsequently
 * commits on the replication node itself, divided by the number of
 * commits; that is, the average lag per operation.</li>
 * </ol>
 *
 * @since 2.0
 */
public interface RepNodeMXBean {

    /**
     * Returns the RepNodeId, in its String form.
     */
    String getRepNodeId();

    /**
     * Returns the reported service status of the Replication Node.
     */
    String getServiceStatus();

    /* Single Operation Interval Latency Info */

    /**
     * The average latency for single operations during a measured interval.
     */
    float getIntervalLatAvg();
    /**
     * Returns the highest latency for single operations measured during an
     * interval.
     */
    int getIntervalLatMax();
    /**
     * Returns the lowest latency for singleton operations measured during an
     * interval.
     */
    int getIntervalLatMin();
    /**
     * Returns the 95th percentile latency for single operations during a
     * measured interval.
     */
    int getIntervalPct95();
    /**
     * Returns the 99th percentile latency for single operations during a
     * measured interval.
     */
    int getIntervalPct99();
    /**
     * Returns the total number of singleton operations during a measured
     * interval.
     */
    int getIntervalTotalOps();
    /**
     * Returns the timestamp at the end of a measured interval.
     */
    Date getIntervalEnd();
    /**
     * Returns the timestamp at the beginning of a measured interval.
     */
    Date getIntervalStart();
    /**
     * Returns singleton operations per second during a measured interval.
     */
    long getIntervalThroughput();

    /* Single Operation Cumulative Latency Info */

    /**
     * Returns the average latency for single operations since service startup.
     */
    float getCumulativeLatAvg();
    /**
     * Returns the highest latency measured for single operations since service
     * startup.
     */
    int getCumulativeLatMax();
    /**
     * Returns the lowest latency measured for single operations since service
     * startup.
     */
    int getCumulativeLatMin();
    /**
     * Returns the 95th percentile latency for single operations since service
     * startup.
     */
    int getCumulativePct95();
    /**
     * Returns the 99th percentile latency for single operations since service
     * startup.
     */
    int getCumulativePct99();
    /**
     * Returns the total number of single operations since service startup.
     */
    int getCumulativeTotalOps();
    /**
     * Returns the timestamp at the end of a cumulative measurement period.
     */
    Date getCumulativeEnd();
    /**
     * Returns the timestamp at the beginning of a cumulative measurement
     * period.
     */
    Date getCumulativeStart();
    /**
     * Returns single operations per second since service startup.
     */
    long getCumulativeThroughput();

    /* Multiple Operation Interval Latency Info */

    /**
     * Returns the average latency for multi-operation sequences during a
     * measured interval.
     */
    float getMultiIntervalLatAvg();
    /**
     * Returns the highest latency measured for multi-operation sequences
     * during a measured interval.
     */
    int getMultiIntervalLatMax();
    /**
     * Returns the lowest latency measured for multi-operation sequences during
     * a measured interval.
     */
    int getMultiIntervalLatMin();
    /**
     * Returns the 95th percentile latency for multi-operation sequences during
     * a measured interval.
     */
    int getMultiIntervalPct95();
    /**
     * Returns the 99th percentile latency for multi-operation sequences during
     * a measured interval.
     */
    int getMultiIntervalPct99();
    /**
     * Returns the total number of single operations performed in
     * multi-operation sequences during a measured interval.
     */
    int getMultiIntervalTotalOps();
    /**
     * Returns the total number of multi-operation sequences during a measured
     * interval.
     */
    int getMultiIntervalTotalRequests();
    /**
     * Returns the timestamp at the end of a measured interval.
     */
    Date getMultiIntervalEnd();
    /**
     * Returns the timestamp at the beginning of a measured interval.
     */
    Date getMultiIntervalStart();
    /**
     * Returns multi-operations sequences per second during a measured interval.
     */
    long getMultiIntervalThroughput();

    /* Multiple Operation Cumulative Latency Info */

    /**
     * Returns the average latency for multi-operation sequences since service
     * startup.
     */
    float getMultiCumulativeLatAvg();
    /**
     * Returns the highest latency measured for multi-operation sequences since
     * service startup.
     */
    int getMultiCumulativeLatMax();
    /**
     * Returns the lowest latency measured for multi-operation sequences since
     * service startup.
     */
    int getMultiCumulativeLatMin();
    /**
     * Returns the 95th percentile latency for multi-operation sequences since
     * service startup.
     */
    int getMultiCumulativePct95();
    /**
     * Returns the 99th percentile latency for multi-operation sequences since
     * service startup.
     */
    int getMultiCumulativePct99();
    /**
     * Returns the total number of single operations performed in
     * multi-operation sequences since service startup.
     */
    int getMultiCumulativeTotalOps();
    /**
     * Returns the total number of multi operation sequences since service
     * startup.
     */
    int getMultiCumulativeTotalRequests();
    /**
     * Returns the timestamp at the end of a cumulative measurement period.
     */
    Date getMultiCumulativeEnd();
    /**
     * Returns Timestamp of service startup; the start time of a cumulative
     * measurement.
     */
    Date getMultiCumulativeStart();
    /**
     * Returns Multi-operations sequences per second since service startup.
     */
    long getMultiCumulativeThroughput();

    /**
     * Returns the <em>average commit lag</em> (in milliseconds) for a given
     * replication node's update operations during a given time interval.
     * The average commit lag is determined by computing the quotient of
     * the <em>total commit lag</em> and the number of commit log records
     * replayed by the node; where each statistic used to compute the
     * quotient is reported by the JE backend.
     *
     * @see com.sleepycat.je.rep.impl.node.ReplayStatDefinition#TOTAL_COMMIT_LAG_MS
     * @see com.sleepycat.je.rep.impl.node.ReplayStatDefinition#N_COMMITS
     */
    long getCommitLag();

    /* RepNode parameters */

    /**
     * Returns Non-default BDB-JE configuration properties.
     */
    String getConfigProperties();
    /**
     * Returns a string that is added to the command line when the Replication
     * Node process is started.
     */
    String getJavaMiscParams();
    /**
     * Returns property settings for the Logging subsystem.
     */
    String getLoggingConfigProps();
    /**
     * If true, then the underlying BDB-JE subsystem will dump statistics into
     * a local .stat file.
     */
    boolean getCollectEnvStats();
    /**
     * Returns the size of the BDB-JE cache, in MBytes.
     */
    int getCacheSize();
    /**
     * Returns the highest latency that will be included in the calculation of
                         latency percentiles.
     */
    int getMaxTrackedLatency();
    /**
     * Returns the collection period for latency statistics, in sec.
     */
    int getStatsInterval();
    /**
     * Returns the size of the Java heap for this Replication Node, in MB.
     */
    int getHeapMB();
    /**
     * Returns the path to the file system mount point where this Replication
     * Node's files are stored.
     */
    String getMountPoint();
    /**
     * If the Replication Node's latency exceeds this value, a latency ceiling
     * notification will be sent.
     */
    int getLatencyCeiling();
    /**
     * Returns the lower bound on Replication Node throughput.  Lower
     * throughput reports will cause a throughput floor notification to be sent.
     */
    int getThroughputFloor();

    /**
     * If the <em>average commit lag</em> (in milliseconds) for a given
     * replication node during a given time interval exceeds the value
     * returned by this method, a notification event will be sent to any
     * parties that have registered interest. The average commit lag is
     * determined by computing the quotient of the <em>total commit lag</em>
     * and the number of commit log records replayed by the node; where
     * each statistic used to compute the quotient is reported by the
     * JE backend.
     *
     * @see com.sleepycat.je.rep.impl.node.ReplayStatDefinition#TOTAL_COMMIT_LAG_MS
     * @see com.sleepycat.je.rep.impl.node.ReplayStatDefinition#N_COMMITS
     */
    long getCommitLagThreshold();
}
