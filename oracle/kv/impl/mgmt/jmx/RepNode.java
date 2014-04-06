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

package oracle.kv.impl.mgmt.jmx;

import java.util.Date;

import javax.management.MBeanNotificationInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.ObjectName;

import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.measurement.LatencyInfo;
import oracle.kv.impl.measurement.PerfStatType;
import oracle.kv.impl.measurement.RepEnvStats;
import oracle.kv.impl.monitor.views.PerfEvent;
import oracle.kv.impl.rep.monitor.StatsPacket;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.mgmt.jmx.RepNodeMXBean;

import com.sleepycat.je.rep.ReplicatedEnvironmentStats;
import com.sleepycat.je.rep.impl.node.ReplayStatDefinition;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.utilint.Latency;

public class RepNode
    extends NotificationBroadcasterSupport
    implements RepNodeMXBean {

    private final RepNodeId rnId;
    private final MBeanServer server;
    private final StorageNode sn;
    private ServiceStatus status;
    private LatencyInfo singleInterval;
    private LatencyInfo singleCumulative;
    private LatencyInfo multiInterval;
    private LatencyInfo multiCumulative;
    private RepEnvStats repEnvStats;
    private RepNodeParams parameters;
    private ObjectName oName;
    long notifySequence = 1L;

    static final String
        NOTIFY_RN_STATUS_CHANGE = "oracle.kv.repnode.status";
    static final String
        NOTIFY_SINGLE_TFLOOR = "oracle.kv.singleop.throughputfloor";
    static final String
        NOTIFY_SINGLE_LCEILING = "oracle.kv.singleop.latencyceiling";
    static final String
        NOTIFY_MULTI_TFLOOR = "oracle.kv.multiop.throughputfloor";
    static final String
        NOTIFY_MULTI_LCEILING = "oracle.kv.multiop.latencyceiling";
    static final String
        NOTIFY_COMMIT_LAG_THRESHOLD = "oracle.kv.repnode.commitlagthreshold";

    public RepNode(RepNodeParams rnp, MBeanServer server, StorageNode sn) {
        this.server = server;
        this.rnId = rnp.getRepNodeId();
        this.sn = sn;
        status = ServiceStatus.UNREACHABLE;

        resetMetrics();

        setParameters(rnp);

        register();
    }

    private void resetMetrics() {
        /*
         * Create a fake LatencyInfo to report when no metrics are available.
         */
        final LatencyInfo li = new LatencyInfo
            (PerfStatType.PUT_IF_ABSENT_INT,
             System.currentTimeMillis(), System.currentTimeMillis(),
             new Latency(0));

        singleInterval = li;
        singleCumulative = li;
        multiInterval = li;
        multiCumulative = li;

        /* Similarly, create a fake RepEnvStats. */
        final StatGroup replayStats =
            new StatGroup(ReplayStatDefinition.GROUP_NAME,
                          ReplayStatDefinition.GROUP_DESC);
        final ReplicatedEnvironmentStats jeRepEnvStats =
            new ReplicatedEnvironmentStats();
        jeRepEnvStats.setStatGroup(replayStats);
        repEnvStats = new RepEnvStats(0L, 0L, jeRepEnvStats);
    }

    private void register() {

        final StringBuffer buf = new StringBuffer(JmxAgent.DOMAIN);
        buf.append(":type=RepNode");
        buf.append(",id=");
        buf.append(getRepNodeId());
        try {
            oName = new ObjectName(buf.toString());
        } catch (MalformedObjectNameException e) {
            throw new IllegalStateException
                ("Unexpected exception creating JMX ObjectName " +
                 buf.toString(), e);
        }

        try {
            server.registerMBean(this, oName);
        } catch (Exception e) {
            throw new IllegalStateException
                ("Unexpected exception registring MBean " + oName.toString(),
                 e);
        }
    }

    public void unregister() {
        if (oName != null) {
            try {
                server.unregisterMBean(oName);
            } catch (Exception e) {
                throw new IllegalStateException
                    ("Unexpected exception while unregistring MBean " +
                     oName.toString(), e);
            }
        }
    }

    @Override
    public MBeanNotificationInfo[] getNotificationInfo() {
        return new MBeanNotificationInfo[]
        {
            new MBeanNotificationInfo
                (new String[]{RepNode.NOTIFY_RN_STATUS_CHANGE},
                 Notification.class.getName(),
                 "Announce a change in this RepNode's service status"),
            new MBeanNotificationInfo
                (new String[]{RepNode.NOTIFY_SINGLE_TFLOOR},
                 Notification.class.getName(),
                 "Single-operation throughput floor violation notification."),
            new MBeanNotificationInfo
                (new String[]{RepNode.NOTIFY_SINGLE_LCEILING},
                 Notification.class.getName(),
                 "Single-operation latency ceiling violation notification."),
            new MBeanNotificationInfo
                (new String[]{RepNode.NOTIFY_MULTI_TFLOOR},
                 Notification.class.getName(),
                 "Multi-operation throughput floor violation notification."),
            new MBeanNotificationInfo
                (new String[]{RepNode.NOTIFY_MULTI_LCEILING},
                 Notification.class.getName(),
                 "Multi-operation latency ceiling violation notification."),
            new MBeanNotificationInfo
                (new String[]{RepNode.NOTIFY_COMMIT_LAG_THRESHOLD},
                 Notification.class.getName(),
                 "Commit lag threshold violation notification.")
        };
    }

    public void setParameters(RepNodeParams rnp) {
        parameters = rnp;
    }

    public synchronized void setPerfStats(StatsPacket packet) {

        singleInterval =
            packet.get(PerfStatType.USER_SINGLE_OP_INT);
        singleCumulative =
            packet.get(PerfStatType.USER_SINGLE_OP_CUM);
        multiInterval =
            packet.get(PerfStatType.USER_MULTI_OP_INT);
        multiCumulative =
            packet.get(PerfStatType.USER_MULTI_OP_CUM);
        repEnvStats = packet.getRepEnvStats();

        final int ceiling = parameters.getLatencyCeiling();
        final int floor = parameters.getThroughputFloor();
        final long lagThreshold = parameters.getCommitLagThreshold();

        Notification notification = null;

        /*
         * Check the interval measurements against their limits to determine
         * whether an trap should be issued.
         */
        if (singleInterval.getLatency().getTotalOps() != 0) {
            if (PerfEvent.latencyCeilingExceeded(ceiling, singleInterval)) {
                notification = new Notification
                    (NOTIFY_SINGLE_LCEILING, oName, notifySequence++,
                     System.currentTimeMillis(),
                     "The latency ceiling limit for single operations " +
                     "of " + ceiling + "ms was violated.");
                notification.setUserData(new Float(getIntervalLatAvg()));
                sendNotification(notification);
                sn.sendProxyNotification(notification);
            }

            if (PerfEvent.throughputFloorExceeded(floor, singleInterval)) {
                notification = new Notification
                    (NOTIFY_SINGLE_TFLOOR, oName, notifySequence++,
                     System.currentTimeMillis(),
                     "The throughput floor limit for single operations " +
                     "of " + floor + " ops/sec was violated.");
                notification.setUserData
                    (new Long(getIntervalThroughput()));
                sendNotification(notification);
                sn.sendProxyNotification(notification);
            }
        }

        if (multiInterval.getLatency().getTotalOps() != 0) {
            if (PerfEvent.latencyCeilingExceeded(ceiling, multiInterval)) {
                notification = new Notification
                    (NOTIFY_MULTI_LCEILING, oName, notifySequence++,
                     System.currentTimeMillis(),
                     "The latency ceiling limit for multi operations " +
                     "of " + ceiling + "ms was violated.");
                notification.setUserData
                    (new Float(getMultiIntervalLatAvg()));
                sendNotification(notification);
                sn.sendProxyNotification(notification);
            }
            if (PerfEvent.throughputFloorExceeded(floor, multiInterval)) {
                notification = new Notification
                    (NOTIFY_MULTI_TFLOOR, oName, notifySequence++,
                     System.currentTimeMillis(),
                     "The throughput floor limit for multi operations " +
                     "of " + floor + " ops/sec was violated.");
                notification.setUserData
                    (new Long(getMultiIntervalThroughput()));
                sendNotification(notification);
                sn.sendProxyNotification(notification);
            }
        }

        if (PerfEvent.commitLagThresholdExceeded(repEnvStats, lagThreshold)) {
            notification = new Notification
                (NOTIFY_COMMIT_LAG_THRESHOLD, oName, notifySequence++,
                 System.currentTimeMillis(),
                 "Exceeded commit lag threshold [" + lagThreshold + " ms]");
            notification.setUserData
                (new Long(getCommitLag()));
            sendNotification(notification);
            sn.sendProxyNotification(notification);
        }
    }

    public synchronized void setServiceStatus(ServiceStatus newStatus) {
        if (status.equals(newStatus)) {
            return;
        }

        final Notification n = new Notification
            (NOTIFY_RN_STATUS_CHANGE, oName, notifySequence++,
             System.currentTimeMillis(),
             "The service status for RepNode " + getRepNodeId() +
             " changed to " + newStatus.toString() + ".");

        n.setUserData(newStatus.toString());

        sendNotification(n);

        /*
         * Also send it from the StorageNode. A client can observe this event
         * by subscribing ether to the StorageNode or to this RepNode.
         */
        sn.sendProxyNotification(n);

        status = newStatus;

        /*
         * Whenever there is a service status change, reset the metrics so that
         * we don't report stale information.
         */
        resetMetrics();
    }

    @Override
    public String getRepNodeId() {
        return rnId.getFullName();
    }

    @Override
    public String getServiceStatus() {
        return status.toString();
    }

    @Override
    public float getIntervalLatAvg() {
        return singleInterval.getLatency().getAvg();
    }

    @Override
    public int getIntervalLatMax() {
        return singleInterval.getLatency().getMax();
    }

    @Override
    public int getIntervalLatMin() {
        return singleInterval.getLatency().getMin();
    }

    @Override
    public int getIntervalPct95() {
        return singleInterval.getLatency().get95thPercent();
    }

    @Override
    public int getIntervalPct99() {
        return singleInterval.getLatency().get99thPercent();
    }

    @Override
    public int getIntervalTotalOps() {
        return singleInterval.getLatency().getTotalOps();
    }

    @Override
    public Date getIntervalEnd() {
        return new Date(singleInterval.getEnd());
    }

    @Override
    public Date getIntervalStart() {
        return new Date(singleInterval.getStart());
    }

    @Override
    public long getIntervalThroughput() {
        return singleInterval.getThroughputPerSec();
    }

    @Override
    public float getCumulativeLatAvg() {
        return singleCumulative.getLatency().getAvg();
    }

    @Override
    public int getCumulativeLatMax() {
        return singleCumulative.getLatency().getMax();
    }

    @Override
    public int getCumulativeLatMin() {
        return singleCumulative.getLatency().getMin();
    }

    @Override
    public int getCumulativePct95() {
        return singleCumulative.getLatency().get95thPercent();
    }

    @Override
    public int getCumulativePct99() {
        return singleCumulative.getLatency().get99thPercent();
    }

    @Override
    public int getCumulativeTotalOps() {
        return singleCumulative.getLatency().getTotalOps();
    }

    @Override
    public Date getCumulativeEnd() {
        return new Date(singleCumulative.getEnd());
    }

    @Override
    public Date getCumulativeStart() {
        return new Date(singleCumulative.getStart());
    }

    @Override
    public long getCumulativeThroughput() {
        return singleCumulative.getThroughputPerSec();
    }

    @Override
    public float getMultiIntervalLatAvg() {
        return multiInterval.getLatency().getAvg();
    }

    @Override
    public int getMultiIntervalLatMax() {
        return multiInterval.getLatency().getMax();
    }

    @Override
    public int getMultiIntervalLatMin() {
        return multiInterval.getLatency().getMin();
    }

    @Override
    public int getMultiIntervalPct95() {
        return multiInterval.getLatency().get95thPercent();
    }

    @Override
    public int getMultiIntervalPct99() {
        return multiInterval.getLatency().get99thPercent();
    }

    @Override
    public int getMultiIntervalTotalOps() {
        return multiInterval.getLatency().getTotalOps();
    }

    @Override
    public int getMultiIntervalTotalRequests() {
        return multiInterval.getLatency().getTotalRequests();
    }

    @Override
    public Date getMultiIntervalEnd() {
        return new Date(multiInterval.getEnd());
    }

    @Override
    public Date getMultiIntervalStart() {
        return new Date(multiInterval.getStart());
    }

    @Override
    public long getMultiIntervalThroughput() {
        return multiInterval.getThroughputPerSec();
    }

    @Override
    public float getMultiCumulativeLatAvg() {
        return multiCumulative.getLatency().getAvg();
    }

    @Override
    public int getMultiCumulativeLatMax() {
        return multiCumulative.getLatency().getMax();
    }

    @Override
    public int getMultiCumulativeLatMin() {
        return multiCumulative.getLatency().getMin();
    }

    @Override
    public int getMultiCumulativePct95() {
        return multiCumulative.getLatency().get95thPercent();
    }

    @Override
    public int getMultiCumulativePct99() {
        return multiCumulative.getLatency().get99thPercent();
    }

    @Override
    public int getMultiCumulativeTotalOps() {
        return multiCumulative.getLatency().getTotalOps();
    }

    @Override
    public int getMultiCumulativeTotalRequests() {
        return multiCumulative.getLatency().getTotalRequests();
    }

    @Override
    public Date getMultiCumulativeEnd() {
        return new Date(multiCumulative.getEnd());
    }

    @Override
    public Date getMultiCumulativeStart() {
        return new Date(multiCumulative.getStart());
    }

    @Override
    public long getMultiCumulativeThroughput() {
        return multiCumulative.getThroughputPerSec();
    }

    @Override
    public long getCommitLag() {
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

    @Override
    public String getConfigProperties() {
        return parameters.getConfigProperties();
    }

    @Override
    public String getJavaMiscParams() {
        return parameters.getJavaMiscParams();
    }

    @Override
    public String getLoggingConfigProps() {
        return parameters.getLoggingConfigProps();
    }

    @Override
    public boolean getCollectEnvStats() {
        return parameters.getCollectEnvStats();
    }

    @Override
    public int getCacheSize() {
        return (int) (parameters.getJECacheSize() / (1024 * 1024));
    }

    @Override
    public int getMaxTrackedLatency() {
        return parameters.getMaxTrackedLatency();
    }

    @Override
    public int getStatsInterval() {
        return parameters.getStatsInterval() / 1000; /* In seconds. */
    }

    @Override
    public int getHeapMB() {
        return (int) parameters.getMaxHeapMB();
    }

    @Override
    public String getMountPoint() {
        return parameters.getMountPointString();
    }

    @Override
    public int getLatencyCeiling() {
        return parameters.getLatencyCeiling();
    }

    @Override
    public int getThroughputFloor() {
        return parameters.getThroughputFloor();
    }

    @Override
    public long getCommitLagThreshold() {
        return parameters.getCommitLagThreshold();
    }
}
