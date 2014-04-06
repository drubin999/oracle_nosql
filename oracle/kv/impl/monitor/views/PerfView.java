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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import oracle.kv.impl.admin.AdminServiceParams;
import oracle.kv.impl.measurement.ConciseStats;
import oracle.kv.impl.measurement.EnvStats;
import oracle.kv.impl.measurement.LatencyInfo;
import oracle.kv.impl.measurement.Measurement;
import oracle.kv.impl.measurement.MeasurementType;
import oracle.kv.impl.measurement.PerfStatType;
import oracle.kv.impl.measurement.RepEnvStats;
import oracle.kv.impl.monitor.Metrics;
import oracle.kv.impl.monitor.Monitor;
import oracle.kv.impl.monitor.MonitorKeeper;
import oracle.kv.impl.monitor.View;
import oracle.kv.impl.monitor.ViewListener;
import oracle.kv.impl.rep.monitor.StatsPacket;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.FormatUtils;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * Takes a StatsPacket from the RepNodes and dispatches it to appropriate
 * listeners.
 */
public class PerfView implements View {

    private final MonitorKeeper admin;

    private final Set<ViewListener<PerfEvent>> listeners;
    private final Logger envStatLogger;

    public PerfView(AdminServiceParams params, MonitorKeeper admin) {
        this.admin = admin;
        listeners = new HashSet<ViewListener<PerfEvent>>();

        envStatLogger =
            LoggerUtils.getStatFileLogger(this.getClass(),
                                          params.getGlobalParams(),
                                          params.getStorageNodeParams());
    }

    @Override
    public String getName() {
        return Monitor.PERF_FILE_VIEW;
    }

    @Override
    public Set<MeasurementType> getTargetMetricTypes() {
        return Collections.singleton(Metrics.RNSTATS);
    }

    /**
     * Distribute the perf stats to any listeners, packaged as
     * a PerfEvent.
     */
    @Override
    public void applyNewInfo(ResourceId resourceId,  Measurement m) {

        final StatsPacket statsPacket = (StatsPacket) m;
        final LatencyInfo singleInterval =
            statsPacket.get(PerfStatType.USER_SINGLE_OP_INT);
        final LatencyInfo singleCumulative =
            statsPacket.get(PerfStatType.USER_SINGLE_OP_CUM);
        final LatencyInfo multiInterval =
            statsPacket.get(PerfStatType.USER_MULTI_OP_INT);
        final LatencyInfo multiCumulative =
            statsPacket.get(PerfStatType.USER_MULTI_OP_CUM);

        final RepEnvStats repStats = statsPacket.getRepEnvStats();

        /*
         * Only create a PerfEvent if there were single or multi operations
         * in this interval, and therefore some kind of new activity to report;
         * or if the commit lag is non-zero.
         */
        if ((singleInterval.getLatency().getTotalOps() != 0) ||
            (multiInterval.getLatency().getTotalOps() != 0) ||
            (PerfEvent.commitLagThresholdExceeded(repStats, 1))) {

            final PerfEvent event = new PerfEvent
                (resourceId,
                 singleInterval, singleCumulative,
                 admin.getLatencyCeiling(resourceId),
                 admin.getThroughputFloor(resourceId),
                 admin.getCommitLagThreshold(resourceId),
                 multiInterval, multiCumulative,
                 repStats);

            for (ViewListener<PerfEvent> listener : listeners) {
                listener.newInfo(resourceId, event);
            }
        }

        /*
         * JE environment and replication stats only go to the appropriate
         * .stat files.
         */
        final EnvStats envStats = statsPacket.getEnvStats();
        if (envStats != null) {
            envStatLogger.info(displayConciseStats(resourceId,
                                                   statsPacket.getStart(),
                                                   statsPacket.getEnd(),
                                                   envStats));
        }

        if (repStats != null) {
            envStatLogger.info(displayConciseStats(resourceId,
                                                   statsPacket.getStart(),
                                                   statsPacket.getEnd(),
                                                   repStats));
        }

        final List<ConciseStats> otherStats = statsPacket.getOtherStats();
        if (otherStats != null) {
            for (ConciseStats stats : otherStats) {
                envStatLogger.info(displayConciseStats(resourceId,
                                                       statsPacket.getStart(),
                                                       statsPacket.getEnd(),
                                                       stats));
            }
        }
    }

    public synchronized void addListener(ViewListener<PerfEvent> l) {
        listeners.add(l);
    }

    public synchronized
        void removeListener(ViewListener<PerfEvent> l) {
        listeners.remove(l);
    }

    @Override
    public void close() {
        /* Nothing to do. */
    }

    private String displayConciseStats(ResourceId resourceId,
                                       long start,
                                       long end,
                                       ConciseStats stats) {
        final StringBuilder sb = new StringBuilder();
        sb.append(resourceId + " (" +
                  FormatUtils.formatTime(start) +  " -> " +
                  FormatUtils.formatTime(end) + ")\n");
        sb.append(stats.getFormattedStats());
        return sb.toString();
    }
}
