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

package oracle.kv.impl.rep;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.measurement.EnvStats;
import oracle.kv.impl.measurement.JVMStats;
import oracle.kv.impl.measurement.LatencyInfo;
import oracle.kv.impl.measurement.PerfStatType;
import oracle.kv.impl.measurement.RepEnvStats;
import oracle.kv.impl.monitor.AgentRepository;
import oracle.kv.impl.monitor.views.PerfEvent;
import oracle.kv.impl.param.DurationParameter;
import oracle.kv.impl.param.ParameterListener;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.rep.monitor.StatsPacket;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.RateLimitingLogger;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironmentStats;
import com.sleepycat.utilint.LatencyStat;
import com.sleepycat.utilint.StatsTracker;

/**
 * Stats pertaining to a single replication node.
 */
public class OperationsStatsTracker implements ParameterListener {

    private static final int WAIT_FOR_HANDLE = 100;
    private final AgentRepository monitorBuffer;
    private final ScheduledExecutorService collector;
    private Future<?> collectorFuture;
    protected List<Listener> listeners = new ArrayList<Listener>();

    /*
     * The actual tracker. This is a member variable that can be replaced
     * if/when new parameters are set.
     */
    private volatile SummarizingStatsTracker tracker;

    /* Timestamp for the start of all operation tracking. */
    private long trackingStart;
    /* Timestamp for the end of the last collection period. */
    private long lastEnd;
    /* End of log at the time environment stats were last collected. */
    private long lastEndOfLog = 0;
    /* Configuration used to collect stats. */
    private final StatsConfig config = new StatsConfig().setClear(true);

    private final Logger logger;
    private final RepNodeService repNodeService;

    /**
     * Log no more than 1 threshold alert every 5 minutes.
     */
    private static final int LOG_SAMPLE_PERIOD_MS = 5 * 60 * 1000;

    /**
     * The max number of types of threshold alerts that will be logged;
     * for example, 'single op interval latency above ceiling',
     * 'multi-op interval throughput below floor', etc.
     */
    private static final int MAX_LOG_TYPES = 5;

    /**
     * Encapsulates the logger used by this class. When this logger is used,
     * for each type of PerfEvent, the rate at which this logger writes 
     * records corresponding to the given type will be bounded; to prevent
     * overwhelming the store's log file.
     */
    private final RateLimitingLogger<String> eventLogger;

    /**
     */
    public OperationsStatsTracker(RepNodeService repNodeService,
                                  ParameterMap map,
                                  AgentRepository monitorBuffer) {

        this.repNodeService = repNodeService;
        this.monitorBuffer = monitorBuffer;
        RepNodeService.Params params = repNodeService.getParams();
        this.logger =
            LoggerUtils.getLogger(OperationsStatsTracker.class, params);
        this.eventLogger = new RateLimitingLogger<String>
            (LOG_SAMPLE_PERIOD_MS, MAX_LOG_TYPES, logger);
        ThreadFactory factory = new CollectorThreadFactory
            (logger, params.getRepNodeParams().getRepNodeId());
        collector = new ScheduledThreadPoolExecutor(1, factory);
        initialize(map);
    }

    /**
     * For unit test only, effectively a no-op stats tracker, to disable stats
     * tracking.
     */
    public OperationsStatsTracker() {
        tracker = new SummarizingStatsTracker(null, 0, 0, 0, 1000);
        monitorBuffer = null;
        collector = null;
        collectorFuture = null;
        logger = null;
        eventLogger = null;
        repNodeService = null;
        trackingStart = 0;
    }

    /**
     * Used for initialization during constructions and from newParameters()
     * NOTE: newParameters() results in loss of cumulative stats and reset of
     * trackingStart.
     */
    private void initialize(ParameterMap map) {
        if (collectorFuture != null) {
            logger.fine("Cancelling current operationStatsCollector");
            collectorFuture.cancel(true);
        }

        tracker = new SummarizingStatsTracker
            (logger,
             map.get(ParameterState.SP_ACTIVE_THRESHOLD).asInt(),
             ParameterUtils.getThreadDumpIntervalMillis(map),
             map.get(ParameterState.SP_THREAD_DUMP_MAX).asInt(),
             ParameterUtils.getMaxTrackedLatencyMillis(map));

        DurationParameter dp =
            (DurationParameter) map.get(ParameterState.SP_INTERVAL);
        Start start = calculateStart
            (Calendar.getInstance(), (int) dp.getAmount(), dp.getUnit());
        logger.fine("Starting operationStatsCollector " + start);
        collectorFuture = collector.scheduleAtFixedRate
            (new CollectStats(), start.delay, start.interval, start.unit);
        lastEnd = System.currentTimeMillis();
        trackingStart = lastEnd;
    }

    public StatsTracker<OpCode> getStatsTracker() {
        return tracker;
    }

    @Override
    synchronized public void newParameters(ParameterMap oldMap,
                                           ParameterMap newMap) {

        /*
         * Caller ensures that the maps are different, check for
         * differences that matter to this class.  Re-init if *any* of the
         * parameters are different.
         */
        if (paramsDiffer(oldMap, newMap, ParameterState.SP_INTERVAL) ||
            paramsDiffer(oldMap, newMap, ParameterState.SP_THREAD_DUMP_MAX) ||
            paramsDiffer(oldMap, newMap, ParameterState.SP_ACTIVE_THRESHOLD) ||
            paramsDiffer(oldMap, newMap, ParameterState.SP_MAX_LATENCY) ||
            paramsDiffer(oldMap, newMap, ParameterState.SP_DUMP_INTERVAL)) {
            initialize(newMap);
        }
    }

    private boolean paramsDiffer(ParameterMap map1,
                                 ParameterMap map2,
                                 String param) {
        return map1.get(param).equals(map2.get(param));
    }

    /*
     * Struct to package together the information needed to schedule the
     * executor. Packaged this way rather than making these fields be class
     * members for easier unit testing.
     */
    static class Start {
        int delay;
        int interval;
        TimeUnit unit;
    }

    static Start calculateStart(Calendar now,
                                int configuredInterval,
                                TimeUnit configuredUnit) {

        Start start = new Start();

        start.unit = configuredUnit;
        start.interval = configuredInterval;

        /*
         * It's easier to calculate a sync'ed up start if the intervals are
         * promoted to their maximum unit.
         */
        switch (configuredUnit) {
        case HOURS:
            /* Round interval up to days. */
            if (configuredInterval > 24) {
                start.unit = TimeUnit.DAYS;
                start.interval = configuredInterval/24;
                if (configuredInterval%24 > 0) {
                    start.interval++;
                }
            }
            break;

        case MINUTES:
            /* Round interval up to hours. */
            if (configuredInterval > 60) {
                start.unit = TimeUnit.HOURS;
                start.interval = configuredInterval/60;
                if (configuredInterval%60 > 0) {
                    start.interval++;
                }
            }
            break;

        case SECONDS:
            /* Round interval up to minutes. */
            if (configuredInterval > 60) {
                start.unit = TimeUnit.MINUTES;
                start.interval = configuredInterval/60;
                if (configuredInterval%60 > 0) {
                    start.interval++;
                }
            }
            break;

        default:
            break;
        }

        /*
         * Calculate delayToNext to see if it's possible to sync up all the rep
         * nodes.  This works fine as long as the unit is an even factor of the
         * next largest granularity unit.
         *
         * i.e. if the interval is 5, 10, 15, 20, 30 minutes, we can figure out
         * a good point in the hour to start. But if the interval doesn't
         * divide into an the next unit evenly, it's not worth doing, so skip
         * it.
         */
        switch (start.unit) {
        case HOURS:
            nextStart(start, now, Calendar.HOUR, 24);
            break;

        case  MINUTES:
            nextStart(start, now, Calendar.MINUTE, 60);
            break;

        case SECONDS:
            nextStart(start, now, Calendar.SECOND, 60);
            break;

        default:
            break;
        }

        return start;
    }

    private static void nextStart(Start start,
                                  Calendar now,
                                  int calField,
                                  int numTotalUnits) {

        if ((numTotalUnits % start.interval) != 0) {
            start.delay = 0;
            return;
        }

        int nowUnits = now.get(calField);
        int nextStartUnit = (nowUnits / start.interval) + 1;
        start.delay = (nextStartUnit * start.interval) - nowUnits;
    }

    public void close() {
        collectorFuture.cancel(true);
    }

    /**
     * Invoked by the async collection job and at rep node close.
     */
    synchronized public void pushStats() {

        logger.fine("Collecting latency stats");
        long useStart = lastEnd;
        long useEnd = System.currentTimeMillis();

        StatsPacket packet = new StatsPacket(useStart, useEnd);

        /* Gather up all the base, per operation stats. */
        for (OpCode op: OpCode.values()) {
            LatencyStat stat = tracker.getIntervalLatency().get(op);

            /*
             * Interval latencies timestamps are set to reflect this period
             * only, while cumulative latencies timestamps reflect the entire
             * lifetime of the stats tracker. That way, cumulative and
             * interval latency throughputs are correct.
             */
            packet.add(new LatencyInfo(op.getIntervalMetric(),
                                       useStart, useEnd,
                                       stat.calculate()));

            stat = tracker.getCumulativeLatency().get(op);
            packet.add(new LatencyInfo(op.getCumulativeMetric(),
                                       trackingStart, useEnd,
                                       stat.calculate()));
        }

        /* Add the summary stats. */
        LatencyStat singleOpsInterval = tracker.getSingleOpsIntervalStat();
        packet.add(new LatencyInfo(PerfStatType.USER_SINGLE_OP_INT,
                                   useStart, useEnd,
                                   singleOpsInterval.calculate()));
        packet.add(new LatencyInfo(PerfStatType.USER_SINGLE_OP_CUM,
                                   trackingStart, useEnd,
                                   tracker.getSingleOpsCumulativeStat().
                                   calculate()));

        LatencyStat multiOpsInterval = tracker.getMultiOpsIntervalStat();
        packet.add(new LatencyInfo(PerfStatType.USER_MULTI_OP_INT,
                                   useStart, useEnd,
                                   multiOpsInterval.calculate()));
        packet.add(new LatencyInfo(PerfStatType.USER_MULTI_OP_CUM,
                                   trackingStart, useEnd,
                                   tracker.getMultiOpsCumulativeStat().
                                   calculate()));

        if (repNodeService.getParams().getRepNodeParams().
            getCollectEnvStats()) {
            ReplicatedEnvironment repEnv =
                repNodeService.getRepNode().getEnv(WAIT_FOR_HANDLE);

            /*
             * Check if the env is open; this method may be called after the
             * repNodeService has stopped.
             */
            if ((repEnv != null) && (repEnv.isValid())) {
                EnvironmentStats envStats = repEnv.getStats(config);

                /*
                 * Collect environment stats if there has been some app
                 * activity, or if there has been some env maintenance related
                 * write activity, independent of the app, say due to cleaning,
                 * checkpointing, replication, etc.
                 */
                if (envStats.getEndOfLog() != lastEndOfLog) {
                    packet.add(new EnvStats(useStart, useEnd, envStats));
                    ReplicatedEnvironmentStats repStats =
                        repEnv.getRepStats(config);
                    packet.add(new RepEnvStats(useStart, useEnd, repStats));
                    lastEndOfLog = envStats.getEndOfLog();
                }
            }
            packet.add(new JVMStats(useStart, useEnd));
        }

        lastEnd = useEnd;

        logThresholdAlerts(Level.WARNING, packet);

        tracker.clearLatency();

        monitorBuffer.add(packet);
        sendPacket(packet);
        logger.fine(packet.toString());
    }

    /**
     * Simple Runnable to send latency stats to the service's monitor agent
     */
    private class CollectStats implements Runnable {

        @Override
        public void run() {
            pushStats();
        }
    }

    /**
     * Collector threads are named KVAgentMonitorCollector and log uncaught
     * exceptions to the monitor logger.
     */
    private class CollectorThreadFactory extends KVThreadFactory {
        private final RepNodeId repNodeId;

        CollectorThreadFactory(Logger logger, RepNodeId repNodeId) {
            super(null, logger);
            this.repNodeId = repNodeId;
        }

        @Override
        public String getName() {
            return  repNodeId + "_MonitorAgentCollector";
        }
    }

    /**
     * This stats tracker adds these customizations:
     *
     * - operations are recorded both in a per-op-type stat, and in a
     * single-op or multi-op summary stat. The summary stats preserve
     * 95th/99th, because those values would be lost if we try to rollup the
     * individual op stats.
     *
     * - NOP ops are excluded.
     */
    private class SummarizingStatsTracker extends StatsTracker<OpCode> {
        private final LatencyStat singleOpsInterval;
        private final LatencyStat singleOpsCumulative;
        private final LatencyStat multiOpsInterval;
        private final LatencyStat multiOpsCumulative;

        public SummarizingStatsTracker(Logger stackTraceLogger,
                                       int activeThreadThreshold,
                                       long threadDumpIntervalMillis,
                                       int threadDumpMax,
                                       int maxTrackedLatencyMillis) {
            super(OpCode.values(), stackTraceLogger, activeThreadThreshold,
                  threadDumpIntervalMillis, threadDumpMax,
                  maxTrackedLatencyMillis);

            singleOpsInterval = new LatencyStat(maxTrackedLatencyMillis);
            singleOpsCumulative = new LatencyStat(maxTrackedLatencyMillis);
            multiOpsInterval = new LatencyStat(maxTrackedLatencyMillis);
            multiOpsCumulative = new LatencyStat(maxTrackedLatencyMillis);
        }

        /**
         * Note that markFinish may be called with a null op type.
         */
        @Override
        public void markFinish(OpCode opType, long startTime, int numRecords) {

            super.markFinish(opType, startTime, numRecords);
            if (numRecords == 0) {
                return;
            }

            if (opType == null) {
                return;
            }

            long elapsed = System.nanoTime() - startTime;
            PerfStatType ptype = opType.getIntervalMetric();
            if (ptype.getParent().equals(PerfStatType.USER_SINGLE_OP_INT)) {
                singleOpsInterval.set(elapsed);
                singleOpsCumulative.set(elapsed);
            } else if (ptype.getParent().equals
                       (PerfStatType.USER_MULTI_OP_INT)){
                multiOpsInterval.set(numRecords, elapsed);
                multiOpsCumulative.set(numRecords, elapsed);
            }
        }

        /**
         * Should be called after each interval latency stat collection, to
         * reset for the next period's collection.
         */
        @Override
        public void clearLatency() {
            super.clearLatency();
            singleOpsInterval.clear();
            multiOpsInterval.clear();
        }

        LatencyStat getSingleOpsIntervalStat() {
            return singleOpsInterval;
        }

        LatencyStat getSingleOpsCumulativeStat() {
            return singleOpsCumulative;
        }

        LatencyStat getMultiOpsIntervalStat() {
            return multiOpsInterval;
        }

        LatencyStat getMultiOpsCumulativeStat() {
            return multiOpsCumulative;
        }
    }

    /**
     * An OperationsStatsTracker.Listener can be implemented by clients of this
     * interface to recieve stats when they are collected.
     */
    public interface Listener {
        void receiveStats(StatsPacket packet);
    }

    public void addListener(Listener lst) {
        listeners.add(lst);
    }

    public void removeListener(Listener lst) {
        listeners.remove(lst);
    }

    private void sendPacket(StatsPacket packet) {
        for (Listener lst : listeners) {
            lst.receiveStats(packet);
        }
    }

    private void logThresholdAlerts(Level level, StatsPacket packet) {

        /* Better to not log at all than risk a possible NPE. */
        if (logger == null || eventLogger == null ||
            repNodeService == null || level == null || packet == null) {
            return;
        }
        if (!logger.isLoggable(level)) {
            return;
        }

        final RepNodeParams params = repNodeService.getRepNodeParams();
        if (params == null) {
            return;
        }

        final int ceiling = params.getLatencyCeiling();
        final int floor = params.getThroughputFloor();
        final long threshold = params.getCommitLagThreshold();
        final long lag = PerfEvent.getCommitLagMs(packet.getRepEnvStats());

        /* For single operation within a given time interval */
        final LatencyInfo singleOpIntervalLatencyInfo =
                        packet.get(PerfStatType.USER_SINGLE_OP_INT);

        if (PerfEvent.latencyCeilingExceeded(
                          ceiling, singleOpIntervalLatencyInfo)) {
            eventLogger.log("single-op-interval-latency", level,
                            "single op interval latency above ceiling [" +
                            ceiling + "]");
        }

        if (PerfEvent.throughputFloorExceeded(
                          floor, singleOpIntervalLatencyInfo)) {
            eventLogger.log("single-op-interval-throughput",
                            level, "single op interval throughput below " +
                            "floor [" + floor + "]");
        }

        /* For multiple operations within a given time interval */
        final LatencyInfo multiOpIntervalLatencyInfo =
            packet.get(PerfStatType.USER_MULTI_OP_INT);

        if (PerfEvent.latencyCeilingExceeded(
                          ceiling, multiOpIntervalLatencyInfo)) {
            eventLogger.log("multi-op-interval-latency", level,
                            "multi-op interval latency above ceiling [" +
                            ceiling + "]");
        }

        if (PerfEvent.throughputFloorExceeded(
                          floor, multiOpIntervalLatencyInfo)) {
            eventLogger.log("multi-op-interval-throughput", level,
                            "multi-op interval throughput below floor [" +
                            floor + "]");
        }

        /* For commit lag averaged over the collection period */
        if (PerfEvent.commitLagThresholdExceeded(lag, threshold)) {
            eventLogger.log("replica-lag", level,
                            "replica lag exceeds threshold [replicaLagMs=" +
                            lag + " threshold=" + threshold + "]");
        }
    }
}
