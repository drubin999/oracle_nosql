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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import oracle.kv.impl.admin.AdminServiceParams;
import oracle.kv.impl.measurement.LatencyInfo;
import oracle.kv.impl.measurement.Measurement;
import oracle.kv.impl.measurement.MeasurementType;
import oracle.kv.impl.measurement.PerfStatType;
import oracle.kv.impl.monitor.Metrics;
import oracle.kv.impl.monitor.Monitor;
import oracle.kv.impl.monitor.View;
import oracle.kv.impl.rep.monitor.StatsPacket;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * A direct view which accepts and summarizes performance information and
 * writes it to .csv files.
 * TODO: How should the resulting .csv file be
 * truncated? The parameter which enables this should be made mutable.
 */
public class CSVFileView implements View {

    private final Logger logger;
    private final Map<ResourceId,PrintStream> detailFiles;
    private final Map<ResourceId,PrintStream> summaryFiles;
    private final AdminServiceParams params;
    private final PerfStatType[] detailStats;
    private final PerfStatType[] summaryStats;

    /*
     * Preserve the results of summarizing the latency stats to use for
     * unit testing the view.
     */
    private Map<PerfStatType, LatencyInfo> summary;

    public CSVFileView(AdminServiceParams params) {
        logger = LoggerUtils.getLogger(this.getClass(), params);
        detailFiles = new HashMap<ResourceId,PrintStream>();
        summaryFiles = new HashMap<ResourceId,PrintStream>();
        this.params = params;
        detailStats = PerfStatType.getDetailedStats();
        summaryStats = PerfStatType.getSummaryStats();
    }

    @Override
    public String getName() {
        return Monitor.INTERNAL_STATS_FILE_VIEW;
    }

    @Override
    public Set<MeasurementType> getTargetMetricTypes() {
        return Collections.singleton(Metrics.RNSTATS);
    }

    @Override
    public void applyNewInfo(ResourceId resourceId, Measurement m) {

        StatsPacket statsPacket = (StatsPacket) m;
        logger.finest("Stats File getting new info from " + resourceId);

        PrintStream detailOut = null;
        PrintStream summaryOut = null;
        Map<String, Long> sortedEnvStats = statsPacket.sortEnvStats();
        synchronized (this) {
            detailOut = detailFiles.get(resourceId);
            summaryOut = summaryFiles.get(resourceId);
            if (detailOut == null) {
                createFiles(resourceId, statsPacket, sortedEnvStats);
                detailOut = detailFiles.get(resourceId);
                summaryOut = summaryFiles.get(resourceId);
            }
        }
        statsPacket.writeStats(detailOut, detailStats, sortedEnvStats);
        summary = statsPacket.summarizeAndWriteStats
            (summaryOut, summaryStats, sortedEnvStats);
    }

    /**
     * Create the .csv files that store per-rep-node statistics.]
     */
    private void createFiles(ResourceId resourceId,
                             StatsPacket packet,
                             Map<String, Long> sortedEnvStats) {
        File rootDir = new File(params.getStorageNodeParams().getRootDirPath());
        String storeName = params.getGlobalParams().getKVStoreName();
        File loggingDir = FileNames.getLoggingDir(rootDir,storeName);

        File detailCSVFile = new File(loggingDir, resourceId.toString() +
                                      FileNames.DETAIL_CSV);
        initOneFile(resourceId, detailCSVFile, detailStats, detailFiles,
                    packet, sortedEnvStats);

        File summaryCSVFile = new File(loggingDir, resourceId.toString() +
                                      FileNames.SUMMARY_CSV);
        initOneFile(resourceId, summaryCSVFile, summaryStats, summaryFiles,
                    packet, sortedEnvStats);
    }

    /**
     * Create the file and write the header for a .csv file
     */
    private void initOneFile(ResourceId resourceId,
                             File file,
                             PerfStatType[] headerList,
                             Map<ResourceId, PrintStream> streamMap,
                             StatsPacket packet,
                             Map<String, Long> sortedEnvStats) {
        PrintStream out = null;
        try {
            out = new PrintStream(file);
        } catch (FileNotFoundException e) {
            // TODO, BOZO, throw proper wrapper exception
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        packet.writeCSVHeader(out, headerList, sortedEnvStats);
        out.println("");
        streamMap.put(resourceId, out);
    }

    @Override
    public void close() {
        for (PrintStream p: detailFiles.values()) {
            p.close();
        }
        for (PrintStream p: summaryFiles.values()) {
            p.close();
        }
    }

    /** For unit testing */
    public Map<PerfStatType, LatencyInfo> getSummary() {
        return summary;
    }
}