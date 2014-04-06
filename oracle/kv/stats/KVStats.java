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

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.table.TableAPI;
import oracle.kv.impl.api.RequestDispatcher;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.parallelscan.StoreIteratorMetricsImpl;
import oracle.kv.impl.api.rgstate.RepNodeState;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;

import com.sleepycat.utilint.Latency;

/**
 * Statistics associated with accessing the KVStore from a client via the
 * KVStore handle. These statistics are from the client's perspective and can
 * therefore vary from client to client depending on the configuration and load
 * on a specific client as well as the network path between the client and the
 * nodes in the KVStore.
 *
 * @see KVStore#getStats(boolean)
 */
public class KVStats implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<OperationMetrics> opMetrics;

    private final List<NodeMetrics> nodeMetrics;

    private final StoreIteratorMetricsImpl storeIteratorMetrics;

    private final long requestRetryCount;

    /**
     * @hidden
     * Internal use only.
     */
    public KVStats(boolean clear,
                   RequestDispatcher requestDispatcher,
                   StoreIteratorMetricsImpl storeIteratorMetrics) {

        opMetrics = new LinkedList<OperationMetrics>();

        for (Map.Entry<OpCode, Latency> entry :
             requestDispatcher.getLatencyStats(clear).entrySet()) {

            opMetrics.add(new OperationMetricsImpl(entry.getKey(),
                                                   entry.getValue()));
        }

        nodeMetrics = new LinkedList<NodeMetrics>();

        final Topology topology =
            requestDispatcher.getTopologyManager().getTopology();
        for (RepNodeState rns :
            requestDispatcher.getRepGroupStateTable().getRepNodeStates()) {
            nodeMetrics.add(new NodeMetricsImpl(topology, rns));
            if (clear) {
                rns.resetStatsCounts();
            }
        }

        this.storeIteratorMetrics = storeIteratorMetrics;
        if (clear) {
            storeIteratorMetrics.clear();
        }

        requestRetryCount = requestDispatcher.getTotalRetryCount(clear);
    }

    /**
     * Returns a list of metrics associated with each operation supported by
     * KVStore. The following table lists the method names and the name
     * associated with it by the {@link OperationMetrics#getOperationName()}.
     * <p>
     * It's worth noting that the metrics related to the Iterator methods are
     * special, since each use of an iterator call may result in multiple
     * underlying operations depending upon the <code>batchSize</code> used for
     * the iteration.
     * <p>
     * <table border="1">
     * <tr>
     * <td>{@link KVStore#delete}, {@link TableAPI#delete}</td>
     * <td>delete</td>
     * </tr>
     * <tr>
     * <td>{@link KVStore#deleteIfVersion}, {@link TableAPI#deleteIfVersion}</td>
     * <td>deleteIfVersion</td>
     * </tr>
     * <tr>
     * <td>{@link KVStore#execute}, {@link TableAPI#execute}</td>
     * <td>execute</td>
     * </tr>
     * <tr>
     * <td>{@link KVStore#get}, {@link TableAPI#get}</td>
     * <td>get</td>
     * </tr>
     * <tr>
     * <td>{@link KVStore#multiDelete}, {@link TableAPI#multiDelete}</td>
     * <td>multiDelete</td>
     * </tr>
     * <tr>
     * <td>{@link KVStore#multiGet}, {@link TableAPI#multiGet}</td>
     * <td>multiGet</td>
     * </tr>
     * <tr>
     * <td>{@link KVStore#multiGetIterator}</td>
     * <td>multiGetIterator</td>
     * </tr>
     * <tr>
     * <td>{@link KVStore#multiGetKeys}, {@link TableAPI#multiGetKeys}</td>
     * <td>multiGetKeys</td>
     * </tr>
     * <tr>
     * <td>{@link KVStore#multiGetKeysIterator}</td>
     * <td>multiGetKeysIterator</td>
     * </tr>
     * <tr>
     * <td>{@link KVStore#put}, {@link TableAPI#put}</td>
     * <td>put</td>
     * </tr>
     * <tr>
     * <td>{@link KVStore#putIfAbsent}, {@link TableAPI#putIfAbsent}</td>
     * <td>putIfAbsent</td>
     * </tr>
     * <tr>
     * <td>{@link KVStore#putIfPresent}, {@link TableAPI#putIfPresent}</td>
     * <td>putIfPresent</td>
     * </tr>
     * <tr>
     * <td>{@link KVStore#putIfVersion}, {@link TableAPI#putIfVersion}</td>
     * <td>putIfVersion</td>
     * </tr>
     * <tr>
     * <td>{@link KVStore#storeIterator}, {@link TableAPI#tableIterator}</td>
     * <td>storeIterator</td>
     * </tr>
     * <tr>
     * <td>{@link KVStore#storeKeysIterator}, {@link TableAPI#tableKeysIterator}</td>
     * <td>storeKeysIterator</td>
     * </tr>
     * <tr>
     * <td>{@link TableAPI#tableIterator(oracle.kv.table.IndexKey, oracle.kv.table.MultiRowOptions, oracle.kv.table.TableIteratorOptions)}</td>
     * <td>indexIterator</td>
     * </tr>
     * <tr>
     * <td>{@link TableAPI#tableKeysIterator(oracle.kv.table.IndexKey, oracle.kv.table.MultiRowOptions, oracle.kv.table.TableIteratorOptions)}</td>
     * <td>indexKeysIterator</td>
     * </tr>
     * </table>
     *
     * @return the list of metrics. One for each of the operations listed
     * above.
     */
    public List<OperationMetrics> getOpMetrics() {
        return opMetrics;
    }

    public StoreIteratorMetrics getStoreIteratorMetrics() {
        return storeIteratorMetrics;
    }

    /**
     * Returns a descriptive string containing metrics for each operation that
     * was actually performed over during the statistics gathering interval,
     * one per line.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        if (requestRetryCount > 0) {
            sb.append(String.format("request retry count= %,d\n",
                                    requestRetryCount));
        }

        for (OperationMetrics metrics : getOpMetrics()) {
            if (metrics.getTotalOps() > 0) {
                sb.append(metrics.toString()).append("\n");
            }
        }

        for (NodeMetrics metrics : getNodeMetrics()) {
            sb.append(metrics.toString()).append("\n");
        }

        final StoreIteratorMetrics metrics = getStoreIteratorMetrics();
        sb.append(metrics.toString()).append("\n");

        return sb.toString();
    }

    /**
     * Returns the metrics associated with each node in the KVStore.
     *
     * @return a list containing one entry for each node in the KVStore.
     */
    public List<NodeMetrics> getNodeMetrics() {
        return nodeMetrics;
    }

    /**
     * Returns the total number of times requests were retried. A single
     * user-level request may be retried transparently at one or more nodes
     * until the request succeeds or it times out. This count reflects those
     * retry operations.
     *
     * @see KVStoreConfig#getRequestTimeout(TimeUnit)
     */
    public long getRequestRetryCount() {
        return requestRetryCount;
    }

    private static class OperationMetricsImpl
        implements OperationMetrics, Serializable {

        private static final long serialVersionUID = 1L;

        private final String operationName;
        private final Latency latency;

        OperationMetricsImpl(OpCode opCode, Latency latency) {
            this.operationName = opCodeToNameMap.get(opCode);
            this.latency = latency;
        }

        @Override
        public String getOperationName() {
            return operationName;
        }

        @Override
        public float getAverageLatencyMs() {
            return latency.getAvg();
        }

        @Override
        public int getMaxLatencyMs() {
            return latency.getMax();
        }

        @Override
        public int getMinLatencyMs() {
            return latency.getMin();
        }

        @Override
        public int getTotalOps() {
            return latency.getTotalOps();
        }

        @Override
        public int getTotalRequests() {
            return latency.getTotalRequests();
        }

        /**
         * Returns a descriptive string containing the values associated with
         * each of the metrics associated with the operation.
         */
        @Override
        public String toString() {
            return String.format("%s: total ops= %,d req= %,d " +
                                 "min/avg/max= %,d/%,.2f/%,d ms",
                                 operationName,
                                 getTotalOps(),
                                 getTotalRequests(),
                                 getMinLatencyMs(),
                                 getAverageLatencyMs(),
                                 getMaxLatencyMs());
        }
    }

    /**
     * Converts an upper case enum name into camel case.
     */
    private static String camelCase(String enumName) {
        StringBuffer sb = new StringBuffer(enumName.length());

        for (int i=0; i < enumName.length(); i++) {

            char nchar = enumName.charAt(i);
            if (nchar == '_') {
                if (i++ >= enumName.length()) {
                    break;
                }
                nchar = Character.toUpperCase(enumName.charAt(i));
            } else {
                nchar = Character.toLowerCase(nchar);
            }
            sb.append(nchar);
        }
        return sb.toString();
    }

    private static Map<OpCode, String> opCodeToNameMap =
        new HashMap<OpCode, String>();

    static {
        for (OpCode opCode : OpCode.values()) {
            opCodeToNameMap.put(opCode, camelCase(opCode.name()));
        }
    }

    private static class NodeMetricsImpl implements NodeMetrics {

        private static final long serialVersionUID = 1L;
        private final RepNodeId repNodeId;
        private final String datacenterName;
        private final boolean isActive;
        private final boolean isMaster;
        private final int maxActiveRequestCount;
        private final long accumRespTimeMs;
        private final long requestCount;
        private final long failedRequestCount;

        private NodeMetricsImpl(Topology topology,
                                RepNodeState rns) {
            repNodeId = rns.getRepNodeId();
            datacenterName = topology.getDatacenter(repNodeId).getName();
            isActive = !rns.reqHandlerNeedsResolution();
            isMaster = rns.getRepState().isMaster();
            maxActiveRequestCount = rns.getMaxActiveRequestCount();
            accumRespTimeMs = rns.getAccumRespTimeMs();
            requestCount = rns.getTotalRequestCount();
            failedRequestCount = rns.getErrorCount();
        }

        @Override
        public String getNodeName() {
            return repNodeId.toString();
        }

        @Override
        public String getDataCenterName() {
            return datacenterName;
        }

        @Override
        public String getZoneName() {
            return datacenterName;
        }

        @Override
        public boolean isActive() {
            return isActive;
        }

        @Override
        public boolean isMaster() {
           return isMaster;
        }

        @Override
        public int getMaxActiveRequestCount() {
            return maxActiveRequestCount;
        }

        @Override
        public long getRequestCount() {
           return requestCount;
        }

        @Override
        public long getFailedRequestCount() {
           return failedRequestCount;
        }

        @Override
        public int getAvLatencyMs() {
            return requestCount > 0 ? (int)(accumRespTimeMs/requestCount) : 0;
        }

        /**
         * Returns a descriptive string containing the values associated with
         * each of the metrics associated with the node.
         */
        @Override
        public String toString() {
            return String.format("%s (Zone %s): isActive= %b, " +
                                 "isMaster= %b, " +
                                 "maxActiveRequests= %,d, " +
                                 "request count= %,d" +
                                 /* Print only if failed. */
                                 ((failedRequestCount == 0) ?
                                  ", " :
                                  String.format("(failed %,d), ",
                                                failedRequestCount)) +
                                 "avRespTime= %,d ms ",
                                 repNodeId.toString(),
                                 datacenterName,
                                 isActive,
                                 isMaster,
                                 maxActiveRequestCount,
                                 requestCount,
                                 getAvLatencyMs());
        }
    }
}
