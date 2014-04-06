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

package oracle.kv.impl.admin.param;

import static oracle.kv.impl.param.ParameterState.COMMON_DISABLED;
import static oracle.kv.impl.param.ParameterState.COMMON_MASTER_BALANCE;
import static oracle.kv.impl.param.ParameterState.COMMON_SN_ID;
import static oracle.kv.impl.param.ParameterState.COMMON_USE_CLIENT_SOCKET_FACTORIES;
import static oracle.kv.impl.param.ParameterState.JE_CACHE_SIZE;
import static oracle.kv.impl.param.ParameterState.JE_HELPER_HOSTS;
import static oracle.kv.impl.param.ParameterState.JE_HOST_PORT;
import static oracle.kv.impl.param.ParameterState.KV_CACHE_MODE;
import static oracle.kv.impl.param.ParameterState.REPNODE_TYPE;
import static oracle.kv.impl.param.ParameterState.REQUEST_QUIESCE_TIME;
import static oracle.kv.impl.param.ParameterState.RN_ADMIN_SO_BACKLOG;
import static oracle.kv.impl.param.ParameterState.RN_ADMIN_SO_CONNECT_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.RN_ADMIN_SO_READ_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.RN_CACHE_MB_MIN;
import static oracle.kv.impl.param.ParameterState.RN_HEAP_MB_MIN;
import static oracle.kv.impl.param.ParameterState.RN_LOGIN_SO_BACKLOG;
import static oracle.kv.impl.param.ParameterState.RN_LOGIN_SO_CONNECT_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.RN_LOGIN_SO_READ_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.RN_MAX_TOPO_CHANGES;
import static oracle.kv.impl.param.ParameterState.RN_MONITOR_SO_BACKLOG;
import static oracle.kv.impl.param.ParameterState.RN_MONITOR_SO_CONNECT_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.RN_MONITOR_SO_READ_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.RN_MOUNT_POINT;
import static oracle.kv.impl.param.ParameterState.RN_NODE_TYPE;
import static oracle.kv.impl.param.ParameterState.RN_NRCONFIG_RETAIN_LOG_FILES;
import static oracle.kv.impl.param.ParameterState.RN_RH_SO_BACKLOG;
import static oracle.kv.impl.param.ParameterState.RN_RH_SO_CONNECT_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.RN_RH_SO_READ_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.RP_RN_ID;
import static oracle.kv.impl.param.ParameterState.SP_COLLECT_ENV_STATS;
import static oracle.kv.impl.util.registry.RegistryUtils.InterfaceType.ADMIN;
import static oracle.kv.impl.util.registry.RegistryUtils.InterfaceType.LOGIN;
import static oracle.kv.impl.util.registry.RegistryUtils.InterfaceType.MAIN;
import static oracle.kv.impl.util.registry.RegistryUtils.InterfaceType.MONITOR;

import java.io.File;
import java.io.Serializable;
import java.util.EnumSet;
import java.util.StringTokenizer;

import oracle.kv.impl.admin.param.StorageNodeParams.RNHeapAndCacheSize;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RMISocketPolicy;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryArgs;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryPair;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.utilint.HostPortPair;
import com.sleepycat.persist.model.Persistent;

/**
 * A class implementing RepNodeParams contains all the per-RepNode operational
 * parameters.
 */
@Persistent
public class RepNodeParams implements Serializable {

    private static final long serialVersionUID = 1L;

    /*
     * Note that NoSQL DB is assuming that we are running on Hotspot Sun/Oracle
     * JVM, and that we are using JVM proprietary flags.
     */
    public static final String PARALLEL_GC_FLAG = "-XX:ParallelGCThreads=";
    public static final String XMS_FLAG = "-Xms";
    public static final String XMX_FLAG = "-Xmx";

    private ParameterMap map;

    /* For DPL */
    public RepNodeParams() {
    }

    public RepNodeParams(ParameterMap map) {
        this.map = map;
    }

    public RepNodeParams(RepNodeParams rnp) {
        this(rnp.getMap().copy());
    }

    /**
     * Create a default RepNodeParams for creating a new rep node.
     */
    public RepNodeParams(StorageNodeId snid,
                         RepNodeId repNodeId,
                         boolean disabled,
                         String haHostname,
                         int haPort,
                         String helperHostname,
                         int helperPort,
                         String mountPoint,
                         NodeType nodeType) {
        this(snid, repNodeId, disabled, haHostname, haPort,
             HostPortPair.getString(helperHostname, helperPort), mountPoint,
             nodeType);
    }

    public RepNodeParams(StorageNodeId snid,
                         RepNodeId repNodeId,
                         boolean disabled,
                         String haHostname,
                         int haPort,
                         String helperHosts,
                         String mountPoint,
                         NodeType nodeType) {
        init(new ParameterMap(), snid, repNodeId, disabled,
             haHostname, haPort, helperHosts, mountPoint, nodeType);
    }

    /**
     * Create RepNodeParams from existing map, which is probably a copy of the
     * policy map
     */
    public RepNodeParams(ParameterMap map,
                         StorageNodeId snid,
                         RepNodeId repNodeId,
                         boolean disabled,
                         String haHostname,
                         int haPort,
                         String helperHosts,
                         String mountPoint,
                         NodeType nodeType) {
        init(map, snid, repNodeId, disabled, haHostname, haPort, helperHosts,
             mountPoint, nodeType);
    }

    /**
     * The core of the constructors
     */
    private void init(ParameterMap newmap,
                      StorageNodeId snid,
                      RepNodeId repNodeId,
                      boolean disabled,
                      String haHostname,
                      int haPort,
                      String helperHosts,
                      String mountPoint,
                      NodeType nodeType) {

        this.map = newmap.filter(EnumSet.of(ParameterState.Info.REPNODE));
        setStorageNodeId(snid);
        setRepNodeId(repNodeId);
        setDisabled(disabled);
        setJENodeHostPort(HostPortPair.getString(haHostname, haPort));
        setJEHelperHosts(helperHosts);
        addDefaults();
        map.setName(repNodeId.getFullName());
        map.setType(REPNODE_TYPE);
        setMountPoint(mountPoint);
        setNodeType(nodeType);
    }

    public ParameterMap getMap() {
        return map;
    }

    private void addDefaults() {
        map.addServicePolicyDefaults(ParameterState.Info.REPNODE);
    }

    public RepNodeId getRepNodeId() {
        return RepNodeId.parse(map.get(RP_RN_ID).asString());
    }

    public void setRepNodeId(RepNodeId rnid) {
        map.setParameter(RP_RN_ID, rnid.getFullName());
    }

    public StorageNodeId getStorageNodeId() {
        return new StorageNodeId(map.get(COMMON_SN_ID).asInt());
    }

    public void setStorageNodeId(StorageNodeId snId) {
        map.setParameter(COMMON_SN_ID,
                         Integer.toString(snId.getStorageNodeId()));
    }

    public File getMountPoint() {
        if (map.exists(RN_MOUNT_POINT)) {
            return new File(map.get(RN_MOUNT_POINT).asString());
        }
        return null;
    }

    public String getMountPointString() {
        if (map.exists(RN_MOUNT_POINT)) {
            return map.get(RN_MOUNT_POINT).asString();
        }
        return null;
    }

    public void setMountPoint(String mountPoint) {
        /*
         * Set the mount point, even if the mount point value is null.
         * Setting it to a null value is how we clear it out if we are
         * moving it to the root dir of a new SN.
         */
        map.setParameter(RN_MOUNT_POINT, mountPoint);
    }

    public void setDisabled(boolean disabled) {
        map.setParameter(COMMON_DISABLED, Boolean.toString(disabled));
    }

    public boolean isDisabled() {
        return map.get(COMMON_DISABLED).asBoolean();
    }

    public long getJECacheSize() {
        return map.get(JE_CACHE_SIZE).asLong();
    }

    public void setJECacheSize(long size) {
        map.setParameter(JE_CACHE_SIZE, Long.toString(size));
    }

    /**
     * Set the RN heap and cache as a function of memory available on the
     * SN, and SN capacity. If either heap or cache is not specified, use
     * the JVM and JE defaults.
     */
    public void setRNHeapAndJECache(RNHeapAndCacheSize heapAndCache) {
        /*
         * If the heap val is null, remove any current -Xms and Xmx flags,
         * else replace them with the new value.
         */
        setJavaMiscParams(replaceOrRemoveJVMArg
                          (getJavaMiscParams(), XMS_FLAG,
                           heapAndCache.getHeapValAndUnit()));
        setJavaMiscParams(replaceOrRemoveJVMArg
                          (getJavaMiscParams(), XMX_FLAG,
                           heapAndCache.getHeapValAndUnit()));
        setJECacheSize(heapAndCache.getCacheBytes());
    }

    public CacheMode getJECacheMode() {
        return (CacheMode) map.get(KV_CACHE_MODE).asEnum();
    }

    public void setJECacheMode(CacheMode mode) {
        map.setParameter(KV_CACHE_MODE, mode.toString());
    }

    /*
     * Return the current value of the max heap size as it is set in the jvm
     * misc params.
     */
    public long getMaxHeapMB() {
        long hb = getMaxHeapBytes(false);
        if (hb == 0) {
            return 0;
        }

        /* Shouldn't be less than 0, JVM spec says -Xmx must be >= 2MB */
        long heapMB = hb >> 20;
        return (heapMB < 0) ? 0 : heapMB;
    }

    /**
     * Return the best guess, in bytes of the heap that this RN has access to.
     * If this method is run within the JVM, we can check the actual heap
     * value. If not, we must see if anything was specified with -Xmx
     */
    private long getMaxHeapBytes(boolean inTargetJVM) {

        if (inTargetJVM) {
            return Runtime.getRuntime().maxMemory();
        }

        String jvmArgs = map.getOrDefault(ParameterState.JVM_MISC).asString();
        return parseJVMArgsForHeap(XMX_FLAG, jvmArgs);
    }

    /*
     * Return the current value of the min heap size as it is set in the jvm
     * misc params, for validity checking.
     */
    public long getMinHeapMB() {
        String jvmArgs = map.getOrDefault(ParameterState.JVM_MISC).asString();
        long minHeap = parseJVMArgsForHeap(XMS_FLAG, jvmArgs);
        if (minHeap == 0) {
            return 0;
        }
        return minHeap >> 20;
    }

    /**
     * Make this a separate method for unit testing. Return the value,
     * in bytes of of any -Xmx or -Xms string
     */
    public static long parseJVMArgsForHeap(String prefix, String jvmArgs) {
        String heapVal = parseJVMArgsForPrefix(prefix, jvmArgs);
        if (heapVal == null) {
            return 0;
        }

        long size = findHeapNum(heapVal, "g");
        if (size != 0) {
            return size << 30;
        }

        size = findHeapNum(heapVal, "m");
        if (size != 0) {
            return size << 20;
        }

        size = findHeapNum(heapVal, "k");
        if (size != 0) {
            return size << 10;
        }

        return Long.parseLong(heapVal);
    }

    public static String parseJVMArgsForPrefix(String prefix, String jvmArgs) {
        if (jvmArgs == null) {
            return null;
        }

        String[] args = jvmArgs.split(prefix);
        if (args.length < 2) {
            return null;
        }

        /*
         * Get the last occurrence of the prefix flag, since it's the last one
         * that has precedence.
         */
        String lastArg = args[args.length-1];
        String[] lastVal = lastArg.split(" ");
        if (lastVal[0].isEmpty()) {
            return null;
        }
        return lastVal[0].toLowerCase();
    }

    /**
     * Parse targetJavaMisc and remove any argument that starts with prefix.
     * If newArg is not null, add in prefix+newArg.
     * For example, if the string is -Xmx10M -XX:ParallelGCThreads=10 and the
     * prefix is -Xmx and the newArg is 5G, return
     *          -XX:ParallelGCThreads=10 -Xmx5G
     * If newArg is null, return
     *          -XX:ParallelGCThreads=10
     */
    public String replaceOrRemoveJVMArg(String targetJavaMiscParams,
                                        String prefix,
                                        String newArg) {
        StringTokenizer tokenizer = new StringTokenizer(targetJavaMiscParams);
        StringBuilder result = new StringBuilder();
        while (tokenizer.hasMoreTokens()) {
            String arg = tokenizer.nextToken();
            if (!arg.startsWith(prefix)) {
                result.append(arg).append(" ");
            }
        }

        if (newArg == null) {
            return result.toString();
        }
        return result.toString() + " " + prefix + newArg;
    }

    private static long findHeapNum(String lastArg, String unit) {

        int unitIndex = lastArg.indexOf(unit);
        if (unitIndex == -1) {
            return 0;
        }

        return Long.parseLong(lastArg.substring(0, unitIndex));
    }

    public boolean getNRConfigRetainLogFiles() {
        return map.getOrDefault(RN_NRCONFIG_RETAIN_LOG_FILES).asBoolean();
    }

    public int getMaxTopoChanges() {
        return map.getOrDefault(RN_MAX_TOPO_CHANGES).asInt();
    }

    /**
     * Validate the JE cache and JVM heap parameters.
     *
     * @param inTargetJVM is true if the current JVM is the target for
     * validation.  This allows early detection of potential memory issues for
     * RepNode startup.

     * TODO: look at actual physical memory on the machine to validate.
     */
    public void validateCacheAndHeap(boolean inTargetJVM) {
        final double maxPercent = 0.90;

        long maxHeapMB = getMaxHeapMB();
        if (inTargetJVM) {
            long actualMemory = Runtime.getRuntime().maxMemory();
            long specHeap = maxHeapMB >> 20;
            if (specHeap > actualMemory) {
                throw new IllegalArgumentException
                ("Specified maximum heap of " + maxHeapMB  +
                 "MB exceeds available JVM memory of " + actualMemory +
                 ", see JVM parameters:" + getJavaMiscParams());
            }
        }

        long maxHeapBytes = getMaxHeapBytes(inTargetJVM);
        if (maxHeapBytes > 0 && getJECacheSize() > 0) {
            checkMinSizes(maxHeapBytes, (int)(getJECacheSize()/(1024*1024)));
            if ((getJECacheSize()/((double)maxHeapBytes)) > maxPercent) {
                String msg = "Parameter " + ParameterState.JE_CACHE_SIZE +
                    " (" + getJECacheSize() + ") may not exceed 90% of " +
                    "available Java heap space (" + maxHeapBytes + ")";
                throw new IllegalArgumentException(msg);
            }
        }

        long minHeapMB = getMinHeapMB();
        if (minHeapMB > maxHeapMB) {
            throw new IllegalArgumentException
            ("Mininum heap of " + minHeapMB + " exceeds maximum heap of " +
             maxHeapMB + ", see JVM parameters:" + getJavaMiscParams());
        }
    }

    private void checkMinSizes(long heapBytes, int cacheMB) {
        if ((heapBytes >> 20) < RN_HEAP_MB_MIN) {
            String msg = "JVM heap must be at least " + RN_HEAP_MB_MIN +
                " MB, specified value is " + (heapBytes>>20) + " MB";
            throw new IllegalArgumentException(msg);
        }
        if (cacheMB < RN_CACHE_MB_MIN) {
            String msg = "Cache size must be at least " + RN_CACHE_MB_MIN +
                " MB, specified value is " + cacheMB + " MB";
            throw new IllegalArgumentException(msg);
        }
    }

    public int getRequestQuiesceMs() {
        final long quiesceMs = ParameterUtils.
            getDurationMillis(map, REQUEST_QUIESCE_TIME);
        return (quiesceMs > Integer.MAX_VALUE) ?
                Integer.MAX_VALUE : (int)quiesceMs;
    }

    public boolean getCollectEnvStats() {
        return map.get(SP_COLLECT_ENV_STATS).asBoolean();
    }

    public int getHAPort() {
        return HostPortPair.getPort(getJENodeHostPort());
    }

    public String getJENodeHostPort() {
        return map.get(JE_HOST_PORT).asString();
    }

    public String getJEHelperHosts() {
        return map.get(JE_HELPER_HOSTS).asString();
    }

    public SocketFactoryPair getRHSFP(RMISocketPolicy rmiPolicy,
                                      String servicePortRange,
                                      String csfName,
                                      String kvStoreName) {
        SocketFactoryArgs args = new SocketFactoryArgs();

        args.setSsfName(MAIN.interfaceName()).
            setSsfBacklog(map.getOrDefault(RN_RH_SO_BACKLOG).asInt()).
            setSsfPortRange(servicePortRange).
            setCsfName(csfName).
            setUseCsf(getUseClientSocketFactory()).
            setCsfConnectTimeout((int)ParameterUtils.getDurationMillis(
                                     map, RN_RH_SO_CONNECT_TIMEOUT)).
            setCsfReadTimeout((int)ParameterUtils.getDurationMillis(
                                  map, RN_RH_SO_READ_TIMEOUT)).
            setKvStoreName(kvStoreName);

        return rmiPolicy.getBindPair(args);
    }

    public SocketFactoryPair getAdminSFP(RMISocketPolicy rmiPolicy,
                                         String servicePortRange,
                                         String csfName) {
        SocketFactoryArgs args = new SocketFactoryArgs();

        args.setSsfName(ADMIN.interfaceName()).
            setSsfBacklog(map.getOrDefault(RN_ADMIN_SO_BACKLOG).asInt()).
            setSsfPortRange(servicePortRange).

            setCsfName(csfName).
            setUseCsf(getUseClientSocketFactory()).
            setCsfConnectTimeout((int)ParameterUtils.getDurationMillis(
                                     map, RN_ADMIN_SO_CONNECT_TIMEOUT)).
            setCsfReadTimeout((int)ParameterUtils.getDurationMillis(
                                  map, RN_ADMIN_SO_READ_TIMEOUT));

        return rmiPolicy.getBindPair(args);
    }

    /**
     * Returns the SFP used by the RepNode for its ULS
     */
    public SocketFactoryPair getLoginSFP(RMISocketPolicy policy,
                                         String servicePortRange,
                                         String csfName) {
        SocketFactoryArgs args = new SocketFactoryArgs();

        args.setSsfName(LOGIN.interfaceName()).
            setSsfBacklog(map.getOrDefault(RN_LOGIN_SO_BACKLOG).asInt()).
            setSsfPortRange(servicePortRange).
            setCsfName(csfName).
            setUseCsf(getUseClientSocketFactory()).
            setCsfConnectTimeout((int)ParameterUtils.getDurationMillis(
                                     map, RN_LOGIN_SO_CONNECT_TIMEOUT)).
            setCsfReadTimeout((int)ParameterUtils.getDurationMillis(
                                  map, RN_LOGIN_SO_READ_TIMEOUT));

        return policy.getBindPair(args);
    }


    public SocketFactoryPair getMonitorSFP(RMISocketPolicy rmiPolicy,
                                           String servicePortRange,
                                           String csfName) {
        SocketFactoryArgs args = new SocketFactoryArgs();

        args.setSsfName(MONITOR.interfaceName()).
            setSsfBacklog(map.getOrDefault(RN_MONITOR_SO_BACKLOG).asInt()).
            setSsfPortRange(servicePortRange).
            setCsfName(csfName).
            setUseCsf(getUseClientSocketFactory()).
            setCsfConnectTimeout((int)ParameterUtils.getDurationMillis(
                                     map, RN_MONITOR_SO_CONNECT_TIMEOUT)).
            setCsfReadTimeout((int)ParameterUtils.getDurationMillis(
                                  map, RN_MONITOR_SO_READ_TIMEOUT));

        return rmiPolicy.getBindPair(args);
    }

    public boolean getUseClientSocketFactory() {
        return (!ClientSocketFactory.isDisabled() &&
                map.exists(COMMON_USE_CLIENT_SOCKET_FACTORIES)) ?
            map.get(COMMON_USE_CLIENT_SOCKET_FACTORIES).asBoolean() :
            false;
    }

    /**
     * Set the JE HA nodeHostPort and helperHost fields.
     */
    public void setJENodeHostPort(String nodeHostPort) {
        map.setParameter(ParameterState.JE_HOST_PORT, nodeHostPort);
    }

    public void setJEHelperHosts(String helperHost) {
        map.setParameter(ParameterState.JE_HELPER_HOSTS, helperHost);
    }

    /* -- Partition migration parameters -- */

    /**
     * Gets the maximum number of concurrent partition migration sources.
     *
     * @return the maximum number of concurrent sources
     */
    public int getConcurrentSourceLimit() {
        return map.getOrDefault(
                ParameterState.RN_PM_CONCURRENT_SOURCE_LIMIT).asInt();
    }

    /**
     * Gets the maximum number of concurrent partition migration targets.
     *
     * @return the maximum number of concurrent migration targets
     */
    public int getConcurrentTargetLimit() {
        return map.getOrDefault(
                ParameterState.RN_PM_CONCURRENT_TARGET_LIMIT).asInt();
    }

    /**
     * Gets the wait time (in milliseconds) before trying a partition migration
     * service request after a busy response.
     *
     * @return the wait time after a busy service response
     */
    public long getWaitAfterBusy() {
        return ParameterUtils.getDurationMillis(map,
                                         ParameterState.RN_PM_WAIT_AFTER_BUSY);
    }

    /**
     * Gets the wait time (in milliseconds) before trying a partition migration
     * service request after an error response.
     *
     * @return the wait time after an error service response
     */
    public long getWaitAfterError() {
        return ParameterUtils.getDurationMillis(map,
                                         ParameterState.RN_PM_WAIT_AFTER_ERROR);
    }

    /**
     * Gets the socket read or write timeout (in milliseconds) for the
     * partition migration stream.
     *
     * @return the socket read or write timeout
     */
    public int getReadWriteTimeout() {
        return (int)ParameterUtils.getDurationMillis(map,
                                 ParameterState.RN_PM_SO_READ_WRITE_TIMEOUT);
    }

    /**
     * Gets the socket connect timeout (in milliseconds) for the partition
     * migration stream.
     *
     * @return the socket connect timeout
     */
    public int getConnectTImeout() {
        return (int)ParameterUtils.getDurationMillis(map,
                                 ParameterState.RN_PM_SO_CONNECT_TIMEOUT);
    }

    public int getActiveThreshold() {
        return map.get(ParameterState.SP_ACTIVE_THRESHOLD).asInt();
    }

    public String getLoggingConfigProps() {
        return map.get(ParameterState.JVM_LOGGING).asString();
    }

    public String getJavaMiscParams() {
        return map.getOrDefault(ParameterState.JVM_MISC).asString();
    }

    public void setJavaMiscParams(String misc) {
        map.setParameter(ParameterState.JVM_MISC, misc);
    }

    public int getMaxTrackedLatency() {
        return (int) ParameterUtils.getDurationMillis
            (map, ParameterState.SP_MAX_LATENCY);
    }

    public int getStatsInterval() {
        return (int) ParameterUtils.getDurationMillis
            (map, ParameterState.SP_INTERVAL);
    }

    public String getConfigProperties() {
        return map.get(ParameterState.JE_MISC).asString();
    }

    public boolean getMasterBalance() {
        return map.getOrDefault(COMMON_MASTER_BALANCE).asBoolean();
    }

    public void setMasterBalance(boolean masterBalance) {
        map.setParameter(COMMON_MASTER_BALANCE,
                         (masterBalance ? "true" : "false"));
    }

    public void setLatencyCeiling(int ceiling) {
        map.setParameter(ParameterState.SP_LATENCY_CEILING,
                         Integer.toString(ceiling));
    }

    public int getLatencyCeiling() {
        return map.getOrZeroInt(ParameterState.SP_LATENCY_CEILING);
    }

    public void setThroughputFloor(int floor) {
        map.setParameter(ParameterState.SP_THROUGHPUT_FLOOR,
                         Integer.toString(floor));
    }

    public int getThroughputFloor() {
        return map.getOrZeroInt(ParameterState.SP_THROUGHPUT_FLOOR);
    }

    public void setCommitLagThreshold(long threshold) {
        map.setParameter(ParameterState.SP_COMMIT_LAG_THRESHOLD,
                         Long.toString(threshold));
    }

    public long getCommitLagThreshold() {
        return map.getOrZeroLong(ParameterState.SP_COMMIT_LAG_THRESHOLD);
    }

    /**
     * This percent of the RN heap will be set at the JE cache.
     */
    public int getRNCachePercent() {
        return map.getOrZeroInt(ParameterState.RN_CACHE_PERCENT);
    }

    public int getParallelGCThreads() {
        String jvmArgs = getJavaMiscParams();
        String val = parseJVMArgsForPrefix(PARALLEL_GC_FLAG, jvmArgs);
        if (val == null) {
            return 0;
        }

        return Integer.parseInt(val);
    }

    /**
     * Set the -XX:ParallelGCThreads flag. If gcThreads is null, clear the
     * setting from the jvm params.
     */
    public void setParallelGCThreads(int gcThreads) {
        String newVal = (gcThreads == 0) ? null : Integer.toString(gcThreads);

        setJavaMiscParams(replaceOrRemoveJVMArg(getJavaMiscParams(),
                                                PARALLEL_GC_FLAG, newVal));
    }

    /**
     * Get the node's JE HA node type.
     *
     * @return the node type
     */
    public NodeType getNodeType() {
        return NodeType.valueOf(map.getOrDefault(RN_NODE_TYPE).asString());
    }

    /**
     * Set the node's JE HA node type.
     *
     * @param nodeType the node type
     */
    public void setNodeType(final NodeType nodeType) {
        map.setParameter(RN_NODE_TYPE, nodeType.name());
    }

    /*
     * The following accessors are for session and token cache configuration
     */

    public void setSessionLimit(String value) {
        map.setParameter(ParameterState.COMMON_SESSION_LIMIT, value);
    }

    public int getSessionLimit() {
        return map.getOrDefault(ParameterState.COMMON_SESSION_LIMIT).asInt();
    }

    public void setLoginCacheSize(String value) {
        map.setParameter(ParameterState.COMMON_LOGIN_CACHE_SIZE, value);
    }

    public int getLoginCacheSize() {
        return map.getOrDefault(ParameterState.COMMON_LOGIN_CACHE_SIZE).asInt();
    }

    @Override
    public String toString() {
        return map.toString();
    }
}
