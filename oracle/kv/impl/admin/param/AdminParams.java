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

import java.io.Serializable;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import oracle.kv.impl.param.DurationParameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.StorageNodeId;

import com.sleepycat.je.rep.utilint.HostPortPair;
import com.sleepycat.persist.model.Persistent;

/**
 * Provides the configuration parameters for an instance of the Admin.  This
 * will include configuration parameters of the replicated environment upon
 * which it is built.
 */
@Persistent
public class AdminParams implements Serializable {

    private static final long serialVersionUID = 1;
    private ParameterMap map;

    /* For DPL */
    public AdminParams() {
    }

    public AdminParams(ParameterMap map) {
        this.map = map;
    }

    /**
     * For bootstapping, before any policy Admin params are available. All
     * local fields are set to default values.
     */
    public AdminParams(AdminId adminId,
                       StorageNodeId storageNodeId,
                       int httpPort) {
        this(new ParameterMap(), adminId, storageNodeId, httpPort);
    }

    /**
     * This is used by the planner.
     */
    public AdminParams(ParameterMap newmap,
                       AdminId adminId,
                       StorageNodeId snid,
                       int httpPort) {
        this.map = newmap.filter(EnumSet.of(ParameterState.Info.ADMIN));
        setAdminId(adminId);
        setStorageNodeId(snid);
        setHttpPort(httpPort);
        addDefaults();
        map.setName(adminId.getFullName());
        map.setType(ParameterState.ADMIN_TYPE);
    }

    private void addDefaults() {
        map.addServicePolicyDefaults(ParameterState.Info.ADMIN);
        setDisabled(false);
    }

    public ParameterMap getMap() {
        return map;
    }

    public void setAdminId(AdminId aid) {
        map.setParameter(ParameterState.AP_ID,
                         Integer.toString(aid.getAdminInstanceId()));
    }

    public AdminId getAdminId() {
        return new AdminId(map.getOrZeroInt(ParameterState.AP_ID));
    }

    public StorageNodeId getStorageNodeId() {
        return new StorageNodeId
            (map.getOrZeroInt(ParameterState.COMMON_SN_ID));
    }

    public void setStorageNodeId(StorageNodeId snId) {
        map.setParameter(ParameterState.COMMON_SN_ID,
                         Integer.toString(snId.getStorageNodeId()));
    }

    public void setHttpPort(int httpPort) {
        map.setParameter(ParameterState.COMMON_ADMIN_PORT,
                         Integer.toString(httpPort));
    }

    public int getHttpPort() {
        return map.getOrZeroInt(ParameterState.COMMON_ADMIN_PORT);
    }

    public String getNodeHostPort() {
        return map.get(ParameterState.JE_HOST_PORT).asString();
    }

    public int getWaitTimeout() {
        DurationParameter dp =
            (DurationParameter) map.get(ParameterState.AP_WAIT_TIMEOUT);
        return (int) dp.getAmount();
    }

    public TimeUnit getWaitTimeoutUnit() {
        DurationParameter dp =
            (DurationParameter) map.get(ParameterState.AP_WAIT_TIMEOUT);
        return dp.getUnit();
    }

    public void setDisabled(boolean disabled) {
        map.setParameter(ParameterState.COMMON_DISABLED,
                         Boolean.toString(disabled));
    }

    public boolean isDisabled() {
        return map.get(ParameterState.COMMON_DISABLED).asBoolean();
    }

    public boolean createCSV() {
        return  map.get(ParameterState.MP_CREATE_CSV).asBoolean();
    }

    public int getLogFileCount() {
        return map.getOrZeroInt(ParameterState.AP_LOG_FILE_COUNT);
    }

    public int getLogFileLimit() {
        return map.getOrZeroInt(ParameterState.AP_LOG_FILE_LIMIT);
    }

    public long getPollPeriodMillis() {
        DurationParameter dp =
            (DurationParameter) map.get(ParameterState.MP_POLL_PERIOD);
        return dp.toMillis();
    }

    public String getHelperHosts() {
        return map.get(ParameterState.JE_HELPER_HOSTS).asString();
    }

    public long getEventExpiryAge() {
        DurationParameter dp =
            (DurationParameter) map.get(ParameterState.AP_EVENT_EXPIRY_AGE);
        return dp.toMillis();
    }

    public void setEventExpiryAge(String age) {
        map.setParameter(ParameterState.AP_EVENT_EXPIRY_AGE, age);
    }

    /**
     * Gets the broadcast topology retry delay. This delay is the time between
     * attempts to update RNs when trying to meet the threshold.
     *
     * @return the broadcast topology retry delay
     */
    public long getBroadcastTopoRetryDelayMillis() {
        return ParameterUtils.getDurationMillis(map,
                                ParameterState.AP_BROADCAST_TOPO_RETRY_DELAY);
    }

    /**
     * Gets the broadcast topology threshold. The threshold is the percent
     * of RNs that must be successfully updated during a broadcast.
     *
     * @return the broadcast topology threshold
     */
    public int getBroadcastTopoThreshold() {
        return
           map.getOrDefault(ParameterState.AP_BROADCAST_TOPO_THRESHOLD).asInt();
    }

    /**
     * Gets the maximum number of topology changes that are retained in the
     * stored topology. Only the latest maxTopoChanges are retained in the
     * stored topology.
     */
    public int getMaxTopoChanges() {
        return
           map.getOrDefault(ParameterState.AP_MAX_TOPO_CHANGES).asInt();
    }

    /**
     * Set the JE HA nodeHostPort to nodeHostname:haPort and the
     * helper host to helperHostname:helperPort.
     */
    public void setJEInfo(String nodeHostname, int haPort,
                          String helperHostname, int helperPort) {

        setJEInfo(HostPortPair.getString(nodeHostname, haPort),
                  HostPortPair.getString(helperHostname, helperPort));
    }

    /**
     * Set the JE HA nodeHostPort and helperHost fields.
     */
    public void setJEInfo(String nodeHostPort, String helperHost) {
        map.setParameter(ParameterState.JE_HOST_PORT, nodeHostPort);
        map.setParameter(ParameterState.JE_HELPER_HOSTS, helperHost);
    }

    /**
     * The total time, in milliseconds, that we should wait for an RN to
     * become consistent with a given lag target.
     */
    public int getAwaitRNConsistencyPeriod() {
        return (int) ParameterUtils.getDurationMillis
            (map, ParameterState.AP_WAIT_RN_CONSISTENCY);
    }

    /**
     * The total time, in milliseconds, that we should wait to contact a
     * service that is currently unreachable.
     */
    public DurationParameter getServiceUnreachablePeriod() {
        return (DurationParameter)
            map.getOrDefault(ParameterState.AP_WAIT_UNREACHABLE_SERVICE);
    }

    /**
     * The time that we should wait to contact an admin that failed over.
     */
    public DurationParameter getAdminFailoverPeriod() {
        return (DurationParameter)
            map.getOrDefault(ParameterState.AP_WAIT_ADMIN_FAILOVER);
    }

    /**
     * The time that we should wait to contact an RN that failed over.
     */
    public DurationParameter getRNFailoverPeriod() {
        return (DurationParameter)
            map.getOrDefault(ParameterState.AP_WAIT_RN_FAILOVER);
    }

    /**
     * The time that we should wait to check on the status of a partition
     * migration.
     */
    public DurationParameter getCheckPartitionMigrationPeriod() {
        return (DurationParameter)
            map.getOrDefault(ParameterState.AP_CHECK_PARTITION_MIGRATION);
    }

    /**
     * The time that we should wait to check on the status of an add index
     * operation.
     */
    public DurationParameter getCheckAddIndexPeriod() {
        return (DurationParameter)
            map.getOrDefault(ParameterState.AP_CHECK_ADD_INDEX);
    }
    
    public void setHelperHost(String helpers) {
        map.setParameter(ParameterState.JE_HELPER_HOSTS, helpers);
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
}
