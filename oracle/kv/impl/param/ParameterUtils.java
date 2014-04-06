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

package oracle.kv.impl.param;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import oracle.kv.RequestLimitConfig;
import oracle.kv.impl.util.ConfigUtils;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.ReplicationMutableConfig;
import com.sleepycat.je.rep.impl.RepParams;

/**
 * See header comments in Parameter.java for an overview of Parameters.
 *
 * This is a utility class with some useful methods for handling Parameters.
 */
public class ParameterUtils {

    public final static String HELPER_HOST_SEPARATOR = ",";

    private final ParameterMap map;

    public ParameterUtils(ParameterMap map) {
        this.map = map;
    }

    /**
     * Default values for JE EnvironmentConfig, ReplicationConfig
     */
    private static final String DEFAULT_CONFIG_PROPERTIES=
        EnvironmentConfig.TXN_DURABILITY + "=" +
        "write_no_sync,write_no_sync,simple_majority;" +
        EnvironmentConfig.NODE_MAX_ENTRIES + "=128;" +
        EnvironmentConfig.CLEANER_THREADS + "=2;" +
        EnvironmentConfig.LOG_FILE_CACHE_SIZE + "=2000;" +
        EnvironmentConfig.CLEANER_READ_SIZE + "=1048576;" +
        EnvironmentConfig.CHECKPOINTER_BYTES_INTERVAL + "=200000000;" +
        EnvironmentConfig.ENV_RUN_EVICTOR + "=true;" +
        EnvironmentConfig.LOG_WRITE_QUEUE_SIZE + "=2097152;" +
        EnvironmentConfig.LOG_NUM_BUFFERS + "=16;" +
        EnvironmentConfig.EVICTOR_MAX_THREADS + "=2;" +
        EnvironmentConfig.LOG_FILE_MAX + "=1073741824;" +
        EnvironmentConfig.CLEANER_MIN_UTILIZATION + "=40;" +
        EnvironmentConfig.LOG_FAULT_READ_SIZE + "=4096;" +
        EnvironmentConfig.LOG_ITERATOR_READ_SIZE + "=1048576;" +
        EnvironmentConfig.LOCK_N_LOCK_TABLES + "=97;" +
        EnvironmentConfig.LOCK_TIMEOUT + "=10 s;" +

        /* Replication.  Not all of these are documented publicly */
        ReplicationConfig.ENV_UNKNOWN_STATE_TIMEOUT + "=10 s;" +
        ReplicationConfig.TXN_ROLLBACK_LIMIT + "=10;" +
        ReplicationConfig.REPLICA_ACK_TIMEOUT + "=5 s;" +
        ReplicationConfig.CONSISTENCY_POLICY + "=NoConsistencyRequiredPolicy;" +
        ReplicationMutableConfig.REPLAY_MAX_OPEN_DB_HANDLES + "=100;" +

        /*
         * Use timeouts shorter than the default 30sec to speed up failover
         * in network hardware level failure situations.
         */
        ReplicationConfig.FEEDER_TIMEOUT + "=10 s;" +
        ReplicationConfig.REPLICA_TIMEOUT + "=10 s;" +

        EnvironmentParams.REP_PARAM_PREFIX +
        "preHeartbeatTimeoutMs=5000000000;" +
        EnvironmentParams.REP_PARAM_PREFIX +
        "vlsn.distance=1000000;" +
        EnvironmentParams.REP_PARAM_PREFIX +
        "vlsn.logCacheSize=128;" +
        EnvironmentConfig.EVICTOR_CRITICAL_PERCENTAGE + "=20";

    /**
     * Return an EnvironmentConfig set with the relevant parameters in this
     * object.
     */
    public EnvironmentConfig getEnvConfig() {
        EnvironmentConfig ec;
        ec = new EnvironmentConfig(createProperties(true));
        ec.setAllowCreate(true);
        ec.setTransactional(true);
        if (map.exists(ParameterState.JE_CACHE_SIZE)) {
            ec.setCacheSize(map.get(ParameterState.JE_CACHE_SIZE).asLong());
        }
        return ec;
    }

    /**
     * Return a ReplicationConfig set with the relevant parameters in this
     * object.
     */
    public ReplicationConfig getRepEnvConfig() {
        return getRepEnvConfig(null, false);
    }

    public ReplicationConfig getRepEnvConfig(Properties securityProps) {
        return getRepEnvConfig(securityProps, false);
    }

    public ReplicationConfig getRepEnvConfig(Properties securityProps,
                                             boolean validating) {
        ReplicationConfig rc;
        Properties allProps = createProperties(false);
        mergeProps(allProps, securityProps);
        rc = new ReplicationConfig(allProps);
        rc.setConfigParam("je.rep.preserveRecordVersion", "true");

        if (map.exists(ParameterState.JE_HELPER_HOSTS)) {
            rc.setHelperHosts(map.get(ParameterState.JE_HELPER_HOSTS).asString());
        }
        if (map.exists(ParameterState.JE_HOST_PORT)) {

            /*
             * If in validating mode, setting the nodeHostPort param will not
             * work correctly because in that case we are running on the admin
             * host and not the target host.  If validating, treat the
             * host:port as a single-value helper hosts string and validate
             * directly.
             */
            String nhp = map.get(ParameterState.JE_HOST_PORT).asString();
            if (validating) {
                RepParams.HELPER_HOSTS.validateValue(nhp);
            } else {
                rc.setNodeHostPort(nhp);
            }
        }
        return rc;
    }

    private void removeRep(Properties props) {
        Iterator<?> it = props.keySet().iterator();
        while (it.hasNext()) {
            String key = (String) it.next();
            if (key.indexOf(EnvironmentParams.REP_PARAM_PREFIX) != -1) {
                it.remove();
            }
        }
    }

    private void mergeProps(Properties baseProps,
                            Properties mergeProps) {
        if (mergeProps == null) {
            return;
        }
        for (Object propKey : mergeProps.keySet()) {
            String propSKey = (String) propKey;
            String propVal = mergeProps.getProperty(propSKey);
            baseProps.setProperty(propSKey, propVal);
        }
    }

    /**
     * Create a Properties object from the DEFAULT_CONFIG_PROPERTIES String
     * and if present, the configProperties String.  Priority is given to the
     * configProperties (last property set wins).
     */
    public Properties createProperties(boolean removeReplication) {
        Properties props = new Properties();
        String propertyString = DEFAULT_CONFIG_PROPERTIES;
        String configProperties = map.get(ParameterState.JE_MISC).asString();
        if (configProperties != null) {
            propertyString = propertyString + ";" + configProperties;
        }
        try {
            props.load(ConfigUtils.getPropertiesStream(propertyString));
            if (removeReplication) {
                removeRep(props);
            }
        } catch (Exception e) {
            /* TODO: do something about this? */
        }
        return props;
    }

    public static CacheMode getCacheMode(ParameterMap map) {
        CacheModeParameter cmp =
            (CacheModeParameter) map.get(ParameterState.KV_CACHE_MODE);
        if (cmp != null) {
            return cmp.asCacheMode();
        }
        return null;
    }

    public static long getRequestQuiesceTime(ParameterMap map)  {
        return getDurationMillis(map, ParameterState.REQUEST_QUIESCE_TIME);
    }

    public static RequestLimitConfig getRequestLimitConfig(ParameterMap map) {
        final int maxActiveRequests =
            map.get(ParameterState.RN_MAX_ACTIVE_REQUESTS).asInt();
        final int requestThresholdPercent =
            map.get(ParameterState.RN_REQUEST_THRESHOLD_PERCENT).asInt();
        final int nodeLimitPercent =
            map.get(ParameterState.RN_NODE_LIMIT_PERCENT).asInt();

        return new RequestLimitConfig(maxActiveRequests,
                                      requestThresholdPercent,
                                      nodeLimitPercent);
    }

    public static long getThreadDumpIntervalMillis(ParameterMap map)  {
        return getDurationMillis(map, ParameterState.SP_DUMP_INTERVAL);
    }

    public static int getMaxTrackedLatencyMillis(ParameterMap map)  {
        return (int) getDurationMillis(map, ParameterState.SP_MAX_LATENCY);
    }

    public static long getDurationMillis(ParameterMap map, String parameter) {
        DurationParameter dp = (DurationParameter) map.getOrDefault(parameter);
        return dp.toMillis();
    }

    /* Parse the helper host string and return a list */
    public static List<String> helpersAsList(String helperHosts) {
        List<String> helpers = new ArrayList<String>();
        if ((helperHosts == null) || (helperHosts.length() == 0)) {
            return helpers;
        }
    
        String[] split = helperHosts.split(HELPER_HOST_SEPARATOR);
        for (int i = 0; i < split.length; i++) {
            helpers.add(split[i].trim());
        }

        return helpers;
    }
}
