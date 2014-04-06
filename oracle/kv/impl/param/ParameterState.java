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

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import oracle.kv.impl.util.PortRange;

/**
 * IMPORTANT: do NOT modify this file without copying the modifications to
 * webapp/oracle/kv/impl/admin/webapp/shared/ParameterState.java.  See below.
 */

/**
 * See header comments in Parameter.java for an overview of Parameters.
 *
 * This class acts as a type catalog for Parameter state. It defines all of the
 * allowed parameters along with their types, scope and other meta-data,
 * including min/max values and legal string values. All new parameters must be
 * added here. To add a new parameter do 2 things:
 *
 * 1. Add appropriate constants to this file.
 *
 * 2. Add the parameter to the statically initialized map, pstate.
 *
 * Adding a parameter should be a cut/paste exercise using another as an
 * example. Be sure to add a default String value as well as ensuring that any
 * parameter that should be defaulted is specified as a POLICY parameter.
 *
 * In order to be properly validated the static createParameter() should be
 * used. If a parameter is either not defined or has an invalid value that
 * method will throw IllegalStateException.
 *
 * Any changes to parameters that require a "restart" merit special attention:
 * Please be sure to bring such changes to doc group's attention, so that they
 * can make concomitant changes to the Admin documentation. Note that
 * parameters default to requiring restarts, so it takes extra vigilance to
 * when introducing new parameters.
 *
 * IMPORTANT: this file is virtually identical to the file in
 * webapp/oracle/kv/impl/admin/webapp. They are separate because the webapp
 * version cannot import from the kv tree so it has a different package name.
 * If this file is modified, copy all of it to the other tree and just make
 * sure the package names are correct.
 *
 * TODO: Look at automating generation of the webapp copy to avoid mistakes.
 */
public class ParameterState {

    /**
     * Both type and defaultParam exist, although somewhat redundant, so that
     * defaultParam can be null in the case where ParameterState is compiled
     * for the webapp UI.  This is also why the initialization of defaultParam
     * is indirect.
     */
    private final Type type;
    private final Object defaultParam;
    private final Scope scope;
    private final EnumSet<Info> info;
    private final long min;
    private final long max;
    private final String[] validValues;

    /**
     * Parameters to skip in comparisons.
     */
    public static final HashSet<String> skipParams =
        new HashSet<String>() {
        private static final long serialVersionUID = 1L;
        {
            add(COMMON_DISABLED);
        }};

    /**
     * These are the parameters from StorageNodeParams that are used by other
     * services (Admin and RepNode) for their own configuration.  They tend to
     * be used only on startup.  They are listed so that utilities such as
     * VerifyConfiguration can filter on them for validation.
     */
    public static final HashSet<String> serviceParams =
        new HashSet<String>() {
        private static final long serialVersionUID = 1L;
        {
            add(SN_ROOT_DIR_PATH);
            add(COMMON_SN_ID);
            add(COMMON_HOSTNAME);
            add(COMMON_REGISTRY_PORT);
            add(SN_LOG_FILE_COUNT);
            add(SN_LOG_FILE_LIMIT);
        }};

    /**
     * Types of Parameter instances.
     */
    public enum Type {NONE, INT, LONG, BOOLEAN, STRING, CACHEMODE, DURATION};

    /**
     * Scope of a parameter.  This can be used for validation of parameter
     * changes.
     *
     * SERVICE -- the parameter applies per service instance.  E.g. the
     * registry port.
     * REPGROUP -- the parameter can apply across a single RepGroup. E.g Helper
     * hosts.
     * STORE -- the parameter can apply across all instances of the service
     * throughout the store.  E.g. JE cache size for RepNodes.
     */
    public enum Scope {SERVICE, REPGROUP, STORE}

    /**
     * Meta-info about a parameter.  The absence of RONLY implies read-write,
     * absence of NORESTART implies restart required.  HIDDEN is special in
     * that it is internal-only and only displayed conditionally in the UI.
     * HIDDEN parameters, if not read-only, are only visible and settable
     * via the CLI until/unless an "internal" mode is added to the GUI.
     */
    public enum Info {
            ADMIN,      /* Applies to Admin */
            REPNODE,    /* Applies to RepNode */
            SNA,        /* Applies to SNA */
            BOOT,       /* Is a bootstrap parameter */
            GLOBAL,     /* Applies to GlobalParams */
            RONLY,      /* Not directly modifiable by a user */
            NORESTART,  /* Does not require service restart */
            POLICY,     /* Is settable as a policy parameter */
            HIDDEN,     /* Not displayed in UI, settable only by CLI */
            SECURITY,   /* Applies to security */
            TRANSPORT   /* Applies to transports */
    }

    /**
     * Possible values for Boolean type.
     */
    private static String booleanVals[] = new String [] {"false", "true"};

    /**
     * This is the "schema" for the parameters, potentially modified between
     * releases to indicate the need to handle changes.
     */
    public static final int PARAMETER_VERSION = 1;
    public static final int BOOTSTRAP_PARAMETER_VERSION = 2;
    public static final int BOOTSTRAP_PARAMETER_R1_VERSION = 1;

    /**
     * Top-level parameter groupings.  These go into configuration files.
     * String is used vs enum because of serialization.  Changing these
     * changes the persistent representation and should not be done lightly.
     */
    public static final String REPNODE_TYPE = "repNodeParams";
    public static final String ADMIN_TYPE = "adminParams";
    public static final String GLOBAL_TYPE = "globalParams";
    public static final String SNA_TYPE = "storageNodeParams";
    public static final String BOOTSTRAP_TYPE = "bootstrapParams";
    public static final String BOOTSTRAP_PARAMS = "params";
    public static final String BOOTSTRAP_MOUNT_POINTS = "mountPoints";

    /**
     * Top-level security parameter groupings.
     */
    public static final String SECURITY_TYPE = "securityParams";
    public static final String SECURITY_PARAMS = "params";
    public static final String SECURITY_TRANSPORT_TYPE = "transportParams";
    public static final String SECURITY_TRANSPORT_INTERNAL = "internal";
    public static final String SECURITY_TRANSPORT_JE_HA = "ha";
    public static final String SECURITY_TRANSPORT_CLIENT = "client";

    /**
     * The parameters themselves, along with default values
     */

    /**
     * Security params
     */
    public static final String SEC_SECURITY_ENABLED = "securityEnabled";
    public static final String SEC_KEYSTORE_FILE = "keystore";
    public static final String SEC_KEYSTORE_TYPE = "keystoreType";
    public static final String SEC_KEYSTORE_PWD_ALIAS = "keystorePasswordAlias";
    public static final String SEC_TRUSTSTORE_FILE = "truststore";
    public static final String SEC_TRUSTSTORE_TYPE = "truststoreType";
    public static final String SEC_PASSWORD_FILE = "passwordFile";
    public static final String SEC_PASSWORD_CLASS = "passwordClass";
    public static final String SEC_WALLET_DIR = "walletDir";
    public static final String SEC_INTERNAL_AUTH = "internalAuth";
    public static final String SEC_CERT_MODE = "certMode";

    /**
     * Security Transport params
     */
    public static final String SEC_TRANS_TYPE = "transportType";
    public static final String SEC_TRANS_FACTORY = "transportFactory";
    public static final String SEC_TRANS_SERVER_KEY_ALIAS = "serverKeyAlias";
    public static final String SEC_TRANS_CLIENT_KEY_ALIAS = "clientKeyAlias";
    public static final String SEC_TRANS_CLIENT_AUTH_REQUIRED =
        "clientAuthRequired";
    public static final String SEC_TRANS_CLIENT_IDENT_ALLOW =
        "clientIdentityAllowed";
    public static final String SEC_TRANS_SERVER_IDENT_ALLOW =
        "serverIdentityAllowed";
    public static final String SEC_TRANS_ALLOW_CIPHER_SUITES =
        "allowCipherSuites";
    public static final String SEC_TRANS_CLIENT_ALLOW_CIPHER_SUITES =
        "clientAllowCipherSuites";
    public static final String SEC_TRANS_ALLOW_PROTOCOLS =
        "allowProtocols";
    public static final String SEC_TRANS_CLIENT_ALLOW_PROTOCOLS =
        "clientAllowProtocols";

    /**
     * Common to RepNode and Admin
     */
    public static final String COMMON_SN_ID = "storageNodeId";
    public static final String COMMON_SN_ID_DEFAULT = "-1";
    public static final String COMMON_DISABLED = "disabled";
    public static final String COMMON_DISABLED_DEFAULT = "false";

    public static final String COMMON_HOSTNAME = "hostname";
    public static final String COMMON_HOSTNAME_DEFAULT = "localhost";
    public static final String COMMON_HA_HOSTNAME = "haHostname";
    public static final String COMMON_HA_HOSTNAME_DEFAULT = "localhost";
    public static final String COMMON_REGISTRY_PORT = "registryPort";
    public static final String COMMON_REGISTRY_PORT_DEFAULT = "-1";
    public static final String COMMON_ADMIN_PORT = "adminHttpPort";
    public static final String COMMON_ADMIN_PORT_DEFAULT = "-1";

    /**
     * Switch to control master balancing.
     */
    public static final String COMMON_MASTER_BALANCE = "masterBalance";
    public static final String COMMON_MASTER_BALANCE_DEFAULT = "true";

    /**
     * Determine whether keys and values are displayed in exceptions and
     * error messages on the server side.
     */
    public static final String COMMON_HIDE_USERDATA = "hideUserData";
    public static final String COMMON_HIDE_USERDATA_DEFAULT = "true";

    /**
     * The range of ports used by HA for replication.
     */
    public static final String COMMON_PORTRANGE = "haPortRange";
    public static final String COMMON_PORTRANGE_DEFAULT = "1,2";
    /**
     * The range of ports used by RMI services, that is, objects that are
     * are exported are bound to ports in this range.
     */
    public static final String COMMON_SERVICE_PORTRANGE = "servicePortRange";
    /**
     * The default is to use any free port.
     */
    public static final String COMMON_SERVICE_PORTRANGE_DEFAULT =
        PortRange.UNCONSTRAINED;
    public static final String COMMON_STORENAME = "storeName";
    public static final String COMMON_STORENAME_DEFAULT = "nostore";
    public static final String COMMON_CAPACITY = "capacity";
    public static final String COMMON_CAPACITY_DEFAULT = "1";
    public static final String COMMON_NUMCPUS = "numCPUs";
    public static final String COMMON_NUMCPUS_DEFAULT = "0";
    public static final String COMMON_MEMORY_MB = "memoryMB";
    public static final String COMMON_MEMORY_MB_DEFAULT = "0";
    public static final String COMMON_MGMT_CLASS = "mgmtClass";
    public static final String COMMON_MGMT_CLASS_DEFAULT =
        "oracle.kv.impl.mgmt.NoOpAgent";
    public static final String COMMON_MGMT_POLL_PORT = "mgmtPollPort";
    public static final String COMMON_MGMT_POLL_PORT_DEFAULT = "1161";
    public static final String COMMON_MGMT_TRAP_HOST = "mgmtTrapHost";
    public static final String COMMON_MGMT_TRAP_HOST_DEFAULT = null;
    public static final String COMMON_MGMT_TRAP_PORT = "mgmtTrapPort";
    public static final String COMMON_MGMT_TRAP_PORT_DEFAULT = "0";
    public static final String COMMON_SECURITY_DIR = "securityDir";
    public static final String COMMON_SECURITY_DIR_DEFAULT = null;

    /**
     * Use to turn on/off the use of client socket factories. It exists
     * primarily to deal with transitions from older versions which do not
     * have the ClientSocketFactory class, to newer versions that do: A newer
     * server may thus send over an instance of ClientSocketFactory to
     * a older client which does not have the class in its older kv library.
     */
    public static final String COMMON_USE_CLIENT_SOCKET_FACTORIES =
            "useClientSocketFactories";
    public static final String COMMON_USE_CLIENT_SOCKET_FACTORIES_DEFAULT =
        "true";

    /**
     * Use to define how many concurrent sessions are maintained, and how many/
     * how long the tokens are kept.
     */
    public static final String COMMON_SESSION_LIMIT = "sessionLimit";
    public static final String COMMON_SESSION_LIMIT_DEFAULT = "10000";
    public static final String COMMON_LOGIN_CACHE_SIZE = "loginCacheSize";
    public static final String COMMON_LOGIN_CACHE_SIZE_DEFAULT = "10000";

    /**
     * AdminParams
     */
    public static final String AP_ID = "adminId";
    public static final String AP_ID_DEFAULT = "-1";
    public static final String AP_WAIT_TIMEOUT = "waitTimeout";
    public static final String AP_WAIT_TIMEOUT_DEFAULT = "5 MINUTES";
    public static final String AP_LOG_FILE_COUNT = "adminLogFileCount";
    public static final String AP_LOG_FILE_COUNT_DEFAULT = "20";
    public static final String AP_LOG_FILE_LIMIT = "adminLogFileLimit";
    public static final String AP_LOG_FILE_LIMIT_DEFAULT = "4000000";
    public static final String AP_EVENT_EXPIRY_AGE = "eventExpiryAge";
    public static final String AP_EVENT_EXPIRY_AGE_DEFAULT = "30 DAYS";
    public static final String AP_BROADCAST_TOPO_RETRY_DELAY =
            "broadcastTopoDelay";
    public static final String AP_BROADCAST_TOPO_RETRY_DELAY_DEFAULT = "10 s";
    public static final String AP_BROADCAST_TOPO_THRESHOLD =
            "broadcastTopoThreshold";   /* Note this is a percent */
    public static final String AP_BROADCAST_TOPO_THRESHOLD_DEFAULT = "20";
    public static final String AP_MAX_TOPO_CHANGES = "maxTopoChanges";
    public static final String AP_MAX_TOPO_CHANGES_DEFAULT = "1000";
    public static final String AP_WAIT_RN_CONSISTENCY = "waitRNConsistency";
    public static final String AP_WAIT_RN_CONSISTENCY_DEFAULT = "60 MINUTES";
    public static final String AP_WAIT_ADMIN_FAILOVER = "waitAdminFailover";
    public static final String AP_WAIT_ADMIN_FAILOVER_DEFAULT = "1 MINUTES";
    public static final String AP_WAIT_RN_FAILOVER = "waitRNFailover";
    public static final String AP_WAIT_RN_FAILOVER_DEFAULT = "1 MINUTES";
    public static final String AP_WAIT_UNREACHABLE_SERVICE =
        "waitUnreachableService";
    public static final String AP_WAIT_UNREACHABLE_SERVICE_DEFAULT = "3 s";
    public static final String AP_CHECK_PARTITION_MIGRATION =
        "checkPartitionMigration";
    public static final String AP_CHECK_PARTITION_MIGRATION_DEFAULT = "10 s";
    public static final String AP_CHECK_ADD_INDEX = "checkAddIndex";
    public static final String AP_CHECK_ADD_INDEX_DEFAULT = "10 s";


    public static final String AP_NUM_GC_LOG_FILES = "numGCLogFiles";
    public static final String AP_NUM_GC_LOG_FILES_DEFAULT = "10";

    public static final String AP_GC_LOG_FILE_SIZE= "GCLogFileSize";
    public static final String AP_GC_LOG_FILE_SIZE_DEFAULT = "1048576";

    /**
     * BootstrapParams
     */
    public static final String BP_ROOTDIR = "rootDir";
    public static final String BP_ROOTDIR_DEFAULT = "";
    public static final String BP_HOSTING_ADMIN = "hostingAdmin";
    public static final String BP_HOSTING_ADMIN_DEFAULT = "false";
    public static final String BP_FORCE_BOOTSTRAP_ADMIN ="forceBootstrapAdmin";
    public static final String BP_FORCE_BOOTSTRAP_ADMIN_DEFAULT ="false";

    /**
     * GlobalParams
     */
    public static final String GP_ISLOOPBACK = "isLoopback";
    public static final String GP_ISLOOPBACK_DEFAULT = "false";
    public static final String GP_SESSION_TIMEOUT = "sessionTimeout";
    public static final String GP_SESSION_TIMEOUT_DEFAULT = "24 h";
    public static final String GP_SESSION_EXTEND_ALLOW =
        "sessionExtendAllowed";
    public static final String GP_SESSION_EXTEND_ALLOW_DEFAULT = "true";
    public static final String GP_ACCOUNT_ERR_LOCKOUT_THR_COUNT =
        "accountErrorLockoutThresholdCount";
    public static final String GP_ACCOUNT_ERR_LOCKOUT_THR_COUNT_DEFAULT =
        "10";
    public static final String GP_ACCOUNT_ERR_LOCKOUT_THR_INTERVAL =
        "accountErrorLockoutThresholdInterval";
    public static final String GP_ACCOUNT_ERR_LOCKOUT_THR_INTERVAL_DEFAULT =
        "10 min";
    public static final String GP_ACCOUNT_ERR_LOCKOUT_TIMEOUT =
        "accountErrorLockoutTimeout";
    public static final String GP_ACCOUNT_ERR_LOCKOUT_TIMEOUT_DEFAULT =
        "30 min";
    public static final String GP_LOGIN_CACHE_TIMEOUT = "loginCacheTimeout";
    public static final String GP_LOGIN_CACHE_TIMEOUT_DEFAULT = "5 min";

    /**
     * JEParams
     */
    public static final String JE_MISC = "configProperties";
    public static final String JE_MISC_DEFAULT = "";
    public static final String JE_HOST_PORT = "nodeHostPort";
    public static final String JE_HOST_PORT_DEFAULT = "";
    public static final String JE_HELPER_HOSTS = "helperHosts";
    public static final String JE_HELPER_HOSTS_DEFAULT = "";
    public static final String JE_CACHE_SIZE = "cacheSize";
    public static final String JE_CACHE_SIZE_DEFAULT = "0";

    /**
     * JVMParams
     */
    public static final String JVM_MISC = "javaMiscParams";
    public static final String JVM_MISC_DEFAULT = "";
    public static final String JVM_LOGGING = "loggingConfigProps";
    public static final String JVM_LOGGING_DEFAULT = "";

    /**
     * KVSParams (store-wide)
     */
    public static final String KV_CACHE_MODE = "cacheMode";
    public static final String KV_CACHE_MODE_DEFAULT = "EVICT_LN";
    public static final String REQUEST_QUIESCE_TIME = "requestQuiesceTime";
    public static final String REQUEST_QUIESCE_TIME_DEFAULT = "60 s";
    public static final String RN_MAX_ACTIVE_REQUESTS = "rnMaxActiveRequests";
    public static final String RN_MAX_ACTIVE_REQUESTS_DEFAULT = "100";
    public static final String RN_REQUEST_THRESHOLD_PERCENT =
        "rnRequestThresholdPercent";
    public static final String RN_REQUEST_THRESHOLD_PERCENT_DEFAULT = "90";
    public static final String RN_NODE_LIMIT_PERCENT = "rnNodeLimitPercent";
    public static final String RN_NODE_LIMIT_PERCENT_DEFAULT = "85";

    /**
     * Determines whether the original log files are to be retained after a
     * network restore operation on an RN.
     */
    public static final String RN_NRCONFIG_RETAIN_LOG_FILES =
        "rnNRConfigRetainLogFiles";
    public static final String RN_NRCONFIG_RETAIN_LOG_FILES_DEFAULT = "false";

    /**
     * The max number of topo changes to be retained in the topology stored
     * locally at an RN.
     */
    public static final String RN_MAX_TOPO_CHANGES = "rnMaxTopoChanges";
    public static final String RN_MAX_TOPO_CHANGES_DEFAULT = "1000";

    /*
     * Parameters for KVSessionManager internal operation
     */

    /**
     * General timeout for session-related requests.
     */
    public static final String RN_SESS_REQUEST_TIMEOUT =
        "rnSessionRequestTimeout";
    public static final String RN_SESS_REQUEST_TIMEOUT_DEFAULT = "5 s";

    /**
     * Timeout for session lookup requests.
     */
    public static final String RN_SESS_LOOKUP_REQUEST_TIMEOUT =
        "rnSessionLookupRequestTimeout";
    public static final String RN_SESS_LOOKUP_REQUEST_TIMEOUT_DEFAULT =
        "5 s";

    /**
     * Consistency requirement for session lookup.
     */
    public static final String RN_SESS_LOOKUP_CONSISTENCY_LIMIT =
        "rnSessionLookupConsistencyLimit";
    public static final String RN_SESS_LOOKUP_CONSISTENCY_LIMIT_DEFAULT =
        "30 s";

    /**
     * Timeout for session lookup consistency.
     */
    public static final String RN_SESS_LOOKUP_CONSISTENCY_TIMEOUT =
        "rnSessionLookupConsistencyTimeout";
    public static final String RN_SESS_LOOKUP_CONSISTENCY_TIMEOUT_DEFAULT =
        "3 s";

    /**
     * Timeout for session logout requests.
     */
    public static final String RN_SESS_LOGOUT_REQUEST_TIMEOUT =
        "rnSessionLogoutRequestTimeout";
    public static final String RN_SESS_LOGOUT_REQUEST_TIMEOUT_DEFAULT =
        "20 s";

    /**
     * RepNode mount point (optional)
     */
    public static final String RN_MOUNT_POINT = "rnMountPoint";
    public static final String RN_MOUNT_POINT_DEFAULT = "";

    /**
     * These two are not dynamically configurable at this time
     */
    public static final int RN_HEAP_MB_MIN = 32;
    public static final int RN_CACHE_MB_MIN = 22;

    /*
     * The portion of an RN's memory set aside for the JE environment cache.
     */
    public static final String RN_CACHE_PERCENT = "rnCachePercent";
    public static final String RN_CACHE_PERCENT_DEFAULT = "70";

    /**
     * The client and server socket configs associated with RN's request
     * handling interface.
     */
    public static final String RN_RH_SO_BACKLOG = "rnRHSOBacklog";
    public static final String RN_RH_SO_BACKLOG_DEFAULT = "1024";
    public static final String RN_RH_SO_READ_TIMEOUT =
            "rnRHSOReadTimeout";
    public static final String RN_RH_SO_READ_TIMEOUT_DEFAULT = "30 s";
    public static final String RN_RH_SO_CONNECT_TIMEOUT =
            "rnRHSOConnectTimeout";
    public static final String RN_RH_SO_CONNECT_TIMEOUT_DEFAULT = "10 s";

    /**
     * The client and server socket configs associated with the RN's monitor
     * interface
     */
    public static final String RN_MONITOR_SO_BACKLOG = "rnMonitorSOBacklog";
    public static final String RN_MONITOR_SO_BACKLOG_DEFAULT = "0";
    public static final String RN_MONITOR_SO_READ_TIMEOUT =
            "rnMonitorSOReadTimeout";
    public static final String RN_MONITOR_SO_READ_TIMEOUT_DEFAULT = "10 s";
    public static final String RN_MONITOR_SO_CONNECT_TIMEOUT =
            "rnMonitorSOConnectTimeout";
    public static final String RN_MONITOR_SO_CONNECT_TIMEOUT_DEFAULT = "10 s";

    /**
     * The client and server socket configs associated with the RN's admin
     * interface
     */
    public static final String RN_ADMIN_SO_BACKLOG = "rnAdminSOBacklog";
    public static final String RN_ADMIN_SO_BACKLOG_DEFAULT = "0";
    public static final String RN_ADMIN_SO_READ_TIMEOUT =
            "rnAdminSOReadTimeout";
    public static final String RN_ADMIN_SO_READ_TIMEOUT_DEFAULT = "10 s";
    public static final String RN_ADMIN_SO_CONNECT_TIMEOUT =
            "rnAdminSOConnectTimeout";
    public static final String RN_ADMIN_SO_CONNECT_TIMEOUT_DEFAULT = "10 s";

    /**
     * The client and server socket configs associated with the RN's login
     * interface
     */
    public static final String RN_LOGIN_SO_BACKLOG = "rnLoginSOBacklog";
    public static final String RN_LOGIN_SO_BACKLOG_DEFAULT = "0";
    public static final String RN_LOGIN_SO_READ_TIMEOUT =
            "rnLoginSOReadTimeout";
    public static final String RN_LOGIN_SO_READ_TIMEOUT_DEFAULT = "10 s";
    public static final String RN_LOGIN_SO_CONNECT_TIMEOUT =
            "rnLoginSOConnectTimeout";
    public static final String RN_LOGIN_SO_CONNECT_TIMEOUT_DEFAULT = "10 s";

    /**
     * Parameters associated with partition migration.
     */
    public static final String RN_PM_CONCURRENT_SOURCE_LIMIT =
            "rnPMConcurrentSourceLimit";
    public static final String RN_PM_CONCURRENT_SOURCE_LIMIT_DEFAULT = "1";
    public static final String RN_PM_CONCURRENT_TARGET_LIMIT =
            "rnPMConcurrentTargetLimit";
    public static final String RN_PM_CONCURRENT_TARGET_LIMIT_DEFAULT = "2";
    public static final String RN_PM_SO_READ_WRITE_TIMEOUT =
            "rnPMSOReadWriteTimeout";
    public static final String RN_PM_SO_READ_WRITE_TIMEOUT_DEFAULT = "10 s";
    public static final String RN_PM_SO_CONNECT_TIMEOUT =
            "rnPMSOConnectTimeout";
    public static final String RN_PM_SO_CONNECT_TIMEOUT_DEFAULT = "10 s";
    public static final String RN_PM_WAIT_AFTER_BUSY = "rnPMWaitAfterBusy";
    public static final String RN_PM_WAIT_AFTER_BUSY_DEFAULT = "60 s";
    public static final String RN_PM_WAIT_AFTER_ERROR = "rnPMWaitAfterError";
    public static final String RN_PM_WAIT_AFTER_ERROR_DEFAULT = "30 s";

    /** The RN node type. */
    public static final String RN_NODE_TYPE = "rnNodeType";
    public static final String RN_NODE_TYPE_DEFAULT = "ELECTABLE";

    /* RN GC logging config */
    public static final String RN_NUM_GC_LOG_FILES = "rnNumGCLogFiles";
    public static final String RN_NUM_GC_LOG_FILES_DEFAULT = "10";

    public static final String RN_GC_LOG_FILE_SIZE = "rnGCLogFileSize";
    public static final String RN_GC_LOG_FILE_SIZE_DEFAULT = "1048576";

    /**
     * MonitorParams
     */
    public static final String MP_POLL_PERIOD = "collectorPollPeriod";
    public static final String MP_POLL_PERIOD_DEFAULT = "20 s";
    public static final String MP_CREATE_CSV = "createCSV";
    public static final String MP_CREATE_CSV_DEFAULT = "false";

    /**
     * RepNodeParams
     */
    public static final String RP_RN_ID = "repNodeId";
    public static final String RP_RN_ID_DEFAULT = "-1";

    /**
     * StatsParams
     */
    public static final String SP_MAX_LATENCY = "maxTrackedLatency";

    /*
     * Keep these two defaults in sync.  The MS default allows the client
     * to use this default without including ParameterState itself.  See
     * RequestDispatcherImpl where it is used to initialize StatsTracker.
     */
    public static final String SP_MAX_LATENCY_DEFAULT = "1000 ms";
    public static final int SP_MAX_LATENCY_MS_DEFAULT = 1000;
    public static final String SP_INTERVAL = "statsInterval";
    public static final String SP_INTERVAL_DEFAULT = "60 s";
    public static final String SP_DUMP_INTERVAL = "threadDumpInterval";
    public static final String SP_DUMP_INTERVAL_DEFAULT = "5 MINUTES";
    public static final String SP_ACTIVE_THRESHOLD = "activeThreshold";
    public static final String SP_ACTIVE_THRESHOLD_DEFAULT =
        new Integer(Integer.MAX_VALUE).toString();
    public static final String SP_THREAD_DUMP_MAX = "threadDumpMax";
    public static final String SP_THREAD_DUMP_MAX_DEFAULT = "10";
    public static final String SP_COLLECT_ENV_STATS = "collectEnvStats";
    public static final String SP_COLLECT_ENV_STATS_DEFAULT = "true";
    public static final String SP_LATENCY_CEILING = "latencyCeiling";
    public static final String SP_LATENCY_CEILING_DEFAULT = "0";
    public static final String SP_THROUGHPUT_FLOOR = "throughputFloor";
    public static final String SP_THROUGHPUT_FLOOR_DEFAULT = "0";
    public static final String SP_COMMIT_LAG_THRESHOLD = "commitLagThreshold";
    public static final String SP_COMMIT_LAG_THRESHOLD_DEFAULT = "0";

    /**
     * StorageNodeParams
     */
    public static final String SN_ROOT_DIR_PATH = "rootDirPath";
    public static final String SN_ROOT_DIR_PATH_DEFAULT = "";
    public static final String SN_SERVICE_STOP_WAIT = "serviceStopWait";
    public static final String SN_SERVICE_STOP_WAIT_DEFAULT = "120 s";
    public static final String SN_REPNODE_START_WAIT = "repnodeStartWait";
    public static final String SN_REPNODE_START_WAIT_DEFAULT = "120 s";
    public static final String SN_LOG_FILE_COUNT = "serviceLogFileCount";
    public static final String SN_LOG_FILE_COUNT_DEFAULT = "20";
    public static final String SN_LOG_FILE_LIMIT = "serviceLogFileLimit";
    public static final String SN_LOG_FILE_LIMIT_DEFAULT = "2000000";
    public static final String SN_COMMENT = "comment";
    public static final String SN_COMMENT_DEFAULT = "";
    public static final String SN_MAX_LINK_COUNT = "maxLinkCount";
    public static final String SN_MAX_LINK_COUNT_DEFAULT = "500";
    public static final String SN_LINK_EXEC_WAIT = "linkExecWait";
    public static final String SN_LINK_EXEC_WAIT_DEFAULT = "200 s";

    /*
     * Let the user customize the command line used to start managed processes,
     * Example use cases are when the user wants to configure with the numactl
     * wrapper, or to start a RN with profiling.
     */
    public static final String SN_PROCESS_STARTUP_PREFIX =
         "processStartupPrefix";
    public static final String SN_PROCESS_STARTUP_PREFIX_DEFAULT = "";

    /**
     * The software version of the running SN. The version number
     * is updated when the SNA starts up with the new software.
     */
    public static final String SN_SOFTWARE_VERSION = "softwareVersion";
    public static final String SN_SOFTWARE_VERSION_DEFAULT = "";

    /*
     * The amount of SN memory reserved for heap, for all RN processes hosted
     * on this SN.
     */
    public static final String SN_RN_HEAP_PERCENT = "rnHeapPercent";
    public static final String SN_RN_HEAP_PERCENT_DEFAULT = "85";

    /*
     * Constants used to implement a policy for tuning garbage collector
     * threads.
     */
    public static final String SN_GC_THREAD_FLOOR = "gcThreadFloor";
    public static final String SN_GC_THREAD_FLOOR_DEFAULT = "4";
    public static final String SN_GC_THREAD_THRESHOLD = "gcThreadThreshold";
    public static final String SN_GC_THREAD_THRESHOLD_DEFAULT = "8";
    public static final String SN_GC_THREAD_PERCENT = "gcThreadPercent";
    public static final String SN_GC_THREAD_PERCENT_DEFAULT = "62";

    /**
     * The socket configs associated with the services provided by the
     * SN. All timeouts are expressed in ms.
     */
    public static final String SN_ADMIN_SO_BACKLOG = "snAdminSOBacklog";
    public static final String SN_ADMIN_SO_BACKLOG_DEFAULT = "0";

    /*
     * Note that the string values have been left unchanged to avoid
     * compatibility issues with deployed kvstores.
     *
     * The SN read timeout is large to handle the load that occurs when a store
     * is being deployed.  It can take a while to create RepNodes.
     */
    public static final String SN_ADMIN_SO_CONNECT_TIMEOUT = "connectTimeout";
    public static final String SN_ADMIN_SO_CONNECT_TIMEOUT_DEFAULT = "10 s";
    public static final String SN_ADMIN_SO_READ_TIMEOUT = "readTimeout";
    public static final String SN_ADMIN_SO_READ_TIMEOUT_DEFAULT = "140 s";

    public static final String SN_MONITOR_SO_BACKLOG = "snMonitorSOBacklog";
    public static final String SN_MONITOR_SO_BACKLOG_DEFAULT = "0";
    public static final String SN_MONITOR_SO_CONNECT_TIMEOUT =
            "snMonitorSOConnectTimeout";
    public static final String SN_MONITOR_SO_CONNECT_TIMEOUT_DEFAULT = "10 s";
    public static final String SN_MONITOR_SO_READ_TIMEOUT =
            "snMonitorSOReadTimeout";
    public static final String SN_MONITOR_SO_READ_TIMEOUT_DEFAULT = "10 s";

    public static final String SN_LOGIN_SO_BACKLOG = "snLoginSOBacklog";
    public static final String SN_LOGIN_SO_BACKLOG_DEFAULT = "0";
    public static final String SN_LOGIN_SO_CONNECT_TIMEOUT =
            "snLoginSOConnectTimeout";
    public static final String SN_LOGIN_SO_CONNECT_TIMEOUT_DEFAULT = "5 s";
    public static final String SN_LOGIN_SO_READ_TIMEOUT =
            "snLoginSOReadTimeout";
    public static final String SN_LOGIN_SO_READ_TIMEOUT_DEFAULT = "5 s";

    public static final String SN_REGISTRY_SO_BACKLOG = "snRegistrySOBacklog";
    public static final String SN_REGISTRY_SO_BACKLOG_DEFAULT = "1024";
    public static final String SN_REGISTRY_SO_CONNECT_TIMEOUT =
            "snRegistrySOConnectTimeout";
    public static final String SN_REGISTRY_SO_CONNECT_TIMEOUT_DEFAULT = "5 s";

    /*
     * This should probably have been defined as "snRegistrySOReadTimeout", but
     * changing it could have upgrade compatibility issues.
     */
    public static final String SN_REGISTRY_SO_READ_TIMEOUT =
            "snMonitorSOReadTimeout";
    public static final String SN_REGISTRY_SO_READ_TIMEOUT_DEFAULT = "5 s";

    /* Just for UI */
    public static final String SN_DATACENTER = "datacenterId";
    public static final String SN_DATACENTER_DEFAULT = "-1";

    /**
     * Admin service parameters
     */
    /* Configures the socket used by the Admin's command service. */
    public static final String ADMIN_COMMAND_SERVICE_SO_BACKLOG =
        "adminCommandServiceSOBacklog";
    public static final String ADMIN_COMMAND_SERVICE_SO_BACKLOG_DEFAULT = "0";

    /* Configures the sockets used by the Admin's listener. */
    public static final String ADMIN_LISTENER_SO_BACKLOG =
        "adminListenerSOBacklog";
    public static final String ADMIN_LISTENER_SO_BACKLOG_DEFAULT = "100";

    /* Configures the socket used by the Admin's loginservice. */
    public static final String ADMIN_LOGIN_SO_BACKLOG =
        "adminLoginSOBacklog";
    public static final String ADMIN_LOGIN_SO_BACKLOG_DEFAULT = "0";

    /**
     * Class methods
     */
    public ParameterState(Type type,
                          Object defaultParam,
                          EnumSet<Info> info,
                          Scope scope,
                          long min,
                          long max,
                          String[] validValues) {
        this.type = type;
        this.defaultParam = defaultParam;
        this.info = info;
        this.scope = scope;
        this.min = min;
        this.max = max;
        this.validValues = validValues;
        if (validValues == null && type == Type.BOOLEAN) {
            validValues = booleanVals;
        }
    }

    public Type getType() {
        return type;
    }

    public Scope getScope() {
        return scope;
    }

    public boolean appliesTo(Info svc) {
        return info.contains(svc);
    }

    public boolean containsAll(EnumSet<Info> set) {
        return info.containsAll(set);
    }

    public boolean getReadOnly() {
        return appliesTo(Info.RONLY);
    }

    public boolean getPolicyState() {
        return appliesTo(Info.POLICY);
    }

    public boolean restartRequired() {
        return !appliesTo(Info.NORESTART);
    }

    public boolean appliesToRepNode() {
        return appliesTo(Info.REPNODE);
    }

    public boolean appliesToAdmin() {
        return appliesTo(Info.ADMIN);
    }

    public boolean appliesToStorageNode() {
        return appliesTo(Info.SNA);
    }

    public boolean appliesToBootstrap() {
        return appliesTo(Info.BOOT);
    }

    public String[] getPossibleValues() {
        return validValues;
    }

    public boolean isHidden() {
        return appliesTo(Info.HIDDEN);
    }

    /**
     * TODO: add range info for int, long types if the UI wants it.
     */
    public String getValidValues() {
        String msg = null;
        switch (type) {
        case INT:
            msg = "<Integer>";
            break;
        case LONG:
            msg = "<Long>";
            break;
        case BOOLEAN:
            msg = "<Boolean>";
            break;
        case STRING:
            msg = "<String>";
            break;
        case DURATION:
            msg = "<Long TimeUnit>";
            break;
        case CACHEMODE:
            if (validValues != null) {
                msg = Arrays.toString(validValues);
            }
            break;
        case NONE:
            msg = "<INVALIDTYPE>";
            break;
        }
        return msg;
    }

    /**
     * Validate ranges for int and long values.
     */
    public boolean validate(String name, long value, boolean throwIfInvalid) {
        if (value < min) {
            if (throwIfInvalid) {
                throw new IllegalArgumentException
                    ("Value: " + value + " is less than minimum (" +
                     min + ") for parameter " + name);
            }
            return false;
        }
        if (value > max) {
            if (throwIfInvalid) {
                throw new IllegalArgumentException
                    ("Value: " + value + " is greater than maximum (" +
                     max + ") for parameter " + name);
            }
            return false;
        }
        return true;
    }

    private static void noParameter(String param) {
        throw new IllegalStateException("Invalid parameter: " + param);
    }

    public static boolean restartRequired(String parm) {
        ParameterState state = pstate.get(parm);
        if (state != null) {
            return state.restartRequired();
        }
        noParameter(parm);
        return false;
    }

    public static Type getType(String parm) {
        ParameterState state = pstate.get(parm);
        if (state != null) {
            return state.getType();
        }
        noParameter(parm);
        return Type.NONE;
    }

    public static boolean isPolicyParameter(String parm) {
        ParameterState state = pstate.get(parm);
        if (state != null) {
            return state.getPolicyState();
        }
        noParameter(parm);
        return false;
    }

    public static boolean isAdminParameter(String parm) {
        ParameterState state = pstate.get(parm);
        if (state != null) {
            return state.appliesToAdmin();
        }
        noParameter(parm);
        return false;
    }

    public static boolean isRepNodeParameter(String parm) {
        ParameterState state = pstate.get(parm);
        if (state != null) {
            return state.appliesToRepNode();
        }
        noParameter(parm);
        return false;
    }

    public static boolean isStorageNodeParameter(String parm) {
        ParameterState state = pstate.get(parm);
        if (state != null) {
            return state.appliesToStorageNode();
        }
        noParameter(parm);
        return false;
    }

    public static boolean isHiddenParameter(String parm) {
        ParameterState state = pstate.get(parm);
        if (state != null) {
            return state.isHidden();
        }
        noParameter(parm);
        return false;
    }

    public static boolean validate(String name, long value) {
        ParameterState ps = lookup(name);
        if (ps != null) {
            return ps.validate(name, value, false);
        }
        return false;
    }

    public static ParameterState lookup(String parm) {
        return pstate.get(parm);
    }

    /**
     * This is the static map that holds all of the required information.
     * NOTE: all int and long parameters must specify valid ranges even if
     * they are the default min/max for the type..
     */
    static final Map<String, ParameterState> pstate =
        new HashMap<String, ParameterState>() {
        private static final long serialVersionUID = 1L;

        void putState(String name,
                      String defaultValue,
                      ParameterState.Type type,
                      EnumSet<Info> info) {
            putState(name, defaultValue, type, info, Scope.SERVICE);
        }

        void putState(String name,
                      String defaultValue,
                      ParameterState.Type type,
                      EnumSet<Info> info,
                      Scope scope) {
            putState(name, defaultValue, type, info, scope,
                     (type == Type.INT ? Integer.MIN_VALUE :
                      (type == Type.LONG ? Long.MIN_VALUE : 0)),
                     (type == Type.INT ? Integer.MAX_VALUE :
                      (type == Type.LONG ? Long.MAX_VALUE : 0)),
                     null);
        }

        void putState(String name,
                      String defaultValue,
                      ParameterState.Type type,
                      EnumSet<Info> info,
                      Scope scope,
                      long min,
                      long max,
                      String[] validValues) {
            Object p = new DefaultParameter().create(name, defaultValue, type);
            ParameterState ps = new ParameterState(type, p, info, scope, min,
                                                   max, validValues);
            super.put(name, ps);
        }

        {

            /**
             * Common keys, shared among parameter groups (as defined by the
             * EnumSet passed to the constructor).
             */
            putState(COMMON_SN_ID, COMMON_SN_ID_DEFAULT, Type.INT,
                     EnumSet.of(Info.REPNODE, Info.ADMIN,
                                Info.SNA, Info.RONLY));
            putState(COMMON_DISABLED, COMMON_DISABLED_DEFAULT, Type.BOOLEAN,
                     EnumSet.of(Info.REPNODE, Info.ADMIN, Info.RONLY));
            putState(COMMON_HOSTNAME, COMMON_HOSTNAME_DEFAULT,
                     Type.STRING, EnumSet.of(Info.SNA, Info.BOOT, Info.RONLY));
            putState(COMMON_REGISTRY_PORT, COMMON_REGISTRY_PORT_DEFAULT,
                     Type.INT, EnumSet.of(Info.SNA, Info.BOOT, Info.RONLY),
                     Scope.SERVICE, 0, 65535, null);
            putState(COMMON_HA_HOSTNAME, COMMON_HA_HOSTNAME_DEFAULT,
                     Type.STRING,
                     EnumSet.of(Info.SNA, Info.BOOT, Info.NORESTART));
            putState(COMMON_PORTRANGE, COMMON_PORTRANGE_DEFAULT, Type.STRING,
                     EnumSet.of(Info.SNA, Info.BOOT, Info.NORESTART));
            putState(COMMON_SERVICE_PORTRANGE,
                     COMMON_SERVICE_PORTRANGE_DEFAULT, Type.STRING,
                     EnumSet.of(Info.SNA, Info.BOOT));
            putState(COMMON_ADMIN_PORT, COMMON_ADMIN_PORT_DEFAULT, Type.INT,
                     EnumSet.of(Info.ADMIN, Info.BOOT),
                     Scope.SERVICE, 0, 65535, null);
            putState(COMMON_STORENAME, COMMON_STORENAME_DEFAULT, Type.STRING,
                     EnumSet.of(Info.BOOT, Info.GLOBAL, Info.RONLY),
                     Scope.STORE);
            putState(COMMON_USE_CLIENT_SOCKET_FACTORIES,
                     COMMON_USE_CLIENT_SOCKET_FACTORIES_DEFAULT, Type.BOOLEAN,
                     EnumSet.of(Info.REPNODE, Info.ADMIN, Info.SNA,
                                Info.POLICY, Info.HIDDEN));
            putState(COMMON_CAPACITY, COMMON_CAPACITY_DEFAULT,
                     Type.INT, EnumSet.of(Info.BOOT, Info.SNA, Info.NORESTART),
                     Scope.SERVICE, 1, 1000, null);
            putState(COMMON_NUMCPUS, COMMON_NUMCPUS_DEFAULT,
                     Type.INT, EnumSet.of(Info.BOOT, Info.SNA, Info.NORESTART),
                      Scope.SERVICE, 1, 128, null);
            putState(COMMON_MEMORY_MB, COMMON_MEMORY_MB_DEFAULT,
                     Type.INT,
                     EnumSet.of(Info.BOOT, Info.SNA, Info.NORESTART),
                     Scope.SERVICE, 0, 500000, null);
            putState(COMMON_MGMT_CLASS, COMMON_MGMT_CLASS_DEFAULT,
                     Type.STRING,
                     EnumSet.of(Info.BOOT, Info.SNA, Info.NORESTART));
            putState(COMMON_MGMT_POLL_PORT, COMMON_MGMT_POLL_PORT_DEFAULT,
                     Type.INT,
                     EnumSet.of(Info.BOOT, Info.SNA, Info.NORESTART));
            putState(COMMON_MGMT_TRAP_HOST, COMMON_MGMT_TRAP_HOST_DEFAULT,
                     Type.STRING,
                     EnumSet.of(Info.BOOT, Info.SNA, Info.NORESTART));
            putState(COMMON_MGMT_TRAP_PORT, COMMON_MGMT_TRAP_PORT_DEFAULT,
                     Type.INT,
                     EnumSet.of(Info.BOOT, Info.SNA, Info.NORESTART));
            putState(COMMON_MASTER_BALANCE, COMMON_MASTER_BALANCE_DEFAULT,
                     Type.BOOLEAN,
                     EnumSet.of(Info.SNA, Info.REPNODE,
                                Info.POLICY, Info.HIDDEN));
            putState(COMMON_HIDE_USERDATA,
                     COMMON_HIDE_USERDATA_DEFAULT,
                     Type.BOOLEAN,
                     EnumSet.of(Info.ADMIN, Info.REPNODE, Info.POLICY,
                                Info.NORESTART));
            putState(COMMON_SECURITY_DIR, COMMON_SECURITY_DIR_DEFAULT,
                     Type.STRING,
                     EnumSet.of(Info.BOOT, Info.SNA, Info.NORESTART));
            putState(COMMON_SESSION_LIMIT, COMMON_SESSION_LIMIT_DEFAULT,
                     Type.INT,
                     EnumSet.of(Info.REPNODE, Info.ADMIN, Info.SNA,
                                Info.NORESTART),
                     Scope.STORE);
            putState(COMMON_LOGIN_CACHE_SIZE, COMMON_LOGIN_CACHE_SIZE_DEFAULT,
                     Type.INT,
                     EnumSet.of(Info.REPNODE, Info.ADMIN, Info.SNA,
                                Info.NORESTART),
                     Scope.STORE);

            /**
             * AdminParams
             */
            putState(AP_ID, AP_ID_DEFAULT, Type.INT,
                     EnumSet.of(Info.ADMIN, Info.RONLY));
            putState(AP_WAIT_TIMEOUT, AP_WAIT_TIMEOUT_DEFAULT, Type.DURATION,
                     EnumSet.of(Info.ADMIN, Info.POLICY,
                                Info.NORESTART, Info.HIDDEN), Scope.STORE);
            putState(AP_LOG_FILE_COUNT, AP_LOG_FILE_COUNT_DEFAULT, Type.INT,
                     EnumSet.of(Info.ADMIN, Info.POLICY), Scope.STORE);
            putState(AP_LOG_FILE_LIMIT, AP_LOG_FILE_LIMIT_DEFAULT, Type.INT,
                     EnumSet.of(Info.ADMIN, Info.POLICY), Scope.STORE);
            putState(AP_EVENT_EXPIRY_AGE, AP_EVENT_EXPIRY_AGE_DEFAULT,
                     Type.DURATION,
                     EnumSet.of(Info.ADMIN, Info.POLICY, Info.NORESTART),
                     Scope.STORE);
            putState(AP_BROADCAST_TOPO_RETRY_DELAY,
                     AP_BROADCAST_TOPO_RETRY_DELAY_DEFAULT, Type.DURATION,
                     EnumSet.of(Info.ADMIN, Info.POLICY,
                                Info.NORESTART, Info.HIDDEN), Scope.STORE);
            putState(AP_BROADCAST_TOPO_THRESHOLD,
                     AP_BROADCAST_TOPO_THRESHOLD_DEFAULT, Type.INT,
                     EnumSet.of(Info.ADMIN, Info.POLICY,
                                Info.NORESTART, Info.HIDDEN),
                     Scope.STORE, 1, 100, null); /* threshold is a % */
            putState(AP_MAX_TOPO_CHANGES,
                     AP_MAX_TOPO_CHANGES_DEFAULT, Type.INT,
                     EnumSet.of(Info.ADMIN, Info.POLICY,
                                Info.NORESTART, Info.HIDDEN),
                     Scope.STORE);

            /*
             * Time period that the admin waits for a relocated RepNode to
             * come up and acquire necessary data before deleting the old
             * instance of the RepNode.
             */
            putState(AP_WAIT_RN_CONSISTENCY, AP_WAIT_RN_CONSISTENCY_DEFAULT,
                     Type.DURATION,
                     EnumSet.of(Info.ADMIN, Info.NORESTART, Info.POLICY,
                                Info.HIDDEN), Scope.STORE);

            /* Polling time interval when a service can't be reached */
            putState(AP_WAIT_UNREACHABLE_SERVICE,
                     AP_WAIT_UNREACHABLE_SERVICE_DEFAULT, Type.DURATION,
                     EnumSet.of(Info.ADMIN, Info.NORESTART, Info.POLICY,
                                Info.HIDDEN), Scope.STORE);

            /* Polling time interval when the admin has failed over */
            putState(AP_WAIT_ADMIN_FAILOVER, AP_WAIT_ADMIN_FAILOVER_DEFAULT,
                     Type.DURATION,
                     EnumSet.of(Info.ADMIN, Info.NORESTART, Info.POLICY,
                                Info.HIDDEN), Scope.STORE);

            /* Polling time interval when a RN has failed over */
            putState(AP_WAIT_RN_FAILOVER, AP_WAIT_RN_FAILOVER_DEFAULT,
                     Type.DURATION,
                     EnumSet.of(Info.ADMIN, Info.NORESTART, Info.POLICY,
                                Info.HIDDEN), Scope.STORE);

            /* Parameters to control GC logging in the Admin. */
            putState(AP_NUM_GC_LOG_FILES,
                     AP_NUM_GC_LOG_FILES_DEFAULT, Type.INT,
                     EnumSet.of(Info.ADMIN, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);

            putState(AP_GC_LOG_FILE_SIZE,
                     AP_GC_LOG_FILE_SIZE_DEFAULT, Type.INT,
                     EnumSet.of(Info.ADMIN, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);

            /*
             * Polling time interval to check if a partition migration has
             * finished.
             */
            putState(AP_CHECK_PARTITION_MIGRATION,
                     AP_CHECK_PARTITION_MIGRATION_DEFAULT,
                     Type.DURATION,
                     EnumSet.of(Info.ADMIN, Info.NORESTART, Info.POLICY,
                                Info.HIDDEN), Scope.STORE);

            /*
             * Polling time interval to check if an index population task has
             * finished.
             */
            putState(AP_CHECK_ADD_INDEX,
                     AP_CHECK_ADD_INDEX_DEFAULT,
                     Type.DURATION,
                     EnumSet.of(Info.ADMIN, Info.NORESTART, Info.POLICY,
                                Info.HIDDEN), Scope.STORE);

            /**
             * BootstrapParams
             */
            putState(BP_ROOTDIR, BP_ROOTDIR_DEFAULT,
                     Type.STRING, EnumSet.of(Info.BOOT, Info.RONLY));
            putState(BP_HOSTING_ADMIN, BP_HOSTING_ADMIN_DEFAULT,
                     Type.BOOLEAN, EnumSet.of(Info.BOOT, Info.RONLY));
            putState(BP_FORCE_BOOTSTRAP_ADMIN,
                     BP_FORCE_BOOTSTRAP_ADMIN_DEFAULT,
                     Type.BOOLEAN, EnumSet.of(Info.BOOT, Info.RONLY));

            /**
             * GlobalParams
             */
            putState(GP_ISLOOPBACK, GP_ISLOOPBACK_DEFAULT, Type.BOOLEAN,
                     EnumSet.of(Info.GLOBAL, Info.RONLY, Info.HIDDEN));

            putState(GP_SESSION_TIMEOUT, GP_SESSION_TIMEOUT_DEFAULT,
                     Type.DURATION,
                     EnumSet.of(Info.SECURITY, Info.GLOBAL, Info.NORESTART),
                     Scope.STORE);
            putState(GP_SESSION_EXTEND_ALLOW,
                     GP_SESSION_EXTEND_ALLOW_DEFAULT, Type.BOOLEAN,
                     EnumSet.of(Info.SECURITY, Info.GLOBAL, Info.NORESTART),
                     Scope.STORE);
            putState(GP_ACCOUNT_ERR_LOCKOUT_THR_COUNT,
                     GP_ACCOUNT_ERR_LOCKOUT_THR_COUNT_DEFAULT, Type.INT,
                     EnumSet.of(Info.SECURITY, Info.GLOBAL, Info.NORESTART),
                     Scope.STORE);
            putState(GP_ACCOUNT_ERR_LOCKOUT_THR_INTERVAL,
                     GP_ACCOUNT_ERR_LOCKOUT_THR_INTERVAL_DEFAULT,
                     Type.DURATION,
                     EnumSet.of(Info.SECURITY, Info.GLOBAL, Info.NORESTART),
                     Scope.STORE);
            putState(GP_ACCOUNT_ERR_LOCKOUT_TIMEOUT,
                     GP_ACCOUNT_ERR_LOCKOUT_TIMEOUT_DEFAULT, Type.DURATION,
                     EnumSet.of(Info.SECURITY, Info.GLOBAL, Info.NORESTART),
                     Scope.STORE);
            putState(GP_LOGIN_CACHE_TIMEOUT, GP_LOGIN_CACHE_TIMEOUT_DEFAULT,
                     Type.DURATION,
                     EnumSet.of(Info.SECURITY, Info.GLOBAL, Info.NORESTART),
                     Scope.STORE);

            /**
             * JEParams
             * TODO: maybe validate JE_MISC as a Properties list
             */
            putState(JE_MISC, JE_MISC_DEFAULT, Type.STRING,
                     EnumSet.of(Info.REPNODE, Info.ADMIN, Info.POLICY),
                     Scope.STORE);
            putState(JE_HOST_PORT, JE_HOST_PORT_DEFAULT, Type.STRING,
                     EnumSet.of(Info.REPNODE, Info.ADMIN, Info.RONLY,
                                Info.HIDDEN));
            putState(JE_HELPER_HOSTS, JE_HELPER_HOSTS_DEFAULT, Type.STRING,
                     EnumSet.of(Info.REPNODE, Info.ADMIN,
                                Info.HIDDEN, Info.NORESTART),
                     Scope.REPGROUP);
            putState(JE_CACHE_SIZE, JE_CACHE_SIZE_DEFAULT, Type.LONG,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.NORESTART),
                     Scope.STORE);

            /**
             * JVMParams
             */
            putState(JVM_MISC, JVM_MISC_DEFAULT, Type.STRING,
                     EnumSet.of(Info.REPNODE, Info.ADMIN, Info.POLICY),
                     Scope.STORE);
            /* TODO: logging changes are restart-required, this may change */
            putState(JVM_LOGGING, JVM_LOGGING_DEFAULT, Type.STRING,
                     EnumSet.of(Info.REPNODE, Info.ADMIN, Info.POLICY),
                     Scope.STORE);

            /**
             * KVSParams
             */
            putState(KV_CACHE_MODE, KV_CACHE_MODE_DEFAULT, Type.CACHEMODE,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.HIDDEN),
                     Scope.STORE, 0, 0,  /* min, max n/a */
                     new String[] {"DEFAULT", "KEEP_HOT",
                                   "UNCHANGED", "MAKE_COLD",
                                   "EVICT_LN", "EVICT_BIN",
                                   "DYNAMIC"});

            /**
             * MonitorParams (Admin only)
             */
            putState(MP_POLL_PERIOD, MP_POLL_PERIOD_DEFAULT, Type.DURATION,
                     EnumSet.of(Info.ADMIN, Info.POLICY, Info.NORESTART),
                     Scope.STORE);
            putState(MP_CREATE_CSV, MP_CREATE_CSV_DEFAULT, Type.BOOLEAN,
                     EnumSet.of(Info.ADMIN, Info.POLICY, Info.NORESTART,
                                Info.HIDDEN),
                     Scope.STORE);

            /**
             * RepNodeParams
             */
            putState(RP_RN_ID, RP_RN_ID_DEFAULT, Type.STRING,
                     EnumSet.of(Info.REPNODE, Info.RONLY));
            putState(RN_MOUNT_POINT, RN_MOUNT_POINT_DEFAULT, Type.STRING,
                     EnumSet.of(Info.REPNODE, Info.RONLY));
            putState(RN_CACHE_PERCENT, RN_CACHE_PERCENT_DEFAULT, Type.INT,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.NORESTART),
                     Scope.SERVICE, 1, 90, null);
            putState(REQUEST_QUIESCE_TIME, REQUEST_QUIESCE_TIME_DEFAULT,
                     Type.DURATION,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);
            putState(RN_MAX_ACTIVE_REQUESTS,  RN_MAX_ACTIVE_REQUESTS_DEFAULT,
                     Type.INT,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);
            putState(RN_NODE_LIMIT_PERCENT, RN_NODE_LIMIT_PERCENT_DEFAULT,
                     Type.INT,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);
            putState(RN_REQUEST_THRESHOLD_PERCENT,
                     RN_REQUEST_THRESHOLD_PERCENT_DEFAULT, Type.INT,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);
            putState(RN_NRCONFIG_RETAIN_LOG_FILES,
                     RN_NRCONFIG_RETAIN_LOG_FILES_DEFAULT, Type.BOOLEAN,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);
            putState(RN_MAX_TOPO_CHANGES,
                     RN_MAX_TOPO_CHANGES_DEFAULT, Type.INT,
                     EnumSet.of(Info.REPNODE, Info.POLICY,
                                Info.NORESTART, Info.HIDDEN),
                     Scope.STORE);
            putState(RN_SESS_REQUEST_TIMEOUT, RN_SESS_REQUEST_TIMEOUT_DEFAULT,
                     Type.DURATION,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);
            putState(RN_SESS_LOOKUP_REQUEST_TIMEOUT,
                     RN_SESS_LOOKUP_REQUEST_TIMEOUT_DEFAULT,
                     Type.DURATION,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);
            putState(RN_SESS_LOOKUP_CONSISTENCY_LIMIT,
                     RN_SESS_LOOKUP_CONSISTENCY_LIMIT_DEFAULT,
                     Type.DURATION,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);
            putState(RN_SESS_LOOKUP_CONSISTENCY_TIMEOUT,
                     RN_SESS_LOOKUP_CONSISTENCY_TIMEOUT_DEFAULT,
                     Type.DURATION,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);
            putState(RN_SESS_LOGOUT_REQUEST_TIMEOUT,
                     RN_SESS_LOGOUT_REQUEST_TIMEOUT_DEFAULT,
                     Type.DURATION,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);
            putState(RN_RH_SO_BACKLOG, RN_RH_SO_BACKLOG_DEFAULT, Type.INT,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);
            putState(RN_RH_SO_CONNECT_TIMEOUT,
                     RN_RH_SO_CONNECT_TIMEOUT_DEFAULT, Type.DURATION,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);
            putState(RN_RH_SO_READ_TIMEOUT, RN_RH_SO_READ_TIMEOUT_DEFAULT,
                     Type.DURATION,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);
            putState(RN_ADMIN_SO_BACKLOG, RN_ADMIN_SO_BACKLOG_DEFAULT,
                     Type.INT,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);
            putState(RN_ADMIN_SO_CONNECT_TIMEOUT,
                     RN_ADMIN_SO_CONNECT_TIMEOUT_DEFAULT, Type.DURATION,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);
            putState(RN_ADMIN_SO_READ_TIMEOUT,
                     RN_ADMIN_SO_READ_TIMEOUT_DEFAULT, Type.DURATION,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);
            putState(RN_MONITOR_SO_BACKLOG,
                     RN_MONITOR_SO_BACKLOG_DEFAULT, Type.INT,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);
            putState(RN_MONITOR_SO_CONNECT_TIMEOUT,
                     RN_MONITOR_SO_CONNECT_TIMEOUT_DEFAULT, Type.DURATION,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);
            putState(RN_MONITOR_SO_READ_TIMEOUT,
                     RN_MONITOR_SO_READ_TIMEOUT_DEFAULT, Type.DURATION,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);
            putState(RN_NUM_GC_LOG_FILES,
                     RN_NUM_GC_LOG_FILES_DEFAULT, Type.INT,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);
            putState(RN_GC_LOG_FILE_SIZE,
                     RN_GC_LOG_FILE_SIZE_DEFAULT, Type.INT,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);
            putState(RN_LOGIN_SO_BACKLOG,
                     RN_LOGIN_SO_BACKLOG_DEFAULT, Type.INT,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);
            putState(RN_LOGIN_SO_CONNECT_TIMEOUT,
                     RN_LOGIN_SO_CONNECT_TIMEOUT_DEFAULT, Type.DURATION,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);
            putState(RN_LOGIN_SO_READ_TIMEOUT,
                     RN_LOGIN_SO_READ_TIMEOUT_DEFAULT, Type.DURATION,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);

            putState(RN_PM_CONCURRENT_SOURCE_LIMIT,
                     RN_PM_CONCURRENT_SOURCE_LIMIT_DEFAULT, Type.INT,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);
            putState(RN_PM_CONCURRENT_TARGET_LIMIT,
                     RN_PM_CONCURRENT_TARGET_LIMIT_DEFAULT, Type.INT,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);
            putState(RN_PM_SO_READ_WRITE_TIMEOUT,
                     RN_PM_SO_READ_WRITE_TIMEOUT_DEFAULT, Type.DURATION,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);
            putState(RN_PM_SO_CONNECT_TIMEOUT,
                     RN_PM_SO_CONNECT_TIMEOUT_DEFAULT, Type.DURATION,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);
            putState(RN_PM_WAIT_AFTER_BUSY,
                     RN_PM_WAIT_AFTER_BUSY_DEFAULT, Type.DURATION,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);
            putState(RN_PM_WAIT_AFTER_ERROR,
                     RN_PM_WAIT_AFTER_ERROR_DEFAULT, Type.DURATION,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);
            putState(RN_NODE_TYPE,
                     RN_NODE_TYPE_DEFAULT, Type.STRING,
                     EnumSet.of(Info.REPNODE, Info.BOOT, Info.RONLY),
                     Scope.SERVICE);

            /**
             * StatsParams (RepNode only)
             */
            putState(SP_MAX_LATENCY, SP_MAX_LATENCY_DEFAULT, Type.DURATION,
                     EnumSet.of(Info.REPNODE, Info.NORESTART, Info.POLICY));
            putState(SP_INTERVAL, SP_INTERVAL_DEFAULT, Type.DURATION,
                     EnumSet.of(Info.REPNODE, Info.NORESTART, Info.POLICY),
                     Scope.STORE);
            putState(SP_DUMP_INTERVAL, SP_DUMP_INTERVAL_DEFAULT, Type.DURATION,
                     EnumSet.of(Info.REPNODE, Info.NORESTART, Info.POLICY,
                                Info.HIDDEN), Scope.STORE);
            putState(SP_ACTIVE_THRESHOLD,
                     SP_ACTIVE_THRESHOLD_DEFAULT, Type.INT,
                     EnumSet.of(Info.REPNODE, Info.NORESTART, Info.POLICY,
                                Info.HIDDEN), Scope.STORE);
            putState(SP_THREAD_DUMP_MAX, SP_THREAD_DUMP_MAX_DEFAULT, Type.INT,
                     EnumSet.of(Info.REPNODE, Info.NORESTART, Info.POLICY,
                                Info.HIDDEN), Scope.STORE);
            putState(SP_COLLECT_ENV_STATS,
                     SP_COLLECT_ENV_STATS_DEFAULT, Type.BOOLEAN,
                     EnumSet.of(Info.REPNODE, Info.NORESTART, Info.POLICY),
                     Scope.STORE);
            putState(SP_LATENCY_CEILING, SP_LATENCY_CEILING_DEFAULT, Type.INT,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.NORESTART),
                     Scope.STORE);
            putState(SP_THROUGHPUT_FLOOR, SP_THROUGHPUT_FLOOR_DEFAULT,
                     Type.INT,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.NORESTART),
                     Scope.STORE);
            putState(SP_COMMIT_LAG_THRESHOLD,
                     SP_COMMIT_LAG_THRESHOLD_DEFAULT, Type.LONG,
                     EnumSet.of(Info.REPNODE, Info.POLICY, Info.NORESTART),
                     Scope.STORE);

            /**
             * StorageNodeParams
             */
            putState(SN_ROOT_DIR_PATH, SN_ROOT_DIR_PATH_DEFAULT,
                     Type.STRING, EnumSet.of(Info.SNA, Info.RONLY));
            putState(SN_DATACENTER, SN_DATACENTER_DEFAULT, Type.INT,
                     EnumSet.of(Info.SNA, Info.RONLY));
            putState(SN_SERVICE_STOP_WAIT, SN_SERVICE_STOP_WAIT_DEFAULT,
                     Type.DURATION,
                     EnumSet.of(Info.SNA, Info.POLICY,
                                Info.HIDDEN, Info.NORESTART), Scope.STORE);
            putState(SN_REPNODE_START_WAIT, SN_REPNODE_START_WAIT_DEFAULT,
                     Type.DURATION,
                     EnumSet.of(Info.SNA, Info.POLICY,
                                Info.HIDDEN, Info.NORESTART), Scope.STORE);
            putState(SN_LOG_FILE_COUNT, SN_LOG_FILE_COUNT_DEFAULT, Type.INT,
                     EnumSet.of(Info.SNA, Info.POLICY), Scope.STORE);
            putState(SN_LOG_FILE_LIMIT, SN_LOG_FILE_LIMIT_DEFAULT, Type.INT,
                     EnumSet.of(Info.SNA, Info.POLICY), Scope.STORE);
            putState(SN_COMMENT, SN_COMMENT_DEFAULT, Type.STRING,
                     EnumSet.of(Info.SNA, Info.RONLY));
            putState(SN_LINK_EXEC_WAIT, SN_LINK_EXEC_WAIT_DEFAULT,
                     Type.DURATION, EnumSet.of(Info.SNA, Info.POLICY,
                                               Info.HIDDEN, Info.NORESTART));
            putState(SN_SOFTWARE_VERSION, SN_SOFTWARE_VERSION_DEFAULT,
                     Type.STRING, EnumSet.of(Info.BOOT, Info.HIDDEN,
                                             Info.NORESTART, Info.RONLY));
            putState(SN_MAX_LINK_COUNT, SN_MAX_LINK_COUNT_DEFAULT, Type.INT,
                     EnumSet.of(Info.SNA, Info.POLICY, Info.HIDDEN,
                                Info.NORESTART));
            putState(SN_RN_HEAP_PERCENT,
                     SN_RN_HEAP_PERCENT_DEFAULT, Type.INT,
                     EnumSet.of(Info.SNA, Info.POLICY, Info.NORESTART),
                     Scope.SERVICE,
                     1, 95, null);
            putState(SN_GC_THREAD_FLOOR,
                     SN_GC_THREAD_FLOOR_DEFAULT, Type.INT,
                     EnumSet.of(Info.SNA, Info.POLICY, Info.NORESTART,
                                Info.HIDDEN));
            putState(SN_GC_THREAD_THRESHOLD,
                     SN_GC_THREAD_THRESHOLD_DEFAULT, Type.INT,
                     EnumSet.of(Info.SNA, Info.POLICY, Info.NORESTART,
                                Info.HIDDEN));
            putState(SN_GC_THREAD_PERCENT,
                     SN_GC_THREAD_PERCENT_DEFAULT, Type.INT,
                     EnumSet.of(Info.SNA, Info.POLICY, Info.NORESTART,
                                Info.HIDDEN));
            putState(SN_PROCESS_STARTUP_PREFIX,
                     SN_PROCESS_STARTUP_PREFIX_DEFAULT,
                     Type.STRING, EnumSet.of(Info.SNA, Info.POLICY,
                                             Info.HIDDEN, Info.NORESTART));

            putState(SN_ADMIN_SO_BACKLOG, SN_ADMIN_SO_BACKLOG_DEFAULT,
                     Type.INT, EnumSet.of(Info.SNA, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);
            /*
             * These timeouts could be NORESTART but it'd mean re-exporting all
             * interfaces.  This is possible but a TODO.  If done, it would not
             * affect existing handles as a restart would.  The timeouts are in
             * milliseconds.
             */
            putState(SN_ADMIN_SO_CONNECT_TIMEOUT,
                     SN_ADMIN_SO_CONNECT_TIMEOUT_DEFAULT, Type.DURATION,
                     EnumSet.of(Info.SNA, Info.POLICY, Info.HIDDEN));
            putState(SN_ADMIN_SO_READ_TIMEOUT,
                     SN_ADMIN_SO_READ_TIMEOUT_DEFAULT, Type.DURATION,
                     EnumSet.of(Info.SNA, Info.POLICY, Info.HIDDEN));

            putState(SN_MONITOR_SO_BACKLOG, SN_MONITOR_SO_BACKLOG_DEFAULT,
                     Type.INT, EnumSet.of(Info.SNA, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);
            putState(SN_MONITOR_SO_CONNECT_TIMEOUT,
                     SN_MONITOR_SO_CONNECT_TIMEOUT_DEFAULT, Type.DURATION,
                     EnumSet.of(Info.SNA, Info.POLICY, Info.HIDDEN));
            putState(SN_MONITOR_SO_READ_TIMEOUT,
                     SN_MONITOR_SO_READ_TIMEOUT_DEFAULT, Type.DURATION,
                     EnumSet.of(Info.SNA, Info.POLICY, Info.HIDDEN));
            putState(SN_LOGIN_SO_BACKLOG, SN_LOGIN_SO_BACKLOG_DEFAULT,
                     Type.INT, EnumSet.of(Info.SNA, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);
            putState(SN_LOGIN_SO_CONNECT_TIMEOUT,
                     SN_LOGIN_SO_CONNECT_TIMEOUT_DEFAULT, Type.DURATION,
                     EnumSet.of(Info.SNA, Info.POLICY, Info.HIDDEN));
            putState(SN_LOGIN_SO_READ_TIMEOUT,
                     SN_LOGIN_SO_READ_TIMEOUT_DEFAULT, Type.DURATION,
                     EnumSet.of(Info.SNA, Info.POLICY, Info.HIDDEN));
            putState(SN_REGISTRY_SO_BACKLOG,
                     SN_REGISTRY_SO_BACKLOG_DEFAULT, Type.INT,
                     EnumSet.of(Info.SNA, Info.POLICY, Info.HIDDEN),
                     Scope.STORE);
            putState(SN_REGISTRY_SO_CONNECT_TIMEOUT,
                     SN_REGISTRY_SO_CONNECT_TIMEOUT_DEFAULT, Type.DURATION,
                     EnumSet.of(Info.SNA, Info.POLICY, Info.HIDDEN));
            putState(SN_REGISTRY_SO_READ_TIMEOUT,
                     SN_REGISTRY_SO_READ_TIMEOUT_DEFAULT, Type.DURATION,
                     EnumSet.of(Info.SNA, Info.POLICY, Info.HIDDEN));

            putState(ADMIN_COMMAND_SERVICE_SO_BACKLOG,
                     ADMIN_COMMAND_SERVICE_SO_BACKLOG_DEFAULT,
                     Type.INT, EnumSet.of(Info.ADMIN, Info.SNA, Info.POLICY,
                                          Info.HIDDEN),
                     Scope.STORE);
            putState(ADMIN_LISTENER_SO_BACKLOG,
                     ADMIN_LISTENER_SO_BACKLOG_DEFAULT,
                     Type.INT, EnumSet.of(Info.ADMIN, Info.SNA, Info.POLICY,
                                          Info.HIDDEN),
                     Scope.STORE);
            putState(ADMIN_LOGIN_SO_BACKLOG,
                     ADMIN_LOGIN_SO_BACKLOG_DEFAULT,
                     Type.INT, EnumSet.of(Info.ADMIN, Info.SNA, Info.POLICY,
                                          Info.HIDDEN),
                     Scope.STORE);

            /**
             * SecurityParams
             */
            putState(SEC_SECURITY_ENABLED, "false",
                     Type.BOOLEAN, EnumSet.of(Info.SECURITY, Info.RONLY));
            putState(SEC_KEYSTORE_FILE, "",
                     Type.STRING, EnumSet.of(Info.SECURITY, Info.RONLY));
            putState(SEC_KEYSTORE_TYPE, "",
                     Type.STRING, EnumSet.of(Info.SECURITY, Info.RONLY));
            putState(SEC_KEYSTORE_PWD_ALIAS, "",
                     Type.STRING, EnumSet.of(Info.SECURITY, Info.RONLY));
            putState(SEC_TRUSTSTORE_FILE, "",
                     Type.STRING, EnumSet.of(Info.SECURITY, Info.RONLY));
            putState(SEC_TRUSTSTORE_TYPE, "",
                     Type.STRING, EnumSet.of(Info.SECURITY, Info.RONLY));
            putState(SEC_PASSWORD_FILE, "",
                     Type.STRING, EnumSet.of(Info.SECURITY, Info.RONLY));
            putState(SEC_PASSWORD_CLASS, "",
                     Type.STRING, EnumSet.of(Info.SECURITY, Info.RONLY));
            putState(SEC_WALLET_DIR, "",
                     Type.STRING, EnumSet.of(Info.SECURITY, Info.RONLY));
            putState(SEC_INTERNAL_AUTH, "",
                     Type.STRING, EnumSet.of(Info.SECURITY, Info.RONLY));
            putState(SEC_CERT_MODE, "",
                     Type.STRING, EnumSet.of(Info.SECURITY, Info.RONLY));

            putState(SEC_TRANS_TYPE, "",
                     Type.STRING, EnumSet.of(Info.TRANSPORT, Info.RONLY));
            putState(SEC_TRANS_FACTORY, "",
                     Type.STRING, EnumSet.of(Info.TRANSPORT, Info.RONLY));
            putState(SEC_TRANS_SERVER_KEY_ALIAS, "",
                     Type.STRING, EnumSet.of(Info.TRANSPORT, Info.RONLY));
            putState(SEC_TRANS_CLIENT_KEY_ALIAS, "",
                     Type.STRING, EnumSet.of(Info.TRANSPORT, Info.RONLY));
            putState(SEC_TRANS_CLIENT_AUTH_REQUIRED, "false",
                     Type.BOOLEAN, EnumSet.of(Info.TRANSPORT, Info.RONLY));
            putState(SEC_TRANS_CLIENT_IDENT_ALLOW, "",
                     Type.STRING, EnumSet.of(Info.TRANSPORT, Info.RONLY));
            putState(SEC_TRANS_SERVER_IDENT_ALLOW, "",
                     Type.STRING, EnumSet.of(Info.TRANSPORT, Info.RONLY));
            putState(SEC_TRANS_ALLOW_CIPHER_SUITES, "",
                     Type.STRING, EnumSet.of(Info.TRANSPORT, Info.RONLY));
            putState(SEC_TRANS_ALLOW_PROTOCOLS, "",
                     Type.STRING, EnumSet.of(Info.TRANSPORT, Info.RONLY));
            putState(SEC_TRANS_CLIENT_ALLOW_CIPHER_SUITES, "",
                     Type.STRING, EnumSet.of(Info.TRANSPORT, Info.RONLY));
            putState(SEC_TRANS_CLIENT_ALLOW_PROTOCOLS, "",
                     Type.STRING, EnumSet.of(Info.TRANSPORT, Info.RONLY));
        }};

    public static Set<Map.Entry<String, ParameterState>> getAllParameters() {
        return pstate.entrySet();
    }

    public Object getDefaultParameter() {
        return defaultParam;
    }

    public static Map<String, ParameterState> getMap() {
        return pstate;
    }
}
