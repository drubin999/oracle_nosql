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

import static oracle.kv.impl.param.ParameterState.ADMIN_COMMAND_SERVICE_SO_BACKLOG;
import static oracle.kv.impl.param.ParameterState.ADMIN_LISTENER_SO_BACKLOG;
import static oracle.kv.impl.param.ParameterState.ADMIN_LOGIN_SO_BACKLOG;
import static oracle.kv.impl.param.ParameterState.BOOTSTRAP_MOUNT_POINTS;
import static oracle.kv.impl.param.ParameterState.COMMON_CAPACITY;
import static oracle.kv.impl.param.ParameterState.COMMON_HA_HOSTNAME;
import static oracle.kv.impl.param.ParameterState.COMMON_HOSTNAME;
import static oracle.kv.impl.param.ParameterState.COMMON_MASTER_BALANCE;
import static oracle.kv.impl.param.ParameterState.COMMON_MEMORY_MB;
import static oracle.kv.impl.param.ParameterState.COMMON_MGMT_CLASS;
import static oracle.kv.impl.param.ParameterState.COMMON_MGMT_POLL_PORT;
import static oracle.kv.impl.param.ParameterState.COMMON_MGMT_TRAP_HOST;
import static oracle.kv.impl.param.ParameterState.COMMON_MGMT_TRAP_PORT;
import static oracle.kv.impl.param.ParameterState.COMMON_NUMCPUS;
import static oracle.kv.impl.param.ParameterState.COMMON_PORTRANGE;
import static oracle.kv.impl.param.ParameterState.COMMON_REGISTRY_PORT;
import static oracle.kv.impl.param.ParameterState.COMMON_SERVICE_PORTRANGE;
import static oracle.kv.impl.param.ParameterState.COMMON_SN_ID;
import static oracle.kv.impl.param.ParameterState.COMMON_USE_CLIENT_SOCKET_FACTORIES;
import static oracle.kv.impl.param.ParameterState.SNA_TYPE;
import static oracle.kv.impl.param.ParameterState.SN_ADMIN_SO_BACKLOG;
import static oracle.kv.impl.param.ParameterState.SN_ADMIN_SO_CONNECT_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.SN_ADMIN_SO_READ_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.SN_COMMENT;
import static oracle.kv.impl.param.ParameterState.SN_LINK_EXEC_WAIT;
import static oracle.kv.impl.param.ParameterState.SN_LOG_FILE_COUNT;
import static oracle.kv.impl.param.ParameterState.SN_LOG_FILE_LIMIT;
import static oracle.kv.impl.param.ParameterState.SN_MAX_LINK_COUNT;
import static oracle.kv.impl.param.ParameterState.SN_LOGIN_SO_BACKLOG;
import static oracle.kv.impl.param.ParameterState.SN_LOGIN_SO_CONNECT_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.SN_LOGIN_SO_READ_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.SN_MONITOR_SO_BACKLOG;
import static oracle.kv.impl.param.ParameterState.SN_MONITOR_SO_CONNECT_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.SN_MONITOR_SO_READ_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.SN_REGISTRY_SO_BACKLOG;
import static oracle.kv.impl.param.ParameterState.SN_REGISTRY_SO_CONNECT_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.SN_REGISTRY_SO_READ_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.SN_REPNODE_START_WAIT;
import static oracle.kv.impl.param.ParameterState.SN_RN_HEAP_PERCENT;
import static oracle.kv.impl.param.ParameterState.SN_ROOT_DIR_PATH;
import static oracle.kv.impl.param.ParameterState.SN_SERVICE_STOP_WAIT;
import static oracle.kv.impl.util.registry.RegistryUtils.InterfaceType.LOGIN;
import static oracle.kv.impl.util.registry.RegistryUtils.InterfaceType.MAIN;
import static oracle.kv.impl.util.registry.RegistryUtils.InterfaceType.TRUSTED_LOGIN;
import java.io.Serializable;
import java.util.EnumSet;
import java.util.List;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.param.DefaultParameter;
import oracle.kv.impl.param.DurationParameter;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterState.Info;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.PortRange;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.registry.RMISocketPolicy;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryArgs;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryPair;
import oracle.kv.impl.util.registry.ServerSocketFactory;
import oracle.kv.impl.util.registry.ClearServerSocketFactory;
import com.sleepycat.persist.model.Persistent;

/**
 * A class implementing StorageNodeParams contains the per-StorageNode
 * operational parameters, and information about the machine on which it runs.
 */
@Persistent(version=1)
public class StorageNodeParams implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The name of the SSF associated with the registry.
     */
    private static final String REGISTRY_SSF_NAME = "registry";

    /**
     * A name to supply as the store name portion of a CSF name.  This name
     * will not conflict with any store name that the configure command has
     * accepted as valid.
     */
    private static final String CSF_EMPTY_STORE_NAME = "$";

    public final static RNHeapAndCacheSize NO_CACHE_AND_HEAP_SPEC =
        new RNHeapAndCacheSize(0, 0);

    private ParameterMap map;
    private ParameterMap mountMap;

    public static final String[] REGISTRATION_PARAMS = {
        COMMON_HA_HOSTNAME,
        COMMON_PORTRANGE,
        COMMON_SERVICE_PORTRANGE,
        COMMON_CAPACITY,
        COMMON_NUMCPUS,
        COMMON_MEMORY_MB,
        COMMON_MGMT_CLASS,
        COMMON_MGMT_POLL_PORT,
        COMMON_MGMT_TRAP_HOST,
        COMMON_MGMT_TRAP_PORT,
        SN_ROOT_DIR_PATH
    };

    /* For DPL */
    public StorageNodeParams() {
    }

    public StorageNodeParams(ParameterMap map) {
        this(map, null);
    }

    public StorageNodeParams(ParameterMap map, ParameterMap mountMap) {
        this.map = map;
        map.setType(SNA_TYPE);
        map.setName(SNA_TYPE);
        this.mountMap = mountMap;
    }

    public StorageNodeParams(LoadParameters lp) {
        map = lp.getMap(SNA_TYPE);
        mountMap = lp.getMap(BOOTSTRAP_MOUNT_POINTS);
    }

    /**
     * Used by the Admin to hold values obtained from the GUI and passed to the
     * Planner.
     */
    public StorageNodeParams(String hostname,
                             int registryPort,
                             String comment) {
        this(null, hostname, registryPort, comment);
    }

    public StorageNodeParams(StorageNodeId storageNodeId,
                             String hostname,
                             int registryPort,
                             String comment) {
        this(new ParameterMap(SNA_TYPE,
                              SNA_TYPE),
             storageNodeId, hostname, registryPort, comment);
    }

    /**
     * Used by the planner to create a StorageNodeParams to register a new
     * StorageNode. The root dir, haHostName, and haPortRange fields are
     * uninitialized, because it is unknown before the node is registered.
     * These values are dictated by the SNA.
     */
    public StorageNodeParams(ParameterMap map,
                             StorageNodeId storageNodeId,
                             String hostname,
                             int registryPort,
                             String comment) {

        this.map = map;

        map.setType(SNA_TYPE);
        map.setName(SNA_TYPE);
        if (storageNodeId != null) {
            setStorageNodeId(storageNodeId);
        }
        setHostname(hostname);
        setRegistryPort(registryPort);
        if (comment != null) {
            setComment(comment);
        }
        addDefaults();
        map = getFilteredMap();
    }

    private void addDefaults() {
        map.addServicePolicyDefaults(Info.SNA);
    }

    public ParameterMap getFilteredMap() {
        return map.filter(EnumSet.of(Info.SNA));
    }

    public ParameterMap getMap() {
        return map;
    }

    public ParameterMap getMountMap() {
        return mountMap;
    }

    public void setMountMap(ParameterMap mountMap) {
        this.mountMap = mountMap;
    }

    public List<String> getMountPoints() {
        if (mountMap == null) {
            return null;
        }
        return BootstrapParams.getMountPoints(mountMap);
    }

    /**
     * Returns a ParameterMap that can be used to add or remove mount points
     * for a given SN.
     * @param snp the StorageNodeParams used to initialize the mount map
     * @param add if true, add the specified mount point. If false, remove that
     * mount point.
     * @param mountPoint the value to add or remove from the list of mount
     * points
     */
    public static ParameterMap changeMountMap(Parameters params,
                                              StorageNodeParams snp,
                                              boolean add,
                                              String mountPoint)
        throws IllegalCommandException {

        ParameterMap mountMap = snp.getMountMap();

        if (!add) {
            if (mountMap == null || !mountMap.exists(mountPoint)) {
                throw new IllegalCommandException
                    ("Can't remove non-existent storage directory: " +
                      mountPoint);
            }
        }

        ParameterMap newMap;
        if (mountMap != null) {
            newMap = mountMap.copy();
        } else {
            newMap = BootstrapParams.createMountMap();
        }

        if (add) {
            BootstrapParams.addMountPoint(newMap, mountPoint);
        } else {
            newMap.remove(mountPoint);
        }

        /* Check if this mount point is in use by an RN. */
        String badMapInfo = validateMountMap(mountMap, params,
                                             snp.getStorageNodeId());
        if (badMapInfo != null) {
            throw new IllegalCommandException(badMapInfo);
        }
        return newMap;
    }

    /**
     * Return a non-null error message if a mount point is in use.
     */
    public static String validateMountMap(ParameterMap mountMap,
                                          Parameters p,
                                          StorageNodeId snid) {
        for (RepNodeParams rnp : p.getRepNodeParams()) {
            if (rnp.getStorageNodeId().equals(snid)) {
                String mp = rnp.getMountPointString();
                if (mp != null && !mountMap.exists(mp)) {
                    return "Cannot remove an in-use storage directory. " +
                           "Storage directory " +  mp +
                           " is in use by RepNode " + rnp.getRepNodeId();
                }
            }
        }
        return null;
    }

    /**
     * The StorageNodeAgent is the owner and arbiter of these fields. Set
     * these fields in this instance using the returned values from the SNA.
     */
    public void setInstallationInfo(ParameterMap bootMap,
                                    ParameterMap mountMap,
                                    boolean hostingAdmin) {
        if (bootMap != null) {
            BootstrapParams bp = new BootstrapParams(bootMap, mountMap);
            setRootDirPath(bp.getRootdir());
            setHAHostname(bp.getHAHostname());
            String haPortRange = bp.getHAPortRange();
            PortRange.validateHA(haPortRange);
            setHAPortRange(haPortRange);

            String servicePortRange = bp.getServicePortRange();
            if (servicePortRange != null) {
                PortRange.validateService(servicePortRange);
                setServicePortRange(servicePortRange);
                PortRange.validateDisjoint(haPortRange, servicePortRange);
            }
            setCapacity(bp.getCapacity());

            if (servicePortRange != null) {
                /* Verify that the port range is sufficient */
                final boolean isSecure = bp.getSecurityDir() != null;
                PortRange.validateSufficientPorts(servicePortRange,
                                                  bp.getCapacity(),
                                                  isSecure,
                                                  hostingAdmin,
                                                  bp.getMgmtPorts());
            }
            setNumCPUs(bp.getNumCPUs());
            setMemoryMB(bp.getMemoryMB());
            setMgmtClass(bp.getMgmtClass());
            setMgmtPollingPort(bp.getMgmtPollingPort());
            setMgmtTrapHost(bp.getMgmtTrapHost());
            setMgmtTrapPort(bp.getMgmtTrapPort());
        }
        this.mountMap = mountMap.copy();
    }

    /**
     * Return false for a StorageNodeParams that has a blank root dir,
     * HA hostname and HA portrange. This is true when the SNA has not yet
     * been registered.
     */
    public boolean isRegistered() {
        return ((getHAHostname() != null) &&
                (getRootDirPath() != null) &&
                (getHAPortRange() != null));
    }

    public String getHostname() {
        return map.get(COMMON_HOSTNAME).asString();
    }

    public void setHostname(String hostname) {
        map.setParameter(COMMON_HOSTNAME, hostname);
    }

    public String getHAHostname() {
        return map.get(COMMON_HA_HOSTNAME).asString();
    }

    public void setHAHostname(String hostname) {
        map.setParameter(COMMON_HA_HOSTNAME, hostname);
    }

    public String getHAPortRange() {
        return map.get(COMMON_PORTRANGE).asString();
    }

    public void setHAPortRange(String range) {
        map.setParameter(COMMON_PORTRANGE, range);
    }

    public String getServicePortRange() {
        return map.getOrDefault(COMMON_SERVICE_PORTRANGE).asString();
    }

    public void setServicePortRange(String range) {
        map.setParameter(COMMON_SERVICE_PORTRANGE, range);
    }

    /**
     * Get the root directory.
     */
    public String getRootDirPath() {
        return map.get(SN_ROOT_DIR_PATH).asString();
    }

    public void setRootDirPath(String path) {
        map.setParameter(SN_ROOT_DIR_PATH, path);
    }

    /**
     * Physical topo parameters from the bootstrap configuration.
     */
    public void setNumCPUs(int ncpus) {
        map.setParameter(COMMON_NUMCPUS,
                         Integer.toString(ncpus));
    }

    public int getNumCPUs() {
        return map.getOrZeroInt(COMMON_NUMCPUS);
    }

    public void setCapacity(int capacity) {
        map.setParameter(COMMON_CAPACITY,
                         Integer.toString(capacity));
    }

    public void setMemoryMB(int memoryMB) {
        map.setParameter(COMMON_MEMORY_MB,
                         Integer.toString(memoryMB));
    }

    public int getMemoryMB() {
        return map.getOrZeroInt(COMMON_MEMORY_MB);
    }

    /**
     * Returns the registry port number.
     */
    public int getRegistryPort() {
        return map.getOrZeroInt(COMMON_REGISTRY_PORT);
    }

    public void setRegistryPort(int port) {
        map.setParameter(COMMON_REGISTRY_PORT,
                         Integer.toString(port));
    }

    public StorageNodeId getStorageNodeId() {
        return new StorageNodeId
            (map.getOrZeroInt(COMMON_SN_ID));
    }

    public void setStorageNodeId(StorageNodeId snid) {
        map.setParameter(COMMON_SN_ID,
                         Integer.toString(snid.getStorageNodeId()));
    }

    /**
     * Return a free-form comment string associated with the Node.
     */
    public String getComment() {
        return map.get(SN_COMMENT).asString();
    }

    /**
     * Set a free-form comment string associated with the Node.
     */
    public void setComment(String c) {
        map.setParameter(SN_COMMENT, c);
    }

    /**
     * @return the loggingFileCount
     */
    public int getLogFileCount() {
        return map.getOrZeroInt(SN_LOG_FILE_COUNT);
    }

    /**
     * @return the loggingFileLimit
     */
    public int getLogFileLimit() {
        return map.getOrZeroInt(SN_LOG_FILE_LIMIT);
    }

    public int getServiceWaitMillis() {
        return (int) ParameterUtils.getDurationMillis
            (map, SN_SERVICE_STOP_WAIT);
    }

    public int getRepNodeStartSecs() {
        return ((int) ParameterUtils.getDurationMillis
                (map, SN_REPNODE_START_WAIT))/1000;
    }

    public int getLinkExecWaitSecs() {
        return ((int) ParameterUtils.getDurationMillis
                (map, SN_LINK_EXEC_WAIT))/1000;
    }

    public int getMaxLinkCount() {
        return map.getOrZeroInt(SN_MAX_LINK_COUNT);
    }

    private int getDurationParamMillis(String param) {
        return (int)ParameterUtils.getDurationMillis(map, param);
    }

    private static int getDefaultDurationParamMillis(String param) {
        DurationParameter dp =
            (DurationParameter) DefaultParameter.getDefaultParameter(param);

        return (int) dp.toMillis();
    }

    /**
     * Create a string of the form snId(hostname:registryPort) to clearly
     * identify this SN, usually for debugging purposes.
     */
    public String displaySNIdAndHost() {
        return getStorageNodeId() + "(" + getHostname() + ":" +
            getRegistryPort() + ")";
    }

    /**
     * Returns the SFP used by the Admin for its CS
     */
    public SocketFactoryPair getAdminCommandServiceSFP(RMISocketPolicy policy,
                                                       String storeName) {
        SocketFactoryArgs args = new SocketFactoryArgs();

        final String csfName = ClientSocketFactory.factoryName(
            (storeName == null ? CSF_EMPTY_STORE_NAME : storeName),
            AdminId.getPrefix(), MAIN.interfaceName());

        args.setSsfName(MAIN.interfaceName()).
            setSsfBacklog(map.getOrDefault(
                              ADMIN_COMMAND_SERVICE_SO_BACKLOG).asInt()).
            setSsfPortRange(getServicePortRange()).
            setCsfName(csfName);

        return policy.getBindPair(args);
    }

    /**
     * Returns the SFP used by the Admin for its ULS
     */
    public SocketFactoryPair getAdminLoginSFP(RMISocketPolicy policy,
                                              String storeName) {
        SocketFactoryArgs args = new SocketFactoryArgs();

        final String csfName = ClientSocketFactory.factoryName(
            (storeName == null ? CSF_EMPTY_STORE_NAME : storeName),
            AdminId.getPrefix(), LOGIN.interfaceName());

        args.setSsfName(LOGIN.interfaceName()).
            setSsfBacklog(map.getOrDefault(
                              ADMIN_LOGIN_SO_BACKLOG).asInt()).
            setSsfPortRange(getServicePortRange()).
            setCsfName(csfName);

        return policy.getBindPair(args);
    }

    /**
     * Returns the SSF used by Admin Listeners.  These listeners are not
     * servers, and so do not have access to the keystores needed to function
     * as an SSL server, so we always create these as clear.  This is
     * unfortunate in a secure environment, but the information transmitted is
     * very low in sensitivity. The information transmitted is simply a wake-up
     * call with a timestamp to notify listeners that have registered that
     * a new event is available to be retrieved.  The actual event is retrieved
     * through the secure interface.
     * See oracle.kv.impl.admin.criticalevent.EventRecorder and
     * oracle.kv.impl.monitor.TrackerListener for usage.
     */
    public ServerSocketFactory getAdminListenerSSF() {
        return ClearServerSocketFactory.create
            (MAIN.interfaceName(),
             map.getOrDefault(ADMIN_LISTENER_SO_BACKLOG).asInt(),
             getServicePortRange());
    }

    /**
     * Returns the SFP used by the SNA's Admin service
     */
    public SocketFactoryPair getStorageNodeAdminSFP(RMISocketPolicy policy,
                                                    String csfName) {
        SocketFactoryArgs args = new SocketFactoryArgs();

        args.setSsfName(MAIN.interfaceName()).
            setSsfBacklog(map.getOrDefault(SN_ADMIN_SO_BACKLOG).asInt()).
            setSsfPortRange(getServicePortRange()).
            setCsfName(csfName).
            setUseCsf(getUseClientSocketFactory()).
            setCsfConnectTimeout(getDurationParamMillis(
                                     SN_ADMIN_SO_CONNECT_TIMEOUT)).
            setCsfReadTimeout(getDurationParamMillis(
                                  SN_ADMIN_SO_READ_TIMEOUT));

        return policy.getBindPair(args);
    }

    /**
     * Returns the SFP used by the SNA's TrustedLogin service
     */
    public SocketFactoryPair getTrustedLoginSFP(RMISocketPolicy policy,
                                                String csfName) {
        SocketFactoryArgs args = new SocketFactoryArgs();

        args.setSsfName(TRUSTED_LOGIN.interfaceName()).
            setSsfBacklog(map.getOrDefault(SN_LOGIN_SO_BACKLOG).asInt()).
            setSsfPortRange(getServicePortRange()).
            setCsfName(csfName).
            setUseCsf(getUseClientSocketFactory()).
            setCsfConnectTimeout(getDurationParamMillis(
                                     SN_LOGIN_SO_CONNECT_TIMEOUT)).
            setCsfReadTimeout(getDurationParamMillis(
                                  SN_LOGIN_SO_READ_TIMEOUT));

        return policy.getBindPair(args);
    }

    public SocketFactoryPair getMonitorSFP(RMISocketPolicy policy,
                                           String csfName) {
        SocketFactoryArgs args = new SocketFactoryArgs();

        args.setSsfName(MAIN.interfaceName()).
            setSsfBacklog(map.getOrDefault(SN_MONITOR_SO_BACKLOG).asInt()).
            setSsfPortRange(getServicePortRange()).
            setCsfName(csfName).
            setUseCsf(getUseClientSocketFactory()).
            setCsfConnectTimeout(getDurationParamMillis(
                                     SN_MONITOR_SO_CONNECT_TIMEOUT)).
            setCsfReadTimeout(getDurationParamMillis(
                                  SN_MONITOR_SO_READ_TIMEOUT));

        return policy.getBindPair(args);
    }


    public SocketFactoryPair getRegistrySFP(RMISocketPolicy policy) {
        SocketFactoryArgs args = new SocketFactoryArgs();

        args.setSsfName(REGISTRY_SSF_NAME).
            setSsfBacklog(map.getOrDefault(SN_REGISTRY_SO_BACKLOG).asInt()).
            setSsfPortRange(PortRange.UNCONSTRAINED).
            setCsfName(ClientSocketFactory.registryFactoryName()).
            setUseCsf(getUseClientSocketFactory()).
            setCsfConnectTimeout(0).
            setCsfReadTimeout(0);

        return policy.getRegistryPair(args);
    }

    public static SocketFactoryPair getDefaultRegistrySFP(
        RMISocketPolicy policy) {

        SocketFactoryArgs args = new SocketFactoryArgs();

        args.setSsfName(MAIN.interfaceName()).
            setSsfBacklog(DefaultParameter.getDefaultParameter(
                              ADMIN_LISTENER_SO_BACKLOG).asInt()).
            setSsfPortRange(DefaultParameter.getDefaultParameter(
                                COMMON_SERVICE_PORTRANGE).asString()).
            setCsfName(ClientSocketFactory.registryFactoryName()).
            setCsfConnectTimeout(getDefaultDurationParamMillis(
                                     SN_MONITOR_SO_CONNECT_TIMEOUT)).
            setCsfReadTimeout(getDefaultDurationParamMillis(
                                  SN_MONITOR_SO_READ_TIMEOUT));

        /*
         * TODO: consider whether this conditional block can be removed given
         * changes to the ClientSocketFactory mechanism.
         */
        if (TestStatus.isActive() && policy.isPolicyOptional()) {

            /*
             * Don't use socket factories for most tests.
             */
            return new SocketFactoryPair(null, null);
        }

        return policy.getRegistryPair(args);
    }

    /**
     * Note that this factory is only for use on the requesting client side. We
     * do not make provisions for supplying a CSF when creating a registry,
     * it's always null when the registry is created.
     */
    public void setRegistryCSF(SecurityParams sp) {

        final RMISocketPolicy rmiPolicy = sp.getRMISocketPolicy();

        final SocketFactoryArgs args = new SocketFactoryArgs().
            setCsfName(ClientSocketFactory.registryFactoryName()).
            setUseCsf(getUseClientSocketFactory()).
            setCsfConnectTimeout(
                getDurationParamMillis(SN_REGISTRY_SO_CONNECT_TIMEOUT)).
            setCsfReadTimeout(
                getDurationParamMillis(SN_REGISTRY_SO_READ_TIMEOUT));

        RegistryUtils.setServerRegistryCSF(rmiPolicy.getRegistryCSF(args));
    }

    // TODO: revisit after USE_CLIENT_SOCKET_FACTORIES is a global parameter
    public boolean getUseClientSocketFactory() {
        return (!ClientSocketFactory.isDisabled() &&
                map.exists(COMMON_USE_CLIENT_SOCKET_FACTORIES)) ?
            map.get(COMMON_USE_CLIENT_SOCKET_FACTORIES).asBoolean() :
            false;
    }

    public int getCapacity() {
        return map.getOrDefault(COMMON_CAPACITY).asInt();
    }

    public boolean getMasterBalance() {
        return map.getOrDefault(COMMON_MASTER_BALANCE).asBoolean();
    }

    public void setMasterBalance(boolean masterBalance) {
        map.setParameter(COMMON_MASTER_BALANCE,
                         (masterBalance ? "true" : "false"));
    }

    /**
     * If mgmt class is not set, return the default no-op class name.
     */
    public String getMgmtClass() {
        return map.getOrDefault(COMMON_MGMT_CLASS).asString();
    }

    public void setMgmtClass(String mgmtClass) {
        map.setParameter(COMMON_MGMT_CLASS, mgmtClass);
    }

    /**
     * Return the snmp polling port, or zero if it is not set.
     */
    public int getMgmtPollingPort() {
        return map.getOrZeroInt(COMMON_MGMT_POLL_PORT);
    }

    public void setMgmtPollingPort(int port) {
        map.setParameter(COMMON_MGMT_POLL_PORT, Integer.toString(port));
    }

    /**
     * If the trap host string is not set, just return null.
     */
    public String getMgmtTrapHost() {
        return map.get(COMMON_MGMT_TRAP_HOST).asString();
    }

    public void setMgmtTrapHost(String hostname) {
        map.setParameter(COMMON_MGMT_TRAP_HOST, hostname);
    }

    /**
     * Return the snmp trap port, or zero if it is not set.
     */
    public int getMgmtTrapPort() {
        return map.getOrZeroInt(COMMON_MGMT_TRAP_PORT);
    }

    public void setMgmtTrapPort(int port) {
        map.setParameter(COMMON_MGMT_TRAP_PORT, Integer.toString(port));
    }

    /*
     * Validate parameters.  TODO: implement more validation.
     */
    public void validate() {
        String servicePortRange =
            map.get(COMMON_SERVICE_PORTRANGE).toString();
        if (servicePortRange != null) {
            PortRange.validateService(servicePortRange);
            String haRange = getHAPortRange();
            if (haRange != null) {
                PortRange.validateDisjoint(servicePortRange, haRange);
            }
        }
    }

    /**
     * The min value used in the parallel gc thread calculation, which is
     * min(4, <num cores in the node>);
     */
    public int getGCThreadFloor() {
        return map.getOrDefault(ParameterState.SN_GC_THREAD_FLOOR).asInt();
    }

    /**
     * The max value used in the parallel gc thread calculation, which is
     * 8 as defined by Java Performance.
     */
    public int getGCThreadThreshold() {
        return map.getOrDefault(ParameterState.SN_GC_THREAD_THRESHOLD).asInt();
    }

    /**
     * The percentage to use for cpus over the max value used in the parallel
     * gc thread calculation, which is 5/8 as defined by Java Performance.
     */
    public int getGCThreadPercent() {
        return map.getOrDefault(ParameterState.SN_GC_THREAD_PERCENT).asInt();
    }

    /**
     * The -XX:ParallelGCThreads value is set based on the guidelines in the
     * Java Performance book for Java 6 Update 23.
     *
     * "As of Java 6 Update 23, the number of parallel garbage collection
     * threads defaults to the number returned by the Java API
     * Runtime.availableProcessors() if the number returned is less than or
     * equal to 8; otherwise, it defaults to 5/8 the number returned by
     * Runtime.availableProcessors(). A general guideline for the number of
     * parallel garbage collection threads to set in the presence of multiple
     * applications on the same system is taking the total number of virtual
     * processors (the value returned by Runtime.availableProcessors()) and
     * dividing it by the number of applications running on the system,
     * assuming that load and Java heap sizes are similar among the
     * applications."
     *
     * The definition above creates a graph that ascends in a linear fashion
     * and then decreases in slope when the x axis reaches 8. We've found that
     * a gc thread value of less than 4 is insufficient, so we are adding a
     * floor to the graph. The minimum gc threads for any RN will be
     * MIN(<gcThreadFloor which defaults to 4>,<num cpus in the node>).
     */
    public int calcGCThreads() {
        return calcGCThreads(getNumCPUs(),
                             getCapacity(),
                             getGCThreadFloor(),
                             getGCThreadThreshold(),
                             getGCThreadPercent());
    }

    public static int calcGCThreads(int numCPUs,
                                    int capacity,
                                    int gcThreadFloor,
                                    int gcThreadThreshold,
                                    int gcThreadPercent) {

        /* Enforce the condition that gcThreadFloor is <= gcThreadThreshold */
        int floor = (gcThreadFloor < gcThreadThreshold) ? gcThreadFloor :
            gcThreadThreshold;
        /*
         * The absolute minimum number of gc threads we will use is the smaller
         * of the number of cpus, or the floor
         */
        int minGCThreads = Math.min(numCPUs, floor);
        int numCPUsPerRN = numCPUs/capacity;

        if (numCPUs == 0) {
            /* we are not specifying gc threads */
            return 0;
        }

        /* Return the calculated minimum */
        if (numCPUsPerRN <= minGCThreads) {
            return minGCThreads;
        }

        /*
         * Number of threads returned increases as a function of the number
         * of cpus.
         */
        if (numCPUsPerRN <= gcThreadThreshold){
            return numCPUsPerRN;
        }

        /* curve flattens out. */
        return Math.max(gcThreadThreshold,
                        ((numCPUsPerRN * gcThreadPercent)/100));
    }

    /**
     * Return the percentage of SN memory that will be allocated for the
     * RN processes that are hosted by this SN.
     */
    public int getRNHeapPercent() {
        return map.getOrDefault(SN_RN_HEAP_PERCENT).asInt();
    }

    /**
     * @param numRNsOnSN is the number of RNS hosted by a SN. If the number of
     * RNs is > capacity of the hosting SN, we will use that existing RN number
     * to divide the memory.
     *
     * This can happen if we are decreasing the capacity of a SN that already
     * hosts RNs, or if are migrating the contents of one SN to another.  We do
     * not want to use capacity only, because this could result in a SN hosting
     * multiple RNs that collectively exceed the memory of the node.
     *
     * When this happens, we have overridden the rule that capacity determines
     * heap sizes. We will have to check memory/heap sizings when RNs are
     * deployed or relocated, to see if the sizings need to be changed to
     * conform more to the capacity rules.
     */
    public RNHeapAndCacheSize calculateRNHeapAndCache(ParameterMap policyMap,
                                                      int numRNsOnSN,
                                                      int rnCachePercent) {
        return calculateRNHeapAndCache(policyMap,
                                       getCapacity(),
                                       numRNsOnSN,
                                       getMemoryMB(),
                                       getRNHeapPercent(),
                                       rnCachePercent);
    }

    public static RNHeapAndCacheSize
        calculateRNHeapAndCache(ParameterMap policyMap,
                                int capacity,
                                int numRNsOnSN,
                                int memoryMB,
                                int rnHeapPercent,
                                int rnCachePercent) {

        String jvmArgs = policyMap.getOrDefault(ParameterState.JVM_MISC).
            asString();
        long policyHeapBytes = RepNodeParams.parseJVMArgsForHeap
            (RepNodeParams.XMX_FLAG, jvmArgs);
        long policyCacheBytes =
            policyMap.getOrDefault(ParameterState.JE_CACHE_SIZE).asLong();

        if (memoryMB == 0) {
            /*
             * Memory not specified for this SN. If the policy params for heap
             * or cache are set, use those, else default to 0.
             */
            if ((policyCacheBytes != 0) ||
                (policyHeapBytes != 0)) {
                return new RNHeapAndCacheSize((policyHeapBytes >> 20),
                                              policyCacheBytes);
            }
            return NO_CACHE_AND_HEAP_SPEC;
        }

        /*
         * Convert to bytes and remove the portion reserved for the file system
         * cache.
         */
        long snaMem = memoryMB; // must be a long!
        final long memoryForHeapBytes =
            ((snaMem << 20) * rnHeapPercent) / 100L;

        /*
         * If there are more RNs on the SN than capacity, use that to divide
         * up resources. If there are fewer RNs than capacity, use capacity
         * as the divisor.
         */
        int divisor = ((numRNsOnSN != 0) && (numRNsOnSN > capacity)) ?
            numRNsOnSN : capacity;

        /*
         * The heap has to be >= 2MB, per the JVM spec and >= our mandated
         * minimum of 32MB per RN.
         */
        long perRNHeap = memoryForHeapBytes / divisor;

        /* But if the heap is set in the policy, override our calculations */
        if (policyHeapBytes != 0) {
            perRNHeap = policyHeapBytes;
        }

        if (perRNHeap < ParameterState.RN_HEAP_MB_MIN << 20) {
            return NO_CACHE_AND_HEAP_SPEC;
        }

        long perRNCacheBytes = (perRNHeap * rnCachePercent) / 100;
        /* But if the cache is set in the policy, override our calculations */
        if (policyCacheBytes != 0) {
            perRNCacheBytes = policyCacheBytes;
        }

        if (perRNCacheBytes < ParameterState.RN_CACHE_MB_MIN << 20) {
            return NO_CACHE_AND_HEAP_SPEC;
        }

        return new RNHeapAndCacheSize(perRNHeap >> 20, perRNCacheBytes);
    }

    /**
     * A little struct to emphasize that RN cache and heap are calculated
     * together.
     */
    public static class RNHeapAndCacheSize {
        final long heapMB;
        final long cacheBytes;

        private RNHeapAndCacheSize(long heapMB, long cacheBytes) {
            this.heapMB = heapMB;
            this.cacheBytes = cacheBytes;
        }

        public long getCacheBytes() {
            return cacheBytes;
        }

        public long getHeapMB() {
            return heapMB;
        }

        /**
         * Return the heapMB value in in a form that can be appended to the
         * -Xmx or Xms prefix. If the heapMB is 0, return null;
         */
        public String getHeapValAndUnit() {
            if (heapMB == 0) {
                return null;
            }

            return heapMB + "M";
        }
        
        @Override
        public String toString() {
            return "heapMB=" + heapMB + " cacheBytes=" + cacheBytes;
        }
    }

    /**
     * Obtain a user specified prefix for starting up managed processes.
     * This can be useful when applying configuration scripts such as numactl
     * or using profiling.
     */
    public String getProcessStartupPrefix() {
        return map.get(ParameterState.SN_PROCESS_STARTUP_PREFIX).asString();
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
