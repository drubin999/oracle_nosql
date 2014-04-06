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

import static oracle.kv.impl.param.ParameterState.SN_ADMIN_SO_BACKLOG;
import static oracle.kv.impl.param.ParameterState.SN_LOGIN_SO_BACKLOG;
import static oracle.kv.impl.param.ParameterState.SN_LOGIN_SO_CONNECT_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.SN_LOGIN_SO_READ_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.SN_REGISTRY_SO_CONNECT_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.SN_REGISTRY_SO_READ_TIMEOUT;
import static oracle.kv.impl.util.registry.RegistryUtils.InterfaceType.MAIN;
import static oracle.kv.impl.util.registry.RegistryUtils.InterfaceType.TRUSTED_LOGIN;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import oracle.kv.KVVersion;
import oracle.kv.impl.param.DefaultParameter;
import oracle.kv.impl.param.DurationParameter;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.param.StringParameter;
import oracle.kv.impl.util.PortRange;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RMISocketPolicy;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryArgs;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryPair;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * The Storage Node Agent bootstrap properties
 *
 * The storeName and storageNodeId members are only set when the Agent has been
 * registered.  The presence of the storeName is the indicator that the
 * agent has been registered.
 */
public class BootstrapParams {

    /**
     * A name to supply as the store name portion of a CSF name.  This name
     * will not conflict with any store name that the configure command has
     * accepted as valid.
     */
    private static final String CSF_EMPTY_STORE_NAME = "$";

    private final ParameterMap map;
    private ParameterMap mountMap;

    public BootstrapParams(ParameterMap map, ParameterMap mountMap) {
        this.map = map;
        map.setName(ParameterState.BOOTSTRAP_PARAMS);
        map.setType(ParameterState.BOOTSTRAP_TYPE);
        setMountMap(mountMap);
    }

    /**
     * This constructor mostly useful for generating a BootstrapParams
     * configuration file.  In that case the storeName will often be null.
     */
    public BootstrapParams(String rootDir,
                           String hostname,
                           String haHostname,
                           String haPortRange,
                           String servicePortRange,
                           String storeName,
                           int rmiRegistryPort,
                           int adminHttpPort,
                           int capacity,
                           List<String> mountPoints,
                           boolean isSecure) {

        map = new ParameterMap(ParameterState.BOOTSTRAP_PARAMS,
                               ParameterState.BOOTSTRAP_TYPE);
        mountMap = createMountMap();
        map.setParameter(ParameterState.BP_ROOTDIR, rootDir);
        map.setParameter(ParameterState.COMMON_HOSTNAME, hostname);
        map.setParameter(ParameterState.COMMON_HA_HOSTNAME, haHostname);

        map.setParameter(ParameterState.COMMON_PORTRANGE, haPortRange);
        map.setParameter(ParameterState.COMMON_STORENAME, storeName);
        map.setParameter(ParameterState.COMMON_REGISTRY_PORT,
                         Integer.toString(rmiRegistryPort));
        map.setParameter(ParameterState.COMMON_ADMIN_PORT,
                         Integer.toString(adminHttpPort));
        map.setParameter(ParameterState.COMMON_SN_ID, "0");
        map.setParameter(ParameterState.BP_HOSTING_ADMIN,
                         Boolean.toString(false));
        if (mountPoints != null) {
            addMountPoints(mountMap, mountPoints);
        }

        if (capacity > 0) {
            setCapacity(capacity);
        }
        if (servicePortRange != null) {
            PortRange.validateService(servicePortRange);
            PortRange.validateDisjoint(haPortRange, servicePortRange);
            map.setParameter(ParameterState.COMMON_SERVICE_PORTRANGE,
                             servicePortRange);
            PortRange.validateSufficientPorts(servicePortRange,
                                              capacity,
                                              isSecure,
                                              (adminHttpPort != 0 ?
                                               true : false),
                                              getMgmtPorts());
        }
        map.setParameter(ParameterState.SN_SOFTWARE_VERSION,
                         KVVersion.CURRENT_VERSION.getNumericVersionString());
    }

    /**
     * Constructor for test purposes that defaults the servicePortRange for
     * tests where the default is adequate.
     */
    public BootstrapParams(String rootDir,
                           String hostname,
                           String haHostname,
                           String haPortRange,
                           String storeName,
                           int rmiRegistryPort,
                           int adminHttpPort,
                           int capacity,
                           List<String> mountPoints,
                           boolean isSecure) {

        this(rootDir,
             hostname,
             haHostname,
             haPortRange,
             null,
             storeName,
             rmiRegistryPort,
             adminHttpPort,
             capacity,
             mountPoints,
             isSecure);
    }

    public ParameterMap getMap() {
        return map;
    }

    public ParameterMap getMountMap() {
        return mountMap;
    }

    public void setMountMap(ParameterMap newMap) {
        if (newMap == null) {
            createMountMap();
            return;
        }
        mountMap = newMap;
        mountMap.setName(ParameterState.BOOTSTRAP_MOUNT_POINTS);
        mountMap.setType(ParameterState.BOOTSTRAP_TYPE);
        mountMap.setValidate(false);
    }

    public List<String> getMountPoints() {
        if (mountMap == null) {
            return null;
        }
        return getMountPoints(mountMap);
    }

    public void addMountPoint(String mp) {
        addMountPoint(mountMap, mp);
    }

    private static void addMountPoints(ParameterMap mMap,
                                       List<String> mountPoints) {
        for (String mountPoint : mountPoints) {
            validateMountPoint(mountPoint);
            StringParameter mp = new StringParameter(mountPoint, "");
            mMap.put(mp);
        }
    }

    public static void addMountPoint(ParameterMap mMap, String mp) {
        List<String> list = new ArrayList<String>(1);
        list.add(mp);
        addMountPoints(mMap, list);
    }

    /**
     * Important: mount points are the names of the name/value
     * pairs in the mount map.  Values are empty.
     */
    public static List<String> getMountPoints(ParameterMap mMap) {
        List<String> mps = new ArrayList<String>(mMap.size());
        for (Parameter p : mMap.values()) {
            mps.add(p.getName());
        }
        return mps;
    }

    public static ParameterMap createMountMap() {
        return new ParameterMap(ParameterState.BOOTSTRAP_MOUNT_POINTS,
                                ParameterState.BOOTSTRAP_TYPE,
                                false,
                                ParameterState.PARAMETER_VERSION);
    }

    public int getRegistryPort() {
        return map.getOrZeroInt(ParameterState.COMMON_REGISTRY_PORT);
    }

    public String getRootdir() {
        return map.get(ParameterState.BP_ROOTDIR).asString();
    }

    public void setRootdir(String rootDir) {
        map.setParameter(ParameterState.BP_ROOTDIR, rootDir);
    }

    public String getStoreName() {
        return map.get(ParameterState.COMMON_STORENAME).asString();
    }

    public int getId() {
        return map.getOrZeroInt(ParameterState.COMMON_SN_ID);
    }

    public void setStoreName(String storeName) {
        map.setParameter(ParameterState.COMMON_STORENAME, storeName);
    }

    public void setId(int snId) {
        map.setParameter(ParameterState.COMMON_SN_ID, Integer.toString(snId));
    }

    public String getHostname() {
        return map.get(ParameterState.COMMON_HOSTNAME).asString();
    }

    /**
     * If HA HOSTNAME is not set, return the generic hostname.
     */
    public String getHAHostname() {
        String ret = map.get(ParameterState.COMMON_HA_HOSTNAME).asString();
        if (ret == null) {
            ret = getHostname();
        }
        return ret;
    }

    public void setHAHostname(String newName) {
        map.setParameter(ParameterState.COMMON_HA_HOSTNAME, newName);
    }

    public String getHAPortRange() {
        return map.get(ParameterState.COMMON_PORTRANGE).asString();
    }

    public void setHAPortRange(String newRange) {
        map.setParameter(ParameterState.COMMON_PORTRANGE, newRange);
    }

    /* This will return null if the parameter does not exist */
    public String getServicePortRange() {
        return map.get(ParameterState.COMMON_SERVICE_PORTRANGE).asString();
    }

    public void setServicePortRange(String newRange) {
        map.setParameter(ParameterState.COMMON_SERVICE_PORTRANGE, newRange);
    }

    public boolean isHostingAdmin() {
        return map.get(ParameterState.BP_HOSTING_ADMIN).asBoolean();
    }

    public void setHostingAdmin(boolean value) {
        map.setParameter(ParameterState.BP_HOSTING_ADMIN,
                         Boolean.toString(value));
    }

    public int getAdminHttpPort() {
        return map.getOrZeroInt(ParameterState.COMMON_ADMIN_PORT);
    }

    public int getStorageNodeId() {
        return map.getOrZeroInt(ParameterState.COMMON_SN_ID);
    }

    public int getNumCPUs() {
        return map.getOrDefault(ParameterState.COMMON_NUMCPUS).asInt();
    }

    public int getCapacity() {
        return map.getOrDefault(ParameterState.COMMON_CAPACITY).asInt();
    }

    public int getMemoryMB() {
        return map.getOrDefault(ParameterState.COMMON_MEMORY_MB).asInt();
    }

    public SocketFactoryPair getStorageNodeAgentSFP(RMISocketPolicy policy) {
        SocketFactoryArgs args = new SocketFactoryArgs();

        if (getServicePortRange() == null && policy.isPolicyOptional()) {
            return new SocketFactoryPair(null, null);
        }

        final String csfName =
            ClientSocketFactory.factoryName(CSF_EMPTY_STORE_NAME,
                                            "sna",
                                            MAIN.interfaceName());

        args.setSsfName(MAIN.interfaceName()).
            setSsfBacklog(map.getOrDefault(SN_ADMIN_SO_BACKLOG).asInt()).
            setSsfPortRange(getServicePortRange()).
            setCsfName(csfName);

        return policy.getBindPair(args);
    }

    public SocketFactoryPair getSNATrustedLoginSFP(RMISocketPolicy policy) {
        SocketFactoryArgs args = new SocketFactoryArgs();

        final String csfName =
            ClientSocketFactory.factoryName(CSF_EMPTY_STORE_NAME,
                                            "sna",
                                            TRUSTED_LOGIN.interfaceName());

        args.setSsfName(TRUSTED_LOGIN.interfaceName()).
            setSsfBacklog(map.getOrDefault(SN_LOGIN_SO_BACKLOG).asInt()).
            setCsfConnectTimeout(
                getDurationParamMillis(SN_LOGIN_SO_CONNECT_TIMEOUT)).
            setCsfReadTimeout(
                getDurationParamMillis(SN_LOGIN_SO_READ_TIMEOUT)).
            setSsfPortRange(getServicePortRange()).
            setCsfName(csfName);

        return policy.getBindPair(args);
    }

    /**
     * Initialize the registry CSF at bootstrap time.
     */
    public static void initRegistryCSF(SecurityParams sp) {
        String registryCsfName = ClientSocketFactory.registryFactoryName();
        SocketFactoryArgs args =
            new SocketFactoryArgs().setCsfName(registryCsfName).
            setCsfConnectTimeout(
                getDefaultDurationParamMillis(SN_REGISTRY_SO_CONNECT_TIMEOUT)).
            setCsfReadTimeout(
                getDefaultDurationParamMillis(SN_REGISTRY_SO_READ_TIMEOUT));

        RegistryUtils.setServerRegistryCSF(
            sp.getRMISocketPolicy().getRegistryCSF(args));
    }

    public void setNumCPUs(int ncpus) {
        map.setParameter(ParameterState.COMMON_NUMCPUS,
                         Integer.toString(ncpus));
    }

    public void setCapacity(int capacity) {
        map.setParameter(ParameterState.COMMON_CAPACITY,
                         Integer.toString(capacity));
    }

    public void setMemoryMB(int memoryMB) {
        map.setParameter(ParameterState.COMMON_MEMORY_MB,
                         Integer.toString(memoryMB));
    }

    public int getMgmtPorts() {
        if (map.exists(ParameterState.COMMON_MGMT_CLASS))
            return 1;
        return 0;
    }

    /**
     * If mgmt class is not set, return the default no-op class name.
     */
    public String getMgmtClass() {
        return map.getOrDefault(ParameterState.COMMON_MGMT_CLASS).asString();
    }

    public void setMgmtClass(String mgmtClass) {
        map.setParameter(ParameterState.COMMON_MGMT_CLASS, mgmtClass);
    }

    /**
     * Return the snmp polling port, or zero if it is not set.
     */
    public int getMgmtPollingPort() {
        return map.getOrZeroInt(ParameterState.COMMON_MGMT_POLL_PORT);
    }

    public void setMgmtPollingPort(int port) {
        map.setParameter
            (ParameterState.COMMON_MGMT_POLL_PORT, Integer.toString(port));
    }

    /**
     * If the trap host string is not set, just return null.
     */
    public String getMgmtTrapHost() {
        return map.get(ParameterState.COMMON_MGMT_TRAP_HOST).asString();
    }

    public void setMgmtTrapHost(String hostname) {
        map.setParameter(ParameterState.COMMON_MGMT_TRAP_HOST, hostname);
    }

    /**
     * Return the snmp trap port, or zero if it is not set.
     */
    public int getMgmtTrapPort() {
        return map.getOrZeroInt(ParameterState.COMMON_MGMT_TRAP_PORT);
    }

    public void setMgmtTrapPort(int port) {
        map.setParameter
            (ParameterState.COMMON_MGMT_TRAP_PORT, Integer.toString(port));
    }

    /**
     * Return the security directory, or null if not set.
     */
    public String getSecurityDir() {
        String result = map.get(ParameterState.COMMON_SECURITY_DIR).asString();
        return (result != null && !result.isEmpty()) ? result : null;
    }

    public void setSecurityDir(String dir) {
        map.setParameter(ParameterState.COMMON_SECURITY_DIR, dir);
    }

    /**
     * Only the structure of the path is validated here, not whether
     * it exists as a directory.  That is done only when used.  It would be
     * nice if there were more things to do.  Possibilities:
     *  - check path length
     *  - look for invalid characters
     */
    public static void validateMountPoint(String path) {
        if (new File(path).isAbsolute()) {
            return;
        }
        throw new IllegalArgumentException
            ("Illegal storage directory: " + path +
             ", storage directories must be valid absolute pathnames");
    }

    /**
     * Gets the version parameter. If the parameter is not present null is
     * returned.
     *
     * @return a version object or null
     */
    public KVVersion getSoftwareVersion() {
        final String versionString =
                map.get(ParameterState.SN_SOFTWARE_VERSION).toString();

        return (versionString == null) ? null :
                                         KVVersion.parseVersion(versionString);
    }

    /**
     * Sets the version parameter. The previous version, if present, is
     * replaced.
     *
     * @param version
     */
    public void setSoftwareVersion(KVVersion version) {
        map.setParameter(ParameterState.SN_SOFTWARE_VERSION,
                         version.getNumericVersionString());
    }

    private static int getDefaultDurationParamMillis(String param) {
        DurationParameter dp =
            (DurationParameter) DefaultParameter.getDefaultParameter(param);

        return (int) dp.toMillis();
    }

    private int getDurationParamMillis(String param) {
        return (int)ParameterUtils.getDurationMillis(map, param);
    }

    /**
     * Sets the forceBootstrapAdmin parameter.
     *
     * @param force
     */
    public void setForceBootstrapAdmin(boolean force) {
        map.setParameter(ParameterState.BP_FORCE_BOOTSTRAP_ADMIN,
                         Boolean.toString(force));
    }

    /**
     * Gets the forceBootstrapAdmin parameter. If the parameter is not present
     * the default value is returned.
     *
     * @return the present value, or default value if the parameter is not
     * present
     */
    public boolean getForceBootstrapAdmin() {
        return map.getOrDefault(
            ParameterState.BP_FORCE_BOOTSTRAP_ADMIN).asBoolean();
    }
}
