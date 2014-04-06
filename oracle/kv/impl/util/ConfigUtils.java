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

package oracle.kv.impl.util;

import static oracle.kv.impl.param.ParameterState.ADMIN_TYPE;
import static oracle.kv.impl.param.ParameterState.BOOTSTRAP_MOUNT_POINTS;
import static oracle.kv.impl.param.ParameterState.BOOTSTRAP_PARAMETER_VERSION;
import static
    oracle.kv.impl.param.ParameterState.BOOTSTRAP_PARAMETER_R1_VERSION;
import static oracle.kv.impl.param.ParameterState.PARAMETER_VERSION;
import static oracle.kv.impl.param.ParameterState.BOOTSTRAP_PARAMS;
import static oracle.kv.impl.param.ParameterState.BOOTSTRAP_TYPE;
import static oracle.kv.impl.param.ParameterState.GLOBAL_TYPE;
import static oracle.kv.impl.param.ParameterState.REPNODE_TYPE;
import static oracle.kv.impl.param.ParameterState.SNA_TYPE;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;

/**
 * Utilities that access parameter and configuration files
 */
public class ConfigUtils {

    /**
     * Separator, newline characters for properties that end up as Java
     * Propertie objects.
     */
    public static final Character PROPERTY_SEPARATOR = ';';
    public static final Character PROPERTY_NEWLINE = '\n';

    private final static String SEC_POLICY_STRING =
        "grant {\n permission java.security.AllPermission;\n};\n";

    public static void createSecurityPolicyFile(File dest) {
        FileOutputStream output = null;
        try {
            dest.createNewFile();
            output = new FileOutputStream(dest);
            output.write(SEC_POLICY_STRING.getBytes());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (output != null) {
                try {
                    output.close();
                } catch (IOException ignored) {
                }
            }
        }
    }

    /**
     * Save the object as a bootstrap configuration file using
     * the path (full pathname) specified.
     */
    public static void createBootstrapConfig(BootstrapParams bp,
                                             String fileName) {

        createBootstrapConfig(bp, new File(fileName));
    }

    public static void createBootstrapConfig(BootstrapParams bp,
                                             File file) {

        LoadParameters lp = new LoadParameters();
        lp.addMap(bp.getMap());
        lp.addMap(bp.getMountMap());
        lp.setVersion(BOOTSTRAP_PARAMETER_VERSION);
        lp.saveParameters(file);
    }

    public static BootstrapParams getBootstrapParams(File configFile) {
        return getBootstrapParams(configFile, null);
    }

    public static BootstrapParams getBootstrapParams
        (File configFile, Logger logger) {

        LoadParameters lp = LoadParameters.getParameters(configFile, logger);
        ParameterMap pm = null;
        ParameterMap mm = null;

        final int bootstrapVersion = lp.getVersion();
        if (bootstrapVersion == BOOTSTRAP_PARAMETER_R1_VERSION) {
            pm = lp.getMap(BOOTSTRAP_TYPE, BOOTSTRAP_TYPE);
        } else {
            pm = lp.getMap(BOOTSTRAP_PARAMS, BOOTSTRAP_TYPE);
            mm = lp.getMap(BOOTSTRAP_MOUNT_POINTS, BOOTSTRAP_TYPE);
        }
        if (pm == null) {
            throw new IllegalStateException
                ("Could not get bootstrap params from file: " + configFile);
        }

        final BootstrapParams bp = new BootstrapParams(pm, mm);

        /*
         * If there is no version in the file, then the previous installed
         * software version was either R1 or an early release of R2. We
         * assume R2.0 for now. Once we can no longer upgrade directly from 1.0
         * we may need to adjust this. We can make a reasonable guess from the
         * bootstrap version which was bumped in R2 but the parameter may not
         * have been updated after a 1.0 -> 2.0 upgrade.
         */
        if (bp.getSoftwareVersion() == null) {
            final KVVersion previousVersion =
                                KVVersion.R2_0_23; // R2.0
             
            bp.setSoftwareVersion(previousVersion);

            if (logger != null) {
                logger.log(Level.WARNING,
                           "Software version missing from configuration " +
                           "file. Assuming installed software is at: {0}",
                           previousVersion.getNumericVersionString());
            }
        }
        return bp;
    }

    public static void createSecurityConfig(SecurityParams sp,
                                            File file) {

        LoadParameters lp = new LoadParameters();
        lp.addMap(sp.getMap());
        for (ParameterMap transportMap : sp.getTransportMaps()) {
            lp.addMap(transportMap);
        }
        lp.setVersion(PARAMETER_VERSION);
        lp.saveParameters(file);
    }

    public static SecurityParams getSecurityParams(File configFile) {
        return getSecurityParams(configFile, null);
    }

    public static SecurityParams getSecurityParams
        (File configFile, Logger logger) {

        final LoadParameters lp =
            LoadParameters.getParametersByType(configFile, logger);
        final SecurityParams sp = new SecurityParams(lp, configFile);

        return sp;
    }

    public static GlobalParams getGlobalParams(File configFile) {
        return getGlobalParams(configFile, null);
    }

    public static GlobalParams getGlobalParams
        (File configFile, Logger logger) {

        LoadParameters lp = LoadParameters.getParameters(configFile, logger);
        ParameterMap pm = lp.getMap(GLOBAL_TYPE);
        if (pm != null) {
            return new GlobalParams(pm);
        }
        throw new IllegalStateException
            ("Could not get GlobalParams from file: " + configFile);
    }

    public static StorageNodeParams getStorageNodeParams(File configFile) {
        return getStorageNodeParams(configFile, null);
    }

    public static StorageNodeParams getStorageNodeParams
        (File configFile, Logger logger) {

        LoadParameters lp = LoadParameters.getParameters(configFile, logger);
        ParameterMap pm = lp.getMap(SNA_TYPE);
        ParameterMap mm = lp.getMap(BOOTSTRAP_MOUNT_POINTS, BOOTSTRAP_TYPE);
        if (pm != null) {
            return new StorageNodeParams(pm, mm);
        }
        throw new IllegalStateException
            ("Could not get StorageNodeParams from file: " + configFile);
    }

    /**
     * Extract the AdminParams from the configFile.  In this case, null is
     * a reasonable return, indicating that the params do not exist.  This
     * method should only be used for compatibility.
     */
    public static AdminParams getAdminParams(File configFile) {
        return getAdminParams(configFile, null);
    }

    public static AdminParams getAdminParams(File configFile, Logger logger) {

        LoadParameters lp = LoadParameters.getParameters(configFile, logger);
        ParameterMap pm = lp.getMapByType(ADMIN_TYPE);
        if (pm != null) {
            return new AdminParams(pm);
        }
        return null;
    }

    /**
     * Extract the specified AdminParams from the configFile.  If the map is
     * not found by name and tryByType is true, get the map by type.
     */
    public static AdminParams getAdminParams(File configFile,
                                             AdminId adminId,
                                             boolean tryByType) {
        return getAdminParams(configFile, adminId, tryByType, null);
    }

    public static AdminParams getAdminParams(File configFile,
                                             AdminId adminId,
                                             boolean tryByType,
                                             Logger logger) {

        LoadParameters lp = LoadParameters.getParameters(configFile, logger);
        ParameterMap pm = lp.getMap(adminId.getFullName(), ADMIN_TYPE);
        if (pm == null && tryByType) {
            pm = lp.getMapByType(ADMIN_TYPE);
        }
        if (pm != null) {
            return new AdminParams(pm);
        }
        return null;
    }

    /**
     * Get RepNodeParams, return null if they do not exist.
     */
    public static RepNodeParams getRepNodeParams(File configFile,
                                                 RepNodeId rnid,
                                                 Logger logger) {

        LoadParameters lp = LoadParameters.getParameters(configFile, logger);
        ParameterMap pm =
            lp.getMap(rnid.getFullName(), REPNODE_TYPE);
        if (pm != null) {
            return new RepNodeParams(pm);
        }
        return null;
    }

    public static List<ParameterMap> getRepNodes(File configFile,
                                                 Logger logger) {

        LoadParameters lp = LoadParameters.getParameters(configFile, logger);
        return lp.getAllMaps(REPNODE_TYPE);
    }

    /**
     * Remove the component from the file.  If the component can't be found by
     * name and type is non-null, remove by type.
     */
    public static ParameterMap removeComponent(File configFile,
                                               ResourceId rid,
                                               String type,
                                               Logger logger) {

        LoadParameters lp = LoadParameters.getParameters(configFile, logger);
        ParameterMap map = lp.removeMap(rid.getFullName());
        if (map == null && type != null) {
            map = lp.removeMapByType(type);
        }
        if (map != null) {
            lp.saveParameters(configFile);
        }
        return map;
    }

    public static ParameterMap getAdminMap(AdminId adminId,
                                           StorageNodeParams snp,
                                           GlobalParams gp,
                                           Logger logger) {
        ParameterMap map =
            getParameterMap(snp, gp, adminId.getFullName(), logger);

        /**
         * For now, accept getting the map by type vs name.
         */
        if (map == null) {
            map = getParameterMapByType(snp, gp, ADMIN_TYPE, logger);
        }
        return map;
    }

    public static ParameterMap getRepNodeMap(StorageNodeParams snp,
                                             GlobalParams gp,
                                             RepNodeId rnid,
                                             Logger logger) {
        return getParameterMap(snp, gp, rnid.getFullName(), logger);
    }

    public static ParameterMap getGlobalMap(StorageNodeParams snp,
                                            GlobalParams gp,
                                            Logger logger) {
        return getParameterMapByType(snp, gp, GLOBAL_TYPE, logger);
    }

    private static ParameterMap getParameterMap(StorageNodeParams snp,
                                                GlobalParams gp,
                                                String service,
                                                Logger logger) {
        File configFile = FileNames.getSNAConfigFile(snp.getRootDirPath(),
                                                     gp.getKVStoreName(),
                                                     snp.getStorageNodeId());
        LoadParameters lp = LoadParameters.getParameters(configFile, logger);
        return lp.getMap(service);
    }

    private static ParameterMap getParameterMapByType(StorageNodeParams snp,
                                                      GlobalParams gp,
                                                      String type,
                                                      Logger logger) {
        File configFile = FileNames.getSNAConfigFile(snp.getRootDirPath(),
                                                     gp.getKVStoreName(),
                                                     snp.getStorageNodeId());
        LoadParameters lp = LoadParameters.getParameters(configFile, logger);
        return lp.getMapByType(type);
    }

    /**
     * Turn parameter format of "name=value;name1=value1;..." to
     * an InputStream compatible with java.util.Properties.
     */
    public static InputStream getPropertiesStream(String properties) {
        String newProps =
            properties.replace(PROPERTY_SEPARATOR, PROPERTY_NEWLINE);
        return new ByteArrayInputStream(newProps.getBytes());
    }

    /**
     * Store properties in a file.
     * @param props a set of properties to store
     * @param comment an optional comment to add to the file
     * @param dest an abstract file naming the location where the properties
     * will be stored.  The directory containing the location must exist and
     * be writable.
     * @throws IOException if an error occurs while trying to write the file
     */
    public static void storeProperties(Properties props,
                                       String comment,
                                       File dest)
        throws IOException {

        FileOutputStream output = null;
        try {
            dest.createNewFile();
            output = new FileOutputStream(dest);
            props.store(output, comment);
        } finally {
            if (output != null) {
                try {
                    output.close();
                } catch (IOException ignored) /* CHECKSTYLE:OFF */ {
                } /* CHECKSTYLE:ON */
            }
        }
    }
}
