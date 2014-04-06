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

package oracle.kv.impl.sna;

import java.io.File;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import oracle.kv.impl.admin.AdminService;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.registry.RegistryUtils;

public class ManagedAdmin extends ManagedService {

    protected SecurityParams sp;
    protected AdminParams ap;
    protected CommandServiceAPI cs;
    private LoadParameters lp;

    /**
     * Constructor used by the SNA client.
     */
    public ManagedAdmin(SecurityParams sp,
                        AdminParams ap,
                        File kvRoot,
                        File kvSNDir,
                        String kvName) {
        super(kvRoot, sp.getConfigDir(), kvSNDir, kvName,
              ADMIN_NAME, ap.getAdminId().getFullName(), ap.getMap());

        this.sp = sp;
        this.ap = ap;
        cs = null;
    }

    /**
     * Constructor used by the SNA client when creating ManagedBootstrapAdmin.
     * In this case kvSNDir is actually the bootstrap dir, kvName is null, and
     * kvConfigFile is the bootstrap params file name.
     */
    public ManagedAdmin(File rootDir,
                        File secDir,
                        String serviceName) {
        super(null, secDir, rootDir, null, ADMIN_NAME,
              serviceName, null);
        sp = null;
        ap = null;
        cs = null;
    }

    /**
     * Constructor used by the service instance upon startup.
     */
    public ManagedAdmin(String kvSecDir,
                        String kvSNDir,
                        String kvName,
                        String serviceClass,
                        String serviceName) {
        super(null, nullableFile(kvSecDir), new File(kvSNDir),
              kvName, serviceClass, serviceName, null);
        resetParameters(true);
        startLogger(AdminService.class, ap.getAdminId(), lp);
        cs = null;
    }

    @Override
    public void resetParameters(boolean inTarget) {
        sp = getSecurityParameters();
        lp = getParameters();

        /**
         * Upgrade-friendly code will check first for name, type and if that
         * fails, just type, which is what happened in R1.
         */
        params = lp.getMap(serviceName, ParameterState.ADMIN_TYPE);
        if (params == null) {
            params = lp.getMapByType(ParameterState.ADMIN_TYPE);
        }

        /*
         * For a BootstrapAdmin, don't allow a broken AdminParams to be
         * constructed.
         */
        if (params != null) {
            ap = new AdminParams(params);
        }
    }

    @Override
    public String getDefaultJavaArgs(String overrideJvmArgs) {
        if ((ap == null) || (ap.getMap() == null)) {
            /* Bootstrap Admin */
            return "";
        }
        final Parameter gcLogFiles =
            ap.getMap().getOrDefault(ParameterState.AP_NUM_GC_LOG_FILES);
        final Parameter gcLogFileSize =
            ap.getMap().getOrDefault(ParameterState.AP_GC_LOG_FILE_SIZE);
        final String resourceName = ap.getAdminId().toString();

        return AdminService.DEFAULT_JAVA_ARGS +
            getGCLoggingArgs(gcLogFiles, gcLogFileSize, resourceName);
    }

    /**
     * Called from the service manager.
     */
    @Override
    public ResourceId getResourceId() {
        if (ap != null) {
            return ap.getAdminId();
        }

        /**
         * This means that it is the bootstrap admin.  Manufacture an object
         * because the caller isn't prepared for a null object.  0 is never
         * used as a real admin ID.
         */
        return new AdminId(0);
    }

    @Override
    public void resetHandles() {
        cs = null;
    }

    public AdminParams getAdminParams() {
        return ap;
    }


    /**
     * Get the CommandService interface for the admin.  Called in the context
     * of the SNA.
     */
    public CommandServiceAPI getAdmin(StorageNodeAgent sna)
        throws RemoteException {

        if (cs == null) {
            try {
                cs = RegistryUtils.getAdmin(sna.getHostname(),
                                            sna.getRegistryPort(),
                                            sna.getLoginManager());
            } catch (NotBoundException ne) {
                final String msg = "Cannot get admin handle from Registry: " +
                    ne.getMessage();
                sna.getLogger().severe(msg);
                throw new RemoteException(msg, ne);
            }
        }
        return cs;
    }

    /**
     * This method must be run in the execution context of the service.
     */
    @Override
    public void start(boolean threads) {

        final AdminService as = new AdminService(threads);
        as.initialize(sp, ap, lp);
        as.start();
    }
}
