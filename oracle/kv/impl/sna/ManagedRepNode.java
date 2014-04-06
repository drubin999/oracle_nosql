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

import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.rep.RepNodeService;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.ServiceUtils;
import oracle.kv.impl.util.registry.RegistryUtils;

public class ManagedRepNode extends ManagedService {

    private SecurityParams sp;
    private RepNodeParams rnp;
    private RepNodeAdminAPI repNodeAdmin;
    private LoadParameters lp;

    /**
     * Constructor used by the SNA client.
     */
    public ManagedRepNode(SecurityParams sp,
                          RepNodeParams rnp,
                          File kvRoot,
                          File kvSNDir,
                          String kvName) {
        super(kvRoot, sp.getConfigDir(), kvSNDir, kvName,
              REP_NODE_NAME, rnp.getRepNodeId().getFullName(), rnp.getMap());

        this.sp = sp;
        this.rnp = rnp;
        repNodeAdmin = null;
    }

    /**
     * Constructor used by the service instance upon startup.
     */
    public ManagedRepNode(String kvSecDir,
                          String kvSNDir,
                          String kvName,
                          String serviceClass,
                          String serviceName) {

        super(null, nullableFile(kvSecDir), new File(kvSNDir),
              kvName, serviceClass, serviceName, null);
        resetParameters(!usingThreads);
        startLogger(RepNodeService.class, rnp.getRepNodeId(), lp);
        repNodeAdmin = null;
    }

    @Override
    public void resetParameters(boolean inTarget) {
        sp = getSecurityParameters();
        lp = getParameters();
        params = lp.getMap(serviceName, ParameterState.REPNODE_TYPE);
        rnp = new RepNodeParams(params);
        rnp.validateCacheAndHeap(inTarget);
    }

    @Override
    public String getDefaultJavaArgs(String overrideJvmArgs) {
        String defaultJavaArgs =
            RepNodeService.DEFAULT_MISC_JAVA_ARGS + " " +
               (((overrideJvmArgs != null) &&
                  overrideJvmArgs.contains("-XX:+UseG1GC")) ?
                RepNodeService.DEFAULT_G1_GC_ARGS :
                RepNodeService.DEFAULT_CMS_GC_ARGS);

        final String resourceName = rnp.getRepNodeId().toString();
        final Parameter numGCLogFiles =
            rnp.getMap().getOrDefault(ParameterState.RN_NUM_GC_LOG_FILES);
        final Parameter gcLogFileSize =
            rnp.getMap().getOrDefault(ParameterState.RN_GC_LOG_FILE_SIZE);

        return defaultJavaArgs +
            getGCLoggingArgs(numGCLogFiles, gcLogFileSize, resourceName);
    }

    /**
     * Called from the service manager.
     */
    @Override
    public ResourceId getResourceId() {
        if (rnp != null) {
            return rnp.getRepNodeId();
        }
        throw new IllegalStateException("No resource id");
    }

    @Override
    public void resetHandles() {
        repNodeAdmin = null;
    }

    @Override
    public String getJVMArgs() {
        if (params != null) {
            String args = "";
            if (params.exists(ParameterState.JVM_MISC)) {
                args += params.get(ParameterState.JVM_MISC).asString();
            }
            return args;
        }
        return null;
    }

    public RepNodeParams getRepNodeParams() {
        return rnp;
    }

    /**
     * This method is called in the context of the SNA (manager of the service)
     * and not the running service itself.
     */
    public RepNodeAdminAPI getRepNodeAdmin(StorageNodeAgent sna)
        throws RemoteException {

        if (repNodeAdmin == null) {
            try {
                repNodeAdmin = RegistryUtils.getRepNodeAdmin
                    (sna.getStoreName(), sna.getHostname(),
                     sna.getRegistryPort(), rnp.getRepNodeId(),
                     sna.getLoginManager());
            } catch (NotBoundException ne) {
                final String msg = "Cannot get handle from Registry: " +
                    rnp.getRepNodeId().getFullName() + ": " + ne.getMessage();
                sna.getLogger().severe(msg);
                throw new RemoteException(msg, ne);
            }
        }
        return repNodeAdmin;
    }

    /**
     * Like getRepNodeAdmin but with a timeout.  There are two methods because
     * of the timeout overhead, and situations where the handle should be
     * immediately available or it's an error.
     *
     * This method is called in the context of the SNA (manager of the service)
     * and not the running service itself.
     */
    protected RepNodeAdminAPI waitForRepNodeAdmin(StorageNodeAgent sna,
                                                  int timeoutSec) {

        if (repNodeAdmin == null) {
            try {
                final ServiceStatus[] target = {ServiceStatus.RUNNING};
                repNodeAdmin = ServiceUtils.waitForRepNodeAdmin
                    (sna.getStoreName(), sna.getHostname(),
                     sna.getRegistryPort(), rnp.getRepNodeId(),
                     sna.getStorageNodeId(), sna.getLoginManager(),
                     timeoutSec, target);
            } catch (Exception e) {
                final String msg =
                    "Cannot get RepNodeAdmin handle from Registry: " +
                    rnp.getRepNodeId().getFullName() + ": " + e.getMessage();
                sna.getLogger().severe(msg);
                return null;
            }
        }
        return repNodeAdmin;
    }

    /**
     * This method must be run in the execution context of the service.
     */
    @Override
    public void start(boolean threads) {

        final RepNodeService rns = new RepNodeService(threads);

        /**
         * The ProcessFaultHandler created in the constructor does not have
         * a Logger instance.  It will be reset in newProperties().
         */
        rns.getFaultHandler().setLogger(logger);
        rns.getFaultHandler().execute
            (new ProcessFaultHandler.Procedure<RuntimeException>() {
                @Override
                public void execute() {
                    rns.initialize(sp, rnp, lp);
                    rns.start();
                }
            });
    }
}
