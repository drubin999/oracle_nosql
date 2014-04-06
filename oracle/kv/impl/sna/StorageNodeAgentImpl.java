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
import java.lang.reflect.Constructor;
import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.KVStoreRole;
import oracle.kv.impl.security.annotations.PublicMethod;
import oracle.kv.impl.security.annotations.SecureAPI;
import oracle.kv.impl.security.annotations.SecureAutoMethod;
import oracle.kv.impl.security.annotations.SecureR2Method;
import oracle.kv.impl.test.RemoteTestInterface;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.registry.VersionedRemoteImpl;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * The class that implements the StorageNodeAgentInterface (SNA) interface used
 * to manage all processes on a physical machine (Storage Node, or SN).  The
 * SNA is responsible for
 *
 * 1.  Hosting the RMI registry for processes on the SN
 * 2.  Configuring and starting processes on the SN (RepNode and Admin)
 * 3.  Monitoring process state and restarting them if necessary
 * 4.  Terminating processes if requested, gracefully if possible.
 *
 * Once provisioned, the process that hosts this object is intended to be
 * started automatically by the Operating System though a mechanism such as
 * /etc/init on *nix.  There is one instance of this object for each KVStore
 * provisioned on a Storage Node, most likely hosted by independent processes.
 *
 * This is the interface class.  Most of the work is done in the
 * StorageNodeAgent class.
 */
@SecureAPI
public final class StorageNodeAgentImpl
    extends VersionedRemoteImpl implements StorageNodeAgentInterface {

    private final StorageNodeAgent sna;
    private final SNAFaultHandler faultHandler;

    /**
     * A conditional instance, created if the class can be found.
     */
    private RemoteTestInterface rti;
    private static final String TEST_INTERFACE_NAME =
        "oracle.kv.impl.sna.StorageNodeAgentTestInterface";

    public StorageNodeAgentImpl() {
        this(true);
    }

    /**
     * A constructor that allows the caller to indicate that the bootstrap
     * admin service should or should not be started.
     */
    public StorageNodeAgentImpl(boolean createBootstrapAdmin) {

        sna = new StorageNodeAgent(this, createBootstrapAdmin);

        faultHandler = new SNAFaultHandler(this);
        rti = null;
    }

    public StorageNodeAgent getStorageNodeAgent() {
        return sna;
    }

    public SNAFaultHandler getFaultHandler() {
        return faultHandler;
    }

    /**
     * A bunch of pass-through methods do the StorageNodeAgent methods.
     */
    public Logger getLogger() {
        return sna.getLogger();
    }

    public boolean parseArgs(String args[]) {
        return sna.parseArgs(args);
    }

    public void start()
        throws RemoteException {

        sna.start();
    }

    public void addShutdownHook() {
        sna.addShutdownHook();
    }

    public boolean isRegistered() {
        return sna.isRegistered();
    }

    public String getStoreName() {
        return sna.getStoreName();
    }

    public int getRegistryPort() {
        return sna.getRegistryPort();
    }

    /**
     * Can the test interface be created?
     */
    public void startTestInterface() {
        try {
            Class<?> cl = Class.forName(TEST_INTERFACE_NAME);
            Constructor<?> c = cl.getConstructor(getClass());
            rti = (RemoteTestInterface) c.newInstance(this);
            rti.start(SerialVersion.CURRENT);
        } catch (Exception ignored) {
        }
    }

    public void stopTestInterface()
        throws RemoteException {

        if (rti != null) {
            rti.stop(SerialVersion.CURRENT);
        }
    }

    private void checkRegistered(String method) {
        sna.checkRegistered(method);
    }

    private void logInfo(String msg) {
        sna.getLogger().info(msg);
    }

    private void logFine(String msg) {
        sna.getLogger().fine(msg);
    }

    /**
     * Begin StorageNodeAgentInterface
     *
     * General considerations:
     * o Only one register() call during the lifetime of the agent.
     * o Single-thread the state-changing calls.
     * o Ensure that services never see or read an inconsistent configuration
     *   file.
     */

    @Override
    @PublicMethod
    public StorageNodeStatus ping(short serialVersion) {
        return ping(null, serialVersion);
    }

    @Override
    @PublicMethod
    public StorageNodeStatus ping(AuthContext authCtx,
                                  short serialVersion) {

        return faultHandler.execute
            (new ProcessFaultHandler.SimpleOperation<StorageNodeStatus>() {

                @Override
                public StorageNodeStatus execute() {
                    return sna.getStatus();
                }
            });
    }

    @Override
    @SecureR2Method
    public List<ParameterMap> register(final ParameterMap gpMap,
                                      final ParameterMap snpMap,
                                      final boolean hostingAdmin,
                                      short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public synchronized List<ParameterMap> register(final ParameterMap gpMap,
                                                    final ParameterMap snpMap,
                                                    final boolean hostingAdmin,
                                                    AuthContext authCtx,
                                                    short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            (new ProcessFaultHandler.
             Operation<List<ParameterMap>, RemoteException>() {
                @Override
                public List<ParameterMap> execute()
                    throws RemoteException {
                    GlobalParams gp = new GlobalParams(gpMap);
                    StorageNodeParams snp = new StorageNodeParams(snpMap);
                    return sna.register(gp, snp, hostingAdmin);
                }
            });
    }

    @SecureR2Method
    @Override
    public void shutdown(final boolean stopServices,
                         final boolean force,
                         short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    /**
     * Stops a running Storage Node Agent and maybe all running services.
     */
    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public synchronized void shutdown(final boolean stopServices,
                                      final boolean force,
                                      AuthContext authCtx,
                                      short serialVersion)
        throws RemoteException {

        faultHandler.execute
            (new ProcessFaultHandler.Procedure<RemoteException>() {
                @Override
                public void execute()
                    throws RemoteException {

                    sna.shutdown(stopServices, force);
                }
            });
    }

    @Override
    @SecureR2Method
    public boolean createAdmin(final ParameterMap adminParams,
                               short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }
    /**
     * Create and start a Admin instance.  If this SN is hosting the primary
     * Admin and this method is called the Admin is not started.  It is assumed
     * to be already managed.
     */
    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public synchronized boolean createAdmin(final ParameterMap adminParams,
                                            AuthContext authCtx,
                                            short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            (new ProcessFaultHandler.Operation<Boolean, RemoteException>() {
                @Override
                public Boolean execute()
                    throws RemoteException {

                    return sna.createAdmin(new AdminParams(adminParams));
                }
            });
    }

    @Override
    @SecureR2Method
    public boolean startAdmin(short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    /**
     * Start an already-created Admin instance
     */
    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public synchronized boolean startAdmin(AuthContext authCtx,
                                           short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            (new ProcessFaultHandler.Operation<Boolean, RemoteException>() {
                @Override
                public Boolean execute()
                    throws RemoteException {

                    checkRegistered("startAdmin");
                    logInfo("startAdmin called");

                    AdminParams ap =
                        ConfigUtils.getAdminParams(sna.getKvConfigFile());
                    if (ap == null) {
                        String msg =
                            "Attempt to start an Admin when none is configured";
                        logInfo(msg);
                        throw new IllegalStateException(msg);
                    }
                    return sna.startAdmin(ap);
                }
            });
    }

    @Override
    @SecureR2Method
    public boolean stopAdmin(final boolean force,
                             short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    /**
     * Stop the running Admin instance
     */
    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public synchronized boolean stopAdmin(final boolean force,
                                          AuthContext authCtx,
                                          short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            (new ProcessFaultHandler.Operation<Boolean, RemoteException>() {
                @Override
                public Boolean execute()
                    throws RemoteException {

                    checkRegistered("stopAdmin");
                    return sna.stopAdmin(null, force);
                }
            });
    }

    @Override
    @SecureR2Method
    public boolean destroyAdmin(final AdminId adminId,
                                final boolean deleteData,
                                short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    /**
     * Destroy the Admin instance.  After this it will be necessary to use
     * createAdmin() if another Admin is required.
     */
    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public synchronized boolean destroyAdmin(final AdminId adminId,
                                             final boolean deleteData,
                                             AuthContext authCtx,
                                             short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            (new ProcessFaultHandler.Operation<Boolean, RemoteException>() {
                @Override
                public Boolean execute()
                    throws RemoteException {

                    checkRegistered("destroyAdmin");
                    return sna.destroyAdmin(adminId, deleteData);
                }
            });
    }

    @Override
    @SecureR2Method
    public boolean repNodeExists(final RepNodeId repNodeId,
                                 short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    /**
     * Does this RepNode exist in the configuration file?
     */
    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public synchronized boolean repNodeExists(final RepNodeId repNodeId,
                                              AuthContext authCtx,
                                              short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            (new ProcessFaultHandler.
             Operation<Boolean, RemoteException>() {
                @Override
                public Boolean execute()
                    throws RemoteException {
                    checkRegistered("repNodeExists");
                    if (sna.lookupRepNode(repNodeId) != null) {
                        return true;
                    }
                    return false;
                }
            });
    }

    /**
     * Create a new RepNode instance based on the configuration information.
     */
    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public synchronized boolean
        createRepNode(final ParameterMap params,
                      final Set<Metadata<? extends MetadataInfo>> metadataSet,
                      AuthContext authCtx,
                      short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            (new ProcessFaultHandler.Operation<Boolean, RemoteException>() {
                @Override
                public Boolean execute()
                    throws RemoteException {

                    checkRegistered("createRepNode");
                    RepNodeParams repNodeParams = new RepNodeParams(params);
                    return sna.createRepNode(repNodeParams, metadataSet);
                }
            });
    }

    @Deprecated
    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public boolean createRepNode(ParameterMap repNodeParams,
                                 Topology topology,
                                 AuthContext authCtx,
                                 short serialVersion)
        throws RemoteException {
        
        final Set<Metadata<? extends MetadataInfo>> metadataSet =
                new HashSet<Metadata<? extends MetadataInfo>>(1);
        metadataSet.add(topology);
        return createRepNode(repNodeParams,
                             metadataSet,
                             authCtx, serialVersion);
    }
    
    @Deprecated
    @Override
    @SecureR2Method
    public boolean createRepNode(final ParameterMap params,
                                 final Topology topology,
                                 short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }
    
    @Override
    @SecureR2Method
    public boolean startRepNode(final RepNodeId repNodeId,
                                short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    /**
     * Start an already-create RepNode.  This implicitly sets its state to
     * active.
     */
    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public synchronized boolean startRepNode(final RepNodeId repNodeId,
                                             AuthContext authCtx,
                                             short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            (new ProcessFaultHandler.Operation<Boolean, RemoteException>() {
                @Override
                public Boolean execute()
                    throws RemoteException {

                    checkRegistered("startRepNode");
                    return sna.startRepNode(repNodeId);
                }
            });
    }

    @Override
    @SecureR2Method
    public boolean stopRepNode(final RepNodeId rnid,
                               final boolean force,
                               short serialVersion)
        throws  RemoteException {

        throw invalidR2MethodException();
    }

    /**
     * Stop a running RepNode
     */
    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public synchronized boolean stopRepNode(final RepNodeId rnid,
                                            final boolean force,
                                            AuthContext authCtx,
                                            short serialVersion)
        throws  RemoteException {

        return faultHandler.execute
            (new ProcessFaultHandler.Operation<Boolean, RemoteException>() {
                @Override
                public Boolean execute()
                    throws RemoteException {

                    checkRegistered("stopRepNode");
                    return sna.stopRepNode(rnid, force);
                }
            });
    }

    @Override
    @SecureR2Method
    public boolean destroyRepNode(final RepNodeId rnid,
                                  final boolean deleteData,
                                  short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    /**
     * Stop and destroy a RepNode.  After this it will be necessary to
     * re-create the instance using createRepNode().
     */
    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public synchronized boolean destroyRepNode(final RepNodeId rnid,
                                               final boolean deleteData,
                                               AuthContext authCtx,
                                               short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            (new ProcessFaultHandler.Operation<Boolean, RemoteException>() {
                @Override
                public Boolean execute()
                    throws RemoteException {

                    checkRegistered("destroyRepNode");
                    String serviceName = rnid.getFullName();
                    logInfo(serviceName + ": destroyRepNode called");

                    /**
                     * Ignore the return value from stopRepNode.  The node may
                     * be running or not, it doesn't matter.
                     */
                    boolean retval = true;
                    try {
                        sna.stopRepNode(rnid, true);
                    } finally {
                        retval =
                            sna.removeConfigurable(rnid, null, deleteData);
                    }
                    return retval;
                }
            });
    }

    @Override
    @SecureR2Method
    public void newRepNodeParameters(final ParameterMap params,
                                     short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    /**
     * Only write the new parameters. The admin will notify the RepNode or
     * restart it if that is required. Allow this call to be made for RepNodes
     * that are not running. The parameters are a full replacement, not a merge
     * set.
     */
    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public synchronized void newRepNodeParameters(final ParameterMap params,
                                                  AuthContext authCtx,
                                                  short serialVersion)
        throws RemoteException {

        faultHandler.execute
            (new ProcessFaultHandler.Procedure<RemoteException>() {
                @Override
                public void execute()
                    throws RemoteException {

                    checkRegistered("newRepNodeParameters");
                    RepNodeParams repNodeParams = new RepNodeParams(params);
                    String serviceName =
                        repNodeParams.getRepNodeId().getFullName();
                    logInfo(serviceName + ": newRepNodeParameters called");

                    /**
                     * Change the config file so the RN can see the state.
                     */
                    sna.replaceRepNodeParams(repNodeParams);
                }
            });
    }

    @Override
    @SecureR2Method
    public void newAdminParameters(final ParameterMap params,
                                   short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    /**
     * Only write the new parameters.  The admin will notify the Admin or
     * restart it if that is required.  Allow this call to be made for Admin
     * instances that are not running.  The parameters are a full replacement,
     * not a merge set.
     */
    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public synchronized void newAdminParameters(final ParameterMap params,
                                                AuthContext authCtx,
                                                short serialVersion)
        throws RemoteException {

        faultHandler.execute
            (new ProcessFaultHandler.Procedure<RemoteException>() {
                @Override
                public void execute()
                    throws RemoteException {

                    checkRegistered("newAdminParameters");
                    AdminId adminId = new AdminParams(params).getAdminId();
                    logInfo(adminId.getFullName() +
                            ": newAdminParameters called");

                    /**
                     * Change the config file so the Admin can see the state.
                     */
                    sna.replaceAdminParams(adminId, params);
                }
            });
    }

    @Override
    @SecureR2Method
    public void newStorageNodeParameters(final ParameterMap params,
                                         short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public synchronized void newStorageNodeParameters
        (final ParameterMap params,
         AuthContext authCtx,
         short serialVersion)
        throws RemoteException {

        faultHandler.execute
            (new ProcessFaultHandler.Procedure<RemoteException>() {
                @Override
                public void execute()
                    throws RemoteException {

                    checkRegistered("newStorageNodeParameters");
                    logInfo("newStorageNodeParameters called");
                    sna.newParams(params);
                }
            });
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public void newGlobalParameters(final ParameterMap params,
                                    AuthContext authCtx,
                                    short serialVersion)
        throws RemoteException {

        faultHandler.execute
        (new ProcessFaultHandler.Procedure<RemoteException>() {
            @Override
            public void execute()
                throws RemoteException {

                checkRegistered("newGlobalParameters");
                final GlobalParams globalParams = new GlobalParams(params);
                logInfo("newGlobalParameters called");

                /*
                 * Change the config file so the admin and repnode can see the
                 * state
                 */
                sna.replaceGlobalParams(globalParams);
            }
        });
    }

    /**
     * Snapshot methods
     */

    @Override
    @SecureR2Method
    public void createSnapshot(final RepNodeId rnid,
                               final String name,
                               short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public synchronized void createSnapshot(final RepNodeId rnid,
                                            final String name,
                                            AuthContext authCtx,
                                            short serialVersion)
        throws RemoteException {

        faultHandler.execute
            (new ProcessFaultHandler.Procedure<RemoteException>() {
                @Override
                public void execute()
                    throws RemoteException {

                    checkRegistered("snapshot RepNode");
                    logInfo("createSnapshot called for " + rnid +
                            ", snapshot name: " + name);
                    sna.snapshotRepNode(rnid, name);
                }
            });
    }

    @Override
    @SecureR2Method
    public void removeSnapshot(final RepNodeId rnid,
                               final String name,
                               short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public synchronized void removeSnapshot(final RepNodeId rnid,
                                            final String name,
                                            AuthContext authCtx,
                                            short serialVersion)
        throws RemoteException {

        faultHandler.execute
            (new ProcessFaultHandler.Procedure<RemoteException>() {
                @Override
                public void execute()
                    throws RemoteException {
                    checkRegistered("removeSnapshot");
                    logInfo("removeSnapshot called, name is " + name);
                    sna.removeSnapshot(rnid, name);
                }
            });
    }

    @Override
    @SecureR2Method
    public void removeAllSnapshots(final RepNodeId rnid,
                                   short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public synchronized void removeAllSnapshots(final RepNodeId rnid,
                                                AuthContext authCtx,
                                                short serialVersion)
        throws RemoteException {

        faultHandler.execute
            (new ProcessFaultHandler.Procedure<RemoteException>() {
                @Override
                public void execute()
                    throws RemoteException {
                    checkRegistered("removeAllSnapshots");
                    logInfo("removeAllSnapshots called");
                    sna.removeSnapshot(rnid, null);
                }
            });
    }

    @Override
    @SecureR2Method
    public void createSnapshot(final AdminId aid,
                               final String name,
                               short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public synchronized void createSnapshot(final AdminId aid,
                                            final String name,
                                            AuthContext authCtx,
                                            short serialVersion)
        throws RemoteException {

        faultHandler.execute
            (new ProcessFaultHandler.Procedure<RemoteException>() {
                @Override
                public void execute()
                    throws RemoteException {

                    checkRegistered("snapshot Admin");
                    logInfo("createSnapshot called for " + aid +
                            ", snapshot name: " + name);
                    sna.snapshotAdmin(aid, name);
                }
            });
    }

    @Override
    @SecureR2Method
    public void removeSnapshot(final AdminId aid,
                               final String name,
                               short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public synchronized void removeSnapshot(final AdminId aid,
                                            final String name,
                                            AuthContext authCtx,
                                            short serialVersion)
        throws RemoteException {

        faultHandler.execute
            (new ProcessFaultHandler.Procedure<RemoteException>() {
                @Override
                public void execute()
                    throws RemoteException {
                    checkRegistered("removeSnapshot");
                    logInfo("removeSnapshot called, name is " + name);
                    sna.removeSnapshot(aid, name);
                }
            });
    }

    @Override
    @SecureR2Method
    public void removeAllSnapshots(final AdminId aid,
                                   short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public synchronized void removeAllSnapshots(final AdminId aid,
                                                AuthContext authCtx,
                                                short serialVersion)
        throws RemoteException {

        faultHandler.execute
            (new ProcessFaultHandler.Procedure<RemoteException>() {
                @Override
                public void execute()
                    throws RemoteException {
                    checkRegistered("removeAllSnapshots");
                    logInfo("removeAllSnapshots called");
                    sna.removeSnapshot(aid, null);
                }
            });
    }

    @Override
    @SecureR2Method
    public String [] listSnapshots(short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public String [] listSnapshots(AuthContext authCtx,
                                   short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            (new ProcessFaultHandler.Operation<String[], RemoteException>() {
                @Override
                public String[] execute()
                    throws RemoteException {

                    checkRegistered("listSnapshots");
                    logInfo("listSnapshots called");
                    return sna.listSnapshots();
                }
            });
    }

    @Override
    @SecureR2Method
    public LoadParameters getParams(short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public LoadParameters getParams(AuthContext authCtx,
                                    short serialVersion)
        throws RemoteException {

        return faultHandler.
            execute(new ProcessFaultHandler.SimpleOperation<LoadParameters>() {

            @Override
            public LoadParameters execute() {
                return sna.getParams();
            }
        });
    }

    @Override
    @SecureR2Method
    public StringBuilder getStartupBuffer(final ResourceId rid,
                                          final short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public StringBuilder getStartupBuffer(final ResourceId rid,
                                          AuthContext authCtx,
                                          final short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            (new ProcessFaultHandler.SimpleOperation<StringBuilder>() {
            @Override
            public StringBuilder execute() {
                checkRegistered("getStartupBuffer");
                return sna.getStartupBuffer(rid);
            }
        });
    }

    /**
     * End StorageNodeAgentInterface
     */

    /**
     * The Storage Node Agent main.  It requires two arguments:
     * -root <SNA_bootstrapdir>
     *    This is the directory in which the SNA looks for a bootstrap
     *    configuration file.  It is not the KV store root but could be if
     *    desired.
     * -config <bootstrap_file_name>
     *    This is the file that contains the bootstrap parameters required by
     *    BootstrapParams.  It has a specific format.
     * [-threads ] If present this argument means "run in thread mode" which
     *    causes the SNA to create threads instead of processes for its managed
     *    services.
     *
     * TODO: remove -k flag and put the same function in a standalone utility.
     */
    public static void main(String[] args) {

        final StorageNodeAgentImpl snai = new StorageNodeAgentImpl();
        final StorageNodeAgent sna = snai.getStorageNodeAgent();
        try {
            final boolean shutdown = sna.parseArgs(args);
            final File configPath = new File(sna.getBootstrapDir(),
                                             sna.getBootstrapFile());
            if (shutdown) {
                if (!configPath.exists()) {
                    System.err.println("Bootstrap config file " +
                                       configPath + " does not exist");
                    return;
                }

                if (sna.verbose()) {
                    System.err.println("Stopping SNA based on " +
                                       configPath);
                }

                /* Try to stop a specific SNA. */
                if (sna.stopRunningAgent()) {
                    return;
                }

                /*
                 * This code will run if there is a problem shutting down the
                 * SNA above.
                 */
                if (sna.verbose()) {
                    System.err.println("Attempting to stop all SNAs in root " +
                                       "directory " + sna.getBootstrapDir());
                }

                /*
                 * Kill processes related to the target kvRoot.
                 */
                final String rootPath = CommandParser.ROOT_FLAG + " " +
                    sna.getBootstrapDir();
                ManagedService.killManagedProcesses
                    ("StorageNodeAgentImpl", rootPath, null, sna.getLogger());
                ManagedService.killManagedProcesses
                    (rootPath, null, sna.getLogger());
            } else {
                sna.addShutdownHook();
                if (sna.verbose()) {
                    System.err.println("Starting SNA based on " +
                                       configPath);
                }
                sna.start();
            }
        } catch (Exception e) {
            if (sna.getLogger() != null) {
                sna.getLogger().
                    severe("Failed to start SNA: " + e.getMessage() + "\n" +
                           LoggerUtils.getStackTrace(e));
            }
        }
    }

    /**
     * Begin MasterBalancingInterface
     */

    @Override
    @SecureR2Method
    public void noteState(final StateInfo stateInfo,
                          final short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public void noteState(final StateInfo stateInfo,
                          final AuthContext authCtx,
                          final short serialVersion)
        throws RemoteException {

        faultHandler.execute
        (new ProcessFaultHandler.Procedure<RemoteException>() {
            @Override
            public void execute()
                throws RemoteException {
                checkRegistered("noteState");
                logInfo("noteState called");
                sna.getMasterBalanceManager().
                    noteState(stateInfo, authCtx, serialVersion);
            }
        });
    }

    @Override
    @SecureR2Method
    public MDInfo getMDInfo(final short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public MDInfo getMDInfo(final AuthContext authCtx,
                            final short serialVersion)
        throws RemoteException {

        return faultHandler.execute
                (new ProcessFaultHandler.Operation<MDInfo,
                                                   RemoteException>() {
                    @Override
                    public MDInfo execute()
                        throws RemoteException {

                        checkRegistered("getMD");
                        logFine("getMD called");
                        return sna.getMasterBalanceManager().
                                getMDInfo(authCtx, serialVersion);
                    }
                });
    }

    @Override
    @SecureR2Method
    public boolean getMasterLease(final MasterLeaseInfo masterLease,
                                  final short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public boolean getMasterLease(final MasterLeaseInfo masterLease,
                                  final AuthContext authCtx,
                                  final short serialVersion)
        throws RemoteException {

        return faultHandler.execute
                (new ProcessFaultHandler.Operation<Boolean,
                                                   RemoteException>() {
                    @Override
                    public Boolean execute()
                        throws RemoteException {

                        checkRegistered("lease");
                        logFine("Master lease called");
                        return sna.getMasterBalanceManager().
                                getMasterLease(masterLease, authCtx,
                                               serialVersion);
                    }
                });
    }

    @Override
    @SecureR2Method
    public boolean cancelMasterLease(final StorageNode lesseeSN,
                                     final RepNode rn,
                                     final short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public boolean cancelMasterLease(final StorageNode lesseeSN,
                                     final RepNode rn,
                                     final AuthContext authCtx,
                                     final short serialVersion)
        throws RemoteException {

        return faultHandler.execute
                (new ProcessFaultHandler.Operation<Boolean,
                                                   RemoteException>() {
                    @Override
                    public Boolean execute()
                        throws RemoteException {

                        checkRegistered("cancelLease");
                        logFine("cancelLease called");
                        return sna.getMasterBalanceManager().
                                cancelMasterLease(lesseeSN, rn,
                                                  authCtx, serialVersion);

                    }
                });
    }

    @Override
    @SecureR2Method
    public void overloadedNeighbor(final StorageNodeId storageNodeId,
                                   final short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public void overloadedNeighbor(final StorageNodeId storageNodeId,
                                   final AuthContext authCtx,
                                   final short serialVersion)
        throws RemoteException {

        faultHandler.execute
        (new ProcessFaultHandler.Procedure<RemoteException>() {
            @Override
            public void execute()
                throws RemoteException {

                sna.getMasterBalanceManager().
                    overloadedNeighbor(storageNodeId, authCtx, serialVersion);
            }
        });
    }
}
