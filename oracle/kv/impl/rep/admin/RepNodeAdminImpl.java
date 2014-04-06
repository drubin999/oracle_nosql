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

package oracle.kv.impl.rep.admin;

import java.lang.reflect.Constructor;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.KVSecurityException;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.api.TopologyInfo;
import oracle.kv.impl.fault.ClientAccessException;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.measurement.ServiceStatusChange;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.metadata.MetadataKey;
import oracle.kv.impl.mgmt.RepNodeStatusReceiver;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterListener;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.rep.OperationsStatsTracker;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.RepNodeService;
import oracle.kv.impl.rep.RepNodeStatus;
import oracle.kv.impl.rep.migration.PartitionMigrationStatus;
import oracle.kv.impl.rep.monitor.StatsPacket;
import oracle.kv.impl.security.AccessChecker;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.ConfigurationException;
import oracle.kv.impl.security.ExecutionContext;
import oracle.kv.impl.security.KVStoreRole;
import oracle.kv.impl.security.KVStoreRolePrincipal;
import oracle.kv.impl.security.OperationContext;
import oracle.kv.impl.security.SecureProxy;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.security.annotations.PublicMethod;
import oracle.kv.impl.security.annotations.SecureAPI;
import oracle.kv.impl.security.annotations.SecureAutoMethod;
import oracle.kv.impl.security.annotations.SecureInternalMethod;
import oracle.kv.impl.security.annotations.SecureR2Method;
import oracle.kv.impl.test.RemoteTestInterface;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.ServiceStatusTracker;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RMISocketPolicy;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryPair;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.registry.RegistryUtils.InterfaceType;
import oracle.kv.impl.util.registry.VersionedRemoteImpl;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.vlsn.VLSNIndex;
import com.sleepycat.je.util.DbBackup;
import com.sleepycat.je.utilint.VLSN;

/**
 * The implementation of the RN administration protocol.
 */
@SecureAPI
public class RepNodeAdminImpl
    extends VersionedRemoteImpl implements RepNodeAdmin {

    /**
     *  The repNode being administered
     */
    private final RepNode repNode;
    private final RepNodeService repNodeService;

    /**
     * The fault handler associated with the service.
     */
    private final RepNodeAdminFaultHandler faultHandler;
    private final Logger logger;

    /**
     * The exportable/bindable version of this object
     */
    private RepNodeAdmin exportableRepNodeAdmin;

    private DbBackup dbBackup;

    /**
     * A remote reference to the status collector on the local StorageNode.
     */
    RepNodeStatusReceiver statusReceiver = null;

    /**
     * A conditional instance, created if the class can be found.
     */
    private RemoteTestInterface rti;
    private static final int REQUEST_QUIESCE_POLL_MS = 100;
    private static final int REQUEST_QUIESCE_MS = 10000;

    /* Set to true during shutdown when a shutdown request is in progress */
    private volatile boolean shutdownActive = false;

    private static final String TEST_INTERFACE_NAME=
        "oracle.kv.impl.rep.RepNodeTestInterface";

    public RepNodeAdminImpl(RepNodeService repNodeService, RepNode repNode) {

        this.repNodeService = repNodeService;
        this.repNode = repNode;
        rti = null;
        logger =
            LoggerUtils.getLogger(this.getClass(), repNodeService.getParams());

        faultHandler = new RepNodeAdminFaultHandler(repNodeService,
                                                    logger,
                                                    ProcessExitCode.RESTART);
    }

    private void assertRunning() {

        ServiceStatus status =
            repNodeService.getStatusTracker().getServiceStatus();
        if (status != ServiceStatus.RUNNING) {
            throw new IllegalRepNodeServiceStateException
                ("RepNode is not RUNNING, current status is " + status);
        }
    }

    /**
     * Create the test interface if it can be found.
     */
    private void startTestInterface() {
        try {
            Class<?> cl = Class.forName(TEST_INTERFACE_NAME);
            Constructor<?> c = cl.getConstructor(repNodeService.getClass());
            rti = (RemoteTestInterface) c.newInstance(repNodeService);
            rti.start(SerialVersion.CURRENT);
        } catch (Exception ignored) {
        }
    }

    @Override
    @SecureR2Method
    public void newParameters(short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public void newParameters(AuthContext authCtx, short serialVersion)
        throws RemoteException {
        faultHandler.execute(new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                repNodeService.newParameters();
            }
        });
    }

    /* no R2-compatible version */
    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public void newGlobalParameters(AuthContext authCtx, short serialVersion)
        throws RemoteException {
        faultHandler.execute(new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                repNodeService.newGlobalParameters();
            }
        });
    }

    @Override
    @SecureR2Method
    public Topology getTopology(short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public Topology getTopology(AuthContext authCtx, short serialVersion) {
        return faultHandler.
            execute(new ProcessFaultHandler.SimpleOperation<Topology>() {

            @Override
            public Topology execute() {
                final ServiceStatus status =
                    repNodeService.getStatusTracker().getServiceStatus();

                return (status == ServiceStatus.RUNNING) ?
                       repNode.getTopology() : null ;
            }
        });
    }

    @Override
    @SecureR2Method
    public int getTopoSeqNum(short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public int getTopoSeqNum(AuthContext authCtx, short serialVersion)
        throws RemoteException {

        return faultHandler.
        execute(new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                final ServiceStatus status =
                    repNodeService.getStatusTracker().getServiceStatus();

                if (status != ServiceStatus.RUNNING) {
                    return 0;
                }

                final Topology topology = repNode.getTopology();
                return (topology != null) ?
                        topology.getSequenceNumber() :
                        Topology.EMPTY_SEQUENCE_NUMBER;
            }
        });
    }

    @Override
    @SecureR2Method
    public LoadParameters getParams(short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public LoadParameters getParams(AuthContext authCtx, short serialVersion) {
        return faultHandler.
            execute(new ProcessFaultHandler.SimpleOperation<LoadParameters>() {

            @Override
            public LoadParameters execute() {
                assertRunning();
                return repNode.getAllParams();
            }
        });
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public void configure(final Set<Metadata<? extends MetadataInfo>> metadataSet,
                          AuthContext authCtx,
                          short serialVersion) {
        faultHandler.execute(new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                for (Metadata md : metadataSet) {
                    repNode.updateMetadata(md);
                }
            }
        });
    }
    
    @Override
    @SecureR2Method
    public void configure(final Topology topology, short serialVersion)
        throws RemoteException {
        throw invalidR2MethodException();
    }
    
    @Override
    @Deprecated
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public void configure(final Topology topology,
                          AuthContext authCtx,
                          short serialVersion) {
        faultHandler.execute(new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {    
                repNode.updateMetadata(topology);
            }
        });
    }

    @Override
    @SecureR2Method
    @Deprecated
    public void updateTopology(final Topology newTopology, short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    /**
     * Introduced in R3 to support the creation of SecureProxy for
     * #updateTopology(Topology, short)
     * @deprecated
     */
    @Deprecated
    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public void updateTopology(final Topology newTopology,
                               AuthContext authCtx,
                               short serialVersion)
        throws RemoteException {

        updateMetadata(newTopology, authCtx, serialVersion);
    }

    @Deprecated
    @Override
    @SecureR2Method
    public int updateTopology(final TopologyInfo topoInfo,
                              final short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    /**
     * Introduced in R3 to support the creation of SecureProxy for
     * #updateTopology(TopologyInfo, short)
     * @deprecated
     */
    @Deprecated
    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public int updateTopology(final TopologyInfo topoInfo,
                              final AuthContext authCtx,
                              short serialVersion)
        throws RemoteException {

        return updateMetadata(topoInfo, authCtx, serialVersion);
    }

    @Deprecated
    @Override
    @SecureR2Method
    public void shutdown(final boolean force, short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    /**
     * Shutdown the rep node service. Note that invoking shutdown will result
     * in a callback via the {@link #stop() method}, which will unregister the
     * admin from the registry. It should not impact this remote call which
     * is already in progress.
     */
    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public void shutdown(final boolean force, AuthContext authCtx,
                         short serialVersion)
        throws RemoteException {

        faultHandler.execute(new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                logger.log(Level.INFO, "RepNodeAdmin shutdown({0})", force);
                shutdownActive = true;
                try {
                    repNodeService.stop(force);
                } finally {
                    shutdownActive = false;
                }
            }
        });
    }

    /**
     * Starts up the admin component, binding its stub in the registry, so that
     * it can start accepting remote admin requests.
     *
     * @throws RemoteException
     */
    public void startup()
        throws RemoteException {

        faultHandler.execute
        (new ProcessFaultHandler.Procedure<RemoteException>() {

            @Override
            public void execute() throws RemoteException {
                final String kvsName = repNodeService.getParams().
                        getGlobalParams().getKVStoreName();
                final RepNodeParams rnp = repNodeService.getRepNodeParams();
                final StorageNodeParams snp =
                    repNodeService.getParams().getStorageNodeParams();

                final String csfName = ClientSocketFactory.
                        factoryName(kvsName,
                                    RepNodeId.getPrefix(),
                                    InterfaceType.ADMIN.interfaceName());

                RMISocketPolicy rmiPolicy = repNodeService.getParams().
                    getSecurityParams().getRMISocketPolicy();
                SocketFactoryPair sfp =
                    rnp.getAdminSFP(rmiPolicy,
                                    snp.getServicePortRange(),
                                    csfName);

                if (sfp.getServerFactory() != null) {
                    sfp.getServerFactory().setConnectionLogger(logger);
                }
                initExportableRepNodeAdmin();
                repNodeService.rebind(exportableRepNodeAdmin,
                                      InterfaceType.ADMIN,
                                      sfp.getClientFactory(),
                                      sfp.getServerFactory());

                logger.info("RepNodeAdmin registered");
                startTestInterface();
            }
        });
    }

    private void initExportableRepNodeAdmin() {
        try {
            exportableRepNodeAdmin =
                SecureProxy.create(
                    RepNodeAdminImpl.this,
                    repNodeService.getRepNodeSecurity().getAccessChecker(),
                    faultHandler);
            logger.info(
                "Successfully created secure proxy for the repnode admin");
        } catch (ConfigurationException ce) {
            logger.info("Unable to create proxy: " + ce + " : " +
                        ce.getMessage());
            throw new IllegalStateException("Unabled to create proxy", ce);
        }
    }

    /**
     * Unbind the admin entry from the registry.
     *
     * If any exceptions are encountered, during the unbind, they are merely
     * logged and otherwise ignored, so that other components can continue
     * to be shut down.
     */
    public void stop() {
        try {
            repNodeService.unbind(exportableRepNodeAdmin,
                                  RegistryUtils.InterfaceType.ADMIN);
            logger.info("RepNodeAdmin stopping");
            if (rti != null) {
                rti.stop(SerialVersion.CURRENT);
            }

            /*
             * Wait for the admin requests to quiesce within the
             * requestQuiesceMs period now that new admin requests have been
             * blocked.
             */
            final boolean quiesced =
                new PollCondition(REQUEST_QUIESCE_POLL_MS,
                                  REQUEST_QUIESCE_MS) {

                @Override
                protected boolean condition() {
                    return faultHandler.getActiveRequests() ==
                           (shutdownActive ? 1 : 0);
                }

            }.await();

            if (!quiesced) {
                logger.info(faultHandler.getActiveRequests() +
                            " admin requests were active on close.");
            }
        } catch (RemoteException e) {
            logger.log(Level.INFO,
                       "Ignoring exception while stopping repNodeAdmin", e);
            return;
        }
    }

    @Override
    @SecureR2Method
    public RepNodeStatus ping(short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @PublicMethod
    public RepNodeStatus ping(AuthContext authCtx, short serialVersion) {

        return faultHandler.
        execute(new ProcessFaultHandler.SimpleOperation<RepNodeStatus>() {

            @Override
            public RepNodeStatus execute() {
                ServiceStatus status =
                    repNodeService.getStatusTracker().getServiceStatus();
                State state = State.DETACHED;
                long currentVLSN = 0;
                try {
                    ReplicatedEnvironment env = repNode.getEnv(1);
                    if (env != null) {
                        final RepImpl repImpl = RepInternal.getRepImpl(env);

                        /* May be null if env is invalidated */
                        final VLSNIndex vlsnIndex = (repImpl == null) ?
                                null :
                                RepInternal.getRepImpl(env).getVLSNIndex();
                        /* May be null if DETACHED. */
                        currentVLSN =  (vlsnIndex == null) ?
                                VLSN.NULL_VLSN.getSequence() :
                                vlsnIndex.getRange().getLast().getSequence();
                        try {
                            state = env.getState();
                        } catch (IllegalStateException iae) {
                            /* State cannot be queried if detached. */
                            state = State.DETACHED;
                        }
                    }
                } catch (EnvironmentFailureException ignored) {

                    /*
                     * The environment could be invalid.
                     */
                }
                String haHostPort =
                            repNode.getRepNodeParams().getJENodeHostPort();
                return new RepNodeStatus(status, state,
                                         currentVLSN, haHostPort,
                                         repNode.getMigrationStatus());
            }
        });
    }

    @Override
    @SecureR2Method
    public RepNodeInfo getInfo(short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public RepNodeInfo getInfo(AuthContext authCtx, short serialVersion)
        throws RemoteException {

        return faultHandler.
        execute(new ProcessFaultHandler.SimpleOperation<RepNodeInfo>() {

            @Override
            public RepNodeInfo execute() {
                assertRunning();
                return new RepNodeInfo(repNode);
            }
        });
    }

    @Override
    @SecureR2Method
    public String [] startBackup(short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public String [] startBackup(AuthContext authCtx, short serialVersion)
        throws RemoteException {

        return faultHandler.
        execute(new ProcessFaultHandler.SimpleOperation<String []>() {

            @Override
            public String [] execute() {
                assertRunning();

                if (dbBackup != null) {
                    logger.warning("startBackup: dbBackup not null");
                    dbBackup.endBackup();
                }

                /*
                 * TODO: consider a checkpoint...
                 */
                ReplicatedEnvironment env = repNode.getEnv(1);
                if (env == null) {
                    throw new
                        OperationFaultException("Environment unavailable");
                }
                logger.info("startBackup: starting backup/snapshot");
                dbBackup = new DbBackup(env);
                dbBackup.startBackup();
                return dbBackup.getLogFilesInBackupSet();
            }
        });
    }

    @Override
    @SecureR2Method
    public long stopBackup(short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public long stopBackup(AuthContext authCtx, short serialVersion)
        throws RemoteException {

        return faultHandler.
        execute(new ProcessFaultHandler.SimpleOperation<Long>() {

            @Override
            public Long execute() {
                assertRunning();
                logger.info("Ending backup/snapshot");
                long lastFile = -1;
                if (dbBackup != null) {
                    lastFile = dbBackup.getLastFileInBackupSet();
                    dbBackup.endBackup();
                    dbBackup = null;
                }
                return lastFile;
            }

        });
    }

    @Override
    @SecureR2Method
    public boolean updateMemberHAAddress(final String groupName,
                                         final String targetNodeName,
                                         final String targetHelperHosts,
                                         final String newNodeHostPort,
                                         short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public boolean updateMemberHAAddress(final String groupName,
                                         final String targetNodeName,
                                         final String targetHelperHosts,
                                         final String newNodeHostPort,
                                         AuthContext authCtx,
                                         short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            (new ProcessFaultHandler.Operation<Boolean, RemoteException>() {

            @Override
            public Boolean execute() {
                assertRunning();
                try {
                    repNodeService.updateMemberHAAddress(groupName,
                                                         targetNodeName,
                                                         targetHelperHosts,
                                                         newNodeHostPort);
                    return true;
                } catch (UnknownMasterException e) {
                    return false;
                }
            }
        });
    }


    @Override
    @SecureR2Method
    public boolean initiateMasterTransfer(final RepNodeId replicaId,
                                          final int timeout,
                                          final TimeUnit timeUnit,
                                          final short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public boolean initiateMasterTransfer(final RepNodeId replicaId,
                                          final int timeout,
                                          final TimeUnit timeUnit,
                                          final AuthContext authCtx,
                                          final short serialVersion)
        throws RemoteException {

        return faultHandler.
            execute(new ProcessFaultHandler.SimpleOperation<Boolean>() {

                @Override
                public Boolean execute() {
                    final ServiceStatus status = repNodeService.
                        getStatusTracker().getServiceStatus();

                    return (status != ServiceStatus.RUNNING) ?
                        false :
                        repNode.initiateMasterTransfer(replicaId,
                                                       timeout, timeUnit);
                }
            });
    }

    @Override
    @SecureR2Method
    public PartitionMigrationState
                    migratePartition(final PartitionId partitionId,
                                     final RepGroupId sourceRGId,
                                     short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public PartitionMigrationState
                    migratePartition(final PartitionId partitionId,
                                     final RepGroupId sourceRGId,
                                     AuthContext authCtx,
                                     short serialVersion)
        throws RemoteException {

        return faultHandler.
        execute(
            new ProcessFaultHandler.SimpleOperation<PartitionMigrationState>() {

            @Override
            public PartitionMigrationState execute() {
                assertRunning();
                return repNode.migratePartition(partitionId, sourceRGId);
            }
        });
    }

    @Override
    @SecureR2Method
    public PartitionMigrationState
                    getMigrationState(final PartitionId partitionId,
                                      short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public PartitionMigrationState
                    getMigrationState(final PartitionId partitionId,
                                      AuthContext authCtx,
                                      short serialVersion)
        throws RemoteException {

        return faultHandler.
        execute(
            new ProcessFaultHandler.SimpleOperation<PartitionMigrationState>() {

            @Override
            public PartitionMigrationState execute() {
                try {
                    assertRunning();
                    return repNode.getMigrationState(partitionId);
                } catch (IllegalRepNodeServiceStateException irnsse) {
                    return PartitionMigrationState.UNKNOWN.setCause(irnsse);
                }
            }
        });
    }

    @Override
    @SecureR2Method
    public PartitionMigrationState canCancel(final PartitionId partitionId,
                                             short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public PartitionMigrationState canCancel(final PartitionId partitionId,
                                             AuthContext authCtx,
                                             short serialVersion)
        throws RemoteException {

        return faultHandler.
        execute(
            new ProcessFaultHandler.SimpleOperation<PartitionMigrationState>() {

            @Override
            public PartitionMigrationState execute() {
                try {
                    assertRunning();
                    return repNode.canCancel(partitionId);
                } catch (IllegalRepNodeServiceStateException irnsse) {
                    return PartitionMigrationState.UNKNOWN.setCause(irnsse);
                }
            }
        });
    }

    @Override
    @SecureR2Method
    public boolean canceled(final PartitionId partitionId,
                            final RepGroupId targetRGId,
                            short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public boolean canceled(final PartitionId partitionId,
                            final RepGroupId targetRGId,
                            AuthContext authCtx,
                            short serialVersion)
        throws RemoteException {

        return faultHandler.
        execute(new ProcessFaultHandler.SimpleOperation<Boolean>() {

            @Override
            public Boolean execute() {
                final ServiceStatus status = repNodeService.
                        getStatusTracker().getServiceStatus();

                return (status != ServiceStatus.RUNNING) ?
                        false : repNode.canceled(partitionId, targetRGId);
            }
        });
    }

    @Override
    @SecureR2Method
    public PartitionMigrationStatus
                    getMigrationStatus(final PartitionId partitionId,
                                       short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public PartitionMigrationStatus
                    getMigrationStatus(final PartitionId partitionId,
                                       AuthContext authCtx,
                                       short serialVersion)
        throws RemoteException {

        return faultHandler.
        execute(
           new ProcessFaultHandler.SimpleOperation<PartitionMigrationStatus>() {

            @Override
            public PartitionMigrationStatus execute() {
                assertRunning();
                return repNode.getMigrationStatus(partitionId);
            }
        });
    }

    @Override
    @SecureR2Method
    public void installStatusReceiver(final RepNodeStatusReceiver receiver,
                                      short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public void installStatusReceiver(final RepNodeStatusReceiver receiver,
                                      AuthContext authCtx,
                                      short serialVersion)
        throws RemoteException {

        statusReceiver = receiver;

        faultHandler.execute
            (new ProcessFaultHandler.Procedure<RemoteException>() {

            @Override
            public void execute() throws RemoteException {
                ServiceStatusTracker rnStatusTracker =
                    repNodeService.getStatusTracker();
                ServiceStatus status = rnStatusTracker.getServiceStatus();
                ServiceStatusChange s =
                    new ServiceStatusChange(status);
                receiver.updateRnStatus(s);
                rnStatusTracker.addListener(new StatusListener());

                OperationsStatsTracker opStatsTracker =
                    repNodeService.getOpStatsTracker();
                opStatsTracker.addListener(new OpStatsListener());

                repNodeService.addParameterListener
                    (new ParameterChangeListener());
            }
        });
    }

    @Override
    @SecureR2Method
    public boolean awaitConsistency(final long targetTime,
                                    final int timeout,
                                    final TimeUnit timeoutUnit,
                                    short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public boolean awaitConsistency(final long targetTime,
                                    final int timeout,
                                    final TimeUnit timeoutUnit,
                                    AuthContext authCtx,
                                    short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            (new ProcessFaultHandler.SimpleOperation<Boolean>() {

            @Override
            public Boolean execute() {
                return repNode.awaitConsistency(targetTime, timeout,
                                                timeoutUnit);
            }
        });
    }

    @Override
    @SecureR2Method
    public int getMetadataSeqNum(final MetadataType type, short serialVersion) {

        throw invalidR2MethodException();
    }

    @Override
    /*
     * Minimum required authentication is AUTHENTICATED.  Security metadata
     * requires INTERNAL role.
     */ 
    @SecureInternalMethod
    public int getMetadataSeqNum(final MetadataType type,
                                 final AuthContext authCtx,
                                 short serialVersion) {

        return faultHandler.
            execute(new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                /* Check access rights */
                checkMetadataAccess(type);

                final ServiceStatus status =
                    repNodeService.getStatusTracker().getServiceStatus();

                if (status != ServiceStatus.RUNNING) {
                    return Metadata.EMPTY_SEQUENCE_NUMBER;
                }

                return repNode.getMetadataSeqNum(type);
            }
        });
    }

    @Override
    @SecureR2Method
    public Metadata<?> getMetadata(final MetadataType type,
                                   short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    /*
     * Minimum required authentication is AUTHENTICATED.  Security metadata
     * requires INTERNAL role.
     */ 
    @SecureInternalMethod
    public Metadata<?> getMetadata(final MetadataType type,
                                   final AuthContext authCtx,
                                   short serialVersion) {

        return faultHandler.
            execute(new ProcessFaultHandler.SimpleOperation<Metadata<?>>() {

            @Override
            public Metadata<?> execute() {
                /* Check access rights */
                checkMetadataAccess(type);

                final ServiceStatus status =
                    repNodeService.getStatusTracker().getServiceStatus();

                return (status == ServiceStatus.RUNNING) ?
                       repNode.getMetadata(type) : null ;
            }
        });
    }

    @Override
    @SecureR2Method
    public MetadataInfo getMetadata(final MetadataType type,
                                    final int seqNum,
                                    short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    /*
     * Minimum required authentication is AUTHENTICATED.  Security metadata
     * requires INTERNAL role.
     */ 
    @SecureInternalMethod
    public MetadataInfo getMetadata(final MetadataType type,
                                    final int seqNum,
                                    final AuthContext authCtx,
                                    short serialVersion) {

        return faultHandler.
            execute(new ProcessFaultHandler.SimpleOperation<MetadataInfo>() {

            @Override
            public MetadataInfo execute() {
                /* Check access rights */
                checkMetadataAccess(type);

                final ServiceStatus status =
                    repNodeService.getStatusTracker().getServiceStatus();

                return (status == ServiceStatus.RUNNING) ?
                       repNode.getMetadata(type, seqNum) : null ;
            }
        });
    }

    @Override
    @SecureR2Method
    public MetadataInfo getMetadata(final MetadataType type,
                                    final MetadataKey key,
                                    final int seqNum,
                                    short serialVersion) {

        throw invalidR2MethodException();
    }

    @Override
    /*
     * Minimum required authentication is AUTHENTICATED.  Security metadata
     * requires INTERNAL role.
     */ 
    @SecureInternalMethod
    public MetadataInfo getMetadata(final MetadataType type,
                                    final MetadataKey key,
                                    final int seqNum,
                                    final AuthContext authCtx,
                                    short serialVersion) {

        return faultHandler.
            execute(new ProcessFaultHandler.SimpleOperation<MetadataInfo>() {

            @Override
            public MetadataInfo execute() {
                /* Check access rights */
                checkMetadataAccess(type);

                final ServiceStatus status =
                    repNodeService.getStatusTracker().getServiceStatus();

                return (status == ServiceStatus.RUNNING) ?
                       repNode.getMetadata(type, key, seqNum) : null ;
            }
        });
    }

    @Override
    @SecureR2Method
    public void updateMetadata(final Metadata<?> newMetadata,
                               short serialVersion) {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public void updateMetadata(final Metadata<?> newMetadata,
                               AuthContext authCtx,
                               short serialVersion) {

        faultHandler.execute(new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                assertRunning();
                if (!repNode.updateMetadata(newMetadata)) {
                    throw new
                       OperationFaultException("Update " +
                                               newMetadata.getType() +
                                               " metadata seq# " +
                                               newMetadata.getSequenceNumber() +
                                               " failed");
                }
            }
        });
    }

    @Override
    @SecureR2Method
    public int updateMetadata(final MetadataInfo metadataInfo,
                              short serialVersion) {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public int updateMetadata(final MetadataInfo metadataInfo,
                              AuthContext authCtx,
                              short serialVersion) {

        return faultHandler.
            execute(new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                assertRunning();
                return repNode.updateMetadata(metadataInfo);
            }
        });
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public boolean addIndexComplete(final String indexId,
                                    final String tableName,
                                    AuthContext authCtx,
                                    short serialVersion) {
        return faultHandler.execute
            (new ProcessFaultHandler.SimpleOperation<Boolean>() {
            @Override
            public Boolean execute() {
                return repNode.addIndexComplete(indexId, tableName);
            }
        });
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public boolean removeTableDataComplete(final String tableName,
                                           AuthContext authCtx,
                                           short serialVersion) {
        return faultHandler.execute
            (new ProcessFaultHandler.SimpleOperation<Boolean>() {
            @Override
            public Boolean execute() {
                return repNode.removeTableDataComplete(tableName);
            }
        });
    }

    /**
     * Verify that the caller of a metadata request has sufficient
     * authorization to access it.
     *
     * @throw SessionAccessException if there is an internal security error
     * @throw KVSecurityException if the a security exception is generated by
     * the requesting client
     */
    private void checkMetadataAccess(MetadataType mdType)
        throws SessionAccessException, KVSecurityException {

        final AccessChecker accessChecker = 
            repNodeService.getRepNodeSecurity().getAccessChecker();

        if (accessChecker != null) {
            try {
                accessChecker.checkAccess(ExecutionContext.getCurrent(),
                                          new MetadataContext(mdType));
            } catch (KVSecurityException kvse) {
                throw new ClientAccessException(kvse);
            }
        }
    }

    /**
     * Provides an implementation of OperationContext for access checking when
     * Metadata is requested.
     */
    private static class MetadataContext implements OperationContext {
        private final MetadataType mdType;

        /*
         * An immmutable role list, used when an operation is requested that
         * requires authentication.
         */
        private static final List<KVStoreRolePrincipal> authenticatedRoleList =
            Collections.unmodifiableList(
                Arrays.asList( new KVStoreRolePrincipal[] {
                        KVStoreRolePrincipal.AUTHENTICATED }));

        /*
         * An immmutable role list, used when an operation is requested that
         * requires internal authentication.
         */
        private static final List<KVStoreRolePrincipal> internalRoleList =
            Collections.unmodifiableList(
                Arrays.asList( new KVStoreRolePrincipal[] {
                        KVStoreRolePrincipal.INTERNAL }));

        private MetadataContext(MetadataType type) {
            this.mdType = type;
        }

        @Override
        public String describe() {
            return "Metadata request for type: " + mdType;
        }

        @Override
        public List<KVStoreRolePrincipal> getRequiredRoles() {
            /* Security Metadata requires INTERNAL authentication */
            if (mdType.equals(MetadataType.SECURITY)) {
                return internalRoleList;
            }
            return authenticatedRoleList;
        }
    }

    /**
     * The status listener for updating the status receiver.
     */
    private class StatusListener implements ServiceStatusTracker.Listener {
        @Override
        public void update(ServiceStatusChange prevStatus,
                           ServiceStatusChange newStatus) {

            try {
                RepNodeAdminImpl.this.statusReceiver.updateRnStatus(newStatus);
            } catch (RemoteException re) {
                /* If we fail to deliver, who can we tell about it? */
                logger.log(Level.WARNING,
                           "Failure to deliver status update to SNA", re);
                return;
            }
        }
    }

    /**
     * The perf stats listener for updating the status receiver.
     */
    private class OpStatsListener implements OperationsStatsTracker.Listener {
        @Override
        public void receiveStats(StatsPacket packet) {
            try {
                RepNodeAdminImpl.this.statusReceiver.receiveStats(packet);
            } catch (RemoteException re) {
                /* If we fail to deliver, who can we tell about it? */
                logger.log(Level.WARNING,
                           "Failure to deliver perf stats to MgmtAgent", re);
                return;
            }
        }
    }

    /**
     * The parameter listener for updating the status receiver.
     */
    private class ParameterChangeListener implements ParameterListener {

        @Override
        public void newParameters(ParameterMap oldMap, ParameterMap newMap) {
            try {
                RepNodeAdminImpl.this.statusReceiver.receiveNewParams(newMap);
            } catch (RemoteException re) {
                /* If we fail to deliver, who can we tell about it? */
                logger.log(Level.WARNING,
                           "Failure to deliver parameter change to MgmtAgent",
                           re);
                return;
            }
        }
    }
}
