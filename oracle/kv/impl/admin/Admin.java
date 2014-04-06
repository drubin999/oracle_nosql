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

package oracle.kv.impl.admin;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.KVSecurityConstants;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.KVVersion;
import oracle.kv.LoginCredentials;
import oracle.kv.impl.admin.criticalevent.CriticalEvent;
import oracle.kv.impl.admin.criticalevent.EventRecorder;
import oracle.kv.impl.admin.criticalevent.EventRecorder.LatestEventTimestamps;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.DatacenterParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.param.StorageNodePool;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.BasicPlannerImpl;
import oracle.kv.impl.admin.plan.DeploymentInfo;
import oracle.kv.impl.admin.plan.ExecutionState.ExceptionTransfer;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.StatusReport;
import oracle.kv.impl.admin.topo.RealizedTopology;
import oracle.kv.impl.admin.topo.Rules;
import oracle.kv.impl.admin.topo.Rules.Results;
import oracle.kv.impl.admin.topo.TopologyBuilder;
import oracle.kv.impl.admin.topo.TopologyCandidate;
import oracle.kv.impl.admin.topo.TopologyDiff;
import oracle.kv.impl.admin.topo.TopologyStore;
import oracle.kv.impl.api.table.FieldMap;
import oracle.kv.impl.api.table.TableMetadataProxy;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.metadata.MetadataStore;
import oracle.kv.impl.monitor.Monitor;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterListener;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterTracker;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.security.ClientProxyCredentials;
import oracle.kv.impl.security.KVStoreUserPrincipal;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.login.LoginUpdater;
import oracle.kv.impl.security.metadata.SecurityMetadataProxy;
import oracle.kv.impl.sna.StorageNodeStatus;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.StorageNodeMap;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.TopologyPrinter;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.impl.util.UserDataControl;
import oracle.kv.impl.util.VersionUtil;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.JENotifyHooks.LogRewriteListener;
import oracle.kv.impl.util.server.JENotifyHooks.RecoveryListener;
import oracle.kv.impl.util.server.JENotifyHooks.RedirectHandler;
import oracle.kv.impl.util.server.JENotifyHooks.SyncupListener;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.rep.InsufficientAcksException;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.InsufficientReplicasException;
import com.sleepycat.je.rep.MemberNotFoundException;
import com.sleepycat.je.rep.NetworkRestore;
import com.sleepycat.je.rep.NetworkRestoreConfig;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.ReplicaConsistencyException;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.ReplicationNetworkConfig;
import com.sleepycat.je.rep.RollbackException;
import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.StateChangeListener;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;
import com.sleepycat.je.rep.utilint.HostPortPair;
import com.sleepycat.je.utilint.StoppableThread;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.IndexNotAvailableException;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;
import com.sleepycat.persist.evolve.Deleter;
import com.sleepycat.persist.evolve.Mutations;
import com.sleepycat.persist.model.AnnotationModel;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.EntityModel;
import com.sleepycat.persist.model.PrimaryKey;

/**
 * An instance of the Admin class provides the general API underlying the
 * administrative interface.  It also owns the global Topology, Parameters and
 * Monitor instances, and provides interfaces for fetching and storing them.
 */
public class Admin implements PlannerAdmin {

    public static final String ADMIN_STORE_NAME = "AdminEntityStore";
    public static final String CAUSE_CREATE = "after create";
    public static final String CAUSE_APPROVE = "after approval";
    private static final String CAUSE_CANCEL = "after cancel";
    private static final String CAUSE_INTERRUPT = "after interrupt";
    public static final String CAUSE_INTERRUPT_REQUEST =
        "after interrupt requested";
    public static final String CAUSE_EXEC = "after execution";
    private static final long ADMIN_JE_CACHE_SIZE = 0;
    private final AdminId adminId;
    private AdminId masterId;

    /* Persistent storage of the current topology, candidates, and history. */
    private TopologyStore topoStore;

    /*
     * The parameters used to configure the Admin instance.
     */
    private final AdminServiceParams myParams;

    private BasicPlannerImpl planner;

    /*
     * Track listeners for parameter changes.
     */
    private final ParameterTracker parameterTracker;

    /*
     * Track listeners for global parameter changes.
     */
    private final ParameterTracker globalParameterTracker;

    /*
     * Monitoring services in the store.
     */
    private Monitor monitor;

    /*
     * Subsystem for recording significant monitor events.
     */
    private EventRecorder eventRecorder;

    private final Logger logger;

    private Parameters parameters;

    private Memo memo;

    private final EnvironmentConfig envConfig;
    private final ReplicationConfig repConfig;
    private final File envDir;
    private final File snapshotDir;
    private final StateChangeListener listener;
    private ReplicatedEnvironment environment;

    private EntityStore eStore;

    private final AdminService owner; /* May be null. */

    /*
     * startupStatus is used to coordinate admin initialization between the
     * current thread and the thread that has been spawned to asynchronously do
     * the longer-running init tasks.
     */
    private StartupStatus startupStatus;
    private boolean closing = false;
    private int eventStoreCounter = 0;
    private int eventStoreAgingFrequency = 100;

    /* Plans to restart when the Admin comes up. */
    private final Set<Integer> restartSet;

    /*
     * This is the minimum version that the store is operating at. This
     * starts at the current release prerequisite since this admin should
     * not be able to be installed if the prereq was not met.
     */
    private volatile KVVersion storeVersion = KVVersion.PREREQUISITE_VERSION;

    /**
     * Public constructors.
     */
    public Admin(AdminServiceParams params) {
        this(params, null);
    }

    Admin(AdminServiceParams params, AdminService owner) {

        this.owner = owner;

        parameters = null;
        myParams = params;
        parameterTracker = new ParameterTracker();
        globalParameterTracker = new ParameterTracker();

        final AdminParams adminParams = myParams.getAdminParams();
        final StorageNodeParams snParams = myParams.getStorageNodeParams();

        this.adminId = adminParams.getAdminId();

        envDir =
            FileNames.getEnvDir(snParams.getRootDirPath(),
                                myParams.getGlobalParams().getKVStoreName(),
                                null, snParams.getStorageNodeId(), adminId);
        snapshotDir = FileNames.getSnapshotDir
                      (snParams.getRootDirPath(),
                       myParams.getGlobalParams().getKVStoreName(),
                       null, snParams.getStorageNodeId(), adminId);

        final boolean created = FileNames.makeDir(envDir);

        /*
         * Add a log handler to send logging messages on the Admin process to
         * the Monitor. This must happen before the first monitor is
         * instantiated.  This registration lasts the lifetime of the Admin.
         */
        LoggerUtils.registerMonitorAdminHandler
            (myParams.getGlobalParams().getKVStoreName(), adminId, this);

        /*
         * Monitor should exist before the planner, and preferably before
         * any loggers, so that Admin logging can be sent to the store-wide
         * view.
         */
        monitor = new Monitor(myParams, this, getLoginManager());

        /*
         * By creating the event recorder early in the bootstrapping process, we
         * put a stake in the ground with the Trackers to avoid the pruning of
         * early events.  However, we can't start up the recorder until after
         * the persistent environment is viable.
         */
        eventRecorder = new EventRecorder(this);

        addParameterListener(monitor);
        addParameterListener(UserDataControl.getParamListener());

        logger = LoggerUtils.getLogger(this.getClass(), myParams);

        /*
         * Any socket timeouts observed by the ClientSocketFactory in this
         * process will be logged to this logger.
         */
        ClientSocketFactory.setTimeoutLogger(logger);

        logger.info("Initializing Admin for store: " +
                    myParams.getGlobalParams().getKVStoreName());
        if (created) {
            logger.info("Created new admin environment dir: " + envDir);
        }

        logger.info("JVM Runtime maxMemory (bytes): " +
                    Runtime.getRuntime().maxMemory());
        logger.info("Non-default JE properties for environment: " +
                    new ParameterUtils(adminParams.getMap()).
                    createProperties(false));

        envConfig = createEnvironmentConfig();
        repConfig = createReplicationConfig();

        listener = new Listener();

        restartSet = Collections.synchronizedSet(new HashSet<Integer>());

        renewRepEnv(null, null);
    }

    public Logger getLogger() {
        return logger;
    }

    public EntityStore getEStore() {
        return eStore;
    }

    /**
     * Process any plans that were not in a terminal state when the Admin
     * failover happened.
     *
     * 1.RUNNING plans ->INTERRUPT_REQUESTED -> INTERRUPTED, and
     *    will be restarted.
     * 2.INTERRUPT_REQUESTED plans -> INTERRUPTED and are not restarted. The
     *    failover is as if the cleanup phase was interrupted by the user.
     * 3.INTERRUPTED plans are left as is.
     *
     * interrupted, and retry them.
     */
    private void readAndRecoverPlans() {

        final Map<Integer, Plan> activePlans =
            new RunTransaction<Map<Integer, Plan>>
                (environment, RunTransaction.readOnly, logger) {

                @Override
                Map<Integer, Plan> doTransaction(Transaction txn) {
                    Map<Integer, Plan> plans = null;
                    /*
                     * Synchronized on planner, as are all manipulations of the
                     * Plan catalog and Plan database.
                     */
                    synchronized (planner) {
                        plans = AbstractPlan.fetchActivePlans(eStore,
                                                              txn,
                                                              planner,
                                                              myParams);
                    }

                    topoStore.initCachedStartTime(txn);
                    logger.log(Level.FINE, "Fetched {0} plans.", plans.size());

                    return plans;
                }

            }.run();

        /* Recover any plans that are not in a terminal state. */
        synchronized (restartSet) {
            restartSet.clear();
            for (Plan p : activePlans.values()) {
                final Plan restart = planner.recover(p);
                if (restart != null) {
                    restartSet.add(restart.getId());
                }
            }
        }
    }

    /**
     * Restart plans that were RUNNING before the Admin failed over.
     */
    private void restartPlans() {
        synchronized (restartSet) {
            for (Integer planId : restartSet) {
                logger.info("Restarting plan " + planId);
                executePlan(planId, false);
            }
        }
    }

    /**
     * Create an EnvironmentConfig specific for the Admin.
     */
    private EnvironmentConfig createEnvironmentConfig() {
        final EnvironmentConfig config = new EnvironmentConfig();
        config.setAllowCreate(true);
        config.setTransactional(true);
        config.setCacheSize(ADMIN_JE_CACHE_SIZE);
        return config;
    }

    /**
     * Create a ReplicationConfig specific for the Admin.
     */
    private ReplicationConfig createReplicationConfig() {
        final AdminParams ap = myParams.getAdminParams();

        final ReplicationConfig config =
            new ParameterUtils(ap.getMap()).getRepEnvConfig();

        config.setGroupName
           (getAdminRepGroupName(myParams.getGlobalParams().getKVStoreName()));

        config.setNodeName(getAdminRepNodeName(ap.getAdminId()));
        config.setNodeType(NodeType.ELECTABLE);
        config.setLogFileRewriteListener
            (new AdminLogRewriteListener(snapshotDir, myParams));

        if (myParams.getSecurityParams() != null) {
            final Properties haProps =
                myParams.getSecurityParams().getJEHAProperties();
            logger.info("DataChannelFactory: " +
                        haProps.getProperty(
                            ReplicationNetworkConfig.CHANNEL_TYPE));
            config.setRepNetConfig(ReplicationNetworkConfig.create(haProps));
        }

        return config;
    }

    ReplicationNetworkConfig getRepNetConfig() {
        return repConfig.getRepNetConfig();
    }

    public static String getAdminRepGroupName(String kvstoreName) {
        return kvstoreName + "Admin";
    }

    public static String getAdminRepNodeName(AdminId adminId) {
        return (Integer.toString(adminId.getAdminInstanceId()));
    }

    private synchronized void
    renewRepEnv(ReplicatedEnvironment prevRepEnv,
                RollbackException rollbackException) {

        if (!(((prevRepEnv == null) && (rollbackException == null)) ||
              ((prevRepEnv != null) && (rollbackException != null)))) {
            throw new IllegalStateException
                ("This environment should only be created anew or after a " +
                 "rollback exception. preRepEnv is null=" +
                 (prevRepEnv == null) + " rollbackException=" +
                 rollbackException);
        }

        if ((environment != null) && (environment != prevRepEnv)) {
            /* It's already been renewed by some other thread. */
            return;
        }

        /* Clean up previous environment. */
        if (prevRepEnv != null) {
            logger.log(Level.INFO,
                       "Closing environment handle in response " +
                       "to exception", rollbackException);
            if (eStore != null) {
                eStore.close();
                eStore = null;
            }

            try {
                prevRepEnv.close();
            } catch (DatabaseException e) {
                /* Ignore the exception, but log it. */
                logger.log(Level.INFO, "Exception closing environment", e);
            }
        }

        environment = openEnv();
        topoStore = new TopologyStore(this);

        /*
         * Some initialization of the Admin takes place in a separate thread
         * triggered by the StateChangeListener callback.  We'll wait here to
         * be notified that the initialization is complete.
         */
        startupStatus = new StartupStatus();
        environment.setStateChangeListener(listener);
        startupStatus.waitForIsReady(this);

        logger.info("Replicated environment handle " +
                    ((prevRepEnv == null) ? "" : "re-") + "established." +
                    " Cache size: " + environment.getConfig().getCacheSize() +
                    ", State: " + environment.getState());
    }

    /**
     * Returns a replicated environment handle, dealing with any recoverable
     * exceptions in the process.
     *
     * @return the replicated environment handle. The handle may be in the
     * Master, Replica, or Unknown state.
     */
    private ReplicatedEnvironment openEnv() {

        final boolean networkRestoreDone = false;

        /*
         * Plumb JE environment logging and progress listening output to
         * KVStore monitoring.
         */
        envConfig.setLoggingHandler(new AdminRedirectHandler(myParams));
        envConfig.setRecoveryProgressListener
            (new AdminRecoveryListener(myParams));
        repConfig.setSyncupProgressListener(new AdminSyncupListener(myParams));

        while (true) {
            try {
                return new ReplicatedEnvironment(envDir, repConfig, envConfig);
            } catch (InsufficientLogException ile) {
                if (networkRestoreDone) {

                    /*
                     * Should have made progress after the earlier network
                     * restore, propagate the exception to the caller so it
                     * can be logged and propagated back to the client.
                     */
                    throw ile;
                }
                final NetworkRestore networkRestore = new NetworkRestore();
                final NetworkRestoreConfig config = new NetworkRestoreConfig();
                config.setLogProviders(null);
                networkRestore.execute(ile, config);
                continue;
            } catch (RollbackException rbe) {
                final Long time = rbe.getEarliestTransactionCommitTime();
                logger.info("Rollback exception retrying: " +
                            rbe.getMessage() +
                            ((time ==  null) ?
                             "" :
                             " Rolling back to: " + new Date(time)));
                continue;
            } catch (Throwable t) {
                final String msg = "unexpected exception creating environment";
                logger.log(Level.WARNING, msg, t);
                throw new IllegalStateException(msg, t);
            }
        }
    }

    /**
     * A custom Handler for JE log handling.  This class provides a unique
     * class scope for the purpose of logger creation.
     */
    private static class AdminRedirectHandler extends RedirectHandler {
        AdminRedirectHandler(AdminServiceParams adminServiceParams) {
            super(LoggerUtils.getLogger(AdminRedirectHandler.class,
                                        adminServiceParams));
        }
    }

    /**
     * A custom Handler for JE recovery recovery notification.  This class
     * exists only to provide a unique class scope for the purpose of logger
     * creation.
     */
    private static class AdminRecoveryListener extends RecoveryListener {
        AdminRecoveryListener(AdminServiceParams adminServiceParams) {
            super(LoggerUtils.getLogger(AdminRecoveryListener.class,
                                        adminServiceParams));
        }
    }

    /**
     * A custom Handler for JE syncup progress notification.  This class
     * provides a unique class scope for the purpose of logger creation.
     */
    private static class AdminSyncupListener extends SyncupListener {
        AdminSyncupListener(AdminServiceParams adminServiceParams) {
            super(LoggerUtils.getLogger(AdminSyncupListener.class,
                                        adminServiceParams));
        }
    }

    /**
     * A custom Handler for JE log rewrite notification.  This class provides
     * a unique class scope for the purpose of logger creation.
     */
    private static class AdminLogRewriteListener extends LogRewriteListener {
        AdminLogRewriteListener(File snapshotDir,
                                AdminServiceParams adminServiceParams) {
            super(snapshotDir,
                  LoggerUtils.getLogger(AdminLogRewriteListener.class,
                                        adminServiceParams));
        }
    }

    /**
     * Initialize the Admin database.  If it already exists, read its contents
     * into the in-memory caches.  If not, create the structures that it needs.
     *
     * TODO: we need to check for the situation where an environment exists,
     * and the parameters passed in to the admin do not match those that are
     * already stored, which means that the config.xml is not consistent with
     * the database. This could happen if there is a failure during:
     *  - a change to admin params (which is not yet implemented)
     *  - there is a failure when the admin is deployed.
     * The database should be considered the point of authority, which means
     * that changing params needs to be carefully orchestrated with changes
     * to the database.

     * Open may be called multiple times on the same environment.  It should be
     * idempotent.  It is called from the StateChangeListener when the
     * environment changes state.
     */
    private void open() {

        new RunTransaction<Void>(environment,
                                 RunTransaction.sync, logger) {

            @Override
            Void doTransaction(Transaction txn) {

                final Topology topology = topoStore.readTopology(txn);
                parameters = readParameters(txn);
                memo = readMemo(txn);

                if (topology == null || parameters == null || memo == null) {
                    if (!(topology == null &&
                          parameters == null &&
                          memo == null)) {
                        throw new IllegalStateException
                            ("Inconsistency in Admin database: " +
                             "One of Topology, Parameters, or Memo is missing");
                    }

                    /* We are populating the database for the first time. */
                    logger.info("Initializing Admin database");

                    final String storeName =
                        myParams.getGlobalParams().getKVStoreName();
                    topoStore.save(txn, new RealizedTopology(storeName));

                    logger.fine("Creating Parameters");

                    /* Seed new parameters with info from myParams. */
                    parameters = new Parameters(storeName);
                    parameters.persist(eStore, txn);

                    logger.fine("Creating Memo");
                    memo = new Memo(1, new LatestEventTimestamps(0L, 0L, 0L));
                    memo.persist(eStore, txn);

                    logger.info("Admin database initialized");

                } else {
                    logger.info("Using existing Admin database");

                    // TODO: add verification that the params passed into the
                    // constructor match what is in the database.
                }
                return null;
            }
        }.run();

        /*
         * Now that the persistent environment is established, we can start
         * recording events.  Until now, any recordable events that occurred
         * would be buffered in the Trackers.  Now we can pull them out, record
         * them, and allow the Trackers to prune them.
         */
        eventRecorder.start(memo.getLatestEventTimestamps());

        planner = new BasicPlannerImpl(this, myParams, getNextId());
    }

    /*
     * The StopAdmin task needs a way to shut down the whole service, hence
     * this entry point.
     */
    @Override
    public void stopAdminService(boolean force) {
        if (owner != null) {
            owner.stop(force);
        } else {
            /* If there is no service, just treat this as shutdown. */
            this.shutdown(force);
        }
    }

    /**
     * The force boolean means don't bother to cleanly shutdown JE.
     */
    public synchronized void shutdown(boolean force) {
        closing = true;
        eventRecorder.shutdown();
        if (planner != null) {
            planner.shutdown();
        }
        monitor.shutdown();

        if (force) {
            try {
                eStore.close();
            } catch (Exception possible) /* CHECKSTYLE:OFF */ {
                /*
                 * Ignore exceptions that come from a forced close of the
                 * environment.
                 */
            }/* CHECKSTYLE:ON */
            /* Use an internal interface to close without checkpointing */
            final EnvironmentImpl envImpl =
                DbInternal.getEnvironmentImpl(environment);
            if (envImpl != null) {
                envImpl.close(false);
            }
        } else {
            eStore.close();
            environment.close();
        }
        /* Release all files. */
        LoggerUtils.closeHandlers
            (myParams.getGlobalParams().getKVStoreName());
    }

    /**
     * Notification that there are modified service parameters.
     */
    public synchronized void newParameters() {
        final ParameterMap oldMap = myParams.getAdminParams().getMap();
        final ParameterMap newMap =
            ConfigUtils.getAdminMap(adminId,
                                    myParams.getStorageNodeParams(),
                                    myParams.getGlobalParams(),
                                    logger);

        /* Do nothing if maps are the same */
        if (oldMap.equals(newMap)) {
            return;
        }
        parameterTracker.notifyListeners(oldMap, newMap);
        myParams.setAdminParams(new AdminParams(newMap));
    }

    /**
     * Notification that there are modified global parameters
     */
    synchronized public void newGlobalParameters() {
        ParameterMap oldMap = myParams.getGlobalParams().getMap();
        ParameterMap newMap =
            ConfigUtils.getGlobalMap(myParams.getStorageNodeParams(),
                                     myParams.getGlobalParams(),
                                     logger);

        /* Do nothing if maps are the same */
        if (oldMap.equals(newMap)) {
            logger.info(
                "newGlobalParameters are identical to old global parameters");
            return;
        }
        logger.info("newGlobalParameters: refreshing global parameters");
        myParams.setGlobalParams(new GlobalParams(newMap));
        globalParameterTracker.notifyListeners(oldMap, newMap);
    }

    public void addParameterListener(ParameterListener pl) {
        parameterTracker.addListener(pl);
    }

    public void removeParameterListener(ParameterListener pl) {
        parameterTracker.removeListener(pl);
    }

    private void addGlobalParameterListener(ParameterListener pl) {
        globalParameterTracker.addListener(pl);
    }

    public ReplicatedEnvironment getEnv() {
        return environment;
    }

    /**
     * The concurrency protocol for Topology is that we always give out copies.
     * Updates to the One True Topology happen in the course of Plan execution
     * via savePlanResults, in which a new Topology replaces the old one.  The
     * Planner bears the responsibility for serializing such updates.
     */
    @Override
    public Topology getCurrentTopology() {
        return new RunTransaction<Topology>
            (environment, RunTransaction.readOnly, logger) {
            @Override
            Topology doTransaction(Transaction txn) {
                return topoStore.readTopology(txn);
            }
        }.run();
    }

    /**
     * The concurrency protocol for Parameters is that we give out read-only
     * copies.  Updates happen only through methods on the Admin instance.
     *
     * Updates to parameters objects that occur in the course of plan execution
     * happen in the various flavors of the savePlanResults method.  These
     * updates are serialized with respect to each other by the Planner.
     *
     * Parameters updates that are not controlled by the Planner include
     * changes to StorageNodePools, which occur directly through the public
     * Admin interface, in response to operator actions.  The Planner requires
     * that the contents of a StorageNodePool remain unchanged during the
     * course of plan execution that involves the pool.  This constraint is
     * enforced by a read/write lock on each StorageNodePool object.
     */
    @Override
    public Parameters getCurrentParameters() {
        return new RunTransaction<Parameters>
            (environment, RunTransaction.readOnly, logger) {
            @Override
            Parameters doTransaction(Transaction txn) {
                return readParameters(txn);
            }
        }.run();
    }

    @Override
    public AdminServiceParams getParams() {
        return myParams;
    }

    @Override
    public void saveNextId(int nextId) {
        memo.setPlanId(nextId);
        logger.log(Level.FINE, "Storing Memo, planId = {0}", nextId);

        new RunTransaction<Void>(environment,
                                 RunTransaction.sync, logger) {

            @Override
            Void doTransaction(Transaction txn) {
                memo.persist(eStore, txn);
                return null;
            }
        }.run();
    }

    public int getNextId() {
        if (memo == null) {
            return 0;
        }
        return memo.getPlanId();
    }

    /**
     * Plans can be saved concurrently by concurrently executing tasks.
     */
    @Override
    public void savePlan(final Plan plan, String cause) {

        logger.log(Level.FINE, "Saving {0} {1}", new Object[]{plan, cause});

        /*
         * Synchronized on planner, as are all manipulations of the
         * Plan catalog and Plan database.
         */
        synchronized (planner) {
            /*
             * Assert that this plan was either terminal, or in the Planner's
             * cache, before storing it.
             */
            if (planner.getCachedPlan(plan.getId()) == null &&
                !plan.getState().isTerminal()) {
                throw new IllegalStateException
                    ("Attempting to save plan that is not in the Planner's " +
                     "cache " + plan);
            }

            new RunTransaction<Void>(environment,
                                     RunTransaction.sync, logger) {

                @Override
                Void doTransaction(Transaction txn) {
                    synchronized(plan) {
                       plan.persist(eStore, txn);
                    }
                    return null;
                }
            }.run();
        }
    }

    /**
     * The topology version should ascend with every save. If it does not,
     * something unexpected is happening.
     */
    private void checkTopoVersion(Topology newTopo) {
        final Topology current = getCurrentTopology();
        if (newTopo.getSequenceNumber() <= current.getSequenceNumber()) {
            throw new IllegalStateException
                ("Only save newer topologies. Current version=" +
                 current.getSequenceNumber() + " new version=" +
                 newTopo.getSequenceNumber());
        }
    }

    /**
     * All updates to topology should be transactional and atomic,
     * so the method is synchronized, and the update to the database is
     * in a single transaction.
     */
    @Override
    public synchronized void saveTopo(Topology topology,
                                      DeploymentInfo info,
                                      Plan plan) {
        checkTopoVersion(topology);
        storeTopoAndParams(topology, info, plan);
    }

    /**
     * All updates to topology and params should be transactional and atomic,
     * so the method is synchronized, and the update to the database is
     * in a single transaction.
     */
    @Override
    public synchronized void saveTopoAndRNParam(Topology topology,
                                                DeploymentInfo info,
                                                RepNodeParams rnp,
                                                Plan plan) {
        checkTopoVersion(topology);
        parameters.add(rnp);
        storeTopoAndParams(topology, info, plan);
    }

    /**
     * All updates to topology and params should be transactional and atomic,
     * so the method is synchronized, and the update to the database is
     * in a single transaction.
     */
    @Override
    public synchronized void saveTopoAndParams(Topology topology,
                                               DeploymentInfo info,
                                               DatacenterParams params,
                                               Plan plan) {
        checkTopoVersion(topology);
        parameters.add(params);
        storeTopoAndParams(topology, info, plan);
    }

    /**
     * All updates to topology and params should be transactional and atomic,
     * so the method is synchronized, and the update to the database is
     * in a single transaction.
     */
    @Override
    public synchronized void saveTopoAndParams(Topology topology,
                                               DeploymentInfo info,
                                               StorageNodeParams snp,
                                               GlobalParams gp,
                                               Plan plan) {

        checkTopoVersion(topology);
        parameters.add(snp);
        if (gp != null) {
            parameters.update(gp);
        }

        /* Add the new SN to the pool of all storage nodes */
        final StorageNodePool pool =
            parameters.getStorageNodePool(Parameters.DEFAULT_POOL_NAME);
        pool.add(snp.getStorageNodeId());

        storeTopoAndParams(topology, info, plan);
    }

    /**
     * All updates to topology and params should be transactional and atomic,
     * so the method is synchronized, and the update to the database is
     * in a single transaction.
     */
    @Override
    public synchronized void saveTopoAndRemoveSN(Topology topology,
                                                 DeploymentInfo info,
                                                 StorageNodeId target,
                                                 Plan plan) {

        checkTopoVersion(topology);

        /* Remove the snId from the params and all pools. */
        parameters.remove(target);
        storeTopoAndParams(topology, info, plan);
    }

    /**
     * All updates to topology and params should be transactional and atomic,
     * so the method is synchronized, and the update to the database is
     * in a single transaction.
     */
    @Override
    public synchronized void saveTopoAndRemoveRN(Topology topology,
                                                 DeploymentInfo info,
                                                 RepNodeId targetRN,
                                                 Plan plan) {

        checkTopoVersion(topology);

        /* Remove the rnId from the params and all pools. */
        parameters.remove(targetRN);
        storeTopoAndParams(topology, info, plan);
    }

    /**
     * All updates to topology should be transactional and atomic, so the
     * method is synchronized, and the update to the database is in a single
     * transaction.
     */
    @Override
    public synchronized void saveTopoAndRemoveDatacenter(Topology topology,
                                                         DeploymentInfo info,
                                                         DatacenterId targetId,
                                                         Plan plan) {
        checkTopoVersion(topology);

        /* Remove the targetId from the params. */
        parameters.remove(targetId);
        storeTopoAndParams(topology, info, plan);
    }

    /**
     * All updates to topology and params should be transactional and atomic,
     * so the method is synchronized, and the update to the database is
     * in a single transaction.
     */
    @Override
    public synchronized void saveTopoAndParams(Topology topology,
                                               DeploymentInfo info,
                                               Set<RepNodeParams> repNodeParams,
                                               Set<AdminParams> adminParams,
                                               Plan plan) {

        checkTopoVersion(topology);

        /*
         * Use parameters.update rather than add, because we may be
         * re-executing this upon plan retry.
         */
        for (RepNodeParams rnParams : repNodeParams) {
            parameters.update(rnParams);
        }

        for (AdminParams ap : adminParams) {
            parameters.update(ap);
        }

        storeTopoAndParams(topology, info, plan);
    }

    /**
     * All updates to topology and params should be transactional and atomic,
     * so the method is synchronized, and the update to the database is
     * in a single transaction.
     */
    @Override
    public synchronized void saveParams(Set<RepNodeParams> repNodeParams,
                                        Set<AdminParams> adminParams) {

        /*
         * Use parameters.update rather than add, because we may be
         * re-executing this upon plan retry.
         */
        for (RepNodeParams rnParams : repNodeParams) {
            parameters.update(rnParams);
        }

        for (AdminParams ap : adminParams) {
            parameters.update(ap);
        }

        storeParameters();
    }

    /**
     * Update the shard assignment for a given partition. Since partition
     * migration can be executed in parallel, the read and write of the
     * topology must be atomic.
     */
    @Override
    public synchronized boolean updatePartition(final PartitionId partitionId,
                                                final RepGroupId targetRGId,
                                                final DeploymentInfo info,
                                                final Plan plan) {
        return new RunTransaction<Boolean>
            (environment, RunTransaction.sync, logger) {
            @Override
            Boolean doTransaction(Transaction txn) {
                /*
                 * Be sure to take the plan mutex before any JE locks are
                 * acquired in this transaction, per the synchronization
                 * hierarchy rules.
                 */
                Topology t = null;
                synchronized (plan) {
                    t = topoStore.readTopology(txn);

                    /*
                     * If the partition is already at the target, there is no
                     * need to update the topology.
                     */
                    if (t.get(partitionId).getRepGroupId().equals(targetRGId)){
                        return false;
                    }
                    t.updatePartition(partitionId, targetRGId);

                    /*
                     * Now that the topo has been modified, inform the plan that
                     * the topology is being updated. Save the plan if needed.
                     */
                    if (plan.updatingMetadata(t)) {
                        plan.persist(eStore, txn);
                    }
                }

                topoStore.save(txn, new RealizedTopology(t, info));
                return true;
            }
        }.run();
    }

    @Override
    public synchronized void updateParams(RepNodeParams rnp) {
        parameters.update(rnp);
        storeParameters();
    }

    @Override
    public synchronized void updateParams(StorageNodeParams snp,
                                          GlobalParams gp) {
        parameters.update(snp);
        if (gp != null) {
            parameters.update(gp);
        }
        storeParameters();
    }

    @Override
    public synchronized void updateParams(AdminParams ap) {
        parameters.update(ap);
        storeParameters();
    }

    @Override
    public synchronized void updateParams(GlobalParams gp) {
        parameters.update(gp);
        storeParameters();
    }

    @Override
    public synchronized void addAdminParams(AdminParams ap) {
        final int id = ap.getAdminId().getAdminInstanceId();

        logger.log(Level.FINE, "Saving new AdminParams[{0}]", id);

        final int nAdmins = getAdminCount();
        if (id <= nAdmins) {
            throw new NonfatalAssertionException
                ("Attempting to add an AdminParams " +
                 "for an existing Admin. Id=" + id + " nAdmins=" + nAdmins);
        }

        parameters.add(ap);
        storeParameters();
    }

    @Override
    public synchronized void removeAdminParams(AdminId aid) {

        logger.log(Level.FINE, "Removing AdminParams[{0}]", aid);

        if (parameters.get(aid) == null) {
            throw new MemberNotFoundException
                ("Removing nonexistent params for admin " + aid);
        }

        final int nAdmins = getAdminCount();
        if (nAdmins == 1) {
            throw new NonfatalAssertionException
                ("Attempting to remove the sole Admin instance" + aid);
        }

        parameters.remove(aid);
        storeParameters();
    }

    public synchronized void addStorageNodePool(String name) {

        if (parameters.getStorageNodePool(name) != null) {
            throw new IllegalCommandException
                ("Attempt to add a StorageNodePool name that already exists.");
        }

        parameters.addStorageNodePool(name);
        logger.info("Created Storage Node Pool: " + name);
        storeParameters();
    }

    public synchronized void removeStorageNodePool(String name) {
        if (parameters.getStorageNodePool(name) == null) {
            throw new IllegalCommandException
                ("Attempt to remove a nonexistent StorageNodePool.");
        }

        parameters.removeStorageNodePool(name);
        logger.info("Removed Storage Node Pool: " + name);
        storeParameters();
    }

    public synchronized void addStorageNodeToPool(String name,
                                                  StorageNodeId snId) {

        final StorageNodePool pool = parameters.getStorageNodePool(name);
        if (pool == null) {
            throw new IllegalCommandException("No such Storage Node Pool: " +
                                              name);
        }

        /* Verify that the storage node exists. */
        final Topology current = getCurrentTopology();
        final StorageNode sn = current.get(snId);
        if (sn == null) {
            throw new IllegalCommandException
                ("Attempt to add nonexistent StorageNode to a pool.");
        }

        logger.info("Added Storage Node " + snId.toString() + " to pool: " +
                    name);
        pool.add(snId);
        storeParameters();
    }

    public synchronized void replaceStorageNodePool(String name,
                                                    List<StorageNodeId> ids) {

        final StorageNodePool pool = parameters.getStorageNodePool(name);
        if (pool == null) {
            throw new IllegalCommandException("No such Storage Node Pool: " +
                                              name);
        }

        final Topology current = getCurrentTopology();
        for (StorageNodeId id : ids) {
            /* Verify that the storage node exists. */
            final StorageNode sn = current.get(id);
            if (sn == null) {
                throw new IllegalCommandException
                    ("Attempt to add nonexistent StorageNode to a pool.");
            }
        }

        pool.clear();

        for (StorageNodeId id : ids) {
            pool.add(id);
        }

        storeParameters();
    }

    /**
     * Adds a named topology candidate that has been created by a copy
     * operation. Relies on the transaction to avoid duplicate inserts.
     */
    void addTopoCandidate(final String candidateName,
                          final Topology newTopo) {

        new RunTransaction<Void>(environment, RunTransaction.writeNoSync,
                                 logger) {
            @Override
                Void doTransaction(Transaction txn) {
                if (topoStore.exists(txn, candidateName)) {
                    throw new IllegalCommandException
                        (candidateName +
                         " already exists and can't be added as a new" +
                         " topology candidate.");
                }
                topoStore.save(txn, new TopologyCandidate(candidateName,
                                                          newTopo));
                return null;
            }
        }.run();
    }

    /**
     * Adds a named topology candidate that has been created by a copy of
     * an existing candidate. Relies on the transaction to avoid duplicate
     * inserts.
     */
    void addTopoCandidate(final String candidateName,
                          final String sourceCandidateName) {
        final Topology source =
            getCandidate(sourceCandidateName).getTopology();
        addTopoCandidate(candidateName, source);
    }

    /**
     * Deletes a topology candidate.
     *
     * @throws IllegalCommandException if @param name is not associated with
     * a topology.
     * @throws DatabaseException if there is a problem with the deletion or
     * if a foreign key constraint is violated because the candidate is
     * referenced by a deployed topology. TODO: how to capture that case, and
     * present the error differently?
     */
    void deleteTopoCandidate(final String candidateName) {

        new RunTransaction<Void>(environment,
                                 RunTransaction.writeNoSync, logger) {
            @Override
            Void doTransaction(Transaction txn) {
                if (!topoStore.exists(txn, candidateName)) {
                      throw new IllegalCommandException
                          (candidateName + " doesn't exist");
                }

                topoStore.delete(txn, candidateName);
                return null;
            }
        }.run();
    }

    /**
     * Create a new, initial layout, based on the SNs available in the SN pool.
     * @return a String with a status message suitable for the CLI.
     */
    public String createTopoCandidate(final String topologyName,
                                      final String snPoolName,
                                      final int numPartitions) {

        return new RunTransaction<String>(environment,
                                          RunTransaction.writeNoSync, logger) {
            @Override
            String doTransaction(Transaction txn) {

                if (topoStore.exists(txn, topologyName)) {
                    throw new IllegalCommandException
                        ("Topology " + topologyName + " already exists");
                }

                final StorageNodePool pool =
                    parameters.getStorageNodePool(snPoolName);
                if (pool == null) {
                    throw new IllegalCommandException
                        ("Storage Node Pool " + snPoolName + " not found.");
                }

                final TopologyBuilder tb =
                    new TopologyBuilder(getCurrentTopology(),
                                        topologyName,
                                        pool,
                                        numPartitions,
                                        getCurrentParameters(),
                                        myParams);

                final TopologyCandidate candidate = tb.build();
                topoStore.save(txn, candidate);
                return "Created: " + candidate.getName();
            }
        }.run();
    }

    /**
     * Retrieve a topology candidate from the topo store.
     * @throws IllegalCommandException if the candidate does not exist.
     */
    @Override
    public TopologyCandidate getCandidate(final String candidateName) {
        return new RunTransaction<TopologyCandidate>(environment,
                                                     RunTransaction.readOnly,
                                                     logger) {
            @Override
                TopologyCandidate doTransaction(Transaction txn) {
                return getCandidate(txn, candidateName);
            }
        }.run();
    }

    /**
     * Check that the specified topology exists, throw IllegalCommand if
     * it doesn't.
     */
    private TopologyCandidate getCandidate(Transaction txn,
                                           String candidateName) {
        final TopologyCandidate candidate = topoStore.get(txn, candidateName);
        if (candidate == null) {
            throw new IllegalCommandException
                ("Topology " + candidateName + " does not exist. Use " +
                 " topology list to see all available candidate");
        }

        return candidate;
    }

    /**
     * Present a list of candidate names.
     */
    public List<String> listTopoCandidates() {
        return new RunTransaction<List<String>>(environment,
                                   RunTransaction.readOnly, logger) {
            @Override
            List<String> doTransaction(Transaction txn) {
                return topoStore.getCandidateNames(txn);
            }
        }.run();
    }

    /**
     * Execute the redistribution algorithms on a given topo candidate,
     * updating its layout to use as much of the available store SNs as
     * possible.
     * @param candidateName the name of the target candidate.
     * @param snPoolName represents the resources to be used by in the new
     * layout.
     * @return a message indicating success or failure, to be reported  by the
     * CLI.
     */
    public String redistributeTopology(final String candidateName,
                                       final String snPoolName) {

        return new RunTransaction<String>(environment,
                                          RunTransaction.writeNoSync, logger) {
            @Override
            String doTransaction(Transaction txn) {

                /* check that the target candidate exists.*/
                final TopologyCandidate candidate =
                    getCandidate(txn, candidateName);
                final StorageNodePool pool = freezeSNPool(snPoolName);

                try {
                    /* Create and save a new layout against the specified SNs */
                    final TopologyBuilder tb =
                        new TopologyBuilder(candidate, pool,
                                            getCurrentParameters(),
                                            myParams);
                    final TopologyCandidate newCandidate = tb.build();
                    topoStore.save(txn, newCandidate);
                    return "Redistributed: " + newCandidate.getName();
                } finally {
                    pool.thaw();
                }
            }
        }.run();
    }

    /**
     * Make the specified topology compliant with all the topology rules.
     */
    public String rebalanceTopology(final String candidateName,
                                    final String snPoolName,
                                    final DatacenterId dcId) {

        return new RunTransaction<String>(environment,
                                          RunTransaction.writeNoSync, logger) {
            @Override
            String doTransaction(Transaction txn) {

                final TopologyCandidate candidate =
                    getCandidate(txn, candidateName);
                final StorageNodePool pool = freezeSNPool(snPoolName);

                try {
                    /* Create and save a new layout against the specified SNs */
                    final TopologyBuilder tb =
                        new TopologyBuilder(candidate, pool,
                                            getCurrentParameters(), myParams);

                    final TopologyCandidate newCandidate = tb.rebalance(dcId);
                    topoStore.save(txn, newCandidate);
                    return "Rebalanced: " + newCandidate.getName();
                } finally {
                    pool.thaw();
                }
            }
        }.run();
    }

    /**
     * Check that the specified snPool exists, and freeze it so it is stable
     * while the topology transformation is going on.
     */
    private StorageNodePool freezeSNPool(String snPoolName) {

        final StorageNodePool pool = parameters.getStorageNodePool(snPoolName);
        if (pool == null) {
            throw new IllegalCommandException
                ("Storage Node Pool " + snPoolName + " not found.");
        }
        pool.freeze();
        return pool;
    }

    /**
     * Get a new copy of the Parameters from the database.
     */
    private Parameters readParameters(Transaction txn) {
        final Parameters params = Parameters.fetch(eStore, txn);
        if (params == null) {
            logger.fine("Parameters not found");
        } else {
            logger.fine("Parameters fetched");
        }

        return params;
    }

    /**
     * Get a new copy of the Memo from the database.
     */
    private Memo readMemo(Transaction txn) {
        final Memo m = Memo.fetch(eStore, txn);
        if (m == null) {
            logger.fine("Memo not found");
        } else {
            logger.fine("Memo fetched");
        }

        return m;
    }

    /**
     * Save a new bunch of event records, and the latest timestamp, in the same
     * transaction.
     */
    public void storeEvents(final List<CriticalEvent> events,
                            final EventRecorder.LatestEventTimestamps let,
                            CriticalEvent.EventType eventType) {

        /*
         * Use a lightweight commit for critical-event-storing transactions.
         * If we are storing perf events, use an even lighter-weight commit.
         */
        final TransactionConfig tc =
            (eventType == CriticalEvent.EventType.PERF ?
                                RunTransaction.noSync :
                                RunTransaction.writeNoSync);

        new RunTransaction<Void>(environment, tc, logger) {

            @Override
            Void doTransaction(Transaction txn) {

                for (CriticalEvent pe : events) {
                    pe.persist(eStore, txn);

                    /*
                     * Age the store periodically.
                     */
                    if (eventStoreCounter++ % eventStoreAgingFrequency == 0) {
                        CriticalEvent.ageStore
                            (eStore, txn, getEventExpiryAge());
                    }
                }

                /*
                 * The memo is being modified without synchronization, which is
                 * ok because the method is guaranteed to be called by a single
                 * eventRecorder thread.
                 */
                memo.setLatestEventTimestamps(let);
                memo.persist(eStore, txn);
                return null;
            }
        }.run();
    }

    /**
     * Return a list of event records satisfying the given criteria.
     * For endTime, zero means the current time.
     * For type, null means all types.
     */
    public List<CriticalEvent> getEvents(final long startTime,
                                         final long endTime,
                                         final CriticalEvent.EventType type) {

        return new RunTransaction<List<CriticalEvent>>
            (environment, RunTransaction.readOnly, logger) {

            @Override
            List<CriticalEvent> doTransaction(Transaction txn) {
                return CriticalEvent.fetch
                    (eStore, txn, startTime, endTime, type);
            }
        }.run();
    }

    /**
     * Return a single event that has the given database key.
     */
    public CriticalEvent getOneEvent(final String eventId) {

        return new RunTransaction<CriticalEvent>
            (environment, RunTransaction.readOnly, logger) {

            @Override
            CriticalEvent doTransaction(Transaction txn) {
                return CriticalEvent.fetch(eStore, txn, eventId);
            }
        }.run();
    }

    public int createDeployAdminPlan(String planName,
                                     StorageNodeId snid,
                                     int httpPort) {

        final Plan p = planner.createDeployAdminPlan(planName, snid,
                                                     httpPort);
        savePlan(p, CAUSE_CREATE);
        return p.getId();
    }

    public int createRemoveAdminPlan(String planName,
                                     DatacenterId dcid,
                                     AdminId victim) {
        final Plan p = planner.createRemoveAdminPlan(planName, dcid, victim);
        savePlan(p, CAUSE_CREATE);
        return p.getId();
    }

    public int createDeployDatacenterPlan(String planName,
                                          String datacenterName,
                                          int repFactor,
                                          DatacenterType datacenterType) {

        try {
            Rules.validateReplicationFactor(repFactor);
        } catch (IllegalArgumentException iae) {
            throw new IllegalCommandException("Bad replication factor", iae);
        }
        final Plan p = planner.createDeployDatacenterPlan(
            planName, datacenterName, repFactor, datacenterType);
        savePlan(p, CAUSE_CREATE);
        return p.getId();
    }

    public int createDeploySNPlan(String planName,
                                  DatacenterId datacenterId,
                                  StorageNodeParams inputSNP) {

        final Plan p = planner.createDeploySNPlan
            (planName, datacenterId, inputSNP);
        savePlan(p, CAUSE_CREATE);
        return p.getId();
    }

    public int createDeployTopoPlan(String planName,
                                    String candidateName) {
        final TopologyCandidate candidate = getCandidate(candidateName);

        final Plan p = planner.createDeployTopoPlan(planName,
                                                    candidate);
        savePlan(p, CAUSE_CREATE);
        return p.getId();
    }

    public int createStopAllRepNodesPlan(String planName) {

        final Plan p = planner.createStopAllRepNodesPlan(planName);
        savePlan(p, CAUSE_CREATE);
        return p.getId();
    }

    public int createStartAllRepNodesPlan(String planName) {

        final Plan p = planner.createStartAllRepNodesPlan(planName);
        savePlan(p, CAUSE_CREATE);
        return p.getId();
    }

    public int createStopRepNodesPlan(String planName, Set<RepNodeId> ids) {

        final Plan p = planner.createStopRepNodesPlan(planName, ids);
        savePlan(p, CAUSE_CREATE);
        return p.getId();
    }

    public int createStartRepNodesPlan(String planName, Set<RepNodeId> ids) {

        final Plan p = planner.createStartRepNodesPlan(planName, ids);
        savePlan(p, CAUSE_CREATE);
        return p.getId();
    }

    public int createChangeParamsPlan(String planName,
                                      ResourceId rid,
                                      ParameterMap newParams) {

        final Plan p =
            planner.createChangeParamsPlan(planName, rid, newParams);
        savePlan(p, CAUSE_CREATE);
        return p.getId();
    }

    public int createChangeAllParamsPlan(String planName,
                                         DatacenterId dcid,
                                         ParameterMap newParams) {

        final Plan p =
            planner.createChangeAllParamsPlan(planName, dcid, newParams);
        savePlan(p, CAUSE_CREATE);
        return p.getId();
    }

    public int createChangeAllAdminsPlan(String planName,
                                         DatacenterId dcid,
                                         ParameterMap newParams) {

        final Plan p =
            planner.createChangeAllAdminsPlan(planName, dcid, newParams);
        savePlan(p, CAUSE_CREATE);
        return p.getId();
    }

    public int createChangeGlobalSecurityParamsPlan(String planName,
                                                    ParameterMap newParams) {
        final Plan p =
            planner.createChangeGlobalSecurityParamsPlan(planName, newParams);
        savePlan(p, CAUSE_CREATE);
        return p.getId();
    }

    public int createMigrateSNPlan(String planName,
                                   StorageNodeId oldNode,
                                   StorageNodeId newNode,
                                   int newHttpPort) {

        final Plan p = planner.createMigrateSNPlan(planName, oldNode, newNode,
                                                   newHttpPort);
        savePlan(p, CAUSE_CREATE);
        return p.getId();
    }

    public int createRemoveSNPlan(String planName,
                                  StorageNodeId targetNode) {

        final Plan p = planner.createRemoveSNPlan(planName, targetNode);
        savePlan(p, CAUSE_CREATE);
        return p.getId();
    }

    public int createRemoveDatacenterPlan(String planName,
                                          DatacenterId targetId) {
        final Plan p = planner.createRemoveDatacenterPlan(planName, targetId);
        savePlan(p, CAUSE_CREATE);
        return p.getId();
    }

    public int createRepairPlan(String planName) {
        final Plan p = planner.createRepairPlan(planName);
        savePlan(p, CAUSE_CREATE);
        return p.getId();
    }


    int createAddTablePlan(String planName,
                           String tableId,
                           String parentName,
                           FieldMap fieldMap,
                           List<String> primaryKey,
                           List<String> majorKey,
                           boolean r2compat,
                           int schemaId,
                           String description) {
        final Plan p = planner.createAddTablePlan(planName,
                                                  tableId,
                                                  parentName,
                                                  fieldMap,
                                                  primaryKey,
                                                  majorKey,
                                                  r2compat,
                                                  schemaId,
                                                  description);
        savePlan(p, CAUSE_CREATE);
        return p.getId();
    }

    int createRemoveTablePlan(String planName,
                              String tableName,
                              boolean removeData) {
        final Plan p = planner.createRemoveTablePlan(planName,
                                                     tableName,
                                                     removeData);
        savePlan(p, CAUSE_CREATE);
        return p.getId();
    }

    int createAddIndexPlan(String planName,
                           String indexName,
                           String tableName,
                           String[] indexedFields,
                           String description) {
        final Plan p = planner.createAddIndexPlan(planName, indexName,
                                                  tableName,
                                                  indexedFields, description);
        savePlan(p, CAUSE_CREATE);
        return p.getId();
    }

    int createRemoveIndexPlan(String planName,
                              String indexName,
                              String tableName) {
        final Plan p = planner.createRemoveIndexPlan(planName, indexName,
                                                     tableName);
        savePlan(p, CAUSE_CREATE);
        return p.getId();

    }

    int createEvolveTablePlan(String planName,
                              String tableName,
                              int tableVersion,
                              FieldMap fieldMap) {

        final Plan p = planner.createEvolveTablePlan(planName, tableName,
                                                     tableVersion, fieldMap);
        savePlan(p, CAUSE_CREATE);
        return p.getId();
    }

    public int createCreateUserPlan(String planName,
                                    String userName,
                                    boolean isEnabled,
                                    boolean isAdmin,
                                    char[] plainPassword) {
        final Plan p = planner.createCreateUserPlan(
            planName, userName, isEnabled, isAdmin, plainPassword);
        savePlan(p, CAUSE_CREATE);
        return p.getId();
    }

    public int createChangeUserPlan(String planName,
                                    String userName,
                                    Boolean isEnabled,
                                    char[] plainPassword,
                                    boolean retainPassword,
                                    boolean clearRetainedPassword) {
        final Plan p = planner.createChangeUserPlan(
            planName, userName, isEnabled, plainPassword, retainPassword,
            clearRetainedPassword);
        savePlan(p, CAUSE_CREATE);
        return p.getId();
    }

    public int createDropUserPlan(String planName, String userName) {
        final Plan p = planner.createDropUserPlan(planName, userName);
        savePlan(p, CAUSE_CREATE);
        return p.getId();
    }

    /**
     * Approve the given plan for execution.
     */
    public void approvePlan(int id) {

        final Plan p = getAndCheckPlan(id);
        planner.approvePlan(id);
        savePlan(p, CAUSE_APPROVE);
        logger.info(p + " approved");
    }

    /**
     * Cancel the given plan.
     */
    public void cancelPlan(int id) {

        final Plan p = getAndCheckPlan(id);
        planner.cancelPlan(id);
        savePlan(p, CAUSE_CANCEL);
        logger.info(p + " canceled");
    }

    /**
     * Interrupt the given plan. Must be synchronized because
     * planner.interruptPlan() will take the plan mutex, and then may
     * attempt to save the plan, which requires the Admin mutex. Since the
     * lock hierarchy is Admin->plan, we need to take the Admin mutex now.
     * [#22161]
     */
    public synchronized void interruptPlan(int id) {

        final Plan p = getAndCheckPlan(id);
        planner.interruptPlan(id);
        savePlan(p, CAUSE_INTERRUPT);
        logger.info(p + " interrupted");
    }

    /**
     * Start the execution of a plan. The plan will proceed asynchronously.
     *
     * To check the status and final result of the plan, the caller should
     * examine the state and execution history. AwaitPlan() can be used to
     * synchronously wait for the end of the plan, and to learn the plan status.
     * AssertSuccess() can be used to generate an exception if the plan
     * fails, which will throw a wrapped version of the original exception
     * @param force TODO
     */
    public void executePlan(int id, boolean force) {

        final Plan p = getAndCheckPlan(id);
        planner.executePlan(p, force);
    }

    /**
     * Wait for this plan to finish. If a timeout period is specified and
     * exceeded, the wait will also end. Return the current plan status.
     */
    @Override
    public Plan.State awaitPlan(int id, int timeout, TimeUnit timeoutUnit) {

        final Plan p = getAndCheckPlan(id);
        final PlanWaiter waiter = new PlanWaiter();
        p.addWaiter(waiter);
        logger.log(Level.FINE, "Waiting for plan {0}, timeout={1} {2}",
                   new Object[]{p, timeout, timeoutUnit});

        try {
            if (timeout == 0) {
                waiter.waitForPlanEnd();
            } else {
                final boolean timedOut =
                    waiter.waitForPlanEnd(timeout, timeoutUnit);
                if (timedOut) {
                    logger.log
                        (Level.INFO,
                         "Timed out waiting for plan {0} to finish, state={1}",
                         new Object[] {p, p.getState()});
                }
            }
        } catch (InterruptedException e) {
            logger.log
                (Level.INFO,
                 "Interrupted while waiting for {0} to finish, end state={1}",
                 new Object[] {p, p.getState()});
        } finally {
            p.removeWaiter(waiter);
        }
        logger.log(Level.INFO, "awaitPlan of {0} finished, state={1}",
                   new Object[] {p, p.getState()});
        return p.getState();
    }

    /**
     * Throw an operation fault exception if the plan did not succeed.
     */
    public void assertSuccess(int planId) {
        final Plan p = getAndCheckPlan(planId);
        final Plan.State status = p.getState();
        if (status == Plan.State.SUCCEEDED) {
            return;
        }

        final ExceptionTransfer transfer = p.getLatestRunExceptionTransfer();
        final String msg = p + " ended with " + status;
        if (transfer != null) {
            if (transfer.getFailure() != null) {
                final Throwable failure = transfer.getFailure();
                final OperationFaultException newEx =
                    new OperationFaultException(msg + ": " +
                                                transfer.getDescription(),
                                                failure);
                newEx.setStackTrace(failure.getStackTrace());
                throw newEx;
            } else if (transfer.getDescription() != null) {
                /* No exception saved, but there is some error information */
                throw new OperationFaultException(msg + ": " +
                                                  transfer.getDescription());
            }
        }
        throw new OperationFaultException(msg);
    }

    /**
     * The concurrency protocol for getPlanbyId is that we give out references
     * to the cached Plan of record, if the Plan is in a non-terminal, or
     * active, state.  If the plan is active, then it will be found in the
     * Planner's catalog, which is consulted first in this method.
     *
     * If the plan is not active, we read it straight from the database.  For
     * such plans, it's possible for this method to produce multiple instances
     * of the same plan.  We consider inactive plans to be read-only, so this
     * should not create any synchronization problems.  The read-only-ness of
     * inactive plans is enforced in Admin.savePlan, which is the only means by
     * which a plan is updated in the database.  The terminal-state-ness of
     * uncached plans is asserted below, also.
     */
    public Plan getPlanById(final int id) {

        /*
         * Synchronized on planner, as are all manipulations of the Plan
         * catalog and Plan database.
         */
        synchronized (planner) {
            if (planner != null) {
                final Plan p = planner.getCachedPlan(id);

                if (p != null) {
                    return p;
                }
            }

            return new RunTransaction<Plan>(environment,
                                            RunTransaction.readOnly,
                                            logger) {
                @Override
                Plan doTransaction(Transaction txn) {
                    final Plan uncachedPlan = AbstractPlan.fetchPlanById
                        (id, eStore, txn, planner, myParams);
                    if (uncachedPlan == null) {
                        return null;
                    }
                    if (!uncachedPlan.getState().isTerminal()) {
                        throw new IllegalStateException
                            ("Found non-terminal plan that is not cached. " +
                             uncachedPlan);
                    }
                    return uncachedPlan;
                }

            }.run();
        }
    }

    /**
     * Get the plan, throw an exception if it doesn't exist.
     */
    private Plan getAndCheckPlan(int id) {
        final Plan p = getPlanById(id);
        if (p == null) {
            throw new IllegalCommandException("Plan id " + id +
                                              " doesn't exist");
        }
        return p;
    }

    /**
     * Return copies of the most recent 100 plans. The result should be used
     * only for display purposes, or, in tests for determining the existence of
     * plans. The plan instances will only be valid for display, and will not
     * be executable, because they will be stripped of memory heavy objects.
     * @deprecated in favor of getPlanRange.
     */
    @Deprecated
    private static int nRecentPlans = 100;
    @Deprecated
    public Map<Integer, Plan> getRecentPlansCopy() {

        synchronized (planner) {
            return new RunTransaction<Map<Integer, Plan>>
                (environment, RunTransaction.readOnly, logger) {

                @Override
                Map<Integer, Plan> doTransaction(Transaction txn) {
                    return AbstractPlan.fetchRecentPlansForDisplay
                        (nRecentPlans, eStore, txn, planner, myParams);
                }
            }.run();
        }
    }

    /**
     * Retrieve the beginning and ending plan ids that satisfy the request.
     *
     * Returns an array of two integers indicating a range of plan id
     * numbers. [0] is the first id in the range, and [1] is the last,
     * inclusive.
     *
     * Operates in three modes:
     *
     *    mode A requests n plans ids following startTime
     *    mode B requests n plans ids preceding endTime
     *    mode C requests a range of plan ids from startTime to endTime.
     *
     *    mode A is signified by endTime == 0
     *    mode B is signified by startTime == 0
     *    mode C is signified by neither startTime nor endTime being == 0.
     *    N is ignored in mode C.
     */
    public int[] getPlanIdRange(final long startTime,
                                final long endTime,
                                final int n) {
        synchronized (planner) {
            return new RunTransaction<int[]>
                (environment, RunTransaction.readOnly, logger) {

                @Override
                int[] doTransaction(Transaction txn) {
                    return AbstractPlan.getPlanIdRange(eStore, txn,
                                                       startTime, endTime, n);
                }
            }.run();
        }
    }

    /**
     * Returns a map of plans starting at firstPlanId.  The number of plans in
     * the map is the lesser of howMany, MAXPLANS, or the number of extant
     * plans with id numbers following firstPlanId.  The range is not
     * necessarily fully populated; while plan ids are mostly sequential, it is
     * possible for values to be skipped.
     *
     * The plan instances will only be valid for display, and will not
     * be executable, because they will be stripped of memory heavy objects.
     */
    public Map<Integer, Plan> getPlanRange(final int firstPlanId,
                                           final int howMany) {
        synchronized (planner) {
            return new RunTransaction<Map<Integer, Plan>>
                (environment, RunTransaction.readOnly, logger) {

                @Override
                Map<Integer, Plan> doTransaction(Transaction txn) {
                    return AbstractPlan.getPlanRange(eStore, txn,
                                                     planner, myParams,
                                                     firstPlanId, howMany);
                }
            }.run();
        }
    }

    public boolean isClosing() {
        return closing;
    }

    /**
     * Store the current parameters.
     */
    private void storeParameters() {
        logger.fine("Storing Parameters");

        new RunTransaction<Void>(environment,
                                 RunTransaction.sync, logger) {

            @Override
            Void doTransaction(Transaction txn) {
                parameters.persist(eStore, txn);
                return null;
            }
        }.run();
    }

    /**
     * Convenience method to store the current topology and parameters in the
     * same transaction
     */
    private void storeTopoAndParams(final Topology topology,
                                    final DeploymentInfo info,
                                    final Plan plan) {
        assert Thread.holdsLock(this);

        logger.log(Level.FINE,
                   "Storing parameters and topology with sequence #: {0}",
                   topology.getSequenceNumber());

        new RunTransaction<Void>(environment,
                                 RunTransaction.sync, logger) {

            @Override
            Void doTransaction(Transaction txn) {
                /*
                 * Inform the plan that the topology is being updated. Save
                 * the plan if needed. Be sure to take the plan mutex before
                 * any JE locks are acquired in this transaction, per the
                 * synchronization hierarchy.
                 */
                synchronized (plan) {
                   if (plan.updatingMetadata(topology)) {
                       plan.persist(eStore, txn);
                   }
                }

                topoStore.save(txn, new RealizedTopology(topology, info));
                parameters.persist(eStore, txn);
                memo.persist(eStore, txn);
                return null;
            }
        }.run();
    }

    @Override
    public StorageNodeParams getStorageNodeParams(StorageNodeId targetSNId) {
        return parameters.get(targetSNId);
    }

    @Override
    public ParameterMap copyPolicy() {
        return parameters.copyPolicies();
    }

    public void setPolicy(ParameterMap policyParams) {
        parameters.setPolicies(policyParams);
        storeParameters();
    }

    public long getEventExpiryAge() {
        return myParams.getAdminParams().getEventExpiryAge();
    }

    /**
     * This method is used only for testing.
     */
    public void setEventStoreAgingFrequency(int f) {
        eventStoreAgingFrequency = f;
    }

    @Override
    public Monitor getMonitor() {
        return monitor;
    }

    @Override
    public AdminId generateAdminId() {
        return parameters.getNextAdminId();
        /*
         * The new value of parameters.nextAdminId will be persisted when the
         * DeployAdminPlan completes and calls
         * savePlanResults(List<AdminParams>).
         */
    }

    @Override
    public int getAdminCount() {
        return parameters.getAdminCount();
    }

    @Override
    public RepNodeParams getRepNodeParams(RepNodeId targetRepNodeId) {
        return parameters.get(targetRepNodeId);
    }

    public GlobalParams getGlobalParams() {
        return parameters.getGlobalParams();
    }

    synchronized EntityStore initEstore() {
        if (eStore == null) {
            final EntityModel model = new AnnotationModel();
            model.registerClass(TableMetadataProxy.class);
            model.registerClass(SecurityMetadataProxy.class);
            final StoreConfig stConfig = new StoreConfig();
            stConfig.setAllowCreate(true);
            stConfig.setTransactional(true);
            stConfig.setModel(model);
            setMutations(stConfig);
            eStore = new EntityStore(environment, ADMIN_STORE_NAME, stConfig);
        }
        return eStore;
    }

    /**
     * Mutations for schema evolution:
     * 1.  Remove RegistryUtils as a class.
     * 2.  Remove RegistryUtils from DeployStorePlan version 0.
     */
    private void setMutations(StoreConfig stConfig) {
        final Mutations mutations = new Mutations();
        mutations.addDeleter
            (new Deleter("oracle.kv.impl.util.registry.RegistryUtils", 0));
        mutations.addDeleter
            (new Deleter("oracle.kv.impl.admin.plan.DeployStorePlan", 0,
                         "registryUtils"));
        stConfig.setMutations(mutations);
    }

    private void checkSchemaVersion(final boolean isMaster) {
        new RunTransaction<Void>(environment, RunTransaction.sync,
                                  logger) {
            @Override
            Void doTransaction(Transaction txn) {

                /*
                 * Check that this release is compatible with the Admin db
                 * schema version, should it already exist.
                 */
                final AdminSchemaVersion schemaVersion =
                    new AdminSchemaVersion(Admin.this, logger);
                if (isMaster) {
                    schemaVersion.checkAndUpdateVersion(txn);
                } else {
                    schemaVersion.checkVersion(txn);
                }
                return null;
            }
        }.run();
    }

    /*
     * Enter a new replication mode. Called in the ReplicationStateChange
     * thread that is created in Admin's StateChangeListener.stateChange
     * implementation.
     */

    private synchronized void enterMode(final StateChangeEvent sce) {

        final State state = sce.getState();

        switch (state) {
        case MASTER:
            enterMasterMode();
            break;
        case REPLICA:
            enterReplicaMode
                (new AdminId(Integer.parseInt(sce.getMasterNodeName())));
            break;
        case DETACHED:
            if (environment == null) {
                logger.info
                    ("Admin replica is detached; environment is null");
            } else {
                final EnvironmentImpl envImpl =
                    DbInternal.getEnvironmentImpl(environment);
                if (envImpl == null) {
                    logger.info
                        ("Admin replica is detached; envImpl is null");
                } else {
                    try {
                        envImpl.checkIfInvalid();
                        logger.info("Admin replica is detached; " +
                                    "env is valid.");
                    } catch (DatabaseException rre) {
                        /*
                         * If this happens, we will simply exit the
                         * Admin process and let the StorageNodeAgent
                         * restart it.
                         */
                        shutdownForForeignThreadFault
                            (rre, "State Change Notifier");
                    }
                }
            }
            /*$FALL-THROUGH$*/
        case UNKNOWN:
            enterDetachedMode();
            break;
        }
    }

    /*
     * Transition to master mode.  This happens when the master starts up from
     * scratch, and when a new master is elected.  Called only from the
     * synchronized method enterMode, above.
     */
    private void enterMasterMode() {
        try {
            startupStatus.setUnready(Admin.this);
            masterId = adminId;
            checkSchemaVersion(true);
            initEstore();
            eventRecorder.shutdown();
            monitor.shutdown();
            removeParameterListener(monitor);
            monitor = new Monitor(myParams, Admin.this, getLoginManager());
            eventRecorder = new EventRecorder(Admin.this);
            open();
            monitor.setupExistingAgents(getCurrentTopology());
            addParameterListener(monitor);
            readAndRecoverPlans();
            startupStatus.setReady(Admin.this);
            restartPlans();
        } catch (RuntimeException e) {
            startupStatus.setError(Admin.this, e);
        }
    }

    /*
     * Transition to replica mode.  This happens on startup of a replica.
     * Called only from the synchronized method enterMode, above.
     */
    private void enterReplicaMode(final AdminId newMaster) {
        try {
            startupStatus.setUnready(Admin.this);
            masterId = newMaster;
            checkSchemaVersion(false);
            initEstore();
            eventRecorder.shutdown();
            monitor.shutdown();
            removeParameterListener(monitor);
            startupStatus.setReady(Admin.this);
        } catch (RuntimeException e) {
            startupStatus.setError(Admin.this, e);
        }
    }

    /*
     * Transition to a mode in which we don't really know what's going on, and
     * hope that it's a temporary situation.
     */
    private void enterDetachedMode() {
        enterReplicaMode(null);
    }

    public synchronized ReplicatedEnvironment.State getReplicationMode() {
        return environment == null ? null : environment.getState();
    }

    public synchronized URI getMasterHttpAddress() {
        final Parameters p = getCurrentParameters();
        final AdminParams ap = p.get(masterId);
        final StorageNodeParams snp = p.get(ap.getStorageNodeId());

        final int httpPort = ap.getHttpPort();
        final String httpHost = snp.getHostname();
        try {
            return new URI("http", null, httpHost, httpPort, null, null, null);
        } catch (URISyntaxException e) {
            throw new NonfatalAssertionException("Unexpected bad URI", e);
        }
    }

    public synchronized URI getMasterRmiAddress() {
        final Parameters p = getCurrentParameters();
        final AdminParams ap = p.get(masterId);
        final StorageNodeParams snp = p.get(ap.getStorageNodeId());

        try {
            return
                new URI("rmi", null, snp.getHostname(), snp.getRegistryPort(),
                        null, null, null);
        } catch (URISyntaxException e) {
            throw new NonfatalAssertionException("Unexpected bad URL", e);
        }
    }

    /**
     * Dump the deployed transaction history.
     */
    public List<String> displayRealizedTopologies(final boolean concise) {
        return new RunTransaction<List<String>>
            (environment, RunTransaction.readOnly, logger) {

            @Override
            List<String> doTransaction(Transaction txn) {
                return topoStore.displayHistory(txn, concise);
            }
        }.run();
    }

    /**
     * Since the start time is the primary key for the historical collection,
     * ensure that the start time for any new realized topology is greater than
     * the start time recorded for the current topology. Due to clock skew in
     * the HA rep group, conceivably the start time could fail to advance if
     * there is admin rep node failover.
     */
    @Override
    public
    long validateStartTime(long proposedStartTime) {
        return topoStore.validateStartTime(proposedStartTime);
    }

    /**
     * Provide information about the a plan's current or
     * last execution run.
     */
    String getPlanStatus(int planId, long options) {
        final Plan p = getAndCheckPlan(planId);
        final StatusReport report = new StatusReport(p, options);
        return report.display();
    }

    /**
     * Admin.Memo is a persistent class for storing singleton information that
     * Admin needs to keep track of.
     */
    @Entity
    public static class Memo {

        private static final String MEMO_KEY = "Memo";

        @SuppressWarnings("unused")
        @PrimaryKey
        private final String memoKey = MEMO_KEY;

        private int planId;
        private EventRecorder.LatestEventTimestamps latestEventTimestamps;
        /**
         *  @deprecated as of R2, use Datacenter repfactor
         */
        @SuppressWarnings("unused")
        @Deprecated
        private int repFactor;

        public Memo(int firstPlanId,
                    EventRecorder.LatestEventTimestamps let) {
            planId = firstPlanId;
            latestEventTimestamps = let;
        }

        @SuppressWarnings("unused")
        private Memo() {
        }

        private int getPlanId() {
            return planId;
        }

        private void setPlanId(int nextId) {
            planId = nextId;
        }

        private LatestEventTimestamps getLatestEventTimestamps() {
            return latestEventTimestamps;
        }

        private void setLatestEventTimestamps
            (EventRecorder.LatestEventTimestamps let) {

            latestEventTimestamps = let;
        }

        public void persist(EntityStore estore, Transaction txn) {
            final PrimaryIndex<String, Memo> mi =
                estore.getPrimaryIndex(String.class, Memo.class);
            mi.put(txn, this);
        }

        public static Memo fetch(EntityStore estore, Transaction txn) {
            final PrimaryIndex<String, Memo> mi =
                estore.getPrimaryIndex(String.class, Memo.class);
            return mi.get(txn, MEMO_KEY, LockMode.READ_COMMITTED);
        }
    }

    public LoadParameters getAllParams() {
        final LoadParameters ret = new LoadParameters();
        ret.addMap(myParams.getGlobalParams().getMap());
        ret.addMap(myParams.getStorageNodeParams().getMap());
        ret.addMap(myParams.getAdminParams().getMap());
        return ret;

    }

    /* For unit test support */
    public BasicPlannerImpl getPlanner() {
        return planner;
    }

    public void syncEventRecorder() {
        eventRecorder.sync();
    }

    /**
     * Listener for the JE HA replication participant's role changes.
     */
    private class Listener implements StateChangeListener {

        /**
         * Takes action based upon the state change. The actions
         * must be simple and fast since they are performed in JE's thread of
         * control.
         */
        @Override
        public void stateChange(final StateChangeEvent sce)
            throws RuntimeException {

            final State state = sce.getState();
            logger.info("State change event: " + new Date(sce.getEventTime()) +
                        ", State: " + state + ", Master: " +
                        ((state.isMaster() || state.isReplica()) ?
                         sce.getMasterNodeName() : "none"));

            /* If we are shutting down, we don't need to do anything. */
            if (closing) {
                return;
            }

            new StoppableThread("ReplicationStateChange") {
                @Override
                public void run() {
                    enterMode(sce);
                }
                @Override
                protected Logger getLogger() {
                    return logger;
                }
            }.start();
        }
    }

    /**
     * A class to wrap the invocation of a database transaction and to handle
     * all the exceptions that might arise.  Derived from the je.rep.quote
     * example in the BDB JE distribution.
     */
    private abstract static class RunTransaction<T> {
        private static final int TRANSACTION_RETRY_MAX = 20;
        private static final int RETRY_WAIT = 3 * 1000;
        public static final TransactionConfig readOnly =
            new TransactionConfig().setDurability(Durability.READ_ONLY_TXN);
        public static final TransactionConfig sync =
            new TransactionConfig().setDurability
            (new Durability(SyncPolicy.SYNC, SyncPolicy.SYNC,
                            ReplicaAckPolicy.SIMPLE_MAJORITY));
        public static final TransactionConfig noSync =
            new TransactionConfig().setDurability
            (new Durability(SyncPolicy.NO_SYNC,
                            SyncPolicy.NO_SYNC,
                            ReplicaAckPolicy.NONE));
        public static final TransactionConfig writeNoSync =
            new TransactionConfig().setDurability
            (new Durability(SyncPolicy.WRITE_NO_SYNC,
                            SyncPolicy.NO_SYNC,
                            ReplicaAckPolicy.NONE));

        private final ReplicatedEnvironment env;
        private final Logger logger;
        private final TransactionConfig config;

        RunTransaction(ReplicatedEnvironment env,
                       TransactionConfig config,
                       Logger logger) {
            this.env = env;
            this.logger = logger;

            /*
             * For unit tests, use local WRITE_NO_SYNC rather than SYNC, for
             * improved performance.
             */
            if (TestStatus.isWriteNoSyncAllowed() &&
                config.getDurability().getLocalSync() == SyncPolicy.SYNC) {
                final Durability newDurability =
                    new Durability(SyncPolicy.WRITE_NO_SYNC,
                                   config.getDurability().getReplicaSync(),
                                   config.getDurability().getReplicaAck());
                this.config = config.clone().setDurability(newDurability);
            } else {
                this.config = config;
            }
        }

        T run() {

            long sleepMillis = 0;
            T retVal = null;

            for (int i = 0; i < TRANSACTION_RETRY_MAX; i++) {

                /* Sleep before retrying. */
                if (sleepMillis != 0) {
                    try {
                        Thread.sleep(sleepMillis);
                    } catch (InterruptedException ignored) {
                    }
                    sleepMillis = 0;
                }

                Transaction txn = null;
                try {
                    txn = env.beginTransaction(null, config);
                    retVal = doTransaction(txn);
                    try {
                        txn.commit();
                    } catch (InsufficientAcksException iae) {

                        logger.log(Level.WARNING, "Insufficient Acks", iae);
                        /*
                         * The write, if there was one, succeeded locally;
                         * this should sort itself out eventually.
                         */
                    }
                    return retVal;

                } catch (InsufficientReplicasException ire) {

                    logger.log
                        (Level.INFO, "Retrying transaction after " +
                         "InsufficientReplicasException", ire);
                    sleepMillis = RETRY_WAIT;
                    /* Loop and retry. */

                } catch (ReplicaWriteException rwe) {

                    /*
                     * Attempted a modification while in the Replica state.
                     * This might happen if the node is transitioning to
                     * Master state.
                     */
                    logger.log(Level.SEVERE,
                               "Node is no longer master: ", rwe);
                    throw rwe;

                } catch (UnknownMasterException ume) {

                    /*
                     * Attempted a modification while in the Replica state.
                     * This might happen if the node is transitioning to
                     * Master state.
                     */
                    logger.log(Level.SEVERE,
                               "Node is no longer master: ", ume);
                    throw ume;

                } catch (LockConflictException lce) {

                    /*
                     * This should not ever happen, because Admin is the sole
                     * user of this database.  Nonetheless, we'll code to retry
                     * this operation.
                     */
                    logger.log
                        (Level.SEVERE,
                         "Retrying transaction after LockConflictException",
                         lce);
                    sleepMillis = RETRY_WAIT;
                    /* Loop and retry. */

                } catch (ReplicaConsistencyException rce) {

                    /*
                     * Retry the transaction to see if the replica becomes
                     * consistent.
                     */
                    logger.log(Level.WARNING,
                               "Retrying transaction after " +
                               "ReplicaConsistencyException", rce);
                    sleepMillis = RETRY_WAIT;
                    /* Loop and retry. */

                } catch (DatabaseException rre) {

                    /*
                     * If this happens, we will let the ProcessFaultHandler
                     * exit the Admin process so that the StorageNodeAgent can
                     * restart it.
                     */
                    throw(rre);

                } finally {
                    TxnUtil.abort(txn);
                }
            }
            throw new IllegalStateException("Transaction retry limit exceeded");
        }

        /**
         * Must be implemented to perform operations using the given
         * Transaction.
         */
        abstract T doTransaction(Transaction txn);
    }

    /* For use in error and usage messages. */
    @Override
    public String getStorewideLogName() {
        return myParams.getStorageNodeParams().getHostname() + ":" +
            monitor.getStorewideLogName();
    }

    /**
     * Tells the Admin whether startup processing, done in another thread, has
     * finished. If any exceptions occur, saves the exception to transport
     * across thread boundaries.
     */
    private static class StartupStatus {
        private enum Status { INITIALIZING, READY, ERROR }

        private Status status;
        private RuntimeException problem;

        StartupStatus() {
            status = Status.INITIALIZING;
        }

        void setReady(Admin admin) {
            synchronized (admin) {
                status = Status.READY;
                admin.notifyAll();
                admin.updateAdminStatus(ServiceStatus.RUNNING);
            }
        }

        void setUnready(Admin admin) {
            synchronized (admin) {
                status = Status.INITIALIZING;
            }
        }

        boolean isReady(Admin admin) {
            synchronized (admin) {
                return status == Status.READY;
            }
        }

        void setError(Admin admin, RuntimeException e) {
            synchronized (admin) {
                status = Status.ERROR;
                problem = e;
                admin.notifyAll();
                admin.updateAdminStatus(ServiceStatus.ERROR_RESTARTING);
            }
        }

        void waitForIsReady(Admin admin) {
            synchronized (admin) {
                while (status == Status.INITIALIZING) {
                    try {
                        admin.wait();
                    } catch (InterruptedException ie) {
                        throw new IllegalStateException
                            ("Interrupted while waiting for Admin " +
                             "initialization", ie);
                    }
                }
            }

            if (status == Status.READY) {
                return;
            }

            if (status == Status.ERROR) {
                throw problem;
            }
        }
    }

    /**
     * Opens a new KVStore handle.  The caller is responsible for closing it.
     * <p>
     * In the future we may decide to maintain an open KVStore rather than
     * opening one whenever it is needed, if the overhead for opening is
     * unacceptable.  However, because the KVStore is associated with a user
     * login in a secure installation, we cannot blindly share KVStore handles.
     */
    public KVStore openKVStore() {
        final Topology topo = getCurrentTopology();
        final StorageNodeMap snMap = topo.getStorageNodeMap();
        final int nHelpers = Math.min(100, snMap.size());
        final String[] helperArray = new String[nHelpers];
        int i = 0;
        for (StorageNode sn : snMap.getAll()) {
            if (i >= nHelpers) {
                break;
            }
            helperArray[i] = sn.getHostname() + ':' + sn.getRegistryPort();
            i += 1;
        }
        final KVStoreConfig config = new KVStoreConfig(topo.getKVStoreName(),
                                                       helperArray);
        final Properties props = new Properties();
        props.setProperty(KVSecurityConstants.TRANSPORT_PROPERTY, "internal");
        config.setSecurityProperties(props);

        final KVStoreUserPrincipal currentUser =
            KVStoreUserPrincipal.getCurrentUser();

        final LoginCredentials creds;
        if (currentUser == null) {
            creds = null;
        } else {
            LoginManager loginMgr = getLoginManager();
            creds = new ClientProxyCredentials(currentUser, loginMgr);
        }
        return KVStoreFactory.getStore(config, creds,
                                       null /* ReauthenticateHandler */);
    }

    public String previewTopology(String targetTopoName, String startTopoName,
                                  boolean verbose) {
        Topology startTopo;
        if (startTopoName == null) {
            startTopo = getCurrentTopology();
        } else {
            final TopologyCandidate startCand = getCandidate(startTopoName);
            startTopo = startCand.getTopology();
        }
        final TopologyCandidate target = getCandidate(targetTopoName);
        final TopologyDiff diff = new TopologyDiff(startTopo, startTopoName,
                                                   target, parameters);
        return diff.display(verbose);
    }

    public String changeRepFactor(final String candidateName,
                                  final String snPoolName,
                                  final DatacenterId dcId,
                                  final int repFactor) {
        return new RunTransaction<String>(environment,
                                          RunTransaction.writeNoSync, logger) {
            @Override
            String doTransaction(Transaction txn) {

                final TopologyCandidate candidate =
                    getCandidate(txn, candidateName);
                final StorageNodePool pool = freezeSNPool(snPoolName);

                try {
                    /* Create and save a new layout against the specified SNs */
                    final TopologyBuilder tb =
                        new TopologyBuilder(candidate, pool,
                                            getCurrentParameters(), myParams);

                    final TopologyCandidate newCandidate =
                        tb.changeRepfactor(repFactor, dcId);
                    topoStore.save(txn, newCandidate);
                    return "Changed replication factor in " +
                        newCandidate.getName();
                } finally {
                    pool.thaw();
                }
            }
        }.run();
    }

    public String validateTopology(String candidateName) {
        Results results = null;
        String prefix = null;
        if (candidateName == null) {
            results = Rules.validate(getCurrentTopology(),
                                     getCurrentParameters(),
                                     true);
            prefix = "the current deployed topology";
        } else {
            final TopologyCandidate candidate = getCandidate(candidateName);
            results = Rules.validate(candidate.getTopology(),
                                     getCurrentParameters(),
                                     false);
            prefix = "topology candidate \"" + candidateName + "\"";
        }
        return "Validation for " + prefix + ":\n" + results;
    }

    public String moveRN(final String candidateName,
                         final RepNodeId rnId,
                         final StorageNodeId snId) {

        return new RunTransaction<String>(environment,
                RunTransaction.writeNoSync, logger) {
            @Override
            String doTransaction(Transaction txn) {

                final TopologyCandidate candidate =
                    getCandidate(txn, candidateName);
                final StorageNodePool pool =
                    freezeSNPool(Parameters.DEFAULT_POOL_NAME);
                try {
                    /* Create and save a new layout against the specified SNs */
                    final TopologyBuilder tb =
                        new TopologyBuilder(candidate, pool,
                                            getCurrentParameters(), myParams);

                    final RepNode oldRN = candidate.getTopology().get(rnId);
                    if (oldRN == null) {
                        return rnId + " doesn't exist, and can't be moved.";
                    }

                    final TopologyCandidate newCandidate =
                        tb.relocateRN(rnId, snId);
                    final RepNode newRN = newCandidate.getTopology().get(rnId);
                    if (newRN == null) {
                        throw new IllegalStateException
                            (rnId +
                             " is missing from the new topology candidate: " +
                             TopologyPrinter.printTopology
                             (newCandidate.getTopology()));
                    }

                    if (!(newRN.getStorageNodeId().equals
                            (oldRN.getStorageNodeId()))) {

                        topoStore.save(txn, newCandidate);
                        return "Moved " + rnId + " from " +
                            oldRN.getStorageNodeId() + " to " +
                            newRN.getStorageNodeId();
                    }

                    return "Couldn't find an eligible SN to house " + rnId;
                } finally {
                    pool.thaw();
                }
            }
        }.run();
    }

    /**
     * Return the latency ceiling associated with the given RepNode.
     */
    @Override
    public int getLatencyCeiling(ResourceId rnid) {
        if (rnid instanceof RepNodeId) {
            final RepNodeParams rnp = parameters.get((RepNodeId) rnid);
            return rnp == null ? 0 : rnp.getLatencyCeiling();

        }

        return 0;
    }

    /**
     * Return the throughput floor associated with the given RepNode.
     */
    @Override
    public int getThroughputFloor(ResourceId rnid) {
        if (rnid instanceof RepNodeId) {
            final RepNodeParams rnp = parameters.get((RepNodeId) rnid);
            return rnp == null ? 0 : rnp.getThroughputFloor();
        }
        return 0;
    }

    /**
     * Return the threshold to apply to the average commit lag computed
     * from the total commit lag and the number of commit log records
     * replayed by the given RepNode, as reported by the JE backend.
     */
    @Override
    public long getCommitLagThreshold(ResourceId rnid) {
        if (rnid instanceof RepNodeId) {
            final RepNodeParams rnp = parameters.get((RepNodeId) rnid);
            return rnp == null ? 0 : rnp.getCommitLagThreshold();

        }
        return 0L;
    }

    private void updateAdminStatus(ServiceStatus newStatus) {
        if (owner == null) {
            return;
        }
        owner.updateAdminStatus(this, newStatus);
    }

    /**
     * Return the TopologyStore.  Used during upgrade.
     */
    TopologyStore getTopoStore() {
        return topoStore;
    }

    /**
     * A method for exiting the Admin process when an exception is thrown
     * from a thread the we don't control.
     */
    public void shutdownForForeignThreadFault(Exception e, String name) {
        logger.log(Level.SEVERE,
                   "Admin shutting down for fault in thread " + name, e);
        if (owner != null) {
            owner.getFaultHandler().queueShutdown(e, ProcessExitCode.RESTART);
        } else {
            /* This would be true only in a test environment. */
            new StoppableThread("StopAdminForForeignThreadFault") {
                @Override
                public void run() {
                    shutdown(true);
                }
                @Override
                protected Logger getLogger() {
                    return Admin.this.getLogger();
                }
            }.start();
        }
    }

    public ReplicationGroupAdmin getReplicationGroupAdmin(AdminId targetId) {
        final AdminParams targetAP = parameters.get(targetId);
        if (targetAP == null) {
            return null;
        }

        final String targetHelperHosts = targetAP.getHelperHosts();

        final Set<InetSocketAddress> helperSockets =
            new HashSet<InetSocketAddress>();
        final StringTokenizer tokenizer =
            new StringTokenizer(targetHelperHosts,
                                ParameterUtils.HELPER_HOST_SEPARATOR);
        while (tokenizer.hasMoreTokens()) {
            final String helper = tokenizer.nextToken();
            helperSockets.add(HostPortPair.getSocket(helper));
        }

        final String storeName = parameters.getGlobalParams().getKVStoreName();
        final String groupName = getAdminRepGroupName(storeName);

        return new ReplicationGroupAdmin(groupName, helperSockets,
                                         repConfig.getRepNetConfig());
    }

    @Override
    public void removeAdminFromRepGroup(AdminId victim) {

        logger.info("Removing Admin replica " + victim +
                    " from the replication group.");

        final ReplicationGroupAdmin rga = getReplicationGroupAdmin(victim);
        if (rga == null) {
            throw new MemberNotFoundException
                ("The admin " + victim + " is not in the rep group.");
        }

        /* This call may also throw MemberNotFoundException. */
        final String victimNodeName = getAdminRepNodeName(victim);

        final int nAdmins = getAdminCount();
        if (nAdmins == 1) {
            throw new NonfatalAssertionException
                ("Attempting to remove the sole Admin instance" + victim);
        }

        rga.removeMember(victimNodeName);
    }

    @Override
    public void transferMaster() {
        logger.info("Transferring Admin mastership");

        final Set<String> replicas = new HashSet<String>();
        for (AdminId r : parameters.getAdminIds()) {
            if (adminId.equals(r)) {
                continue;
            }
            replicas.add(getAdminRepNodeName(r));
        }

        try {
            environment.transferMaster(replicas, 60, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new NonfatalAssertionException
                ("Master transfer failed", e);
        }
    }

    @Override
    public boolean checkStoreVersion(KVVersion requiredVersion) {
        return VersionUtil.compareMinorVersion(getStoreVersion(),
                                               requiredVersion) >= 0;
    }

    @Override
    public KVVersion getStoreVersion() {

        if (VersionUtil.compareMinorVersion(storeVersion,
                                            KVVersion.CURRENT_VERSION) >= 0) {
            return storeVersion;
        }

        /*
         * The store is not at the current minor version, query the nodes to
         * see if they have been upgraded.
         */
        final Topology topology = getCurrentTopology();
        final RegistryUtils registryUtils =
            new RegistryUtils(topology, getLoginManager());

        final List<StorageNodeId> snIds = topology.getStorageNodeIds();

        /* Start at our highest version, downgrade as needed. */
        KVVersion v = KVVersion.CURRENT_VERSION;

        for (StorageNodeId snId : snIds) {
            StorageNodeStatus snStatus = null;

            /* If there are any errors, throw an exception */
            try {
                snStatus = registryUtils.getStorageNodeAgent(snId).ping();
            } catch (RemoteException re) {
                throw new AdminFaultException(re);
            } catch (NotBoundException nbe) {
                throw new AdminFaultException(nbe);
            }

            final KVVersion snVersion = snStatus.getKVVersion();

            if (snVersion.compareTo(KVVersion.PREREQUISITE_VERSION) < 0) {
                final String prereq =
                    KVVersion.PREREQUISITE_VERSION.getNumericVersionString();
                throw new AdminFaultException(
                    new IllegalCommandException(
                        "Node " + snId + " is at software version " +
                        snVersion.getNumericVersionString() +
                        " which does not meet the current prerequisite." +
                        " It must be upgraded to version " + prereq +
                        " or greater."));
            }

            /* If we found someone of lesser minor version, downgrade */
            if (VersionUtil.compareMinorVersion(snVersion, v) < 0) {
                v = snVersion;
            }
        }

        /*
         * Since everyone responded, we can set the store version to what
         * was found.
         */
        synchronized (this) {

            if (v.compareTo(storeVersion) > 0) {
                storeVersion = v;
            }
            return storeVersion;
        }
    }

    @Override
    public <T extends Metadata<? extends MetadataInfo>> T
                                 getMetadata(final Class<T> returnType,
                                             final MetadataType metadataType) {
        logger.log(Level.INFO, // TODO - FINE
                   "Getting {0} metadata",
                   metadataType);

        return new RunTransaction<T>(environment,
                                     RunTransaction.readOnly,
                                     logger) {
            @Override
            T doTransaction(Transaction txn) {
                try {
                    return MetadataStore.read(returnType, metadataType,
                                              eStore, txn);
                } catch (IndexNotAvailableException inae) {

                    /*
                     * The operation is performed on an admin replica and the
                     * primaryIndex for MetadataHolder is not yet created on
                     * the admin master. Treat this case as no metadata is in
                     * the entity store yet and return null.
                     */
                    logger.log(Level.FINE,
                               "Metadata is currently unavailable: {0}",
                               inae);
                    return null;
                }
            }
        }.run();
    }

    @Override
    public void saveMetadata(final Metadata<?> metadata, final Plan plan) {
        logger.log(Level.INFO, // TODO - FINE
                   "Storing {0} ", metadata);

        new RunTransaction<Void>(environment,
                                 RunTransaction.sync,
                                 logger) {
            @Override
            Void doTransaction(Transaction txn) {

                /*
                 * Inform the plan that the metadata is being updated. Save
                 * the plan if needed. Be sure to take the plan mutex before
                 * any JE locks are acquired in this transaction, per the
                 * synchronization hierarchy.
                 */
                synchronized (plan) {
                   if (plan.updatingMetadata(metadata)) {
                       plan.persist(eStore, txn);
                   }
                }

                MetadataStore.save(metadata, eStore, txn);
                return null;
            }
        }.run();
    }

    @Override
    public LoginManager getLoginManager() {
        if (owner == null) {
            return null;
        }
        return owner.getLoginManager();
    }

    boolean isReady() {
        return startupStatus == null ? false : startupStatus.isReady(this);
    }

    void installLoginUpdater() {
        if (owner != null) {
            final AdminSecurity aSecurity = owner.getAdminSecurity();
            final LoginService loginService = owner.getLoginService();
            final LoginUpdater loginUpdater = new LoginUpdater();

            loginUpdater.addServiceParamsUpdaters(aSecurity);
            loginUpdater.addGlobalParamsUpdaters(aSecurity);

            if (loginService != null) {
                loginUpdater.addServiceParamsUpdaters(loginService);
                loginUpdater.addGlobalParamsUpdaters(loginService);
            }

            addParameterListener(loginUpdater.new ServiceParamsListener());
            addGlobalParameterListener(
                loginUpdater.new GlobalParamsListener());
        }
    }
}
