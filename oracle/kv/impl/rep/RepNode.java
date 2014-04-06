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

package oracle.kv.impl.rep;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.api.RequestDispatcher;
import oracle.kv.impl.api.TopologyInfo;
import oracle.kv.impl.api.TopologyManager;
import oracle.kv.impl.api.rgstate.RepGroupState;
import oracle.kv.impl.api.rgstate.RepNodeState;
import oracle.kv.impl.api.table.TableChangeList;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.api.table.TableMetadataProxy;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.fault.RNUnavailableException;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.metadata.MetadataKey;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.rep.RepEnvHandleManager.StateChangeListenerFactory;
import oracle.kv.impl.rep.RepNodeService.Params;
import oracle.kv.impl.rep.admin.RepNodeAdmin;
import oracle.kv.impl.rep.admin.RepNodeAdmin.PartitionMigrationState;
import oracle.kv.impl.rep.masterBalance.MasterBalanceManager;
import oracle.kv.impl.rep.masterBalance.MasterBalanceManagerInterface;
import oracle.kv.impl.rep.masterBalance.MasterBalanceStateTracker;
import oracle.kv.impl.rep.migration.MigrationManager;
import oracle.kv.impl.rep.migration.PartitionMigrationStatus;
import oracle.kv.impl.rep.table.TableManager;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.security.metadata.SecurityMetadataInfo;
import oracle.kv.impl.security.metadata.SecurityMetadataProxy;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.table.Index;
import oracle.kv.table.Table;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.ReplicaConsistencyPolicy;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicaConsistencyException;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationNetworkConfig;
import com.sleepycat.je.rep.RestartRequiredException;
import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.TimeConsistencyPolicy;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.StoreConfig;
import com.sleepycat.persist.StoreNotFoundException;
import com.sleepycat.persist.model.AnnotationModel;
import com.sleepycat.persist.model.EntityModel;

/**
 * A RepNode represents the data stored in a particular node. It is a member of
 * a replication group, holding some number of partitions of the total key
 * space of the data store.
 * <p>
 * This class is responsible for the management of the canonical replication
 * environment handle and database handles. That is, it provides the plumbing
 * to facilitate requests and does not handle the implementation of any user
 * level operations. All such operations are handled by the RequestHandler and
 * its sub-components.
 * <p>
 */
public class RepNode implements TopologyManager.PostUpdateListener,
                                TopologyManager.PreUpdateListener {

    /* Number of retries for DB operations. */
    private static final int NUM_DB_OP_RETRIES = 100;

    /* DB operation delays */
    private static final long RETRY_TIME_MS = 500;

    /**
     * Metadata store name. Note due to backward compatability, the old
     * topology name is reused.
     */
    private static final String METADATA_STORE_NAME = "TopologyEntityStore";

    private RepNodeId repNodeId;

    /**
     * The parameters used to configure the rep node.
     */
    private Params params;

    private final RequestDispatcher requestDispatcher;

    /**
     * The topology manager. All access to the topology, which can change
     * dynamically, is through the manager.
     */
    private final TopologyManager topoManager;

    /**
     * The migration manager. Handles all of the partition migration duties.
     */
    private final MigrationManager migrationManager;

    /**
     * The RN side master balancing component.
     */
    private MasterBalanceManagerInterface masterBalanceManager;

    /**
     * The security metadata manager. All access to the security metadata is
     * through the manager.
     */
    private SecurityMetadataManager securityMDManager;

    /*
     * Manages the partition database handles.
     */
    private PartitionManager partitionManager;

    /*
     * Manages the secondary database handles.
     */
    private TableManager tableManager;

    /**
     * The manager for the environment holding all the databases for the
     * partitions owned by this RepNode. The replicated environment handle can
     * change dynamically as exceptions are encountered. The manager
     * coordinates access to the single shared replicated environment handle.
     */
    private RepEnvHandleManager envManager;
    private final RepNodeService repNodeService;

    /* True if the RepNode has been stopped. */
    private volatile boolean stopped = false;

    private Logger logger;

    public RepNode(Params params, RequestDispatcher requestDispatcher,
                   RepNodeService repNodeService) {
        this.requestDispatcher = requestDispatcher;
        this.topoManager = requestDispatcher.getTopologyManager();
        this.migrationManager = new MigrationManager(this, params);
        this.repNodeService = repNodeService;
    }

    public GlobalParams getGlobalParams() {
        return params.getGlobalParams();
    }

    public RepNodeParams getRepNodeParams() {
        return params.getRepNodeParams();
    }

    public StorageNodeParams getStorageNodeParams() {
        return params.getStorageNodeParams();
    }

    public LoadParameters getAllParams() {
        LoadParameters ret = new LoadParameters();
        ret.addMap(params.getGlobalParams().getMap());
        ret.addMap(params.getStorageNodeParams().getMap());
        ret.addMap(params.getRepNodeParams().getMap());
        return ret;
    }

    /**
     * Returns the dispatchers exception handler. An exception caught by this
     * handler results in the process being restarted by the SNA.
     *
     * @see RepNodeService.ThreadExceptionHandler
     *
     * @return the dispatchers exception handler
     */
    @SuppressWarnings("javadoc")
    public UncaughtExceptionHandler getExceptionHandler() {
        return requestDispatcher.getExceptionHandler();
    }

    /**
     * For testing only
     */
    public MasterBalanceStateTracker getBalanceStateTracker() {
        return masterBalanceManager.getStateTracker();
    }

    // TODO: incremental changes to configuration properties
    public void initialize(Params params1,
                           StateChangeListenerFactory listenerFactory) {
        this.params = params1;
        logger = LoggerUtils.getLogger(this.getClass(), params);
        RepNodeParams repNodeParams = params.getRepNodeParams();
        this.repNodeId = repNodeParams.getRepNodeId();

        /*
         * Must precede opening of the environment handle below.
         */
        if (masterBalanceManager == null) {

            masterBalanceManager = MasterBalanceManager.create(this, logger);
        }
        masterBalanceManager.initialize();
        envManager = new RepEnvHandleManager(this, listenerFactory, params,
                                             repNodeService);

        if (tableManager == null) {
            tableManager = new TableManager(this, params);
        }

        /* Set up the security metadata manager */
        if (securityMDManager == null) {
            securityMDManager = new SecurityMetadataManager(
                this, params.getGlobalParams().getKVStoreName(), logger);
        }

        if (partitionManager == null) {
            partitionManager = new PartitionManager(this, tableManager, params);
        }
        topoManager.setLocalizer(migrationManager);
        topoManager.addPreUpdateListener(this);
        topoManager.addPostUpdateListener(this);
    }

    /**
     * Returns the partition Db config
     *
     * @return the partition Db config
     */
    public DatabaseConfig getPartitionDbConfig() {
        assert partitionManager != null;
        return partitionManager.getPartitionDbConfig();
    }

    /**
     * Opens the database handles used by this rep node.
     * The databases are created if they do not already exist.
     * <p>
     * This method is invoked at startup. At this time, new databases may be
     * created if this node is the master and databases need to be created for
     * the partitions assigned to this node. If the node is a replica, it may
     * need to wait until the databases created on the master have been
     * replicated to it.
     * <p>
     * Post startup, this method is invoked to re-establish database handles
     * whenever the associated environment handle is invalidated and needs to
     * be re-established. Or via the TopologyManager's listener interface
     * whenever the Topology has been updated.
     *
     * @param repEnv the replicated environment handle
     *
     * @param reuseExistingHandles true if any current db handles are valid
     * and should be reused.
     */
    void updateDbHandles(ReplicatedEnvironment repEnv,
                         boolean reuseExistingHandles) {

        Topology topology = topoManager.getLocalTopology();

        if (topology == null) {
            /* No topology and no partitions. Node is being initialized. */
            return;
        }

        /*
         * The migration manager has a handle open, so if an update is
         * required, force an update.
         */
        if (!reuseExistingHandles) {
            migrationManager.updateDbHandles(repEnv);
        }

        securityMDManager.updateDbHandles(repEnv);
        partitionManager.updateDbHandles(topology,
                                         repEnv,
                                         reuseExistingHandles);
        tableManager.updateDbHandles(repEnv, reuseExistingHandles);
    }

    @Override
    public void preUpdate(Topology newTopology) {
        /* Don't wait, we just care about the state: master or not */
        final ReplicatedEnvironment env = envManager.getEnv(0);
        try {
            if ((env == null) || !env.getState().isMaster()) {
                return;
            }
        } catch (EnvironmentFailureException e) {
            /* It's in the process of being re-established. */
            return;
        } catch (IllegalStateException iae) {
            /* A closed environment. */
            return;
        }

        try {
            requestDispatcher.getTopologyManager().
                checkPartitionChanges(new RepGroupId(repNodeId.getGroupId()),
                                      newTopology);
        } catch (IllegalStateException ise) {
            /* The Topology checks failed, force a shutdown. */
            getExceptionHandler().
                uncaughtException(Thread.currentThread(), ise);
        }
    }

    /**
     * Implements the Listener method for topology changes. It's invoked by
     * the TopologyManager and should not be invoked directly.
     * <p>
     * Update the partition map if the topology changes and store it.
     *
     * TODO - This method persist the topology in the local metadata store. For
     * historical reasons this can't use the general metadata handling facility.
     * It would be good to combine some, if not all of this code with the
     * general code.
     */
    @Override
    public boolean postUpdate(Topology newTopology) {

        final ReplicatedEnvironment env = envManager.getEnv(1);
        if (env == null) {
            throw new OperationFaultException("Could not obtain env handle");
        }

        int attempts = NUM_DB_OP_RETRIES;
        RuntimeException lastException = null;
        while (!stopped) {
            if (!env.isValid()) {
                throw new OperationFaultException(
                        "Failed in persistence of " + newTopology.getType() +
                        " metadata, environment not valid");
            }
            try {
                updateDbHandles(env, true);

                EntityStore estore = null;
                try {
                    estore = getMetadataStore(env, true);
                    newTopology.persist(estore, null);

                    /*
                     * Ensure it's in stable storage, since the database is
                     * non-transactional. For unit tests, use WRITE_NO_SYNC
                     * rather than SYNC, for improved performance.
                     */
                    final boolean flushSync =!TestStatus.isWriteNoSyncAllowed();
                    env.flushLog(flushSync);
                    logger.info("Topology stored seq#: " +
                                newTopology.getSequenceNumber());
                    return false;
                } finally {
                    if (estore != null) {
                        TxnUtil.close(logger, env, estore, "topology");
                    }
                }
            } catch (ReplicaWriteException rwe) {
                lastException = rwe;
            } catch (UnknownMasterException ume) {
                lastException = ume;
            } catch (RuntimeException rte) {
                /*
                 * Includes ISEs or EFEs in particular, due to the environment
                 * having been closed or invalidated, but can also include NPE
                 * that may be thrown in certain circumstances.
                 */
                if (!env.isValid()) {
                    logger.info("Encountered failed env during topo update. " +
                                rte.getMessage());
                    /*
                     * The RN will recover on its own, fail the update, forcing
                     * the invoker of the update to retry.
                     */
                    throw new OperationFaultException("Failed topology update",
                                                      rte);
                }
                /* Will cause the process to exit. */
                throw rte;
            }
            attempts--;
            if (attempts == 0) {
                throw new OperationFaultException(
                        "Failed in persistence of " + newTopology.getType() +
                        " metadata, operation timed out", lastException);
            }
            try {
                Thread.sleep(RETRY_TIME_MS);
            } catch (InterruptedException ie) {
                /* Should not happen. */
                throw new IllegalStateException(ie);
            }
        }
        return false;
    }

    /**
     * Update the topology at this node with the topology changes in topoInfo.
     *
     * @param topoInfo contains the changes to be made to the topo at this RN
     *
     * @return the post-update topology sequence number
     */
    private int updateTopology(TopologyInfo topoInfo) {
        if (topoInfo.isEmpty()) {
            /* Unexpected, should not happen. */
            logger.warning("Empty change list sent for topology update");

            return getTopoSequenceNumber();
        }

        final int topoSeqNum = getTopoSequenceNumber();

        if (topoInfo.getChanges().get(0).getSequenceNumber() >
            (topoSeqNum + 1)) {
            /*
             * Allow for cases where the update pushes may be obsolete, e.g.
             * when an SN is being migrated, or more generally when its log
             * files have been deleted and it's being re-initialized
             */
            logger.info("Ignoring topo update request. " +
                        "Topo seq num: " + topoSeqNum +
                        " first change: " +
                        topoInfo.getChanges().get(0).getSequenceNumber());
            return topoSeqNum;
        }
        topoManager.update(topoInfo);
        return getTopoSequenceNumber();
    }

    /**
     * Returns the current topo sequence number associated with the RN,
     * or Topology.EMPTY_SEQUENCE_NUMBER if the RN has not been initialized
     * with a topology.
     */
    private int getTopoSequenceNumber() {
        final Topology topology = getTopology();
        return (topology != null) ?
               topology.getSequenceNumber() :
               Topology.EMPTY_SEQUENCE_NUMBER;
    }

    /**
     * Updates the local topology and partition Db map. The basis of the update
     * is the current "official" topology and is only done iff the current
     * topology sequence number is greater than or equal to the specified
     * topoSeqNum. Otherwise, the local topology is left unchanged.
     *
     * @return true if the update was successful
     */
    public boolean updateLocalTopology() {
        if (topoManager.updateLocalTopology()) {
            return true;
        }

        /*
         * We need a newer topology. Send a NOP to the master, since that is
         * where topoSeqNum came from. The master will send the topology
         * in the response.
         */
        logger.log(Level.FINE, "Sending NOP to update topology");
        return sendNOP(new RepGroupId(repNodeId.getGroupId()));
    }

    /**
     * Returns the SecurityMetadataManager for this RepNode.
     */
    public SecurityMetadataManager getSecurityMDManager() {
        return securityMDManager;
    }

    /**
     * Sends a NOP to a node in the specified group. It attempts to send the NOP
     * to the master of that group if known. Otherwise it sends it to a random
     * node in the group.
     *
     * @param groupId
     * @return true if the operation was successful
     */
    public boolean sendNOP(RepGroupId groupId) {
        final RepGroupState rgs = requestDispatcher.getRepGroupStateTable().
                                                        getGroupState(groupId);
        LoginManager lm = repNodeService.getRepNodeSecurity().getLoginManager();
        RepNodeState rns = rgs.getMaster();

        if (rns == null) {
            rns = rgs.getRandomRN(null, null);
        }

        logger.log(Level.FINE, "Sending NOP to {0}", rns.getRepNodeId());

        try {
            if (requestDispatcher.executeNOP(rns, 1000, lm) != null) {
                return true;
            }
        } catch (Exception ex) {
            logger.log(Level.WARNING,
                       "Exception sending NOP to " + rns.getRepNodeId(),
                       ex);
        }
        return false;
    }

    /**
     * Returns the rep node state for the master of the specified group. If
     * the master is not known, null is returned.
     *
     * @param groupId
     * @return the rep node state
     */
    public RepNodeState getMaster(RepGroupId groupId) {
        return requestDispatcher.getRepGroupStateTable().
                                            getGroupState(groupId).getMaster();
    }

    /**
     * Invoked when the replicated environment has been invalidated due to an
     * exception that requires an environment restart, performing the restart
     * asynchronously in a separate thread.
     *
     * @param prevRepEnv the handle that needs to be re-established.
     *
     * @param rbe the exception that required re-establishment of the handle.
     */
    public void asyncEnvRestart(ReplicatedEnvironment prevRepEnv,
                                RestartRequiredException rbe) {
        envManager.asyncRenewRepEnv(prevRepEnv, rbe);
    }

    /**
     * Used to inform the RN about state change events associated with the
     * environment. It, in turn, informs the environment manager.
     */
    public void noteStateChange(ReplicatedEnvironment repEnv,
                                StateChangeEvent stateChangeEvent) {
        if (stopped) {
            return;
        }
        envManager.noteStateChange(repEnv, stateChangeEvent);
        migrationManager.noteStateChange(stateChangeEvent);
        tableManager.noteStateChange(stateChangeEvent);
        masterBalanceManager.noteStateChange(stateChangeEvent);
    }

    /**
     * Returns the environment handle.
     *
     * @param timeoutMs the max amount of time to wait for a handle to become
     * available
     *
     * @see RepEnvHandleManager#getEnv
     */
    public ReplicatedEnvironment getEnv(long timeoutMs) {
        return envManager.getEnv(timeoutMs);
    }

    /**
     * Returns the underlying implementation handle for the environment or null
     * if it cannot be obtained.
     */
    public RepImpl getEnvImpl(long timeoutMs) {
        final ReplicatedEnvironment env = getEnv(timeoutMs);
        return (env != null) ? RepInternal.getRepImpl(env) : null;
    }

    /*
     * Returns the RepEnvHandleManager.
     */
    RepEnvHandleManager getRepEnvManager() {
        return envManager;
    }

    /**
     * Invoked to provide a clean shutdown of the RepNode.
     */
    public void stop(boolean force) {
        if (stopped) {
            logger.info("RepNode already stopped.");
            return;
        }
        stopped = true;
        logger.info("Shutting down RepNode" + (force ? "(force)" : ""));

        migrationManager.shutdown(force);
        tableManager.shutdown();
        closeDbHandles(force);

        if (envManager != null) {
            if (force) {
                /* Use an internal interface to close without checkpointing */
                final ReplicatedEnvironment env = envManager.getEnv(1);
                if (env != null) {
                    EnvironmentImpl envImpl =
                        DbInternal.getEnvironmentImpl(env);
                    if (envImpl != null) {
                        try {
                            envImpl.close(false);
                        } catch (DatabaseException e) {
                            /*
                             * Ignore exceptions during a forced close, just
                             * bail out.
                             */
                            logger.log(Level.INFO,  "Ignoring exception "
                                + "during forced close:", e);
                        }
                    }
                }
            } else {
                envManager.closeEnv();
            }
        }

        /*
         * Shutdown, after the env handle has been closed, so that state
         * changes are communicated by the tracker.
         */
        masterBalanceManager.shutdown();
    }

    /**
     * Closes all database handles, typically as a precursor to closing the
     * environment handle itself. The caller is assumed to have made provisions
     * if any to ensure that the handles are no longer in use.
     */
    void closeDbHandles(boolean force) {
        migrationManager.closeDbHandles(force);
        tableManager.closeDbHandles();
        partitionManager.closeDbHandles();
        securityMDManager.closeDbHandles();
    }

    /**
     * Gets the set of partition ids managed by this node.
     *
     * @return the set of partition ids
     */
    public Set<PartitionId> getPartitions() {
        return partitionManager.getPartitions();
    }

    /**
     * Returns the partition associated with the key
     *
     * @param keyBytes the key used for looking up the database
     *
     * @return the partitionId associated with the key
     */
    public PartitionId getPartitionId(byte[] keyBytes) {
        return partitionManager.getPartitionId(keyBytes);
    }

    /**
     * Returns the database associated with the key.
     *
     * @param keyBytes the key used for looking up the database
     *
     * @return the database associated with the key or null
     */
    public Database getPartitionDB(byte[] keyBytes) {
        return partitionManager.getPartitionDB(keyBytes);
    }

    /**
     * Returns the database associated with the partition.
     *
     * @param partitionId the partition used for looking up the database.
     *
     * @return the database associated with the partition.
     *
     * @throws IncorrectRoutingException is the partition DB is not found
     */
    public Database getPartitionDB(PartitionId partitionId)
            throws IncorrectRoutingException {

        final Database partitionDb =
                partitionManager.getPartitionDB(partitionId);

        if (partitionDb != null) {
            return partitionDb;
        }

        /*
         * The partition database handle was not found. This could be because
         * the request was incorrectly routed or this is the correct node but
         * database has not yet been opened. Check which case it is before
         * throwing an IncorrectRoutingException.
         */
        final String message;
        final Topology topology = getLocalTopology();

        if (topology == null) {
            message = "Partition: " + partitionId + " not present at RepNode " +
                      repNodeId;
        } else if (topology.getPartitionMap().get(partitionId).
                       getRepGroupId().getGroupId() == repNodeId.getGroupId()) {
            /*
             * According to the local topo, the partition should be here. We
             * will still thrown an IncorrectRoutingException to buy time as
             * the operation will be retried.
             */
            message = "Partition: " + partitionId + " missing from RepNode " +
                      repNodeId + ", topology seq#: " +
                      topology.getSequenceNumber();
            logger.log(Level.FINE, message);
            partitionManager.updateDbHandles(topology);
        } else {
            message = "Partition: " + partitionId + " not present at RepNode " +
                      repNodeId + ", topology seq#: " +
                      topology.getSequenceNumber();
        }
        throw new IncorrectRoutingException(message, partitionId);
    }

    /**
     * Called from the migration manager to let is know that the specified
     * partition has officially been removed from this node.
     *
     * @param partitionId
     */
    public void notifyRemoval(PartitionId partitionId) {
        tableManager.notifyRemoval(partitionId);
    }

    /**
     * Starts up the RepNode and creates a replicated environment handle.
     * Having created the environment, it then initializes the topology manager
     * with a copy of the stored topology from it.
     *
     * @return the environment handle that was established
     */
    public ReplicatedEnvironment startup() {

        /* Create the replicated environment handle. */
        boolean created = envManager.renewRepEnv(null, null);
        assert created;

        /*
         * Initialize topology from the topology entity store if it's available
         * there. Note that the absence of a topology will cause the admin
         * component to wait until one is supplied via the RepNodeAdmin
         * interface.
         */
        final ReplicatedEnvironment env = envManager.getEnv(1 /* ms */);

        if (env == null) {
            throw new IllegalStateException("Could not obtain environment " +
                                            "handle without waiting.");
        }

        /*
         * Update the topology manager with a saved copy of the topology
         * from the store.
         */
        EntityStore estore = null;
        try {
            estore = getMetadataStore(env, false);

            final Topology topo = Topology.fetch(estore, null);

            /* The local topo could be null if there was a rollback */
            if (topo == null) {
                logger.info("Store did not contain a topology");
            } else if (topoManager.update(topo)) {
                logger.log(Level.INFO,
                           "Topology fetched sequence#: {0}, updated " +
                           "topology seq# {1}",
                           new Object[]{topo.getSequenceNumber(),
                                        topoManager.getTopology().
                                                        getSequenceNumber()});
            }
        } catch (StoreNotFoundException sne) {
            logger.info("Environment does not contain topology store.");
        } finally {
            if (estore != null) {
                estore.close();
            }
        }

        /* Start thread to push HA state changes to the SNA. */
        masterBalanceManager.startTracker();
        migrationManager.startTracker();
        tableManager.startTracker();

        return env;
    }

    ReplicationNetworkConfig getRepNetConfig() {
        return envManager.getRepNetConfig();
    }

    /**
     * Returns the EntityStore used by this RN. It's the caller's
     * responsibility to close the handle.
     */
    private EntityStore getMetadataStore(Environment env, boolean allowCreate) {
        /* Proxies for non-DLP metadata objects would be added here */
	final EntityModel model = new AnnotationModel();
        model.registerClass(TableMetadataProxy.class);
        model.registerClass(SecurityMetadataProxy.class);
        final StoreConfig stConfig = new StoreConfig();
        stConfig.setAllowCreate(allowCreate);
        stConfig.setTransactional(false);
        stConfig.setModel(model);
        stConfig.setReplicated(false);
        return new EntityStore(env, METADATA_STORE_NAME, stConfig);
    }

    public RepNodeId getRepNodeId() {
        return repNodeId;
    }

    public Topology getTopology() {
        return topoManager.getTopology();
    }

    /**
     * Redirect to the master balance manager
     *
     * @see RepNodeAdmin#initiateMasterTransfer
     */
    public boolean initiateMasterTransfer(RepNodeId replicaId,
                                          int timeout,
                                          TimeUnit timeUnit) {
        return masterBalanceManager.
            initiateMasterTransfer(replicaId, timeout, timeUnit);
    }

    /**
     * Returns the migration manager.
     * @return the migration manager
     */
    public MigrationManager getMigrationManager() {
        return migrationManager;
    }

    /* -- Admin partition migration methods -- */

    /**
     * Starts a partition migration. This must be called
     * after the node is initialized. This method will start a target thread
     * which requests the partition form the source node.
     *
     * @param partitionId ID of the partition to migrate
     * @param sourceRGId ID the current rep group of the partition
     * @return the migration state
     */
    public PartitionMigrationState migratePartition(PartitionId partitionId,
                                                    RepGroupId sourceRGId) {

        /* Source group is this group? Bad. */
        if (repNodeId.getGroupId() == sourceRGId.getGroupId()) {
            return PartitionMigrationState.ERROR.setCause(
                  new IllegalArgumentException("Invalid source " + sourceRGId));
        }

        /* If its already here, then we are done. */
        if (partitionManager.isPresent(partitionId)) {
            return PartitionMigrationState.SUCCEEDED;
        }
        return migrationManager.migratePartition(partitionId, sourceRGId);
    }

    /**
     * Returns the state of a partition migration. A return of SUCCEEDED
     * indicates that the specified partition is complete and durable.
     *
     * If the return value is PartitionMigrationState.ERROR,
     * canceled(PartitionId, RepGroupId) must be invoked on the migration
     * source repNode.
     *
     * @param partitionId a partition ID
     * @return the migration state
     */
    public PartitionMigrationState getMigrationState(PartitionId partitionId) {

        /* If its already here, then we are done. */
        if (partitionManager.isPresent(partitionId)) {
            return PartitionMigrationState.SUCCEEDED;
        }
        final PartitionMigrationState state =
                            migrationManager.getMigrationState(partitionId);

        /*
         * If ERROR, re-check if the partition is here, as a migration
         * could have completed just after the check above.
         */
        if (state.equals(PartitionMigrationState.ERROR)) {
            if (partitionManager.isPresent(partitionId)) {
                return PartitionMigrationState.SUCCEEDED;
            }
        }
        return state;
    }

    /**
     * Requests that a partition migration for the specified partition
     * be canceled. Returns the migration state if there was a migration in
     * progress, otherwise null is returned.
     * If the migration can be canceled it will be stopped and
     * PartitionMigrationState.ERROR is returned. If the migration has passed
     * the "point of no return" in the Transfer of Control protocol or is
     * already completed PartitionMigrationState.SUCCEEDED is returned.
     * All other states indicate that the cancel should be retried.
     *
     * @param partitionId a partition ID
     * @return a migration state or null
     */
    public PartitionMigrationState canCancel(PartitionId partitionId) {
        return migrationManager.canCancel(partitionId);
    }

    /**
     * Cleans up a source migration stream after a cancel or error.
     *
     * @param partitionId a partition ID
     * @param targetRGId the target rep group ID
     * @return true if the cleanup was successful
     */
    public boolean canceled(PartitionId partitionId, RepGroupId targetRGId) {
        return migrationManager.canceled(partitionId, targetRGId);
    }

    /**
     * Gets the status of partition migrations on this node.
     *
     * @return the partition migration status
     */
    public PartitionMigrationStatus[] getMigrationStatus() {
        return migrationManager.getStatus();
    }

    /**
     * Wait for the RN to become consistent with this point in time. Serves
     * as an indication that the RN is both alive and synced up.
     */
    public boolean awaitConsistency(long targetTime,
                                    int timeout,
                                    TimeUnit timeoutUnit) {

        final long timeoutMs = TimeUnit.MILLISECONDS.convert(timeout,
                                                             timeoutUnit);
        final ReplicatedEnvironment env = envManager.getEnv(timeoutMs);

        if (env == null) {

            /*
             * Environment handle unavailable, return false since we can't
             * determine its consistency.
             */
            return false;
        }

        final TransactionConfig txnConfig = new TransactionConfig();

        ReplicaConsistencyPolicy reachedTime =
            new TimeConsistencyPolicy(System.currentTimeMillis() - targetTime,
                                      TimeUnit.MILLISECONDS,
                                      timeout, timeoutUnit);
        txnConfig.setConsistencyPolicy(reachedTime);
        try {
            /* The transaction is just a mechanism to check consistency */
            Transaction txn = env.beginTransaction(null, txnConfig);
            TxnUtil.abort(txn);
        } catch (ReplicaConsistencyException notReady) {
            logger.info(notReady.toString());
            return false;
        }

        return true;
    }

    public PartitionMigrationStatus
                                getMigrationStatus(PartitionId partitionId) {
        return migrationManager.getStatus(partitionId);
    }

    /* -- Unit tests -- */

    /**
     * For unit tests only.
     * Returns the local topology for this node. This should only be used to
     * direct client requests and should NEVER be sent to another node.
     */
    Topology getLocalTopology() {
        return topoManager.getLocalTopology();
    }

    /**
     * Handles version changes when the version found in the environment does
     * not match the KVVersion.CURRENT. If localVersion is not null it is the
     * previous version of repNode. Note that localVersion may be a patch
     * version less than the CURRENT version (downgrade). If localVerion is null
     * this node is being initialized.
     *
     * @param localVersion the version currently in the environment or null
     */
    void versionChange(Environment env, KVVersion localVersion) {

        EntityStore estore = null;
        try {
            estore = getMetadataStore(env, false);

            final Topology topo = Topology.fetch(estore, null);

            if (topo == null) {
                return;
            }
            if (topo.upgrade()) {
                topo.persist(estore, null);
            } else {
                TopologyManager.checkVersion(logger, topo);
            }
        } catch (StoreNotFoundException sne) {

            /*
             * Environment does not yet contain a topology store. Nothing to
             * upgrade.
             */
        } finally {
            if (estore != null) {
                estore.close();
            }
        }
    }

    /**
     * Returns the sequence number associated with the metadata at the RN.
     * If the RN does not contain the metadata null is returned.
     *
     * @param type a metadata type
     *
     * @return the sequence number associated with the metadata
    */
    public Integer getMetadataSeqNum(MetadataType type) {
        final Metadata<?> md = getMetadata(type);
        return (md == null) ? Metadata.EMPTY_SEQUENCE_NUMBER :
                              md.getSequenceNumber();
    }

    /**
     * Gets the metadata for the specified type. If the RN does not contain the
     * metadata null is returned.
     *
     * @param type a metadata type
     *
     * @return metadata or null
     */
    public Metadata<?> getMetadata(MetadataType type) {
        switch (type) {
            case TOPOLOGY:
                return getTopology();
            case TABLE:
                return tableManager.getTableMetadata();
            case SECURITY:
                return securityMDManager.getSecurityMetadata();
        }
        throw new IllegalArgumentException("Unknown metadata type: " + type);
    }

    /**
     * Gets metadata information for the specified type starting from the
     * specified sequence number.
     *
     * @param type a metadata type
     * @param seqNum a sequence number
     *
     * @return metadata info describing the changes
     */
    public MetadataInfo getMetadata(MetadataType type, int seqNum) {
        switch (type) {
            case TOPOLOGY:
                final Topology topo = getTopology();
                return (topo == null) ? TopologyInfo.EMPTY_TOPO_INFO :
                                        getTopology().getChangeInfo(seqNum);
            case TABLE:
                final TableMetadata md = tableManager.getTableMetadata();
                return (md == null) ? TableChangeList.EMPTY_TABLE_INFO :
                                      md.getChangeInfo(seqNum);
            case SECURITY:
                final SecurityMetadata securityMD =
                    securityMDManager.getSecurityMetadata();
                return (securityMD == null) ?
                       SecurityMetadataInfo.EMPTY_SECURITYMD_INFO :
                       securityMD.getChangeInfo(seqNum);
        }
        throw new IllegalArgumentException("Unknown metadata type: " + type);
    }

    /**
     * Gets metadata information for the specified type and key starting from
     * the specified sequence number. If the metadata is not present or cannot
     * be accessed, null is returned.
     *
     * @param type a metadata type
     * @param key a metadata key
     * @param seqNum a sequence number
     *
     * @return metadata info describing the changes or null
     *
     * @throws UnsupportedOperationException if the operation is not supported
     * by the specified metadata type
     */
    public MetadataInfo getMetadata(MetadataType type,
                                    MetadataKey key,
                                    int seqNum) {
        switch (type) {
            case TOPOLOGY:
                /* Topology doesn't rely on MetadataKey for lookup */
                throw new
                    UnsupportedOperationException("Operation not supported " +
                                                  "for metadata type: " + type);
            case SECURITY:
                /* Security doesn't rely on MetadataKey for lookup */
                throw new
                    UnsupportedOperationException("Operation not supported " +
                                                  "for metadata type: " + type);
            case TABLE:
                final TableMetadata md =  tableManager.getTableMetadata();
                if (md == null) {
                    return null;
                }
                return md.getTable((TableMetadata.TableMetadataKey) key);
        }
        throw new IllegalArgumentException("Unknown metadata type: " + type);
    }

    /**
     * Informs the RepNode about an update to the metadata.
     *
     * @param newMetadata the latest metadata
     *
     * @return true if the update is successful
     */
    public boolean updateMetadata(Metadata<?> newMetadata) {
        switch (newMetadata.getType()) {
            case TOPOLOGY:
                return topoManager.update((Topology)newMetadata);
            case TABLE:
                return tableManager.updateMetadata(newMetadata);
            case SECURITY:
                return securityMDManager.update((SecurityMetadata)newMetadata);
        }
        throw new IllegalArgumentException("Unknown metadata: " + newMetadata);
    }

    /**
     * Informs the RepNode about an update to the metadata.
     *
     * @param metadataInfo describes the changes to be applied
     *
     * @return the post-update metadata sequence number at the node
     */
    public int updateMetadata(MetadataInfo metadataInfo) {
        switch (metadataInfo.getType()) {
            case TOPOLOGY:
                return updateTopology((TopologyInfo)metadataInfo);
            case TABLE:
                return tableManager.updateMetadata(metadataInfo);
            case SECURITY:
                return securityMDManager.update(
                    (SecurityMetadataInfo)metadataInfo);
        }
        throw new IllegalArgumentException("Unknown metadata: " + metadataInfo);
    }

    public boolean addIndexComplete(String indexId, String tableName) {
        return tableManager.addIndexComplete(indexId, tableName);
    }

    public boolean removeTableDataComplete(String tableName) {
        return tableManager.removeTableDataComplete(tableName);
    }

    /**
     * Gets the secondary database for the specified index. Throws a
     * RNUnavailableException if the database is not found.
     *
     * @param indexName the index name
     * @param tableName the table name
     *
     * @return a secondary database
     *
     * @throws RNUnavailableException if the secondary DB is not found
     */
    public SecondaryDatabase getIndexDB(String indexName, String tableName) {
        final SecondaryDatabase db = tableManager.getIndexDB(indexName,
                                                             tableName);
        if (db != null) {
            return db;
        }

        /*
         * The secondary db was not found. This could be because the table
         * metadata has not been initialized, the metadata is out of date, or
         * the secondary DB has not yet been opened. Figure out what it is, and
         * throw RNUnavailableException.
         */
        String message;
        final TableMetadata md =  tableManager.getTableMetadata();

        if (md == null) {
            message = "Table metadata not yet initialized";
        } else if (getIndex(indexName, tableName) == null) {
            message = "Index " + indexName + " not present on RepNode, table " +
                      "metadata seq#: " + md.getSequenceNumber();
        } else {
            message = "Secondary database for " + indexName + " not yet " +
                      "initialized";
        }
        throw new RNUnavailableException(message);
    }

    public LoginManager getLoginManager() {
        return repNodeService == null ? null : repNodeService.getLoginManager();
    }

   /**
    * Gets the table instance for the specified ID. If no table is defined or
    * the RN is not initialized, null is returned.
    *
    * @param tableId a table ID
    * @return the table instance
    */
    public Table getTable(long tableId) {
        return tableManager.getTable(tableId);
    }

    /**
     * Return the named index. If the index or table does not exist, or the
     * table metadata cannot be accessed null is returned.
     *
     * @param indexName the index name
     * @param tableName the table name
     * @return the named index or null
     */
    public Index getIndex(String indexName, String tableName) {
        final TableMetadata md =  tableManager.getTableMetadata();
        if (md != null) {
            TableImpl table = md.getTable(tableName);
            if (table != null) {
                return table.getIndex(indexName);
            }
        }
        return null;
    }
}
