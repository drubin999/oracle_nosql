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

import java.rmi.RemoteException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import oracle.kv.impl.api.TopologyInfo;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.metadata.MetadataKey;
import oracle.kv.impl.mgmt.RepNodeStatusReceiver;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.rep.RepNodeStatus;
import oracle.kv.impl.rep.migration.PartitionMigrationStatus;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.VersionedRemote;

/**
 * The administrative interface to a RepNode process.
 */
public interface RepNodeAdmin extends VersionedRemote {

    /**
     * The possible values for state of a partition migration. The state is
     * returned from the migratePartition and getMigrationState methods.
     *
     * The legal transitions are:
     *
     *                      ------> SUCCEEDED
     * PENDING --> RUNNING /
     *     ^          |    \------> ERROR
     *     |__________|
     */
    public enum PartitionMigrationState {

        /** The partition migration is waiting to start. */
        PENDING,

        /** The partition migration is in-progress. */
        RUNNING,

        /**
         * The partition migration is complete and the partition is durable
         * at its new location
         */
        SUCCEEDED,

        /**
         * An error has occurred or the partition migration has been canceled.
         */
        ERROR,

        /**
         * Indicates that the state is not known.
         */
        UNKNOWN;

        private Exception cause = null;

        /**
         * Sets the cause of the state.
         *
         * @param cause a cause
         * @return the state
         */
        public PartitionMigrationState setCause(Exception cause) {
            this.cause = cause;
            return this;
        }

        /**
         * Gets the cause of the state if it was set, otherwise returns null.
         *
         * @return the cause of null
         */
        public Exception getCause() {
            return cause;
        }
    }

    /**
     * Provides the configuration details of this RepNode.
     * It is expected that it will be called by a
     * {@link oracle.kv.impl.sna.StorageNodeAgentInterface}
     * after the SNA has started the RepNode process for the first time.
     * No methods other than shutdown may be called on the RepNode until
     * it has been configured.
     *
     * @since 3.0
     * @param metadataSet the set of metadata to initialize the RepNode.
     */
    public void configure(Set<Metadata<? extends MetadataInfo>> metadataSet,
                          AuthContext authCtx,
                          short serialVersion)
        throws RemoteException;
    
    /**
     * Added and then deprecated in R3 purely to support the creation of a 
     * SecureProxy.
     */
    @Deprecated
    public void configure(Topology topology,
                          AuthContext authCtx,
                          short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public void configure(Topology topology, short serialVersion)
        throws RemoteException;

    /**
     * Indicates that new parameters are available in the storage node
     * configuration file and that these should be reread.
     *
     * @since 3.0
     */
    public void newParameters(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public void newParameters(short serialVersion)
        throws RemoteException;

    /**
     * Indicates that new global parameters are available in the storage node
     * configuration file and that these should be reread.
     *
     * @since 3.0
     */
    void newGlobalParameters(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Informs the RepNode about an update to the topology of the store.
     * The Topology is passed in in its entirety rather than as a list
     * of changes.
     *
     * @param newTopology the latest topology of the store
     *
     * @deprecated use {@link #updateMetadata(Metadata, AuthContext, short)}
     * @since 3.0
     */
    @Deprecated
    public void updateTopology(Topology newTopology,
                               AuthContext authCtx,
                               short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public void updateTopology(Topology newTopology, short serialVersion)
        throws RemoteException;

    /**
     * Added and then deprecated in R3 purely to support the creation of a 
     * SecureProxy. 
     * Informs the RepNode about an update to the topology of the store.
     *
     * @param topoInfo describes the changes to be applied
     *
     * @return the post-update topo sequence number at the node.
     *
     * If the returned sequence number is <
     * TopologyInfo.changes.get(0).getSequenceNumber() + 1, it means that the
     * changes could not be applied because there was a gap in the required
     * sequence of changes and the complete change list must be resent. This
     * mismatch only happens in rare circumstances where the cached state at
     * the requesting component (client or RN) is obsolete, for example, as a
     * result of some intervening network restore operation.
     *
     * It's possible for the returned sequence number to be greater than the
     * sequence number associated with the last change in the change list if
     * the RN had a more up to date Topology.
     *
     * @since 3.0
     * @deprecated
     */
    @Deprecated
    public int updateTopology(final TopologyInfo topoInfo,
                              final AuthContext authCtx,
                              final short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     *
     * @since 2.0
     */
    @Deprecated
    public int updateTopology(final TopologyInfo topoInfo,
                              final short serialVersion)
        throws RemoteException;

    /**
     * Returns this RN's view of the Topology. In a distributed system like
     * KVS, it may be temporarily different from the Topology at other nodes,
     * but will eventually become eventually consistent.
     *
     * It returns null if the RN is not in the RUNNING state.
     *
     * @since 3.0
     */
    public Topology getTopology(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public Topology getTopology(short serialVersion)
        throws RemoteException;

    /**
     * Returns the sequence number associated with the Topology at the RN.
     *
     *  It returns zero if the RN is not in the RUNNING state.
     *
     * @since 3.0
     */
    public int getTopoSeqNum(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public int getTopoSeqNum(short serialVersion)
        throws RemoteException;

    /**
     * Return this RN's view of its current parameters. Used for configuration
     * verification.
     *
     * @since 3.0
     */
    public LoadParameters getParams(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public LoadParameters getParams(short serialVersion)
        throws RemoteException;

    /**
     * Shuts down this RepNode process cleanly.
     *
     * @param force force the shutdown
     *
     * @since 3.0
     */
    public void shutdown(boolean force,
                         AuthContext authCtx,
                         short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public void shutdown(boolean force, short serialVersion)
        throws RemoteException;

    /**
     * Returns the <code>RepNodeStatus</code> associated with the rep node.
     *
     * @return the service status
     *
     * @since 3.0
     */
    public RepNodeStatus ping(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public RepNodeStatus ping(short serialVersion)
        throws RemoteException;

    /**
     * Returns administrative and configuration information from the
     * repNode. Meant for diagnostic and debugging support.
     *
     * @since 3.0
     */
    public RepNodeInfo getInfo(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public RepNodeInfo getInfo(short serialVersion)
        throws RemoteException;

    /**
     *
     * @since 3.0
     */
    public String [] startBackup(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public String [] startBackup(short serialVersion)
        throws RemoteException;

    /**
     *
     * @since 3.0
     */
    public long stopBackup(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public long stopBackup(short serialVersion)
        throws RemoteException;

    /**
     *
     * @since 3.0
     */
    public boolean updateMemberHAAddress(String groupName,
                                         String fullName,
                                         String targetHelperHosts,
                                         String newNodeHostPort,
                                         AuthContext authCtx,
                                         short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public boolean updateMemberHAAddress(String groupName,
                                         String fullName,
                                         String targetHelperHosts,
                                         String newNodeHostPort,
                                         short serialVersion)
        throws RemoteException;

    /**
     * Initiates a partition migration from the source node. This call must be
     * made on the destination (target) node. The admin must take the following
     * actions for each of the possible returns:
     *
     * SUCCEEDED - The topology is updated to reflect the partitions new
     *             location an broadcast to the store.
     * ERROR - Invoke canceled(PartitionId, RepGroupId) on the master source
     *         repNode.
     * All others - Enter a loop to monitor migration progress by calling
     *              getMigrationState(PartitionId)
     *
     * Note that PartitionMigrationState.UNKNOWN is never returned by
     * this method.
     *
     * @param partitionId the ID of the partition to migrate
     * @param sourceRGId the ID of the partitions current location
     * @param serialVersion the serial version
     * @return the migration state
     * @throws RemoteException
     *
     * @since 3.0
     */
    public PartitionMigrationState migratePartition(PartitionId partitionId,
                                                    RepGroupId sourceRGId,
                                                    AuthContext authCtx,
                                                    short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public PartitionMigrationState migratePartition(PartitionId partitionId,
                                                    RepGroupId sourceRGId,
                                                    short serialVersion)
        throws RemoteException;

    /**
     * Returns the state of a partition migration. The admin must take the
     * following actions for each of the possible returns:
     *
     * SUCCEEDED - The topology is updated to reflect the partitions new
     *             location an broadcast to the store.
     * ERROR - Invoke canceled(PartitionId, RepGroupId) on the master source
     *         repNode.
     * PENDING - No action, retry after delay
     * RUNNING - No action, retry after delay
     * UNKNOWN - Verify target mastership has not changed, retry after delay
     *
     * @param partitionId a partition ID
     * @param serialVersion the serial version
     * @return the migration state
     * @throws RemoteException
     *
     * @since 3.0
     */
    public PartitionMigrationState getMigrationState(PartitionId partitionId,
                                                     AuthContext authCtx,
                                                     short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public PartitionMigrationState getMigrationState(PartitionId partitionId,
                                                     short serialVersion)
        throws RemoteException;

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
     * As with getMigrationState(PartitionId) if the return value is
     * PartitionMigrationState.ERROR, canceled(PartitionId, RepGroupId) must be
     * invoked on the migration source repNode.
     *
     * @param partitionId a partition ID
     * @param serialVersion the serial version
     * @return a migration state or null
     * @throws RemoteException
     *
     * @since 3.0
     */
    public PartitionMigrationState canCancel(PartitionId partitionId,
                                             AuthContext authCtx,
                                             short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public PartitionMigrationState canCancel(PartitionId partitionId,
                                             short serialVersion)
        throws RemoteException;

    /**
     * Cleans up a source migration stream after a cancel or error. If the
     * cleanup was successful true is returned. If false is returned, the
     * call should be retried. This method must be invoked on the master
     * if the source rep group.
     *
     * This method  must be invoked on the migration source repNode whenever
     * PartitionMigrationState.ERROR is returned from a call to
     * getMigrationState(PartitionId) or cancelMigration(PartitionId).
     *
     * @param partitionId a partition ID
     * @param targetRGId the target rep group ID
     * @param serialVersion the serial version
     * @return true if the cleanup was successful
     * @throws RemoteException
     *
     * @since 3.0
     */
    public boolean canceled(PartitionId partitionId,
                            RepGroupId targetRGId,
                            AuthContext authCtx,
                            short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public boolean canceled(PartitionId partitionId,
                            RepGroupId targetRGId,
                            short serialVersion)
        throws RemoteException;

    /**
     * Gets the status of partition migrations for the specified partition. If
     * no status is available, null is returned.
     *
     * @param partitionId a partition ID
     * @return the partition migration status or null
     *
     * @since 3.0
     */
    public PartitionMigrationStatus getMigrationStatus(PartitionId partitionId,
                                                       AuthContext authCtx,
                                                       short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public PartitionMigrationStatus getMigrationStatus(PartitionId partitionId,
                                                       short serialVersion)
        throws RemoteException;

    /**
     * Requests a transfer of master status to the specific replica. This node
     * must currently be the master. The actual transfer is accomplished in the
     * background and may fail. The caller needs to be resilient to this
     * possibility.
     * <p>
     * The request may be rejected, if the node is not currently the master, or
     * it already has an outstanding request for a master transfer to a
     * different replica.
     *
     * @param replicaId the replica that is the target of the master transfer
     * @param timeout the timeout period associated with the transfer
     * @param timeUnit the time unit associated with the timeout
     *
     * @return true if the master transfer request has been accepted
     *
     * @since 3.0
     */
   public boolean initiateMasterTransfer(RepNodeId replicaId,
                                         int timeout,
                                         TimeUnit timeUnit,
                                         AuthContext authCtx,
                                         short serialVersion)
       throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     *
     * @since 2.0
     */
    @Deprecated
    public boolean initiateMasterTransfer(RepNodeId replicaId,
                                          int timeout,
                                          TimeUnit timeUnit,
                                          short serialVersion)
       throws RemoteException;


    /**
     * Install a receiver for RepNode status updates, for delivering metrics
     * and service change information to the standardized monitoring/management
     * agent.
     *
     * @since 3.0
     */
    public void installStatusReceiver(RepNodeStatusReceiver receiver,
                                      AuthContext authCtx,
                                      short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public void installStatusReceiver(RepNodeStatusReceiver receiver,
                                      short serialVersion)
        throws RemoteException;

    /**
     * Return true if this RN is consistent with the target time.
     * Wait timeoutSeconds for consistency.
     *
     * @param targetTime the time to be consistent
     * @param timeout timeout time
     * @param timeoutUnit unit of timeout time
     *
     * @return true if this RN is consistent
     *
     * @since 3.0
     */
    public boolean awaitConsistency(long targetTime,
                                    int timeout,
                                    TimeUnit timeoutUnit,
                                    AuthContext authCtx,
                                    short serialVersion)
        throws RemoteException;
    
    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public boolean awaitConsistency(long targetTime,
                                    int timeout,
                                    TimeUnit timeoutUnit,
                                    short serialVersion)
        throws RemoteException;

    /**
     * Returns the sequence number associated with the metadata at the RN.
     * If the RN does not contain the metadata or the RN is not in the RUNNING
     * state null is returned.
     *
     * @param type a metadata type
     *
     * @return the sequence number associated with the metadata
     *
     * @since 3.0
     */
    public int getMetadataSeqNum(Metadata.MetadataType type,
                                 AuthContext authCtx,
                                 short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public int getMetadataSeqNum(Metadata.MetadataType type,
                                 short serialVersion)
        throws RemoteException;

    /**
     * Gets the metadata for the specified type. If the RN does not contain the
     * metadata or the RN is not in the RUNNING state null is returned.
     *
     * @param type a metadata type
     *
     * @return metadata
     *
     * @since 3.0
     */
    public Metadata<?> getMetadata(Metadata.MetadataType type,
                                   AuthContext authCtx,
                                   short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public Metadata<?> getMetadata(Metadata.MetadataType type,
                                   short serialVersion)
        throws RemoteException;

    /**
     * Gets metadata information for the specified type starting from the
     * specified sequence number. If the RN is not in the RUNNING state null is
     * returned.
     *
     * @param type a metadata type
     * @param seqNum a sequence number
     *
     * @return metadata info describing the changes
     *
     * @since 3.0
     */
    public MetadataInfo getMetadata(Metadata.MetadataType type,
                                    int seqNum,
                                    AuthContext authCtx,
                                    short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public MetadataInfo getMetadata(Metadata.MetadataType type,
                                    int seqNum,
                                    short serialVersion)
        throws RemoteException;

    /**
     * Gets metadata information for the specified type and key starting from
     * the specified sequence number. If the RN is not in the RUNNING state null
     * is returned.
     *
     * @param type a metadata type
     * @param key a metadata key
     * @param seqNum a sequence number
     *
     * @return metadata info describing the changes
     *
     * @throws UnsupportedOperationException if the operation is not supported
     * by the specified metadata type
     *
     * @since 3.0
     */
    public MetadataInfo getMetadata(Metadata.MetadataType type,
                                    MetadataKey key,
                                    int seqNum,
                                    AuthContext authCtx,
                                    short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public MetadataInfo getMetadata(Metadata.MetadataType type,
                                    MetadataKey key,
                                    int seqNum,
                                    short serialVersion)
        throws RemoteException;

    /**
     * Informs the RepNode about an update to the metadata.
     *
     * @param newMetadata the latest metadata
     *
     * @since 3.0
     */
    public void updateMetadata(Metadata<?> newMetadata,
                               AuthContext authCtx,
                               short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public void updateMetadata(Metadata<?> newMetadata, short serialVersion)
        throws RemoteException;

    /**
     * Informs the RepNode about an update to the metadata.
     *
     * @param metadataInfo describes the changes to be applied
     *
     * @return the post-update metadata sequence number at the node
     *
     * @since 3.0
     */
    public int updateMetadata(MetadataInfo metadataInfo,
                              AuthContext authCtx,
                              short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public int updateMetadata(MetadataInfo metadataInfo, short serialVersion)
        throws RemoteException;

    /**
     * @since 3.0
     */
    public boolean addIndexComplete(String indexId,
                                    String tableName,
                                    AuthContext authCtx,
                                    short serialVersion)
        throws RemoteException;

    /**
     * @since 3.0
     */
    public boolean removeTableDataComplete(String tableName,
                                           AuthContext authCtx,
                                           short serialVersion)
        throws RemoteException;
}
