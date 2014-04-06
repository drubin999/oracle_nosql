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
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.metadata.MetadataKey;
import oracle.kv.impl.mgmt.RepNodeStatusReceiver;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.ContextProxy;
import oracle.kv.impl.rep.RepNodeStatus;
import oracle.kv.impl.rep.admin.RepNodeAdmin.PartitionMigrationState;
import oracle.kv.impl.rep.migration.PartitionMigrationStatus;
import oracle.kv.impl.security.login.LoginHandle;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RemoteAPI;

/**
 * The administrative interface to a RepNode process.
 */
public class RepNodeAdminAPI extends RemoteAPI {

    /* Null value that will be filled in by proxyRemote */
    private final static AuthContext NULL_CTX = null;

    private final RepNodeAdmin proxyRemote;

    private RepNodeAdminAPI(RepNodeAdmin remote, LoginHandle loginHdl)
        throws RemoteException {

        super(remote);
        this.proxyRemote = ContextProxy.create(remote, loginHdl,
                                               getSerialVersion());
    }

    public static RepNodeAdminAPI wrap(RepNodeAdmin remote,
                                       LoginHandle loginHdl)
        throws RemoteException {

        return new RepNodeAdminAPI(remote, loginHdl);
    }

    /**
     * Provides the configuration details of this RepNode.  This method may
     * be called once and only once.  It is expected that it will be called
     * by a {@link oracle.kv.impl.sna.StorageNodeAgentInterface}
     * after the SNA has started the RepNode process for the first time.
     * No methods other than shutdown may be called on the RepNode until
     * it has been configured.
     */
    public void configure(Set<Metadata<? extends MetadataInfo>> metadataSet)
        throws RemoteException {

        proxyRemote.configure(metadataSet, NULL_CTX, getSerialVersion());
    }

    /**
     * Indicates that new parameters are available in the storage node
     * configuration file and that these should be reread.
     */
    public void newParameters()
        throws RemoteException {

        proxyRemote.newParameters(NULL_CTX, getSerialVersion());
    }

    /**
     * Indicates that new global parameters are available in the storage node
     * configuration file and that these should be reread.
     */
    public void newGlobalParameters()
        throws RemoteException {

        proxyRemote.newGlobalParameters(NULL_CTX, getSerialVersion());
    }

    /**
     * @see RepNodeAdmin#updateTopology(Topology, short)
     */
    @Deprecated
    public void updateTopology(Topology newTopology)
        throws RemoteException {

        proxyRemote.updateMetadata(newTopology, NULL_CTX, getSerialVersion());
    }

   /**
    * @see RepNodeAdmin#updateTopology(TopologyInfo, short)
    */
    @Deprecated
    public int updateTopology(TopologyInfo topoInfo)
        throws RemoteException {

        return proxyRemote.updateMetadata(topoInfo, NULL_CTX,
                                          getSerialVersion());
    }

    /**
     * Returns this RN's view of the Topology. In a distributed system like
     * KVS, it may be temporarily different from the Topology at other nodes,
     * but will eventually become eventually consistent.
     */
    public Topology getTopology()
        throws RemoteException {

        return proxyRemote.getTopology(NULL_CTX, getSerialVersion());
    }

    /**
     * Returns the sequence number associated with the Topology at the RN.
     */
    public int getTopoSeqNum()
        throws RemoteException {

        return proxyRemote.getTopoSeqNum(NULL_CTX, getSerialVersion());
    }

    /**
     * Return this RN's view of its current parameters. Used for configuration
     * verification.
     */
    public LoadParameters getParams()
        throws RemoteException {
        return proxyRemote.getParams(NULL_CTX, getSerialVersion());
    }

    /**
     * Shuts down this RepNode process cleanly.
     *
     * @param force force the shutdown
     */
    public void shutdown(boolean force)
        throws RemoteException {

        proxyRemote.shutdown(force, NULL_CTX, getSerialVersion());
    }

    /**
     * Returns the <code>RepNodeStatus</code> associated with the rep node.
     *
     * @return the service status
     */
    public RepNodeStatus ping()
        throws RemoteException {

        return proxyRemote.ping(NULL_CTX, getSerialVersion());
    }

    /**
     * Returns administrative and configuration information from the
     * repNode. Meant for diagnostic and debugging support.
     */
    public RepNodeInfo getInfo()
        throws RemoteException {

        return proxyRemote.getInfo(NULL_CTX, getSerialVersion());
    }

    public String [] startBackup()
        throws RemoteException {

        return proxyRemote.startBackup(NULL_CTX, getSerialVersion());
    }

    public long stopBackup()
        throws RemoteException {

        return proxyRemote.stopBackup(NULL_CTX, getSerialVersion());
    }

    /**
     * @param groupName
     * @param targetNodeName
     * @param targetHelperHosts
     * @param newNodeHostPort
     * @return true if this node's address can be updated in the JE
     * group database, false if there is no current master, and we need to
     * retry.
     * @throws RemoteException
     */
    public boolean updateMemberHAAddress(String groupName,
                                         String targetNodeName,
                                         String targetHelperHosts,
                                         String newNodeHostPort)
        throws RemoteException{

        /*
         * The return type of this method has changed since R1.  We do
         * not expect cross-version interoperation using this method,
         * so we simply prohibit it.
         */
        if (getSerialVersion() < 2) {
            throw new UnsupportedOperationException
                ("There was an attempt update the HA address on a RepNode " +
                 "that is running an earlier, incompatible release.  Please " +
                 "upgrade all components of the store before attempting " +
                 "to change the store's configuration.");
        }

        return proxyRemote.updateMemberHAAddress(groupName,
                                                 targetNodeName,
                                                 targetHelperHosts,
                                                 newNodeHostPort,
                                                 NULL_CTX,
                                                 getSerialVersion());
    }

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
     * @return the migration state
     * @throws RemoteException
     */
    public PartitionMigrationState migratePartition(PartitionId partitionId,
                                                    RepGroupId sourceRGId)
        throws RemoteException {

        return proxyRemote.migratePartition(partitionId,
                                            sourceRGId,
                                            NULL_CTX, getSerialVersion());
    }

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
     * @return the migration state
     * @throws RemoteException
     */
    public PartitionMigrationState getMigrationState(PartitionId partitionId)
        throws RemoteException {

        return proxyRemote.getMigrationState(partitionId, NULL_CTX,
                                             getSerialVersion());
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
     * As with getMigrationState(PartitionId) if the return value is
     * PartitionMigrationState.ERROR, canceled(PartitionId, RepGroupId) must be
     * invoked on the migration source repNode.
     *
     * @param partitionId a partition ID
     * @return a migration state or null
     * @throws RemoteException
     */
    public PartitionMigrationState canCancel(PartitionId partitionId)
        throws RemoteException {

        return proxyRemote.canCancel(partitionId, NULL_CTX, getSerialVersion());
    }

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
     * @return true if the cleanup was successful
     * @throws RemoteException
     */
    public boolean canceled(PartitionId partitionId, RepGroupId targetRGId)
        throws RemoteException {

        return proxyRemote.canceled(partitionId, targetRGId, NULL_CTX,
                                    getSerialVersion());
    }

    /**
     * Gets the status of partition migrations for the specified partition. If
     * no status is available, null is returned.
     *
     * @param partitionId a partition ID
     * @return the partition migration status or null
     */
    public PartitionMigrationStatus getMigrationStatus(PartitionId partitionId)
        throws RemoteException {

        return proxyRemote.getMigrationStatus(partitionId, NULL_CTX,
                                              getSerialVersion());
    }

    /**
     * @see RepNodeAdmin#initiateMasterTransfer
     */
    public boolean initiateMasterTransfer(RepNodeId replicaId,
                                          int timeout,
                                          TimeUnit timeUnit)
        throws RemoteException {

        return proxyRemote.initiateMasterTransfer(replicaId, timeout, timeUnit,
                                                  NULL_CTX, getSerialVersion());
    }

    /**
     * Install a receiver for RepNode status updates, for delivering metrics
     * and service change information to the standardized monitoring/management
     * agent.
     */
    public void installStatusReceiver(RepNodeStatusReceiver receiver)
        throws RemoteException {

        proxyRemote.installStatusReceiver(receiver, NULL_CTX,
                                          getSerialVersion());
    }

    public boolean awaitConsistency(long stopTime, int timeout, TimeUnit unit)
        throws RemoteException {
        return proxyRemote.awaitConsistency(stopTime, timeout, unit,
                                       NULL_CTX, getSerialVersion());
    }

    /**
     * @see RepNodeAdmin#getMetadataSeqNum(MetadataType, AuthContext, short)
     */
    public int getMetadataSeqNum(Metadata.MetadataType type)
        throws RemoteException {
        return proxyRemote.getMetadataSeqNum(type, NULL_CTX,
                                             getSerialVersion());
    }

    /**
     * @see RepNodeAdmin#getMetadata(MetadataType, AuthContext, short)
     */
    public Metadata<?> getMetadata(Metadata.MetadataType type)
        throws RemoteException {
        return proxyRemote.getMetadata(type, NULL_CTX, getSerialVersion());
    }

    /**
     * @see RepNodeAdmin#getMetadata(MetadataType, int, AuthContext, short)
     */
    public MetadataInfo getMetadata(Metadata.MetadataType type, int seqNum)
        throws RemoteException {
        return proxyRemote.getMetadata(type, seqNum, NULL_CTX,
                                       getSerialVersion());
    }

    /**
     * @see RepNodeAdmin#getMetadata(MetadataType, MetadataKey, int, AuthContext, short)
     */
    public MetadataInfo getMetadata(Metadata.MetadataType type,
                                    MetadataKey key,
                                    int seqNum)
        throws RemoteException {

        return proxyRemote.getMetadata(type, key, seqNum, NULL_CTX,
                                  getSerialVersion());
    }

    /**
     * @see RepNodeAdmin#updateMetadata(MetadataInfo, AuthContext, short)
     */
    public int updateMetadata(MetadataInfo metadataInfo)
        throws RemoteException {
        return proxyRemote.updateMetadata(metadataInfo, NULL_CTX,
                                     getSerialVersion());
    }

    /**
     * @see RepNodeAdmin#updateMetadata(Metadata, AuthContext, short)
     */
    public void updateMetadata(Metadata<?> newMetadata)
        throws RemoteException {
        proxyRemote.updateMetadata(newMetadata, NULL_CTX, getSerialVersion());
    }

    public boolean addIndexComplete(String indexId,
                                    String tableName)
        throws RemoteException {
        return proxyRemote.addIndexComplete(indexId, tableName,
                                            NULL_CTX, getSerialVersion());
    }

    public boolean removeTableDataComplete(String tableName)
        throws RemoteException {
        return proxyRemote.removeTableDataComplete(tableName,
                                            NULL_CTX, getSerialVersion());
    }
}
