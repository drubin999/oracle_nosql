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

import java.rmi.RemoteException;
import java.util.List;
import java.util.Set;

import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.sna.masterBalance.MasterBalancingInterface;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.VersionedRemote;

/**
 * The interface to the Storage Node Agent.  The SNA is run as a process on
 * each of the Storage Nodes.  It provides process control for each Storage
 * Node as well as a mechanism for passing parameters to the processes it
 * controls.
 *
 * Before a StorageNodeAgent can be used as part of a store, it must be
 * registered by calling the {@link #register} method.  Until an SNA
 * is registered, all other methods will throw an exception.
 *
 * Exceptions thrown from this interface are nearly always indicative of a
 * serious problem such as a corrupt configuration or network problem.  In
 * general the "worker" methods try hard to do what they've been asked.  Most
 * state-changing operations are idempotent in that they can be retried and
 * will ignore the fact that it may be a retry.  This handles the situation
 * where the caller may have exited before knowing the resulting state of the
 * call.
 *
 * A number of the methods imply an expected state when called.  For example,
 * calling createRepNode() implies that the caller expects that the RepNode in
 * question has not already been created.  Rather than throwing an exception
 * the method should log the situation and return a value to the caller
 * indicating that things were not as expected.  The sense of the return values
 * used is true for "the implied state was correct" and false for "the implied
 * state was not correct."  The return values do not indicate success or
 * failure of the operation.  If the operation does not throw an exception it
 * succeeded.
 */
public interface StorageNodeAgentInterface extends
    VersionedRemote, MasterBalancingInterface  {

    /**
     * Returns the service status associated with the SNA
     *
     * @since 3.0
     */
    public StorageNodeStatus ping(AuthContext authCtx,
                                  short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public StorageNodeStatus ping(short serialVersion)
        throws RemoteException;

    /**
     * Registers this Storage Node to be part of a store. This method should be
     * called at most once during the lifetime of a Storage Node. All other
     * methods will fail until this method has been called. Uses the bootstrap
     * hostname and port.
     *
     * After this method is called the handle used to access it will no longer
     * be valid and will need to be re-acquired. The name of the service will
     * also have changed to its permanent name.
     *
     * @param globalParams kvstore wide settings, including the store name.
     * @param storageNodeParams parameters for the new storage node required
     * for it to set up normal service, including registry port and storage
     * node id.
     * @param hostingAdmin set to true if this Storage Node is physically
     * hosting the Admin for the store.
     *
     * @return List<ParameterMap> which has two parameter maps, one for basic
     * storage node information and one that is the map of mount points.  This
     * information is destined for the caller's copy of StorageNodeParams.
     *
     * @since 3.0
     */
    public List<ParameterMap> register(ParameterMap globalParams,
                                       ParameterMap storageNodeParams,
                                       boolean hostingAdmin,
                                       AuthContext authCtx,
                                       short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public List<ParameterMap> register(ParameterMap globalParams,
                                       ParameterMap storageNodeParams,
                                       boolean hostingAdmin,
                                       short serialVersion)
        throws RemoteException;

    /**
     * Stops a running Storage Node Agent, optionally stopping all running
     * services it is managing.
     *
     * @param stopServices if true stop running services
     *
     * @since 3.0
     */
    public void shutdown(boolean stopServices,
                         boolean force,
                         AuthContext authCtx,
                         short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public void shutdown(boolean stopServices,
                         boolean force,
                         short serialVersion)
        throws RemoteException;

    /**
     * Creates and starts a Admin instance in the store.  This will cause a new
     * process to be created containing the Admin.  This should be called for
     * each instance up the Admin, up to the desired Admin replication factor.
     * The Storage Node Agent will continue to start this Admin instance upon
     * future restarts unless it is explicitly stopped.
     *
     * @param adminParams the configuration parameters of this Admin instance
     *
     * @return true if the Admin is successfully created.
     *
     * @throws RuntimeException if the operation failed.
     *
     * @since 3.0
     */
    public boolean createAdmin(ParameterMap adminParams,
                               AuthContext authCtx,
                               short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public boolean createAdmin(ParameterMap adminParams, short serialVersion)
        throws RemoteException;

    /**
     * Starts a Admin instance that has already been defined on this node.  The
     * Admin will be started automatically by this StorageNodeAgent if the
     * Storage Node is restarted.
     *
     * @return true if the operation succeeds.
     *
     * @throws RuntimeException if the operation fails or the service does not
     * exist.
     *
     * @since 3.0
     */
    public boolean startAdmin(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public boolean startAdmin(short serialVersion)
        throws RemoteException;

    /**
     * Stops a Admin instance that has already been defined on this node.  The
     * Admin will no longer be started automatically if the Storage Node is
     * restarted.
     *
     * @param force force a shutdown
     *
     * @return true if the Admin was running, false if it was not.
     *
     * @throws RuntimeException if the operation fails or the service does not
     * exist
     *
     * @since 3.0
     */
    public boolean stopAdmin(boolean force,
                             AuthContext authCtx,
                             short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public boolean stopAdmin(boolean force, short serialVersion)
        throws RemoteException;

    /**
     * Permanently removes an Admin instance running on this Storage Node.
     * Since the StorageNodeAgent cannot know if this is the only Admin
     * instance or not, care should be taken by the Admin itself to prevent
     * removal of the last Admin instance.  This method will stop the admin if
     * it is running.
     *
     * @param adminId the unique identifier of the Admin
     *
     * @param deleteData true if the data stored on disk for this Admin
     *                   should be deleted
     *
     * @return true if the Admin existed, false if it did not.
     *
     * @throws RuntimeException if the operation failed.
     *
     * @since 3.0
     */
    public boolean destroyAdmin(AdminId adminId,
                                boolean deleteData,
                                AuthContext authCtx,
                                short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public boolean destroyAdmin(AdminId adminId, boolean deleteData,
                                short serialVersion)
        throws RemoteException;

    /**
     * Query whether a give RepNode has been defined on this Storage Node, as
     * indicated by its configuration existing in the store's configuration
     * file.  This is not an indication of its runtime status.
     *
     * @param repNodeId the unique identifier of the RepNode
     *
     * @return true if the specified RepNode exists in the configuration file
     *
     * @since 3.0
     */
    public boolean repNodeExists(RepNodeId repNodeId,
                                 AuthContext authCtx,
                                 short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public boolean repNodeExists(RepNodeId repNodeId, short serialVersion)
        throws RemoteException;

    /**
     * Creates and starts a {@link oracle.kv.impl.rep.RepNode} instance
     * on this Storage Node.  This will cause a new process to be started to
     * run the RepNode.  The StorageNodeAgent will continue to start this
     * RepNode if the Storage Node is restarted unless the RepNode is stopped
     * explicitly.
     *
     * Once the configuration file is written so that a restart of the SNA will
     * also start the RepNode this call will unconditionally succeed, even if
     * it cannot actually start or contact the RepNode itself.  This is so that
     * the state of the SNA is consistent with the topology in the admin
     * database.
     *
     * @param repNodeParams the configuration of the RepNode to create
     *
     * @param metadataSet the metadata set for the RepNode
     *
     * @return true if the RepNode is successfully created.
     *
     * @throws RuntimeException if the operation failed.
     *
     * @since 3.0
     */
    public boolean createRepNode(ParameterMap repNodeParams,
                                 Set<Metadata<? extends MetadataInfo>> metadataSet,
                                 AuthContext authCtx,
                                 short serialVersion)
        throws RemoteException;

    /**
     * Added and then deprecated in R3 purely to support the creation of a 
     * SecureProxy.
     */
    @Deprecated
    public boolean createRepNode(ParameterMap repNodeParams,
                                 Topology topology,
                                 AuthContext authCtx,
                                 short serialVersion)
        throws RemoteException;
    
    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public boolean createRepNode(ParameterMap repNodeParams,
                                 Topology topology,
                                 short serialVersion)
        throws RemoteException;

    /**
     * Starts a {@link oracle.kv.impl.rep.RepNode} that has already been
     * defined on this Storage Node.  The RepNode will be started automatically
     * if the Storage Node is restarted or the RepNode exits unexpectedly.
     *
     * @param repNodeId the unique identifier of the RepNode to start
     *
     * @return true if the operation succeeds.
     *
     * @throws RuntimeException if the operation fails or the service does not
     * exist.
     *
     * @since 3.0
     */
    public boolean startRepNode(RepNodeId repNodeId,
                                AuthContext authCtx,
                                short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public boolean startRepNode(RepNodeId repNodeId, short serialVersion)
        throws RemoteException;

    /**
     * Stops a {@link oracle.kv.impl.rep.RepNode} that has already been
     * defined on this Storage Node.  The RepNode will not be started if the
     * Storage node is restarted until {@link #startRepNode} is called.
     *
     * @param repNodeId the unique identifier of the RepNode to stop
     *
     * @param force force a shutdown
     *
     * @return true if the RepNode was running, false if it was not.
     *
     * @throws RuntimeException if the operation failed.
     *
     * @since 3.0
     */
    public boolean stopRepNode(RepNodeId repNodeId,
                               boolean force,
                               AuthContext authCtx,
                               short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public boolean stopRepNode(RepNodeId repNodeId,
                               boolean force,
                               short serialVersion)
        throws RemoteException;

    /**
     * Permanently removes the {@link oracle.kv.impl.rep.RepNode} with
     * the specified RepNodeId.
     *
     * @param repNodeId the unique identifier of the RepNode to destroy
     *
     * @param deleteData true if the data stored on disk for this RepNode
     *                   should be deleted
     *
     * @return true if the RepNode is successfully destroyed.  This will be the
     * case if it does not exist in the first place.
     *
     * @throws RuntimeException if the operation failed.
     *
     * @since 3.0
     */
    public boolean destroyRepNode(RepNodeId repNodeId,
                                  boolean deleteData,
                                  AuthContext authCtx,
                                  short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public boolean destroyRepNode(RepNodeId repNodeId,
                                  boolean deleteData,
                                  short serialVersion)
        throws RemoteException;

     /**
     * Modifies the parameters of a (@link oracle.kv.impl.rep.RepNode}
     * RepNode managed by this StorageNode.  The new parameters will be written
     * out to the storage node's configuration file.  If the service needs
     * notification of the new parameters that is done by the admin/planner.
     *
     * @param repNodeParams the new parameters to configure the rep node. This
     * is a full set of replacement parameters, not partial.
     *
     * @throws RuntimeException if the RepNode is not configured or the
     * operation failed.
     *
     * @since 3.0
     */
    public void newRepNodeParameters(ParameterMap repNodeParams,
                                     AuthContext authCtx,
                                     short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public void newRepNodeParameters(ParameterMap repNodeParams,
                                     short serialVersion)
        throws RemoteException;

    /**
     * Modifies the parameters of an (@link oracle.kv.impl.admin.Admin}
     * Admin managed by this StorageNode.  The new parameters will be written
     * out to the storage node's configuration file.  Any required notification
     * is done by the admin/planner.
     *
     * @param adminParams the new parameters to configure the admin.  This is a
     * full set of replacement parameters, not partial.
     *
     * @throws RuntimeException if the admin is not configured or the
     * operation failed.
     *
     * @since 3.0
     */
     public void newAdminParameters(ParameterMap adminParams,
                                    AuthContext authCtx,
                                    short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
     public void newAdminParameters(ParameterMap adminParams,
                                    short serialVersion)
        throws RemoteException;

    /**
     * Modifies the parameters of the current Storage Node.  The new
     * parameters will be written out to the storage node's configuration file
     * and if also present, the bootstrap config file.
     *
     * @param params the new parameters to configure the storage
     * node.  This can be a partial set but must include both bootstrap and
     * StorageNodeParams to change.  It may also be a map of mount points to
     * be applied to the storage node and the bootstrap parameters.
     *
     * @throws RuntimeException if the StorageNode is not configured or the
     * operation failed.
     *
     * @since 3.0
     */
    public void newStorageNodeParameters(ParameterMap params,
                                         AuthContext authCtx,
                                         short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public void newStorageNodeParameters(ParameterMap params,
                                         short serialVersion)
        throws RemoteException;

    /**
     * Modifies the global parameters of the current Storage Node. The new
     * parameters will be written out to the storage node's configuration file.
     * Any required notification is done by the admin/planner.
     *
     * @param params the new store-wide global parameters
     *
     * @throws RuntimeException if the StorageNode is not configured or the
     * operation failed.
     *
     * @since 3.0
     */
    public void newGlobalParameters(ParameterMap params,
                                    AuthContext authCtx,
                                    short serialVersion)
        throws RemoteException;

    /**
     * Get SNA parameters.
     *
     * @since 3.0
     */
    public LoadParameters getParams(AuthContext authCtx,
                                    short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public LoadParameters getParams(short serialVersion)
        throws RemoteException;

    /**
     * Returns information about service start problems if the service is
     * started as a process.  Problems may be JVM initialization or
     * synchronous failures from the service itself during startup.
     *
     * @param rid is the ResourceId of the service
     *
     * @return the buffer of startup information if there was a problem.  Null
     * is returned if there was no startup problem.
     *
     * @throws RuntimeException if the service does not exist.
     *
     * @since 3.0
     */
    public StringBuilder getStartupBuffer(ResourceId rid,
                                          AuthContext authCtx,
                                          short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public StringBuilder getStartupBuffer(ResourceId rid,
                                          short serialVersion)
        throws RemoteException;


    /**
     * Snapshot methods.
     */

    /**
     * Create the named snapshot.
     *
     * @since 3.0
     */
    public void createSnapshot(RepNodeId rnid,
                               String name,
                               AuthContext authCtx,
                               short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public void createSnapshot(RepNodeId rnid, String name,
                               short serialVersion)
        throws RemoteException;

    /*
     * Create the named snapshot.
     * @since 3.0
     */
    public void createSnapshot(AdminId aid, String name,
                               AuthContext authCtx,
                               short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public void createSnapshot(AdminId aid, String name,
                               short serialVersion)
        throws RemoteException;

    /**
     * Remove the named snapshot from all managed services on this storage node
     *
     * @since 3.0
     */
    public void removeSnapshot(RepNodeId rnid, String name,
                               AuthContext authCtx,
                               short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public void removeSnapshot(RepNodeId rnid, String name,
                               short serialVersion)
        throws RemoteException;

    /**
     * @since 3.0
     */
    public void removeSnapshot(AdminId aid, String name,
                               AuthContext authCtx,
                               short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public void removeSnapshot(AdminId aid, String name,
                               short serialVersion)
        throws RemoteException;

    /**
     * Remove all snapshots all managed services on this storage node
     *
     * @since 3.0
     */
    public void removeAllSnapshots(RepNodeId rnid,
                                   AuthContext authCtx,
                                   short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public void removeAllSnapshots(RepNodeId rnid, short serialVersion)
        throws RemoteException;

    /**
     * @since 3.0
     */
    public void removeAllSnapshots(AdminId aid,
                                   AuthContext authCtx,
                                   short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public void removeAllSnapshots(AdminId aid, short serialVersion)
        throws RemoteException;

    /**
     * List the snapshots present on this Storage Node.  The SN will choose the
     * first managed service it can find and return the list of file names.
     *
     * @return an arry of file names for the snapshots.  If no snapshots are
     * present this is a zero-length array.
     *
     * @since 3.0
     */
    public String [] listSnapshots(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public String [] listSnapshots(short serialVersion)
        throws RemoteException;

}

