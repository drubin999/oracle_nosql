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

import java.net.URI;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.LogRecord;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.criticalevent.CriticalEvent;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.PlanStateChange;
import oracle.kv.impl.admin.topo.TopologyCandidate;
import oracle.kv.impl.api.avro.AvroDdl;
import oracle.kv.impl.api.avro.AvroSchemaMetadata;
import oracle.kv.impl.api.table.FieldMap;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.mgmt.AdminStatusReceiver;
import oracle.kv.impl.monitor.Tracker.RetrievedEvents;
import oracle.kv.impl.monitor.TrackerListener;
import oracle.kv.impl.monitor.views.PerfEvent;
import oracle.kv.impl.monitor.views.ServiceChange;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.metadata.KVStoreUser.UserDescription;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.registry.VersionedRemote;
import oracle.kv.table.FieldDef;

import com.sleepycat.je.rep.ReplicatedEnvironment;

/**
 * This is the interface used by the command line client.
 */
public interface CommandService extends VersionedRemote {

    /**
     * Returns the CommandService's status, which can only be RUNNNING.
     *
     * @since 3.0
     */
    ServiceStatus ping(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    ServiceStatus ping(short serialVersion) throws RemoteException;

    /* -- Topology related APIs -- */

    /**
     * Return a list of the names of all storage node pools.
     *
     * @since 3.0
     */
    List<String> getStorageNodePoolNames(AuthContext authCtx,
                                         short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    List<String> getStorageNodePoolNames(short serialVersion)
        throws RemoteException;

    /**
     * Add a new StorageNodePool.
     *
     * @since 3.0
     */
    void addStorageNodePool(String name,
                            AuthContext authCtx,
                            short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    void addStorageNodePool(String name, short serialVersion)
        throws RemoteException;

    /**
     * Remove a storage node pool.
     *
     * @since 3.0
     */
    void removeStorageNodePool(String name,
                               AuthContext authCtx,
                               short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    void removeStorageNodePool(String name, short serialVersion)
        throws RemoteException;

    /**
     * Get a list of the storage node ids in a pool.
     *
     * @since 3.0
     */
    List<StorageNodeId> getStorageNodePoolIds(String name,
                                              AuthContext authCtx,
                                              short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    List<StorageNodeId> getStorageNodePoolIds(String name, short serialVersion)
        throws RemoteException;

    /**
     * Add a storage node to the pool with the given name.
     *
     * @since 3.0
     */
    void addStorageNodeToPool(String name,
                              StorageNodeId snId,
                              AuthContext authCtx,
                              short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    void addStorageNodeToPool(String name,
                              StorageNodeId snId,
                              short serialVersion)
        throws RemoteException;

    /**
     * Replace the contents of a storage node pool.
     *
     * @since 3.0
     */
    void replaceStorageNodePool(String name,
                                List<StorageNodeId> ids,
                                AuthContext authCtx,
                                short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    void replaceStorageNodePool(String name,
                                List<StorageNodeId> ids,
                                short serialVersion)
        throws RemoteException;

    /**
     * Creates a named topology. The initial configuration will be based on the
     * storage nodes specified by the <code>snPoolName</code> parameter. The
     * number of partitions for a topology is fixed once the topology is
     * created and cannot be changed. The command will throw TODO if there is
     * not enough capacity in the specified <code>snPoolName</code> to satisfy
     * the number of the replication nodes needed to create the topology.
     *
     * @throws IllegalCommandException if the name referenced by
     * <code>candidateName</code> is already associated with a topology
     *
     * @since 3.0
     */
    String createTopology(String candidateName,
                          String snPoolName,
                          int numPartitions,
                          AuthContext authCtx,
                          short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    String createTopology(String candidateName,
                          String snPoolName,
                          int numPartitions,
                          short serialVersion)
        throws RemoteException;

    /**
     * Creates a copy of the current (deployed) topology and associates it with
     * the <code>candidateName</code> parameter. Changes to the copy will have
     * no effect on the current topology.
     *
     * @throws IllegalCommandException is @param candidateName is already
     * associated with a topology
     *
     * @since 3.0
     */
    String copyCurrentTopology(String candidateName,
                               AuthContext authCtx,
                               short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    String copyCurrentTopology(String candidateName,
                               short serialVersion)
        throws RemoteException;

    /**
     * Returns the list of named topologies. If no named topologies exist an
     * empty list is returned.
     *
     * @since 3.0
     */
    List<String> listTopologies(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    List<String> listTopologies(short serialVersion) throws RemoteException;

    /**
     * Delete a named topology. Removing a topology that was used to create a
     * plan will not affect the plan. If @param candidateName is not associated
     * with a topology, this method does nothing.
     *
     * @since 3.0
     */
    String deleteTopology(String candidateName,
                          AuthContext authCtx,
                          short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    String deleteTopology(String candidateName, short serialVersion)
        throws RemoteException;

    /**
     * Re-balances the replication nodes of a data center or store. The set of
     * replication nodes in the topology having the name referenced by the
     * <code>candidateName</code> parameter will be re-balanced across the
     * storage nodes specified by the <code>snPoolName</code> parameter. Both
     * the replication factor and the number of shards is not changed. If the
     * value input for the <code>dcId</code> parameter is not
     * <code>null</code>, the re-balance will only be done to nodes in that
     * data center, otherwise the re-balance will be for all nodes in the
     * store. The method will fail if there is not enough capacity in the
     * specified pool to satisfy the number of the replication nodes needed to
     * implement the change.
     *
     * @throws IllegalCommandException if the name referenced by
     * <code>candidateName</code> is not associated with a topology.
     *
     * @since 3.0
     */
    String rebalanceTopology(String candidateName,
                             String snPoolName,
                             DatacenterId dcId,
                             AuthContext authCtx,
                             short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    String rebalanceTopology(String candidateName, String snPoolName,
                             DatacenterId dcId, short serialVersion)
        throws RemoteException;

    /**
     * Changes the replication factor of a data center and adds or removes
     * replication nodes in the topology having the name referenced by the
     * <code>candidateName</code> parameter. The data center's replication
     * factor is changed and new replication nodes are added and re-balanced
     * across the storage nodes specified by the <code>snPoolName</code>
     * parameter. The number of shards is not changed. The method will fail if
     * there is not enough capacity in the pool referenced by
     * <code>snPoolName</code> to satisfy the number of the replication nodes
     * needed to implement the change, or the rep-factor is smaller then the
     * current data center's replication factor. If the current replication
     * factor and rep-factor are equal this method will do nothing.
     *
     * @throws IllegalCommandException if the name referenced by
     * <code>candidateName</code> is not associated with a topology.
     *
     * @since 3.0
     */
    String changeRepFactor(String candidateName, String snPoolName,
                           DatacenterId dcId, int repFactor,
                           AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    String changeRepFactor(String candidateName, String snPoolName,
                           DatacenterId dcId, int repFactor,
                           short serialVersion)
        throws RemoteException;

    /**
     * Redistributes partitions in the topology having the name referenced by
     * the <code>candidateName</code> parameter. The number of shards will be
     * recalculated and new replication nodes will be added as needed. The new
     * replication nodes will be assigned to the storage nodes specified by the
     * <code>snPoolName</code> parameter. Partitions from existing shards will
     * be re-assigned to the new shards. The method will fail if there is not
     * enough capacity in the pool referenced by <code>snPoolName</code> to
     * satisfy the number of the replication nodes needed to implement the
     * change.
     *
     * @throws IllegalCommandException if the name referenced by
     * <code>candidateName</code> is not associated with a topology.
     *
     * @since 3.0
     */
    String redistributeTopology(String candidateName,
                                String snPoolName,
                                AuthContext authCtx,
                                short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    String redistributeTopology(String candidateName,
                                String snPoolName,
                                short serialVersion)
        throws RemoteException;

    /**
     * Displays the steps necessary to migrate one topology to another. If
     * @param startTopoName is not {@code null} then that topology is used as
     * the starting point otherwise the deployed store's topology is used.
     *
     * @throws IllegalCommandException is {@code targetTopoName} or
     * {@code startTopoName} are not associated with a topology
     *
     * @since 3.0
     */
    String preview(String targetTopoName, String startTopoName,
                   boolean verbose, AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    String preview(String targetTopoName, String startTopoName,
                   boolean verbose, short serialVersion)
        throws RemoteException;

    /* -- Plan APIs -- */

    /**
     * Get a list of the Admins and their parameters.
     *
     * @since 3.0
     */
    List<ParameterMap> getAdmins(AuthContext authCtx, short serialVersion) throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    List<ParameterMap> getAdmins(short serialVersion) throws RemoteException;

    /**
     * Get the specified plan.
     *
     * @since 3.0
     */
    Plan getPlanById(int planId,
                     AuthContext authCtx,
                     short serialVersion) throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    Plan getPlanById(int planId, short serialVersion) throws RemoteException;

    /**
     * Return the map of all plans.
     *
     * @since 3.0
     * @deprecated in favor of getPlanRange.
     */
    @Deprecated
    Map<Integer, Plan> getPlans(AuthContext authCtx,
                                short serialVersion) throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    Map<Integer, Plan> getPlans(short serialVersion) throws RemoteException;

    /**
     * Approve the identified plan.
     *
     * @since 3.0
     */
    void approvePlan(int planId, AuthContext authCtx,
                     short serialVersion) throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    void approvePlan(int planId, short serialVersion) throws RemoteException;

    /**
     * Execute the identified plan. Returns when plan execution is finished.
     * @param force if true, ignor
     *
     * @since 3.0
     */
    void executePlan(int planId, boolean force,
                     AuthContext authCtx,
                     short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    void executePlan(int planId, boolean force, short serialVersion)
        throws RemoteException;

   /**
     * Wait for the plan to finish. If a timeout period is specified, return
     * either when the plan finishes or the timeout occurs.
     * @return the current plan status when the call returns. If the call timed
     * out, the plan may still be running.
     *
     * @since 3.0
     */
    Plan.State awaitPlan(int planId,
                         int timeout,
                         TimeUnit timeUnit,
                         AuthContext authCtx,
                         short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    Plan.State awaitPlan(int planId, int timeout,
                         TimeUnit timeUnit, short serialVersion)
        throws RemoteException;

    /**
     * Cancel a plan.
     *
     * @since 3.0
     */
    void cancelPlan(int planId,
                    AuthContext authCtx,
                    short serialVersion) throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    void cancelPlan(int planId, short serialVersion) throws RemoteException;

    /**
     * Interrupt a plan.
     *
     * @since 3.0
     */
    void interruptPlan(int planId,
                       AuthContext authCtx,
                       short serialVersion) throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    void interruptPlan(int planId, short serialVersion) throws RemoteException;

    /**
     * Retry a plan.
     *
     * @since 3.0
     */
    void retryPlan(int planId,
                   AuthContext authCtx,
                   short serialVersion) throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    void retryPlan(int planId, short serialVersion) throws RemoteException;

    /**
     * Create and run Plans for initial configuration of a node.  This creates,
     * approves and executes plans to deploy a data center, storage node, and
     * admin all in one call.  Because all of the necessary information is in
     * the admin this relieves the client of the burden of collection.
     *
     *
     * @since 3.0
     */
    void createAndExecuteConfigurationPlan(String kvsName,
                                           String dcName,
                                           int repFactor,
                                           AuthContext authCtx,
                                           short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    void createAndExecuteConfigurationPlan(String kvsName,
                                           String dcName,
                                           int repFactor,
                                           short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to deploy a new PRIMARY Datacenter.
     * Note that datacenterComment is unused, and is deprecated as of R2.  This
     * command is only used by R2 and earlier clients.
     *
     * @since 3.0
     */
    int createDeployDatacenterPlan(String planName,
                                   String datacenterName,
                                   int repFactor,
                                   String datacenterComment,
                                   AuthContext authCtx,
                                   short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    int createDeployDatacenterPlan(String planName,
                                   String datacenterName,
                                   int repFactor,
                                   String datacenterComment,
                                   short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to deploy a new Datacenter with the specified type.
     * This command is used by R3 or later clients.
     *
     * @since 3.0
     */
    int createDeployDatacenterPlan(String planName,
                                   String datacenterName,
                                   int repFactor,
                                   DatacenterType datacenterType,
                                   AuthContext authCtx,
                                   short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    int createDeployDatacenterPlan(String planName,
                                   String datacenterName,
                                   int repFactor,
                                   DatacenterType datacenterType,
                                   short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to deploy a new StorageNode.
     *
     * @since 3.0
     */
    int createDeploySNPlan(String planName,
                           DatacenterId datacenterId,
                           String hostName,
                           int registryPort,
                           String comment,
                           AuthContext authCtx,
                           short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    int createDeploySNPlan(String planName,
                           DatacenterId datacenterId,
                           String hostName,
                           int registryPort,
                           String comment,
                           short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to deploy a new Admin service instance.
     *
     * @since 3.0
     */
    int createDeployAdminPlan(String planName,
                              StorageNodeId snid,
                              int httpPort,
                              AuthContext authCtx,
                              short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    int createDeployAdminPlan(String planName,
                              StorageNodeId snid,
                              int httpPort,
                              short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to remove the specified Admin (if <code>aid</code> is
     * non-<code>null</code> and <code>dcid</code> is <code>null</code>), or
     * all Admins deployed to the specified datacenter (if <code>dcid</code> is
     * non-<code>null</code> and <code>aid</code> is <code>null</code>).
     *
     * @param planName the name to assign to the created Plan
     *
     * @param dcid the id of the datacenter containing the Admins to remove.
     * If this parameter and the <code>aid</code> parameter are both
     * non-<code>null</code> or both <code>null</code>, then an
     * <code>IllegalArgumentException</code> is thrown.
     *
     * @param aid the id of the specific Admin to remove. If this parameter
     * and the <code>dcid</code> parameter are both non-<code>null</code> or
     * both <code>null</code>, then an <code>IllegalArgumentException</code>
     * is thrown.
     *
     * @throws IllegalArgumentException if the <code>dcid</code> parameter and
     * the <code>aid</code> parameter are both non-<code>null</code> or both
     * <code>null</code>.
     *
     * @since 3.0
     */
    int createRemoveAdminPlan(String planName,
                              DatacenterId dcid,
                              AdminId aid,
                              AuthContext authCtx,
                              short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    int createRemoveAdminPlan(String planName,
                              DatacenterId dcid,
                              AdminId aid,
                              short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to deploy a topology.
     *
     * @since 3.0
     */
    int createDeployTopologyPlan(String planName,
                                 String candidateName,
                                 AuthContext authCtx,
                                 short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    int createDeployTopologyPlan(String planName,
                                 String candidateName,
                                 short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to shut down the repnodes in a kvstore.
     *
     * @since 3.0
     */
    int createStopAllRepNodesPlan(String planName,
                                  AuthContext authCtx,
                                  short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    int createStopAllRepNodesPlan(String planName, short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to start up the repnodes in a kvstore.
     *
     * @since 3.0
     */
    int createStartAllRepNodesPlan(String planName,
                                   AuthContext authCtx,
                                   short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    int createStartAllRepNodesPlan(String planName, short serialVersion)
        throws RemoteException;

    /**
     * Stop a given set of RepNodes.
     *
     * @since 3.0
     */
    int createStopRepNodesPlan(String planName,
                               Set<RepNodeId> rnids,
                               AuthContext authCtx,
                               short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    int createStopRepNodesPlan(String planName,
                               Set<RepNodeId> rnids,
                               short serialVersion)
        throws RemoteException;

    /**
     * Start a given set of RepNodes.
     *
     * @since 3.0
     */
    int createStartRepNodesPlan(String planName,
                                Set<RepNodeId> rnids,
                                AuthContext authCtx,
                                short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    int createStartRepNodesPlan(String planName,
                                Set<RepNodeId> rnids,
                                short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to alter a service's parameters.
     *
     * @since 3.0
     */
    int createChangeParamsPlan(String planName,
                               ResourceId rid,
                               ParameterMap newParams,
                               AuthContext authCtx,
                               short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    int createChangeParamsPlan(String planName,
                               ResourceId rid,
                               ParameterMap newParams,
                               short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to alter parameters for all RepNodes deployed to the
     * specified datacenter. If <code>null</code> is input for the
     * <code>dcid</code> parameter, then the specified parameters will be
     * changed for all RepNodes from each of the datacenters making up the
     * store.
     *
     * @since 3.0
     */
    int createChangeAllParamsPlan(String planName,
                                  DatacenterId dcid,
                                  ParameterMap newParams,
                                  AuthContext authCtx,
                                  short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    int createChangeAllParamsPlan(String planName,
                                  DatacenterId dcid,
                                  ParameterMap newParams,
                                  short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to alter parameters for all admin services deployed to
     * the specified datacenter. If <code>null</code> is input for the
     * <code>dcid</code> parameter, then the specified parameters will be
     * changed for all admin services from each of the datacenters making up
     * the store.
     *
     * @since 3.0
     */
    int createChangeAllAdminsPlan(String planName,
                                  DatacenterId dcid,
                                  ParameterMap newParams,
                                  AuthContext authCtx,
                                  short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    int createChangeAllAdminsPlan(String planName,
                                  DatacenterId dcid,
                                  ParameterMap newParams,
                                  short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to alter parameters for global security parameters.
     * The specified parameters will be changed for all admin and repnode
     * services from storage nodes in the store.
     *
     * @since 3.0
     */
    int createChangeGlobalSecurityParamsPlan(String planName,
                                             ParameterMap newParams,
                                             AuthContext authCtx,
                                             short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to change a user's information.
     *
     * @since 3.0
     */
    int createChangeUserPlan(String planName,
                             String userName,
                             Boolean isEnabled,
                             char[] newPlainPassword,
                             boolean retainPassword,
                             boolean clearRetainedPassword,
                             AuthContext authCtx,
                             short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to add a user of kvstore.
     *
     * @since 3.0
     */
    int createCreateUserPlan(String planName,
                             String userName,
                             boolean isEnabled,
                             boolean isAdmin,
                             char[] plainPassword,
                             AuthContext authCtx,
                             short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to remove a user of kvstore.
     *
     * @since 3.0
     */
    int createDropUserPlan(String planName,
                           String userName,
                           AuthContext authCtx,
                           short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to move all services from the old storage node to a
     * new storage node.
     *
     * @since 3.0
     */
    int createMigrateSNPlan(String planName,
                            StorageNodeId oldNode,
                            StorageNodeId newNode,
                            int newHttpPort,
                            AuthContext authCtx,
                            short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    int createMigrateSNPlan(String planName,
                            StorageNodeId oldNode,
                            StorageNodeId newNode,
                            int newHttpPort,
                            short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to remove a storage node from the store.
     *
     * @since 3.0
     */
    int createRemoveSNPlan(String planName,
                           StorageNodeId targetNode,
                           AuthContext authCtx,
                           short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    int createRemoveSNPlan(String planName,
                           StorageNodeId targetNode,
                           short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to remove a datacenter from the store.
     *
     * @since 3.0
     */
    int createRemoveDatacenterPlan(String planName,
                                   DatacenterId targetId,
                                   AuthContext authCtx,
                                   short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    int createRemoveDatacenterPlan(String planName,
                                   DatacenterId targetId,
                                   short serialVersion)
        throws RemoteException;


    /**
     * Create a plan that will address topology inconsistencies.
     */
    int createRepairPlan(String planName,
                         AuthContext authCtx,
                         short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to create a new Table in the store.
     *
     * @param planName the name of the plan
     *
     * @param tableId the id of the new table.  This is used in its generated
     * Key objects so it should be short to save space in the store.
     *
     * @param parentName set to a qualified ("." separated) path to a parent
     * table if the new table is a child table, null otherwise.
     *
     * @param fieldMap an object that represents the map of {@link FieldDef}
     * objects that comprises the table, along with the field declaration
     * order.
     *
     * @param primaryKey the list of fields that comprise the primary key for
     * this table.  It must contain at least one field.  For child tables it is
     * a superset of its parent table's primary key.  Primary key fields turn
     * into Keys in requests.
     *
     * @param majorKey the list of primary key fields that comprise the major
     * portion of generated Key objects for the table. This must be strict,
     * ordered subset of the primaryKey if set.  It is only used for top-level
     * tables.  For child tables the major/minor split is either that of
     * the parent table or the boundary between the parent and child table
     * primary keys.  The Key components generated by a child table are
     * implicitly minor-only.
     *
     * @param description an option description of the table, used for
     * human-readable purposes.  This string does not affect table records.
     */
    public int createAddTablePlan(String planName,
                                  String tableId,
                                  String parentName,
                                  FieldMap fieldMap,
                                  List<String> primaryKey,
                                  List<String> majorKey,
                                  boolean r2compat,
                                  int schemaId,
                                  String description,
                                  AuthContext authCtx,
                                  short serialVersion)
        throws RemoteException;

    public int createRemoveTablePlan(String planName,
                                     String tableName,
                                     boolean removeData,
                                     AuthContext authCtx,
                                     short serialVersion)
         throws RemoteException;

    public int createAddIndexPlan(String planName,
                                  String indexName,
                                  String tableName,
                                  String[] indexedFields,
                                  String description,
                                  AuthContext authCtx,
                                  short serialVersion)
        throws RemoteException;

    public int createRemoveIndexPlan(String planName,
                                     String indexName,
                                     String tableName,
                                     AuthContext authCtx,
                                     short serialVersion)
        throws RemoteException;

    public int createEvolveTablePlan(String planName,
                                     String tableName,
                                     int tableVersion,
                                     FieldMap fieldMap,
                                     AuthContext authCtx,
                                     short serialVersion)
         throws RemoteException;

    /**
     * Configure the Admin with a store name.  This command can be used only
     * when the AdminService is running in bootstrap/configuration mode.
     *
     * @since 3.0
     */
    void configure(String storeName, AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    void configure(String storeName, short serialVersion)
        throws RemoteException;

    /**
     * If configured, return the store name, otherwise, null.
     *
     * @since 3.0
     */
    String getStoreName(AuthContext authCtx,
                        short serialVersion) throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    String getStoreName(short serialVersion) throws RemoteException;

    /**
     * Return the pathname of the KV root directory (KVHOME).
     *
     * @since 3.0
     */
    String getRootDir(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    String getRootDir(short serialVersion) throws RemoteException;

    /**
     * Return the whole Topology for listing or browsing.
     *
     * @since 3.0
     */
    Topology getTopology(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    Topology getTopology(short serialVersion) throws RemoteException;

    /**
     * Return the specified Metadata for listing or browsing.
     */
    <T extends Metadata<? extends MetadataInfo>> T
                                  getMetadata(final Class<T> returnType,
                                              final MetadataType metadataType,
                                              AuthContext authCtx,
                                              short serialVersion)
         throws RemoteException;

    /**
     * Retrieve the topology that corresponds to this candidate name.  Invoked
     * with the "topology view candidateName" command.
     *
     * @since 3.0
     */
    TopologyCandidate getTopologyCandidate(String candidateName,
                                           AuthContext authCtx,
                                           short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    TopologyCandidate getTopologyCandidate(String candidateName,
                                           short serialVersion)
        throws RemoteException;

    /**
     * Return the whole Parameters for listing or browsing.
     *
     * @since 3.0
     */
    Parameters getParameters(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    Parameters getParameters(short serialVersion) throws RemoteException;

    /**
     * Return the RepNodeParameters for the specified node.
     *
     * @since 3.0
     */
    ParameterMap getRepNodeParameters(RepNodeId id,
                                      AuthContext authCtx,
                                      short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    ParameterMap getRepNodeParameters(RepNodeId id, short serialVersion)
        throws RemoteException;

    /**
     * Return the Policy parameters from the admin.
     *
     * @since 3.0
     */
    ParameterMap getPolicyParameters(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    ParameterMap getPolicyParameters(short serialVersion)
        throws RemoteException;

    /**
     * Indicates that new parameters are available in the storage node
     * configuration file and that these should be reread.
     *
     * @since 3.0
     */
    void newParameters(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    void newParameters(short serialVersion) throws RemoteException;

    /**
     * Indicates that new global parameters are available in the storage node
     * configuration file and that these should be reread.
     *
     * @since 3.0
     */
    public void newGlobalParameters(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Stop the admin service.
     *
     * @since 3.0
     */
    void stop(boolean force, AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    void stop(boolean force, short serialVersion) throws RemoteException;

    /**
     * Set the policy parameters.
     *
     * @since 3.0
     */
    void setPolicies(ParameterMap policyParams,
                     AuthContext authCtx,
                     short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    void setPolicies(ParameterMap policyParams, short serialVersion)
        throws RemoteException;

    /**
     * Return the current health status for each component.
     *
     * @since 3.0
     */
    Map<ResourceId, ServiceChange> getStatusMap(AuthContext authCtx,
                                                short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    Map<ResourceId, ServiceChange> getStatusMap(short serialVersion)
        throws RemoteException;

    /**
     * Return the current performance status for each component.
     *
     * @since 3.0
     */
    Map<ResourceId, PerfEvent> getPerfMap(AuthContext authCtx,
                                          short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    Map<ResourceId, PerfEvent> getPerfMap(short serialVersion)
        throws RemoteException;

    /**
     * Return the status reporting events that have occurred since a point in
     * time.
     *
     * @since 3.0
     */
    RetrievedEvents<ServiceChange> getStatusSince(long since,
                                                  AuthContext authCtx,
                                                  short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    RetrievedEvents<ServiceChange> getStatusSince(long since,
                                                  short serialVersion)
        throws RemoteException;

    /**
     * Return the performance reporting events that have occurred since a point
     * in time.
     *
     * @since 3.0
     */
    RetrievedEvents<PerfEvent> getPerfSince(long since,
                                            AuthContext authCtx,
                                            short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    RetrievedEvents<PerfEvent> getPerfSince(long since, short serialVersion)
        throws RemoteException;

    /**
     * Return the log records that have been logged since a point in time.
     *
     * @since 3.0
     */
    RetrievedEvents<LogRecord> getLogSince(long since,
                                           AuthContext authCtx,
                                           short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    RetrievedEvents<LogRecord> getLogSince(long since, short serialVersion)
        throws RemoteException;

    /**
     * Return the plan state change events that have occured since a point in
     * time.
     *
     * @since 3.0
     */
    RetrievedEvents<PlanStateChange> getPlanSince(long since,
                                                  AuthContext authCtx,
                                                  short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    RetrievedEvents<PlanStateChange> getPlanSince(long since,
                                                  short serialVersion)
        throws RemoteException;

    /**
     * Register a log tracker listener.
     *
     * @since 3.0
     */
    void registerLogTrackerListener(TrackerListener tl,
                                    AuthContext authCtx,
                                    short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    void registerLogTrackerListener(TrackerListener tl, short serialVersion)
        throws RemoteException;

    /**
     * Remove the registration of a log tracker listener.
     *
     * @since 3.0
     */
    void removeLogTrackerListener(TrackerListener tl,
                                  AuthContext authCtx,
                                  short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    void removeLogTrackerListener(TrackerListener tl, short serialVersion)
        throws RemoteException;

    /**
     * Register a status tracker listener.
     *
     * @since 3.0
     */
    void registerStatusTrackerListener(TrackerListener tl,
                                       AuthContext authCtx,
                                       short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    void registerStatusTrackerListener(TrackerListener tl, short serialVersion)
        throws RemoteException;

    /**
     * Remove the registration of a status tracker listener.
     *
     * @since 3.0
     */
    void removeStatusTrackerListener(TrackerListener tl,
                                     AuthContext authCtx,
                                     short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    void removeStatusTrackerListener(TrackerListener tl, short serialVersion)
        throws RemoteException;

    /**
     * Register a perf tracker listener.
     *
     * @since 3.0
     */
    void registerPerfTrackerListener(TrackerListener tl,
                                     AuthContext authCtx,
                                     short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    void registerPerfTrackerListener(TrackerListener tl, short serialVersion)
        throws RemoteException;

    /**
     * Remove the registration of a perf tracker listener.
     *
     * @since 3.0
     */
    void removePerfTrackerListener(TrackerListener tl,
                                   AuthContext authCtx,
                                   short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    void removePerfTrackerListener(TrackerListener tl, short serialVersion)
        throws RemoteException;

    /**
     * Register a plan tracker listener.
     *
     * @since 3.0
     */
    void registerPlanTrackerListener(TrackerListener tl,
                                     AuthContext authCtx,
                                     short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    void registerPlanTrackerListener(TrackerListener tl, short serialVersion)
        throws RemoteException;

    /**
     * Remove the registration of a plan tracker listener.
     *
     * @since 3.0
     */
    void removePlanTrackerListener(TrackerListener tl,
                                   AuthContext authCtx,
                                   short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    void removePlanTrackerListener(TrackerListener tl, short serialVersion)
        throws RemoteException;

    /**
     * Get a map of log file names.
     *
     * @since 3.0
     */
    Map<String, Long> getLogFileNames(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    Map<String, Long> getLogFileNames(short serialVersion)
        throws RemoteException;

    /**
     * Get the Admin state.
     *
     * @since 3.0
     */
    ReplicatedEnvironment.State getAdminState(AuthContext authCtx,
                                              short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    ReplicatedEnvironment.State getAdminState(short serialVersion)
        throws RemoteException;

    /**
     * Get the master Admin's RMI address.
     *
     * @since 3.0
     */
    URI getMasterRmiAddress(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    URI getMasterRmiAddress(short serialVersion) throws RemoteException;

    /**
     * Get the master Admin's HTTP address.
     *
     * @since 3.0
     */
    URI getMasterHttpAddress(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    URI getMasterHttpAddress(short serialVersion) throws RemoteException;

    /**
     * Get a list of critical events.
     *
     * @since 3.0
     */
    List<CriticalEvent> getEvents(long startTime, long endTime,
                                  CriticalEvent.EventType type,
                                  AuthContext authCtx,
                                  short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    List<CriticalEvent> getEvents(long startTime, long endTime,
                                  CriticalEvent.EventType type,
                                  short serialVersion)
        throws RemoteException;

    /**
     * Get a single critical event.
     *
     * @since 3.0
     */
    CriticalEvent getOneEvent(String eventId,
                              AuthContext authCtx,
                              short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    CriticalEvent getOneEvent(String eventId, short serialVersion)
        throws RemoteException;

    /**
     * Start a backup.
     *
     * @since 3.0
     */
    String[] startBackup(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    String[] startBackup(short serialVersion) throws RemoteException;

    /**
     * Stop a backup.
     *
     * @since 3.0
     */
    long stopBackup(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    long stopBackup(short serialVersion) throws RemoteException;

    /**
     * Update the HA address for an admin member.
     *
     * @since 3.0
     */
    void updateMemberHAAddress(AdminId targetId,
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
    void updateMemberHAAddress(AdminId targetId,
                               String targetHelperHosts,
                               String newNodeHostPort,
                               short serialVersion)
        throws RemoteException;

    /**
     * Verify store configuration.
     *
     * @since 3.0
     */
    VerifyResults verifyConfiguration(boolean showProgress,
                                      boolean listAll,
                                      AuthContext authCtx,
                                      short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    VerifyResults verifyConfiguration(boolean showProgress,
                                      boolean listAll,
                                      short serialVersion)
        throws RemoteException;

    /**
     * Verify upgrade state.
     *
     * @since 3.0
     */
    VerifyResults verifyUpgrade(KVVersion targetVersion,
                                List<StorageNodeId> snIds,
                                boolean showProgress,
                                boolean listAll,
                                AuthContext authCtx,
                                short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    VerifyResults verifyUpgrade(KVVersion targetVersion,
                                List<StorageNodeId> snIds,
                                boolean showProgress,
                                boolean listAll,
                                short serialVersion)
        throws RemoteException;

    /**
     * Verify upgrade prerequisites.
     *
     * @since 3.0
     */
    VerifyResults verifyPrerequisite(KVVersion targetVersion,
                                     KVVersion prerequisiteVersion,
                                     List<StorageNodeId> snIds,
                                     boolean showProgress,
                                     boolean listAll,
                                     AuthContext authCtx,
                                     short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    VerifyResults verifyPrerequisite(KVVersion targetVersion,
                                     KVVersion prerequisiteVersion,
                                     List<StorageNodeId> snIds,
                                     boolean showProgress,
                                     boolean listAll,
                                     short serialVersion)
        throws RemoteException;

    /**
     * Get the Admins configuration parameters.
     *
     * @since 3.0
     */
    LoadParameters getParams(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    LoadParameters getParams(short serialVersion) throws RemoteException;

    /**
     * Get the name of the store-wide log file.
     *
     * @since 3.0
     */
    String getStorewideLogName(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    String getStorewideLogName(short serialVersion) throws RemoteException;

    /**
     * List realized topologies with the "show topology history" command.
     *
     * @since 3.0
     */
    List<String> getTopologyHistory(boolean concise,
                                    AuthContext authCtx,
                                    short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    List<String> getTopologyHistory(boolean concise, short serialVersion)
        throws RemoteException;

    /**
     * Get schema summary map.
     *
     * @since 3.0
     */
    SortedMap<String, AvroDdl.SchemaSummary>
        getSchemaSummaries(boolean includeDisabled,
                           AuthContext authCtx,
                           short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    SortedMap<String, AvroDdl.SchemaSummary>
        getSchemaSummaries(boolean includeDisabled, short serialVersion)
        throws RemoteException;

    /**
     * Get schema details.
     *
     * @since 3.0
     */
    AvroDdl.SchemaDetails getSchemaDetails(int schemaId,
                                           AuthContext authCtx,
                                           short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    AvroDdl.SchemaDetails getSchemaDetails(int schemaId, short serialVersion)
        throws RemoteException;

    /**
     * Add a schema definition.
     *
     * @since 3.0
     */
    AvroDdl.AddSchemaResult addSchema(AvroSchemaMetadata metadata,
                                      String schemaText,
                                      AvroDdl.AddSchemaOptions options,
                                      AuthContext authCtx,
                                      short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    AvroDdl.AddSchemaResult addSchema(AvroSchemaMetadata metadata,
                                      String schemaText,
                                      AvroDdl.AddSchemaOptions options,
                                      short serialVersion)
        throws RemoteException;

    /**
     * Update schema status.
     *
     * @since 3.0
     */
    boolean updateSchemaStatus(int schemaId,
                               AvroSchemaMetadata newMeta,
                               AuthContext authCtx,
                               short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    boolean updateSchemaStatus(int schemaId,
                               AvroSchemaMetadata newMeta,
                               short serialVersion)
        throws RemoteException;

    /**
     * An unadvertised entry point which lets the caller check that a plan
     * succeeded, and provokes an exception containing information about the
     * failure if the plan failed. Used for testing and for situations where we
     * need to programmatically obtain an exception if the plan failed.
     *
     * @throw an OperationFaultException if the plan did not end
     * successfully.
     *
     * @since 3.0
     */
    void assertSuccess(int planId, AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    void assertSuccess(int planId, short serialVersion)
        throws RemoteException;

    /**
     * Get the status of an Admin plan.
     *
     * @since 3.0
     */
    String getPlanStatus(int planId,
                         long options,
                         AuthContext authCtx,
                         short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    String getPlanStatus(int planId, long options, short serialVersion)
        throws RemoteException;

    /**
     * Copy a topolgy within the Admin.
     *
     * @since 3.0
     */
    String copyTopology(String sourceCandidateName,
                        String targetCandidateName,
                        AuthContext authCtx,
                        short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    String copyTopology(String sourceCandidateName,
                        String targetCandidateName,
                        short serialVersion)
        throws RemoteException;

    /**
     * Check validity of a topology.
     *
     * @since 3.0
     */
    String validateTopology(String candidateName,
                            AuthContext authCtx,
                            short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    String validateTopology(String candidateName, short serialVersion)
        throws RemoteException;

    /**
     * Move an RN.
     *
     * @since 3.0
     */
    String moveRN(String candidateName,
                  RepNodeId rnId,
                  StorageNodeId snId,
                  AuthContext authCtx,
                  short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    String moveRN(String candidateName,
                  RepNodeId rnId,
                  StorageNodeId snId,
                  short serialVersion)
        throws RemoteException;

    /**
     * Install a receiver for Admin status updates, for delivering service
     * change information to the standardized monitoring/management agent.
     *
     * @since 3.0
     */
    void installStatusReceiver(AdminStatusReceiver asr,
                               AuthContext authCtx,
                               short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    void installStatusReceiver(AdminStatusReceiver asr, short serialVersion)
        throws RemoteException;

    /**
     * Get a list of nodes to upgrade in an order which will maintain
     * store availability.
     *
     * @since 3.0
     */
    String getUpgradeOrder(KVVersion targetVersion,
                           KVVersion prerequisiteVersion,
                           AuthContext authCtx,
                           short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    String getUpgradeOrder(KVVersion targetVersion,
                           KVVersion prerequisiteVersion,
                           short serialVersion)
        throws RemoteException;

    /**
     * Retrieve the beginning plan id and number of plans that satisfy the
     * request.
     *
     * Returns an array of two integers indicating a range of plan id
     * numbers. [0] is the first id in the range, and [1] number of
     * plan ids in the range.
     *
     * Operates in three modes:
     *
     *    mode A requests howMany plans ids following startTime
     *    mode B requests howMany plans ids preceding endTime
     *    mode C requests a range of plan ids from startTime to endTime.
     *
     *    mode A is signified by endTime == 0
     *    mode B is signified by startTime == 0
     *    mode C is signified by neither startTime nor endTime being == 0.
     *        howMany is ignored in mode C.
     * @since 3.0
     */
    public int[] getPlanIdRange(final long startTime,
                                final long endTime,
                                final int howMany,
                                AuthContext authCtx,
                                short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public int[] getPlanIdRange(final long startTime,
                                final long endTime,
                                final int howMany,
                                short serialVersion)
        throws RemoteException;

    /**
     * Returns a map of plans starting at firstPlanId.  The number of plans in
     * the map is the lesser of howMany, MAXPLANS, or the number of extant
     * plans with id numbers following firstPlanId.  The range is not
     * necessarily fully populated; while plan ids are mostly sequential, it is
     * possible for values to be skipped.
     *
     * @since 3.0
     */
    public Map<Integer, Plan> getPlanRange(final int firstPlanId,
                                           final int howMany,
                                           AuthContext authCtx,
                                           short serialVersion)
        throws RemoteException;


    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public Map<Integer, Plan> getPlanRange(final int firstPlanId,
                                           final int howMany,
                                           short serialVersion)
        throws RemoteException;

    /**
     * Return the brief and detailed description of all users for display
     *
     * @since 3.0
     */
    Map<String, UserDescription> getUsersDescription(AuthContext authCtx,
                                                     short serialVersion)
        throws RemoteException;

    /**
     * Verify if the specified password is correct for the user
     *
     * @since 3.0
     */
    boolean verifyUserPassword(String userName,
                               char[] password,
                               AuthContext authCtx,
                               short serialVersion)
        throws RemoteException;
}
