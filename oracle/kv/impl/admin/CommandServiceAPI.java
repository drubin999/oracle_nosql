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
import oracle.kv.impl.security.ContextProxy;
import oracle.kv.impl.security.login.LoginHandle;
import oracle.kv.impl.security.metadata.KVStoreUser.UserDescription;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.registry.RemoteAPI;

import com.sleepycat.je.rep.ReplicatedEnvironment;

/**
 * This is the interface used by the command line client.
 */
public final class CommandServiceAPI extends RemoteAPI {

    /**
     * Specifying datacenter types is only supported starting with version 4.
     */
    private static final short DATACENTER_TYPE_INITIAL_SERIAL_VERSION =
        SerialVersion.V4;
    private static final AuthContext NULL_CTX = null;

    private final CommandService proxyRemote;

    private CommandServiceAPI(CommandService remote, LoginHandle loginHdl)
        throws RemoteException {

        super(remote);
        this.proxyRemote =
            ContextProxy.create(remote, loginHdl, getSerialVersion());

        /* Don't talk to pre-V2 versions of the service. */
        if (getSerialVersion() < SerialVersion.V2) {
            throw new AdminFaultException(
                new UnsupportedOperationException(
                    "The Admin service is incompatible with this client. " +
                    "Please upgrade the service, or use an older version of" +
                    " the client." +
                    " (Internal local minimum version=" + SerialVersion.V2 +
                    ", internal service version=" + getSerialVersion() + ")"));
        }
    }

    public static CommandServiceAPI wrap(CommandService remote,
                                         LoginHandle loginHdl)
        throws RemoteException {

        return new CommandServiceAPI(remote, loginHdl);
    }

    /**
     * Throws UnsupportedOperationException if a method is not supported by the
     * admin service.
     */
    private void checkMethodSupported(short expectVersion)
        throws UnsupportedOperationException {

        if (getSerialVersion() < expectVersion) {
            throw new AdminFaultException(
                new UnsupportedOperationException(
                    "Command not available because service has not yet been" +
                    " upgraded.  (Internal local version=" + expectVersion +
                    ", internal service version=" + getSerialVersion() + ")"));
        }
    }

    /**
     * Returns the CommandService's status, which can only be RUNNNING.
     */
    public ServiceStatus ping()
        throws RemoteException {

        return proxyRemote.ping(NULL_CTX, getSerialVersion());
    }

    /**
     * Return a list of the names of all storage node pools.
     */
    public List<String> getStorageNodePoolNames()
        throws RemoteException {

        return proxyRemote.getStorageNodePoolNames(NULL_CTX,
                                                   getSerialVersion());
    }

    /**
     * Add a new StorageNodePool.
     */
    public void addStorageNodePool(String name)
        throws RemoteException {

        proxyRemote.addStorageNodePool(name, NULL_CTX, getSerialVersion());
    }

    public void removeStorageNodePool(String name)
        throws RemoteException {

        proxyRemote.removeStorageNodePool(name, NULL_CTX, getSerialVersion());
    }

    /**
     * Get a list of the storage node ids in a pool.
     */
    public List<StorageNodeId> getStorageNodePoolIds(String name)
        throws RemoteException {

        return proxyRemote.getStorageNodePoolIds(name, NULL_CTX,
                                                 getSerialVersion());
    }

    /**
     * Add a storage node to the pool with the given name.
     */
    public void addStorageNodeToPool(String name, StorageNodeId snId)
        throws RemoteException {

        proxyRemote.addStorageNodeToPool(name, snId, NULL_CTX,
                                         getSerialVersion());
    }

    /**
     * TODO: delete? Unused.
     */
    public void replaceStorageNodePool(String name, List<StorageNodeId> ids)
        throws RemoteException {

        proxyRemote.replaceStorageNodePool(name, ids, NULL_CTX,
                                           getSerialVersion());
    }

    /**
     * Create a topology with the "topology create" command.
     */
    public String createTopology(String candidateName,
                                 String snPoolName,
                                 int numPartitions)
        throws RemoteException {

        return proxyRemote.createTopology(
            candidateName, snPoolName, numPartitions,
            NULL_CTX, getSerialVersion());
    }

    /**
     * Copy a topology with the "topology clone-current" command.
     */
    public String copyCurrentTopology(String candidateName)
        throws RemoteException {

        return proxyRemote.copyCurrentTopology(candidateName, NULL_CTX,
                                               getSerialVersion());
    }

    /**
     * Copy a topology with the "topology clone" command.
     */
    public String copyTopology(String candidateName, String sourceCandidate)
        throws RemoteException {

        return proxyRemote.copyTopology(candidateName, sourceCandidate,
                                        NULL_CTX, getSerialVersion());
    }

    /**
     * List topology candidates with the "topology list" command.
     */
    public List<String> listTopologies()
        throws RemoteException {

        return proxyRemote.listTopologies(NULL_CTX, getSerialVersion());
    }

    /**
     * Delete a topology candidate with the "topology delete" command.
     */
    public String deleteTopology(String candidateName)
        throws RemoteException {

        return proxyRemote.deleteTopology(candidateName, NULL_CTX,
                                          getSerialVersion());
    }

    /**
     * Rebalance a topology with the "topology rebalance" command.
     */
    public String rebalanceTopology(String candidateName, String snPoolName,
                                    DatacenterId dcId)
        throws RemoteException {

        return proxyRemote.rebalanceTopology(candidateName, snPoolName,
                                             dcId, NULL_CTX, getSerialVersion());
    }

    /**
     * Change the topology with the "topology change-repfactor" command.
     */
    public String changeRepFactor(String candidateName, String snPoolName,
                                  DatacenterId dcId, int repFactor)
        throws RemoteException {

        return proxyRemote.changeRepFactor(
            candidateName, snPoolName, dcId,
            repFactor, NULL_CTX, getSerialVersion());
    }

    /**
     * Change the topology with the "topology redistribute" command.
     */
    public String redistributeTopology(String candidateName, String snPoolName)
        throws RemoteException {

        return proxyRemote.redistributeTopology(candidateName, snPoolName,
                                                NULL_CTX, getSerialVersion());
    }

    /**
     * Show the transformation steps required to go from topology candidate
     * startTopoName to candidate targetTopoName.
     */
    public String preview(String targetTopoName, String startTopoName,
                          boolean verbose)
        throws RemoteException {

        return proxyRemote.preview(targetTopoName, startTopoName, verbose,
                                   NULL_CTX, getSerialVersion());
    }

    /**
     * Get a list of the Admins and their parameters.
     */
    public List<ParameterMap> getAdmins()
        throws RemoteException {

        return proxyRemote.getAdmins(NULL_CTX, getSerialVersion());
    }

    /**
     * Get the specified plan.
     */
    public Plan getPlanById(int planId)
        throws RemoteException {

        return proxyRemote.getPlanById(planId, NULL_CTX, getSerialVersion());
    }

    /**
     * Return the map of all plans.
     * @deprecated in favor of getPlanRange.
     */
    @Deprecated
    public Map<Integer, Plan> getPlans()
        throws RemoteException {

        return proxyRemote.getPlans(NULL_CTX, getSerialVersion());
    }

    /**
     * Approve the identified plan.
     */
    public void approvePlan(int planId)
        throws RemoteException {

        proxyRemote.approvePlan(planId, NULL_CTX, getSerialVersion());
    }

    /**
     * Execute the identified plan. Returns when plan execution is finished.
     * @param force TODO
     */
    public void executePlan(int planId, boolean force)
        throws RemoteException {

        proxyRemote.executePlan(planId, force, NULL_CTX, getSerialVersion());
    }

    /**
     * Wait for the plan to finish. If a timeout period is specified, return
     * either when the plan finishes or the timeout occurs.
     * @return the current plan status when the call returns. If the call timed
     * out, the plan may still be running.
     * @throws RemoteException
     */
    public Plan.State awaitPlan(int planId, int timeout, TimeUnit timeUnit)
        throws RemoteException {
        return proxyRemote.awaitPlan(planId, timeout, timeUnit, NULL_CTX,
                                     getSerialVersion());
    }

    /**
     * Hidden command - check that a plan has a status of SUCCESS, and throw
     * an exception containing error information if it has failed.
     */
    public void assertSuccess(int planId)
         throws RemoteException {
         proxyRemote.assertSuccess(planId, NULL_CTX, getSerialVersion());
    }
    /**
     * Cancel a plan.
     */
    public void cancelPlan(int planId)
        throws RemoteException {

        proxyRemote.cancelPlan(planId, NULL_CTX, getSerialVersion());
    }

    /**
     * Interrupt a plan.
     */
    public void interruptPlan(int planId)
        throws RemoteException {

        proxyRemote.interruptPlan(planId, NULL_CTX, getSerialVersion());
    }

    /**
     * Retry a plan.
     * @deprecated change this to execute, but leave this for upgrade
     * compatibility
     */
    @Deprecated
    public void retryPlan(int planId)
        throws RemoteException {

        proxyRemote.retryPlan(planId, NULL_CTX, getSerialVersion());
    }

    /**
     * Create and run Plans for initial configuration of a node.  This creates,
     * approves and executes plans to deploy a data center, storage node, and
     * admin all in one call.  Because all of the necessary information is in
     * the admin this relieves the client of the burden of collection.
     *
     */
    public void createAndExecuteConfigurationPlan(String kvsName,
                                                  String dcName,
                                                  int repFactor)
        throws RemoteException {

        proxyRemote.createAndExecuteConfigurationPlan(
            kvsName, dcName, repFactor, NULL_CTX, getSerialVersion());
    }

    /**
     * Create a new Plan to deploy a new Datacenter.
     */
    public int createDeployDatacenterPlan(String planName,
                                          String datacenterName,
                                          int repFactor,
                                          DatacenterType datacenterType)
        throws RemoteException {

        if (!datacenterType.isPrimary()) {
            checkMethodSupported(DATACENTER_TYPE_INITIAL_SERIAL_VERSION);
        }
        if (getSerialVersion() < DATACENTER_TYPE_INITIAL_SERIAL_VERSION) {
            return proxyRemote.createDeployDatacenterPlan(
                planName, datacenterName, repFactor,
                (String) null /* datacenterComment */,
                NULL_CTX, getSerialVersion());
        }
        return proxyRemote.createDeployDatacenterPlan(
            planName, datacenterName, repFactor, datacenterType,
            NULL_CTX, getSerialVersion());
    }

    /**
     * Create a new Plan to deploy a new StorageNode.
     */
    public int createDeploySNPlan(String planName,
                                  DatacenterId datacenterId,
                                  String hostName,
                                  int registryPort,
                                  String comment)
        throws RemoteException {

        return proxyRemote.createDeploySNPlan(planName, datacenterId, hostName,
                                              registryPort, comment,
                                              NULL_CTX, getSerialVersion());
    }

    /**
     * Create a new Plan to deploy a new Admin service instance.
     */
    public int createDeployAdminPlan(String planName,
                                     StorageNodeId snid,
                                     int httpPort)
        throws RemoteException {

        return proxyRemote.createDeployAdminPlan(planName, snid, httpPort,
                                                 NULL_CTX, getSerialVersion());
    }

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
     */
    public int createRemoveAdminPlan(String planName,
                                     DatacenterId dcid,
                                     AdminId aid)
        throws RemoteException {

        if (dcid != null && aid != null) {
            throw new IllegalArgumentException(
                "dcid and aid parameters cannot both be non-null");
        }

        if (dcid == null && aid == null) {
            throw new IllegalArgumentException(
                "dcid and aid parameters cannot both be null");
        }

        return proxyRemote.createRemoveAdminPlan(planName, dcid, aid,
                                                 NULL_CTX, getSerialVersion());
    }

    /**
     * Create a new Plan to shut down the repnodes in a kvstore.
     */
    public int createStopAllRepNodesPlan(String planName)
        throws RemoteException {

        return proxyRemote.createStopAllRepNodesPlan(planName, NULL_CTX,
                                                     getSerialVersion());
    }

    /**
     * Create a new Plan to start up the repnodes in a kvstore.
     */
    public int createStartAllRepNodesPlan(String planName)
        throws RemoteException {

        return proxyRemote.createStartAllRepNodesPlan(planName, NULL_CTX,
                                                      getSerialVersion());
    }

    /**
     * Stop a given set of RepNodes.
     */
    public int createStopRepNodesPlan(String planName, Set<RepNodeId> rnids)
        throws RemoteException {

        return proxyRemote.createStopRepNodesPlan(planName, rnids,
                                                  NULL_CTX, getSerialVersion());
    }

    /**
     * Start a given set of RepNodes.
     */
    public int createStartRepNodesPlan(String planName, Set<RepNodeId> rnids)
        throws RemoteException {

        return proxyRemote.createStartRepNodesPlan(
            planName, rnids, NULL_CTX, getSerialVersion());
    }

    /**
     * Create a new Plan to alter a service's parameters.
     */
    public int createChangeParamsPlan(String planName,
                                      ResourceId rid,
                                      ParameterMap newParams)
        throws RemoteException {

        return proxyRemote.createChangeParamsPlan(
            planName, rid, newParams, NULL_CTX, getSerialVersion());
    }

    /**
     * Create a new Plan to alter parameters for all RepNodes deployed to the
     * specified datacenter; or all RepNodes in all datacenters if
     * <code>null</code> is input for the <code>dcid</code> parameter.
     */
    public int createChangeAllParamsPlan(String planName,
                                         DatacenterId dcid,
                                         ParameterMap newParams)
        throws RemoteException {

        return proxyRemote.createChangeAllParamsPlan(
            planName, dcid, newParams, NULL_CTX, getSerialVersion());
    }

    /**
     * Create a new Plan to alter parameters for all Admins deployed to the
     * specified datacenter; or all Admins in all datacenters if
     * <code>null</code> is input for the <code>dcid</code> parameter.
     */
    public int createChangeAllAdminsPlan(String planName,
                                         DatacenterId dcid,
                                         ParameterMap newParams)
        throws RemoteException {

        return proxyRemote.createChangeAllAdminsPlan(
            planName, dcid, newParams, NULL_CTX, getSerialVersion());
    }

    /**
     * Create a new Plan to alter parameters for global security parameters.
     * The specified parameters will be changed for all admin and repnode
     * services from storage nodes in the store.
     */
    public int createChangeGlobalSecurityParamsPlan(String planName,
                                                    ParameterMap newParams)
        throws RemoteException {

        return proxyRemote.createChangeGlobalSecurityParamsPlan(
            planName, newParams, NULL_CTX, getSerialVersion());
    }

    /**
     * Create a new Plan to add a user of kvstore.
     */
    public int createCreateUserPlan(String planName,
                                    String userName,
                                    boolean isEnabled,
                                    boolean isAdmin,
                                    char[] plainPassword)
        throws RemoteException {
        return proxyRemote.createCreateUserPlan(planName, userName, isEnabled,
                                                isAdmin, plainPassword,
                                                NULL_CTX, getSerialVersion());
    }

    /**
     * Create a new Plan to move all services from the old storage node to a
     * new storage node.
     */
    public int createMigrateSNPlan(String planName,
                                   StorageNodeId oldNode,
                                   StorageNodeId newNode,
                                   int newHttpPort)
        throws RemoteException {

        return proxyRemote.createMigrateSNPlan(planName, oldNode, newNode,
                                               newHttpPort, NULL_CTX,
                                               getSerialVersion());
    }

    /**
     * Create a new Plan to remove a storageNode from the store. The SN must
     * be stopped and must not host any services.
     */
    public int createRemoveSNPlan(String planName, StorageNodeId targetNode)
        throws RemoteException {

        return proxyRemote.createRemoveSNPlan(planName, targetNode,
                                              NULL_CTX, getSerialVersion());
    }

    /**
     * Create a new Plan to remove a datacenter from the store. The
     * datacenter must be empty.
     */
    public int createRemoveDatacenterPlan(String planName,
                                          DatacenterId targetId)
        throws RemoteException {

        return proxyRemote.createRemoveDatacenterPlan(
            planName, targetId, NULL_CTX, getSerialVersion());
    }

    /**
     * Create a plan that will alter the topology. Topology candidates are
     * created with the CLI topology
     * {create|rebalance|redistribute|change-repfactor|move-repnode| commands.
     */
    public int createDeployTopologyPlan(String planName, String candidateName)
        throws RemoteException {

        return proxyRemote.createDeployTopologyPlan
            (planName, candidateName, NULL_CTX, getSerialVersion());
    }


    /**
     * Create a plan that will repair mismatches in the topology.
     */
    public int createRepairPlan(String planName)
        throws RemoteException {
        return proxyRemote.createRepairPlan(planName, NULL_CTX,
                                            getSerialVersion());
    }

    public int createAddTablePlan(String planName,
                                  String tableId,
                                  String parentName,
                                  FieldMap fieldMap,
                                  List<String> primaryKey,
                                  List<String> majorKey,
                                  boolean r2compat,
                                  int schemaId,
                                  String description)
        throws RemoteException {

        return proxyRemote.createAddTablePlan(planName, tableId,
                                              parentName, fieldMap,
                                              primaryKey, majorKey,
                                              r2compat, schemaId, description,
                                              NULL_CTX,
                                              getSerialVersion());
    }

    public int createRemoveTablePlan(final String planName,
                                     final String tableName,
                                     final boolean removeData)
        throws RemoteException {

        return proxyRemote.createRemoveTablePlan(planName, tableName,
                                                 removeData,
                                                 NULL_CTX,
                                                 getSerialVersion());
    }

    public int createAddIndexPlan(String planName,
                                  String indexName,
                                  String tableName,
                                  String[] indexedFields,
                                  String description)
        throws RemoteException {

        return proxyRemote.createAddIndexPlan(planName, indexName,
                                              tableName,
                                              indexedFields, description,
                                              NULL_CTX,
                                              getSerialVersion());
    }

    public int createRemoveIndexPlan(String planName,
                                     String indexName,
                                     String tableName)
        throws RemoteException {

        return proxyRemote.createRemoveIndexPlan(planName, indexName,
                                                 tableName,
                                                 NULL_CTX,
                                                 getSerialVersion());
    }

    public int createEvolveTablePlan(String planName,
                                     String tableName,
                                     int tableVersion,
                                     FieldMap fieldMap)
        throws RemoteException {

        return proxyRemote.createEvolveTablePlan(planName,
                                                 tableName,
                                                 tableVersion, fieldMap,
                                                 NULL_CTX,
                                                 getSerialVersion());
    }

   /**
     * Configure the Admin with a store name.  This command can be used only
     * when the AdminService is running in bootstrap/configuration mode.
     */
    public void configure(String storeName)
        throws RemoteException {

        proxyRemote.configure(storeName, NULL_CTX, getSerialVersion());
    }

    /**
     * If configured, return the store name, otherwise, null.
     */
    public String getStoreName()
        throws RemoteException {

        return proxyRemote.getStoreName(NULL_CTX, getSerialVersion());
    }

    /**
     * Return the pathname of the KV root directory (KVHOME).
     */
    public String getRootDir()
        throws RemoteException {

        return proxyRemote.getRootDir(NULL_CTX, getSerialVersion());
    }

    /**
     * Return the current realized topology for listing or browsing.
     */
    public Topology getTopology()
        throws RemoteException {

        return proxyRemote.getTopology(NULL_CTX, getSerialVersion());
    }

    /**
     * Return the brief and detailed description of all users for showing
     *
     *@return a sorted map of {username, {brief info, detailed info}}
     */
    public Map<String, UserDescription> getUsersDescription()
        throws RemoteException {

        return proxyRemote.getUsersDescription(NULL_CTX, getSerialVersion());
    }

    /**
     * Return the specified Metadata for listing or browsing.
     */
    public <T extends Metadata<? extends MetadataInfo>> T
                                         getMetadata(final Class<T> returnType,
                                                     final MetadataType metadataType)
        throws RemoteException {

        return proxyRemote.getMetadata(returnType, metadataType,
                                       NULL_CTX, getSerialVersion());
    }

    /**
     * Retrieve the topology that corresponds to this candidate name.  Invoked
     * with the "topology view candidateName" command.
     */
    public TopologyCandidate getTopologyCandidate(String candidateName)
        throws RemoteException {

        return proxyRemote.getTopologyCandidate(candidateName, NULL_CTX,
                                                getSerialVersion());
    }

    /**
     * Return the whole Parameters for listing or browsing.
     */
    public Parameters getParameters()
        throws RemoteException {

        return proxyRemote.getParameters(NULL_CTX, getSerialVersion());
    }

    /**
     * Return the RepNodeParameters for the specified node.
     */
    public ParameterMap getRepNodeParameters(RepNodeId id)
        throws RemoteException {

        return proxyRemote.getRepNodeParameters(
            id, NULL_CTX, getSerialVersion());
    }

    /**
     * Return the Policy parameters from the admin.
     */
    public ParameterMap getPolicyParameters()
        throws RemoteException {

        return proxyRemote.getPolicyParameters(NULL_CTX, getSerialVersion());
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
     * Stop the admin service.
     */
    public void stop(boolean force)
        throws RemoteException {

        proxyRemote.stop(force, NULL_CTX, getSerialVersion());
    }

    /**
     * Set the policy parameters.
     */
    public void setPolicies(ParameterMap policyParams)
        throws RemoteException {

        proxyRemote.setPolicies(policyParams, NULL_CTX, getSerialVersion());
    }

    /**
     * Return the current health status for each component.
     */
    public Map<ResourceId, ServiceChange> getStatusMap()
        throws RemoteException {

        return proxyRemote.getStatusMap(NULL_CTX, getSerialVersion());
    }

    /**
     * Return the current performance status for each component.
     */
    public Map<ResourceId, PerfEvent> getPerfMap()
        throws RemoteException {

        return proxyRemote.getPerfMap(NULL_CTX, getSerialVersion());
    }

    /**
     * Return the status reporting events that have occurred since a point in
     * time.
     */
    public RetrievedEvents<ServiceChange> getStatusSince(long since)
        throws RemoteException {

        return proxyRemote.getStatusSince(since, NULL_CTX, getSerialVersion());
    }

    /**
     * Return the performance reporting events that have occurred since a point
     * in time.
     */
    public RetrievedEvents<PerfEvent> getPerfSince(long since)
        throws RemoteException {

        return proxyRemote.getPerfSince(since, NULL_CTX, getSerialVersion());
    }

    /**
     * Return the log records that have been logged since a point in time.
     */
    public RetrievedEvents<LogRecord> getLogSince(long since)
        throws RemoteException {

        return proxyRemote.getLogSince(since, NULL_CTX, getSerialVersion());
    }

    /**
     * Return the plan state change events that have occured since a point in
     * time.
     */
    public RetrievedEvents<PlanStateChange> getPlanSince(long since)
        throws RemoteException {

        return proxyRemote.getPlanSince(since, NULL_CTX, getSerialVersion());
    }

    public void registerLogTrackerListener(TrackerListener tl)
        throws RemoteException {

        proxyRemote.registerLogTrackerListener(
            tl, NULL_CTX, getSerialVersion());
    }

    public void removeLogTrackerListener(TrackerListener tl)
        throws RemoteException {

        proxyRemote.removeLogTrackerListener(tl, NULL_CTX, getSerialVersion());
    }

    public void registerStatusTrackerListener(TrackerListener tl)
        throws RemoteException {

        proxyRemote.registerStatusTrackerListener(
            tl, NULL_CTX, getSerialVersion());
    }

    public void removeStatusTrackerListener(TrackerListener tl)
        throws RemoteException {

        proxyRemote.removeStatusTrackerListener(tl, NULL_CTX, getSerialVersion());
    }

    public void registerPerfTrackerListener(TrackerListener tl)
        throws RemoteException {

        proxyRemote.registerPerfTrackerListener(
            tl, NULL_CTX, getSerialVersion());
    }

    public void removePerfTrackerListener(TrackerListener tl)
        throws RemoteException {

        proxyRemote.removePerfTrackerListener(tl, NULL_CTX, getSerialVersion());
    }

    public void registerPlanTrackerListener(TrackerListener tl)
        throws RemoteException {

        proxyRemote.registerPlanTrackerListener(
            tl, NULL_CTX, getSerialVersion());
    }

    public void removePlanTrackerListener(TrackerListener tl)
        throws RemoteException {

        proxyRemote.removePlanTrackerListener(tl, NULL_CTX, getSerialVersion());
    }

    public Map<String, Long> getLogFileNames()
        throws RemoteException {

        return proxyRemote.getLogFileNames(NULL_CTX, getSerialVersion());
    }

    public ReplicatedEnvironment.State getAdminState()
        throws RemoteException {

        return proxyRemote.getAdminState(NULL_CTX, getSerialVersion());
    }

    public URI getMasterRmiAddress()
        throws RemoteException {

        return proxyRemote.getMasterRmiAddress(NULL_CTX, getSerialVersion());
    }

    public URI getMasterHttpAddress()
        throws RemoteException {

        return proxyRemote.getMasterHttpAddress(NULL_CTX, getSerialVersion());
    }

    public List<CriticalEvent> getEvents(long startTime,
                                         long endTime,
                                         CriticalEvent.EventType type)
        throws RemoteException {

        return proxyRemote.getEvents(startTime, endTime, type, NULL_CTX,
                                     getSerialVersion());
    }

    public CriticalEvent getOneEvent(String eventId)
        throws RemoteException {

        return proxyRemote.getOneEvent(eventId, NULL_CTX, getSerialVersion());
    }

    public String [] startBackup()
        throws RemoteException {

        return proxyRemote.startBackup(NULL_CTX, getSerialVersion());
    }

    public long stopBackup()
        throws RemoteException {

        return proxyRemote.stopBackup(NULL_CTX, getSerialVersion());
    }

    public void updateMemberHAAddress(AdminId targetId,
                                      String targetHelperHosts,
                                      String newNodeHostPort)
        throws RemoteException {

        proxyRemote.updateMemberHAAddress(targetId,
                                          targetHelperHosts,
                                          newNodeHostPort,
                                          NULL_CTX, getSerialVersion());
    }

    /**
     * Run verification on the current topology.
     */
    public VerifyResults verifyConfiguration(boolean showProgress,
                                             boolean listAll)
        throws RemoteException {

        return proxyRemote.verifyConfiguration(showProgress,
                                               listAll,
                                               NULL_CTX, getSerialVersion());
    }

    private static final short UPGRADE_INITIAL_SERIAL_VERSION =
                                                        SerialVersion.V3;

    /**
     * Run upgrade check on the current topology.
     */
    public VerifyResults verifyUpgrade(KVVersion targetVersion,
                                       List<StorageNodeId> snIds,
                                       boolean showProgress, boolean listAll)
        throws RemoteException {

        checkMethodSupported(UPGRADE_INITIAL_SERIAL_VERSION);

        return proxyRemote.verifyUpgrade(targetVersion, snIds,
                                         showProgress, listAll,
                                         NULL_CTX, getSerialVersion());
    }

    public VerifyResults verifyPrerequisite(KVVersion targetVersion,
                                            KVVersion prerequisiteVersion,
                                            List<StorageNodeId> snIds,
                                            boolean showProgress,
                                            boolean listAll)
        throws RemoteException {

        checkMethodSupported(UPGRADE_INITIAL_SERIAL_VERSION);

        return proxyRemote.verifyPrerequisite(targetVersion,
                                              prerequisiteVersion,
                                              snIds,
                                              showProgress, listAll,
                                              NULL_CTX, getSerialVersion());
    }

    /**
     * Get a list of nodes to upgrade in an order which will maintain
     * store availability.
     */
    public String getUpgradeOrder(KVVersion targetVersion,
                                  KVVersion prerequisiteVersion)
        throws RemoteException {

        checkMethodSupported(UPGRADE_INITIAL_SERIAL_VERSION);

        return proxyRemote.getUpgradeOrder(targetVersion,
                                           prerequisiteVersion,
                                           NULL_CTX, getSerialVersion());
    }

    public LoadParameters getParams()
        throws RemoteException {

        return proxyRemote.getParams(NULL_CTX, getSerialVersion());
    }

    public String getStorewideLogName()
        throws RemoteException {

        return proxyRemote.getStorewideLogName(NULL_CTX, getSerialVersion());
    }

    /**
     * List realized topologies with the "show topology history" command.
     */
    public List<String> getTopologyHistory(boolean concise)
        throws RemoteException {

        return proxyRemote.getTopologyHistory(
            concise, NULL_CTX, getSerialVersion());
    }

    /** First serial version where Avro methods were made available. */
    private static final short AVRO_INITIAL_SERIAL_VERSION = SerialVersion.V2;

    public SortedMap<String, AvroDdl.SchemaSummary>
        getSchemaSummaries(boolean includeDisabled)
        throws RemoteException {

        checkMethodSupported(AVRO_INITIAL_SERIAL_VERSION);

        return proxyRemote.getSchemaSummaries(includeDisabled, NULL_CTX,
                                              getSerialVersion());
    }

    public AvroDdl.SchemaDetails getSchemaDetails(int schemaId)
        throws RemoteException {

        checkMethodSupported(AVRO_INITIAL_SERIAL_VERSION);

        return proxyRemote.getSchemaDetails(
            schemaId, NULL_CTX, getSerialVersion());
    }

    public AvroDdl.AddSchemaResult addSchema(AvroSchemaMetadata metadata,
                                             String schemaText,
                                             AvroDdl.AddSchemaOptions options)
        throws RemoteException {

        checkMethodSupported(AVRO_INITIAL_SERIAL_VERSION);

        return proxyRemote.addSchema(metadata, schemaText, options,
                                     NULL_CTX, getSerialVersion());
    }

    public boolean updateSchemaStatus(int schemaId, AvroSchemaMetadata newMeta)
        throws RemoteException {

        checkMethodSupported(AVRO_INITIAL_SERIAL_VERSION);

        return proxyRemote.updateSchemaStatus(schemaId, newMeta,
                                              NULL_CTX, getSerialVersion());
    }

    /**
     * Return a status description of a running plan. See StatusReport for
     * how to set options bits.
     */
    public String getPlanStatus(int planId,
                                long options)
        throws RemoteException {

        return proxyRemote.getPlanStatus(planId, options, NULL_CTX,
                                         getSerialVersion());
    }

    /**
     * If a candidate name is specified, validate that topology. If
     * candidateName is null, validate the current, deployed topology.
     * @return a display of the validation results
     */
    public String validateTopology(String candidateName)
        throws RemoteException {
        return proxyRemote.validateTopology(candidateName, NULL_CTX,
                                            getSerialVersion());
    }

    /**
     * Move a RN off its current SN. If the a target SN is specified, it will
     * only attempt move to that SN. This is an unadvertised option. If no
     * target is specified (if snId is null), the system will choose a new SN.
     */
    public String moveRN(String candidateName, RepNodeId rnId,
                         StorageNodeId snId)
        throws RemoteException {
        return proxyRemote.moveRN(candidateName, rnId, snId, NULL_CTX,
                                  getSerialVersion());
    }

    public void installStatusReceiver(AdminStatusReceiver asr)
        throws RemoteException {

        /*
         * There is no need to check protocol versions; this method is used
         * only by the local storage node.  THe Admin and StorageNode will
         * always be upgraded together.
         */
        proxyRemote.installStatusReceiver(asr, NULL_CTX, getSerialVersion());
    }

    /**
     * Create a new Plan to alter a user's information.
     */
    public int createChangeUserPlan(String planName,
                                    String userName,
                                    Boolean isEnabled,
                                    char[] newPlainPassword,
                                    boolean retainPassword,
                                    boolean clearRetainedPassword)
        throws RemoteException {

        return proxyRemote.createChangeUserPlan(
            planName, userName, isEnabled, newPlainPassword, retainPassword,
            clearRetainedPassword, NULL_CTX, getSerialVersion());
    }

    public int createDropUserPlan(String planName, String userName)
        throws RemoteException {
        return proxyRemote.createDropUserPlan(planName, userName,
                                              NULL_CTX, getSerialVersion());
    }

    public boolean verifyUserPassword(String userName, char[] password)
        throws RemoteException {
        return proxyRemote.verifyUserPassword(userName, password,
                                              NULL_CTX, getSerialVersion());

    }

    public int[] getPlanIdRange(final long startTime,
                                final long endTime,
                                final int howMany)
        throws RemoteException {

        return proxyRemote.getPlanIdRange
            (startTime, endTime, howMany, NULL_CTX, getSerialVersion());
    }

    public Map<Integer, Plan> getPlanRange(final int firstPlanId,
                                           final int howMany)
        throws RemoteException {

        return proxyRemote.getPlanRange(firstPlanId, howMany,
                                        NULL_CTX, getSerialVersion());
    }
}
