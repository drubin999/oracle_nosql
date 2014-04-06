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

package oracle.kv.impl.admin.plan;

import java.util.List;
import java.util.Set;

import oracle.kv.impl.admin.PlannerAdmin;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.topo.TopologyCandidate;
import oracle.kv.impl.api.table.FieldMap;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;

/**
 * Defines an interface for making and executing changes to the topology of the
 * KV Store. The planner encapsulates the full set of functionality that exists
 * around changes to the store. This includes deploying a new store, replacing
 * failed nodes, etc.
 */
public interface Planner {

    /**
     * Change the information of a user.
     * @return a plan for changing a kvstore user in the store
     */
    SecurityMetadataPlan createChangeUserPlan(String planName,
                                              String userName,
                                              Boolean isEnabled,
                                              char[] plainPassword,
                                              boolean retainPassword,
                                              boolean clearRetainedPassword);

    /**
     * Create a user and add it to the kvstore.
     * @return a plan for creating a kvstore user in the store
     */
    SecurityMetadataPlan createCreateUserPlan(String planName,
                                              String userName,
                                              boolean isEnabled,
                                              boolean isAdmin,
                                              char[] plainPassword);

    /**
     * Drop a kvstore user.
     * @return a plan for removing a kvstore user.
     */
    SecurityMetadataPlan createDropUserPlan(String planName, String userName);

    /**
     * Adds a data center to the topology.
     * @return a plan for adding the data center to the Topology
     */
    DeployDatacenterPlan createDeployDatacenterPlan(
        String planName,
        String datacenterName,
        int repFactor,
        DatacenterType datacenterType);

    /**
     * Registers a storageNode with the topology.
     */
    DeploySNPlan createDeploySNPlan(String planName,
                                    DatacenterId datacenterId,
                                    StorageNodeParams inputSNP);

    /**
     * Creates a plan for deploying one or more instances of the Global
     * PlannerAdmin Thingy.
     *
     * @param name the plan name
     * @param snId the storage node that will host the GAT
     *
     * @return a plan for deploying the admin instance(s)
     */
    DeployAdminPlan createDeployAdminPlan(String name,
                                          StorageNodeId snId,
                                          int httpPort);

    /**
     * Creates a plan for deploying the specified topology.
     */
    DeployTopoPlan createDeployTopoPlan(String name,
                                        TopologyCandidate candidate);

    /**
     * Creates a plan for replacing a potentially failed node in the KV
     * Store. Any resources known to be allocated on the failed node will be
     * moved to the new node.  The new node must be an as yet unused node.
     *
     * @param name the plan name
     * @param oldNode the node that has failed or is being replaced
     * @param newNode the node that will take over for the old
     * @param newHttpPort the httpPort that should be used by an admin service
     * on the new node
     *
     * @return a {@link AbstractPlan} for replacing the node
     */
    MigrateSNPlan createMigrateSNPlan(String name,
                                      StorageNodeId oldNode,
                                      StorageNodeId newNode,
                                      int newHttpPort);

    /**
     * Creates a plan for removing a storageNode. Removal is only permitted
     * for stopped storageNodes which do not house any services. It's meant to
     * remove defunct storageNodes after a migration has been run, or if
     * an initial deployment failed.
     *
     * @param name the plan name
     * @param targetNode the node that is being removed.
     * @return a {@link AbstractPlan} for removing the node.
     */
    RemoveSNPlan createRemoveSNPlan(String name, StorageNodeId targetNode);

    /**
     * Creates a plan for removing a datacenter. Removal is only permitted for
     * <em>empty</em> datacenters; that is, datacenters which contain no
     * storage nodes.
     *
     * @param name the plan name
     * @param targetId the id of the datacenter to remove.
     * @return an {@link AbstractPlan} for removing the desired datacenter.
     */
    RemoveDatacenterPlan createRemoveDatacenterPlan(String name,
                                                    DatacenterId targetId);

    public Plan createAddTablePlan(String planName,
                                   String tableId,
                                   String parentName,
                                   FieldMap fieldMap,
                                   List<String> primaryKey,
                                   List<String> majorKey,
                                   boolean r2compat,
                                   int schemaId,
                                   String description);

    public Plan createEvolveTablePlan(String planName,
                                      String tableName,
                                      int tableVersion,
                                      FieldMap fieldMap);

    public Plan createRemoveTablePlan(String planName,
                                      String tableName,
                                      boolean removeData);

    public Plan createAddIndexPlan(String planName,
                                   String indexName,
                                   String tableName,
                                   String[] indexedFields,
                                   String description);

    public Plan createRemoveIndexPlan(String planName,
                                      String indexName,
                                      String tableName);

    /**
     * Stop all repNodes in the kvstore cleanly. RepNodes will not be
     * automatically restarted by the SNA; only an explicit startRepNodes
     * plan will restart them.
     * @param name
     */
    StopAllRepNodesPlan createStopAllRepNodesPlan(String name);

    /**
     * Restart all repNodes in the kvstore.
     * @param name
     */
    StartAllRepNodesPlan createStartAllRepNodesPlan(String name);

    /**
     * Stop the given set of RepNodes.
     */
    StopRepNodesPlan
        createStopRepNodesPlan(String planName, Set<RepNodeId> rnids);

    /**
     * Start the given set of RepNodes.
     */
    StartRepNodesPlan
        createStartRepNodesPlan(String planName, Set<RepNodeId> rnids);

    /**
     * Apply new parameters for the specified resource.
     *
     * @param planName
     * @param rid
     * @param newParams
     */
    Plan createChangeParamsPlan
        (String planName, ResourceId rid, ParameterMap newParams);

    /**
     * Apply new parameters for all RepNodes deployed to the specified
     * datacenter.
     *
     * @param planName the name of the plan to create
     * @param dcid apply the desired parameter change to all RepNodes deployed
     *        to the datacenter with <code>DatacenterId</code> equal to the
     *        value specified for this parameter; where if <code>null</code>
     *        is input, then the specified parameter change will be applied
     *        to all RepNodes in all datacenters
     * @param newParams map containing the desired changes to apply
     */
    Plan createChangeAllParamsPlan(String planName,
                                   DatacenterId dcid,
                                   ParameterMap newParams);


    /**
     * Apply new parameters for all Admins deployed to the specified
     * datacenter.
     *
     * @param planName the name of the plan to create
     * @param dcid apply the desired parameter change to all Admins deployed
     *        to the datacenter with <code>DatacenterId</code> equal to the
     *        value specified for this parameter; where if <code>null</code>
     *        is input, then the specified parameter change will be applied
     *        to all Admins in all datacenters
     * @param newParams map containing the desired changes to apply
     */
    Plan createChangeAllAdminsPlan(String planName,
                                   DatacenterId dcid,
                                   ParameterMap newParams);

    /**
     * Apply new global security parameters for all services deployed in the
     * store.
     *
     * @param planName the name of the plan to create
     * @param newParams map containing the desired changes to apply
     */
    Plan createChangeGlobalSecurityParamsPlan(String planName,
                                              ParameterMap newParams);

    /**
     * Start asynchronous plan execution. To be notified of when a plan has
     * finished execution, register a listener. Only plans that are in the
     * {@link Plan.State#APPROVED} state may be submitted to be executed.
     *
     * @param plan the plan to run
     * @param force if false, the plan will check if the proposed topology
     * introduces new topology violations. If it does, the plan will not
     * execute. If true, the plan will skip any violation checks.
     */
    PlanRun executePlan(Plan plan, boolean force);

    /**
     */
    void approvePlan(int planId);

    /**
     * Cancel a PENDING or APPROVED plan.
     */
    void cancelPlan(int planId);

    /**
     * If the plan is running, stop it and mark it as interrupted. The user
     * will have to retry or rollback the plan.
     */
    void interruptPlan(int planId);

    /**
     * Accessor for the PlannerAdmin.
     */
    PlannerAdmin getAdmin();

    /**
     * TODO.
     * @param PlanId
     * @param planName
     */
    void lockElasticity(int planId, String planName);

    void lockRN(int id, String name, RepNodeId rnId);

    void lockShard(int planId, String planName, RepGroupId rgId);

    void  clearLocks(int planId);

    /**
     * If <code>aid</code> is not <code>null</code>, then removes the Admin
     * with specified <code>AdminId</code>. Otherwise, if <code>dcid</code> is
     * not <code>null</code>, then removes all Admins in the specified
     * datacenter.
     */
    RemoveAdminPlan createRemoveAdminPlan(String name,
                                          DatacenterId dcid,
                                          AdminId aid);

    /**
     * Create a plan that verifies and repairs the topology.
     * @param planName
     */
    RepairPlan createRepairPlan(String planName);

    /**
     * The Planner maintains a cache of active Plans, which is available to the
     * Admin.
     */
    Plan getCachedPlan(int planId);
}
