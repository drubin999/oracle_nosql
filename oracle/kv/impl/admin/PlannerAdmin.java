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

import java.util.Set;
import java.util.concurrent.TimeUnit;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.DatacenterParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.DeploymentInfo;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.TopologyPlan;
import oracle.kv.impl.admin.topo.TopologyCandidate;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.monitor.MonitorKeeper;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;

/**
 * Services provided by the GAT to the Planner are described here.
 */
public interface PlannerAdmin extends MonitorKeeper {

    /**
     * Returns a cloned copy of the latest topology. Will be used by {@link
     * TopologyPlan} to build up the topology changes from plan execution.
     */
    Topology getCurrentTopology();

    /**
     * Returns the current Params.
     */
    AdminServiceParams getParams();

    /**
     * Save this value for use as the next plan id. This value is retrieved
     * from the database and given to the Planner at construction.
     */
    void saveNextId(int nextId);

    /**
     * Persist this plan. The plan may or may not be finished.
     */
    void savePlan(Plan plan, String cause);

    /*
     * The following savePlanResults methods should assert that the new Topo is
     * newer than the current version.
     */

    /**
     * Persist a new topology after a elasticity plan runs.
     */
    void saveTopo(Topology topo, DeploymentInfo info, Plan plan);

    /**
     * Persist a new topology and params after a DeployDatacenterPlan runs.
     */
    void saveTopoAndParams(Topology topo,
                           DeploymentInfo info,
                           DatacenterParams params,
                           Plan plan);

    /**
     * Persist a new topology and params after a DeploySNPlan runs.
     */
    void saveTopoAndParams(Topology topo,
                           DeploymentInfo info,
                           StorageNodeParams snp,
                           GlobalParams gp,
                           Plan plan);

    /**
     * Persist a new topology and params as one transaction after a
     * MigrateStorageNode plan.
     */
    void saveTopoAndParams(Topology topo,
                           DeploymentInfo info,
                           Set<RepNodeParams> repNodeParams,
                           Set<AdminParams> adminParams,
                           Plan plan);

    /**
     * Persist a new set of params as one transaction after a
     * MigrateStorageNode plan.
     */
    void saveParams(Set<RepNodeParams> repNodeParams,
                    Set<AdminParams> adminParams);

    /**
     * Persist a new topology and remove an SN from the params and storage
     * node pools, as one atomic action. Because our policy is to restrict
     * modifying the Parameters instance to the Admin, we can only supply
     * a StorageNodeId and ask the Admin to do the work.
     */
    void saveTopoAndRemoveSN(Topology topology,
                             DeploymentInfo info,
                             StorageNodeId target,
                             Plan plan);

    /**
     * Persist a new topology and params after a RemoveDatacenterPlan runs.
     */
    void saveTopoAndRemoveDatacenter(Topology topology,
                                     DeploymentInfo info,
                                     DatacenterId targetId,
                                     Plan plan);

    /**
     * Save the topology and a single RN params after deploying a single RN.
     */
    void saveTopoAndRNParam(Topology topology,
                            DeploymentInfo info,
                            RepNodeParams rnp,
                            Plan plan);

    /**
     * Persist new AdminParams following successful deployment.
     */
    void addAdminParams(AdminParams ap);

    /**
     * Remove AdminParams associated with the give AdminId.
     */
    void removeAdminParams(AdminId aid);

    /**
     * Persist a new RepNodeParams after a ChangeParamsPlan.
     */
    void updateParams(RepNodeParams rnp);

    /**
     * Persist StorageNodeParams and GlobalParams after deploying a SN.
     */
    void updateParams(StorageNodeParams snp, GlobalParams gp);

    /*
     * Persist a new AdminParams after deploying an Admin.
     */
    void updateParams(AdminParams ap);

    /*
     * Persist a new GlobalParams after a ChangeGlobalSecurityPlan.
     */
    void updateParams(GlobalParams gp);

    /**
     * Returns the StorageNodeParams for the target storage node.
     * @param targetSN
     * @return the relevant StorageNodeParams instance
     */
    StorageNodeParams getStorageNodeParams(StorageNodeId targetSNId);

    /**
     * Returns the RepNodeParams for the target RepNode.
     */
    RepNodeParams getRepNodeParams(RepNodeId targetRepNodeId);

    /**
     * Get an AdminId for a new Admin instance in the system.
     */
    AdminId generateAdminId();

    /**
     * Get the current number of Admin instances in the system.
     */
    int getAdminCount();

    /**
     * Gets a new instance of the policy parameter map that can be
     * additionally customized.
     */
    ParameterMap copyPolicy();

    /**
     * Returns a read-only copy of the Parameters.
     */
    Parameters getCurrentParameters();

    /**
     * Ask the Admin to shut down.
     */
    void stopAdminService(boolean force);

    /**
     * Return the name of the storewide log file.
     */
    String getStorewideLogName();

    /**
     * Ensure that the proposed start time for a new plan which creates a
     * RealizedTopology is a value that is > the deployment time of the current
     * topology.
     */
    long validateStartTime(long deployStartTime);

    /**
     * Update the current topology stored within the AdminDB to reflect
     * the migration of the specified partition to a new shard. Done atomically
     * within a single transaction. If the topology is updated true is returned,
     * otherwise false is returned.
     *
     * @return true if the topology was modified
     */
    boolean updatePartition(PartitionId partitionId,
                            RepGroupId targetRGId,
                            DeploymentInfo info,
                            Plan plan);

    /**
     * Wait for a plan to finish.
     */
    Plan.State awaitPlan(int id, int timeout, TimeUnit timeoutUnit);

    /**
     * Get a topology candidate from the Admin's topo store.
     */
    TopologyCandidate getCandidate(String candidateName);

    /**
     * Remove an Admin replica from its db group.
     * @param victim The id of the Admin replica to be removed.
     */
    void removeAdminFromRepGroup(AdminId victim);

    /**
     * Cause the mastership of the Admin db group to move to another member.
     */
    void transferMaster();

    /**
     * Checks whether all of the nodes in the store are at or above the minor
     * version of {@code requiredVersion}.  Returns {@code true} if the
     * required version is supported by all nodes, and {@code false} if at
     * least one node does not support the required version.  Throws {@link
     * AdminFaultException} if there is a problem determining the versions of
     * the storage nodes or if there is a node does not support the
     * prerequisite version.  Once {@code true} is returned for a given
     * version, future calls to this method will always return {@code true}.
     *
     * <p>Version dependent features should keep calling this method until the
     * required feature is enabled.  Of course, features should be careful to
     * not constantly call these methods at a high rate.
     *
     * @param requiredVersion version to check against
     * @return {@code true} if all of the nodes in the store meet the required
     *         version, and {@code false} if a least one node does not
     * @throws AdminFaultException if there was a problem checking versions or
     *         a node does not support the prerequisite version
     */
    boolean checkStoreVersion(KVVersion requiredVersion);

    /**
     * Gets the highest minor version that all of the nodes in the store are
     * known to be at and support. Throws {@link AdminFaultException} if there
     * is a problem determining the versions of the storage nodes or if there
     * is a node does not support the prerequisite version. Note that nodes may
     * be at different patch levels and this method returns the lowest patch
     * level found.
     *
     * <p>When using this method to enable version sensitive features, callers
     * should compare the returned version to the desired version using {@link
     * oracle.kv.impl.util.VersionUtil#compareMinorVersion}.
     *
     * <p>Note that it is assumed that new nodes added to the store will be at
     * the latest version. This method will not detect new nodes being added
     * which have older software versions.
     *
     * @return the highest minor version that all of the nodes support
     * @throws AdminFaultException if there was a problem checking versions or
     *         a node does not support the prerequisite version
     */
    KVVersion getStoreVersion();
    
    /**
     * Gets the metadata object of the specified type.
     *
     * @param returnType the metadata class to return
     * @param metadataType the metadata type
     * @return a metadata object
     */
     <T extends Metadata<? extends MetadataInfo>> T
                                        getMetadata(Class<T> returnType,
                                                    MetadataType metadataType);

    /**
     * Saves the specified metadata object in the admin's store.
     *
     * @param metadata a metadata object
     * @param plan the plan
     */
    void saveMetadata(Metadata<?> metadata, Plan plan);

    /**
     * Gets the LoginManager for the admin
     */
    LoginManager getLoginManager();
    
    /**
     * Save the topology and remove the specified RN from the params. 
     */
    void saveTopoAndRemoveRN(Topology topo,
                             DeploymentInfo deploymentInfo,
                             RepNodeId rnId,
                             Plan plan);
}
