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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.AdminServiceParams;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.NonfatalAssertionException;
import oracle.kv.impl.admin.PlannerAdmin;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.topo.TopologyCandidate;
import oracle.kv.impl.api.table.FieldMap;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * Provides a basic implementation of a planner, which creates and executes
 * plans. Plan creation consists of populating the plan with tasks. Plan
 * execution is asynchronous.
 *
 * Error Handling
 * ==============
 * IllegalCommandException is used to indicate user error, such as a bad
 * parameter, or a user-provoked illegal plan transition. It is thrown
 * synchronously, in direct response to a user action. Examples:
 *  Bad parameter when creating a plan:
 *  - The user should fix the parameter and resubmit the
 *  User tries an illegal plan state transition, such as executing a plan that
 *  is not approved, approving a plan that is not pending, or executing a plan
 *  that has completed, etc.
 *  - The user should be notified that this was an illegal action
 *
 * OperationFaultException is thrown when plan execution runs into some kind of
 * resource problem, such as a RejectedExecutionException from lack of threads,
 * or a network problem, lack of ports, timeout, etc. In this case, the user is
 * notified and the GUI will present the option of retrying or rolling back the
 * plan.
 *
 * An AdminFaultException is thrown when an unexpected exception occurs during
 * plan execution. The fault handler processes the exception in such a way that
 * the Admin will not go down, but that the exception will be logged under
 * SEVERE and will be dumped to stderr. The problem is not going to get any
 * better without installing a bug fix, but the Admin should not go down.
 * The UI presents the option of retrying or rolling back the plan.
 *
 * Concurrency Limitations:
 * ========================
 * Plans may be created and approved for an indeterminate amount of
 * time before they are executed. However, topology dependent plans must clone
 * a copy of the topology at creation time, and use that to create a set of
 * directions to execute. Because of that, the topology must stay constant from
 * that point to execution point, and therefore only one topology changing
 * plan can be implemented at a time.
 *
 * Synchronization:
 * ========================

 * Methods in this class synchronize on the sole BasicPlanner object when
 * manipulating the two collections of plans: the Catalog in this class, and
 * the persistent collection of plans in the Admin database.  Code blocks in
 * admin.Admin also synchronize on this object for the same reason. We also
 * synchronize on this object when updating individual plans that are already
 * in the collections; however we are considering whether that synchronization
 * can be eliminated since it does not affect either collection of plans.
 * Elimination of that synchronization would simplify the monitor locking
 * hierarchy, which at the moment is had to articulate in a straightforward
 * way.
 *
 * In general, we would like to express the monitor locking sequence as going
 * from big objects to smaller objects, so the Admin would be locked before the
 * Planner, and the Planner before the Plan.  However we observe that in some
 * cases the plan is locked and then wants to update itself, requiring it the
 * thread to synchronize on the Planner, violating the aforementioned ideal.
 * Hence the desire to eliminate synchronization on the Planner for updates to
 * existing plans.  This is a TBD: see deadlocks described in [#22963] and
 * [#22992], both of which we believe have been eliminated, but other deadlocks
 * might be lurking, and therefore a comprehensive survey of synchronization in
 * Admin is called for.
 */

public class BasicPlannerImpl implements Planner {

    /**
     * The executor that we'll use for carrying out execution of the plan and
     * the tasks within it.
     */
    private final ExecutorService executor;

    private final Logger logger;
    private final PlannerAdmin plannerAdmin;
    private final AtomicInteger planIdGenerator;

    private final Catalog catalog;

    /**
     */
    public BasicPlannerImpl(PlannerAdmin plannerAdmin,
                            AdminServiceParams params,
                            int nextPlanId) {

        this.plannerAdmin = plannerAdmin;
        logger = LoggerUtils.getLogger(this.getClass(), params);
        executor = Executors.newCachedThreadPool
            (new KVThreadFactory("Planner", logger));
        catalog = new Catalog();
        planIdGenerator = new AtomicInteger(nextPlanId);
    }

    /**
     * Review all in progress plans. Anything that is in RUNNING state did
     * not finish, and should be deemed to be interrupted. Should be called
     * by the Admin explicitly after the planner is constructed.
     * 1.RUNNING plans ->INTERRUPT_REQUESTED -> INTERRUPTED, and
     *    will be restarted.
     * 2.INTERRUPT_REQUESTED plans -> INTERRUPTED and are not restarted. The
     *    failover is as if the cleanup phase was interrupted by the user.
     * 3.INTERRUPTED plans are left as is.
     */
    public Plan recover(Plan inProgressPlan) {
        if (inProgressPlan == null) {
            return null;
        }

        Plan restart = null;
        final Plan.State originalState = inProgressPlan.getState();
        if (inProgressPlan.getState() == Plan.State.RUNNING) {
            inProgressPlan.markAsInterrupted();
            /* Rerun it */
            restart = inProgressPlan;
        }

        if (inProgressPlan.getState() == Plan.State.INTERRUPT_REQUESTED) {
            /*
             * Let it move to interrupted state and stay there. The user had
             * previously requested an interrupt.
             */
            inProgressPlan.markAsInterrupted();
        }

        logger.log(Level.INFO,
                   "{0} originally in {1}, transitioned to {2}, {3} be " +
                   "restarted automatically",
                   new Object[] {inProgressPlan, originalState,
                                 inProgressPlan.getState(),
                                 (restart == null) ? "will not" : "will"});

        /*
         * All non-terminated plans, including those that are in ERROR or
         * INTERRUPT state should be put in the catalog. Even the
         * non-restarted ones need to be there, so the user can decide manually
         * whether to retry them.
         */
        catalog.addNewPlan(inProgressPlan);
        plannerAdmin.savePlan(inProgressPlan, "Plan Recovery");
        return restart;
    }

    /**
     * Registering a plan is an entry point only needed for testing, or other
     * modes where plans are created outside the PlannerImpl.
     */
    public void register(Plan plan) {
        catalog.addNewPlan(plan);
    }

    /* For unit test support. */
    @Override
    public void clearLocks(int planId) {
        catalog.clearLocks(planId);
    }

    public void shutdown() {
        executor.shutdownNow();
    }

    /*
     * NOTE that all plan creation is serial, so that each type of plan can
     * validate itself in a stable context. The catalog, and plan registration,
     * check for runtime constraints, such as plan exclusiveness, but since
     * registration is only done after the plan is created, it can't validate
     * for non-runtime constraints.  For example, the DeployStorePlan must
     * check whether the topology holds any other repNodes at creation time.
     */

    /**
     * Creates a data center and registers it with the topology.
     * @param planName is a user defined name to identify the plan
     */
    @Override
    public synchronized DeployDatacenterPlan
        createDeployDatacenterPlan(String planName,
                                   String datacenterName,
                                   int repFactor,
                                   DatacenterType datacenterType) {

        final DeployDatacenterPlan plan =
            new DeployDatacenterPlan(planIdGenerator, planName, this,
                                     plannerAdmin.getCurrentTopology(),
                                     datacenterName, repFactor,
                                     datacenterType);
        register(plan);
        return plan;
    }

    /**
     * Creates a storage node and registers it with the topology.
     */
    @Override
    public synchronized DeploySNPlan
        createDeploySNPlan(String planName,
                           DatacenterId datacenterId,
                           StorageNodeParams inputSNP) {

        final DeploySNPlan plan =
            new DeploySNPlan(planIdGenerator, planName, this,
                             plannerAdmin.getCurrentTopology(),
                             datacenterId, inputSNP);
        register(plan);
        return plan;
    }

    /**
     * Creates an Admin instance and updates the Parameters to reflect it.
     * TODO: adminCount is used to derive AdminIds, and must not change
     * from now until this plan has completed.  Enforce this constraint.
     */
    @Override
    public synchronized DeployAdminPlan
        createDeployAdminPlan(String name,
                              StorageNodeId snid,
                              int httpPort) {

        final DeployAdminPlan plan =
            new DeployAdminPlan(planIdGenerator, name, this, snid, httpPort);
        register(plan);
        return plan;
    }

    /**
     * If <code>victim</code> is not <code>null</code>, then removes the Admin
     * with specified <code>AdminId</code>. Otherwise, if <code>dcid</code> is
     * not <code>null</code>, then removes all Admins in the specified
     * datacenter.
     */
    @Override
    public synchronized RemoveAdminPlan
        createRemoveAdminPlan(String name, DatacenterId dcid, AdminId victim) {

        final RemoveAdminPlan plan =
            new RemoveAdminPlan(planIdGenerator, name, this, dcid, victim);
        register(plan);
        return plan;
    }

    /**
     */
    @Override
    public synchronized DeployTopoPlan
        createDeployTopoPlan(String planName,
                             TopologyCandidate candidate) {

        final DeployTopoPlan plan =
            new DeployTopoPlan(planIdGenerator,
                               planName,
                               this,
                               plannerAdmin.getCurrentTopology(),
                               candidate);
        register(plan);
        return plan;
    }

    /**
     * Use to invoke a clean stoppage of all RepNodes in the store. The
     * nodes will be stopped, and will not be restarted by the SNAs until
     * they are restarted.
     */
    @Override
    public synchronized StopAllRepNodesPlan
        createStopAllRepNodesPlan(String planName) {

        final StopAllRepNodesPlan plan =
            new StopAllRepNodesPlan(planIdGenerator, planName, this,
                                    plannerAdmin.getCurrentTopology());
        register(plan);
        return plan;
    }

    /**
     * Use to restart all RepNodes the store. RepNodes that are already
     * nodes will be stopped, and will not be restarted by the SNAs until
     * they are restarted.
     */
    @Override
    public synchronized StartAllRepNodesPlan
        createStartAllRepNodesPlan(String planName) {

        final StartAllRepNodesPlan plan =
            new StartAllRepNodesPlan(planIdGenerator, planName, this,
                                     plannerAdmin.getCurrentTopology());
        register(plan);
        return plan;
    }

    /**
     * Stop the given set of RepNodes.
     */
    @Override
    public synchronized StopRepNodesPlan
        createStopRepNodesPlan(String planName, Set<RepNodeId> rnids) {

        final StopRepNodesPlan plan =
            new StopRepNodesPlan(planIdGenerator, planName, this,
                                 plannerAdmin.getCurrentTopology(), rnids);
        register(plan);
        return plan;
    }

    /**
     * Restart a single RepNode.
     */
    @Override
    public synchronized StartRepNodesPlan
        createStartRepNodesPlan(String planName, Set<RepNodeId> rnids) {

        final StartRepNodesPlan plan =
            new StartRepNodesPlan(planIdGenerator, planName, this,
                                  plannerAdmin.getCurrentTopology(), rnids);
        register(plan);
        return plan;
    }

    @Override
    public synchronized MigrateSNPlan
        createMigrateSNPlan(String planName,
                            StorageNodeId oldNode,
                            StorageNodeId newNode,
                            int newHttpPort) {
        final MigrateSNPlan plan =
            new MigrateSNPlan(planIdGenerator, planName, this,
                              plannerAdmin.getCurrentTopology(),
                              oldNode, newNode, newHttpPort);

        register(plan);
        return plan;
    }

    @Override
    public synchronized RemoveSNPlan
        createRemoveSNPlan(String planName,
                           StorageNodeId targetNode) {
        final RemoveSNPlan plan =
            new RemoveSNPlan(planIdGenerator, planName, this,
                             plannerAdmin.getCurrentTopology(), targetNode);

        register(plan);
        return plan;
    }

    @Override
    public synchronized RemoveDatacenterPlan
        createRemoveDatacenterPlan(String planName,
                                   DatacenterId targetId) {
        final RemoveDatacenterPlan plan =
            new RemoveDatacenterPlan(planIdGenerator, planName, this,
                                     plannerAdmin.getCurrentTopology(),
                                     targetId);

        register(plan);
        return plan;
    }

    @Override
    synchronized public Plan createAddTablePlan(String planName,
                                                String tableId,
                                                String parentName,
                                                FieldMap fieldMap,
                                                List<String> primaryKey,
                                                List<String> majorKey,
                                                boolean r2compat,
                                                int schemaId,
                                                String description) {

        final Plan plan = TablePlanGenerator.
                             createAddTablePlan(planIdGenerator, planName, this,
                                                tableId, parentName,
                                                fieldMap, primaryKey,
                                                majorKey, r2compat, schemaId,
                                                description);
        register(plan);
        return plan;
    }

    @Override
    synchronized public Plan createEvolveTablePlan(String planName,
                                                   String tableName,
                                                   int tableVersion,
                                                   FieldMap fieldMap) {

        final Plan plan = TablePlanGenerator.
                createEvolveTablePlan(planIdGenerator, planName, this,
                                      tableName,
                                      tableVersion, fieldMap);
        register(plan);
        return plan;
    }

    @Override
    public Plan createRemoveTablePlan(String planName,
                                      String tableName,
                                      boolean removeData) {
        final Plan plan = TablePlanGenerator.
                        createRemoveTablePlan(planIdGenerator, planName, this,
                                              plannerAdmin.getCurrentTopology(),
                                              tableName, removeData);
        register(plan);
        return plan;
    }

    @Override
    synchronized public Plan createAddIndexPlan(String planName,
                                                String indexName,
                                                String tableName,
                                                String[] indexedFields,
                                                String description) {
        final Plan plan = TablePlanGenerator.
                           createAddIndexPlan(planIdGenerator, planName, this,
                                              plannerAdmin.getCurrentTopology(),
                                              indexName, tableName,
                                              indexedFields, description);
        register(plan);
        return plan;
    }

    @Override
    synchronized public Plan createRemoveIndexPlan(String planName,
                                                   String indexName,
                                                   String tableName) {
        final Plan plan = TablePlanGenerator.
                      createRemoveIndexPlan(planIdGenerator, planName, this,
                                            plannerAdmin.getCurrentTopology(),
                                            indexName, tableName);
        register(plan);
        return plan;
    }

    /**
     * TODO: future: consolidate change parameters plans for RN, SN, and admin.
     */
    @Override
    public synchronized Plan
        createChangeParamsPlan(String planName,
                               ResourceId rid,
                               ParameterMap newParams) {

        Plan plan = null;
        if (rid instanceof RepNodeId) {
            final Set<RepNodeId> ids = new HashSet<RepNodeId>();
            ids.add((RepNodeId) rid);
            plan =
                new ChangeParamsPlan(planIdGenerator, planName, this,
                                     plannerAdmin.getCurrentTopology(),
                                     ids, newParams);
        } else if (rid instanceof StorageNodeId) {
            plan =
                new ChangeSNParamsPlan(planIdGenerator, planName, this,
                                       (StorageNodeId) rid, newParams);
        } else if (rid instanceof AdminId) {
            plan =
                new ChangeAdminParamsPlan(planIdGenerator, planName, this,
                                          (AdminId) rid, newParams);
        }
        register(plan);
        return plan;
    }

    @Override
    public synchronized Plan
        createChangeAllParamsPlan(String planName,
                                  DatacenterId dcid,
                                  ParameterMap newParams) {

        final Plan plan =
            new ChangeAllParamsPlan(planIdGenerator, planName, this,
                                    plannerAdmin.getCurrentTopology(),
                                    dcid, newParams, logger);
        register(plan);
        return plan;
    }

    @Override
    public synchronized Plan
        createChangeAllAdminsPlan(String planName,
                                  DatacenterId dcid,
                                  ParameterMap newParams) {

        final Plan plan =
            new ChangeAdminParamsPlan(planIdGenerator, planName, this, null,
                                      dcid, plannerAdmin.getCurrentTopology(),
                                      newParams);
        register(plan);
        return plan;
    }

    @Override
    public synchronized Plan
        createChangeGlobalSecurityParamsPlan(String planName,
                                             ParameterMap newParams) {
        final Plan plan =
            new ChangeGlobalSecurityParamsPlan(
                planIdGenerator, planName, this,
                plannerAdmin.getCurrentTopology(), newParams);
        register(plan);
        return plan;
    }

    /**
     * Creates a user and add it to the kvstore.
     */
    @Override
    public synchronized SecurityMetadataPlan
        createCreateUserPlan(String planName,
                             String userName,
                             boolean isEnabled,
                             boolean isAdmin,
                             char[] plainPassword) {

        final SecurityMetadataPlan plan =
                SecurityMetadataPlan.createCreateUserPlan(
                    planIdGenerator, planName, this, userName, isEnabled,
                    isAdmin, plainPassword);
        register(plan);
        return plan;
    }

    /**
     * Change the information of a kvstore user.
     */
    @Override
    public synchronized SecurityMetadataPlan
        createChangeUserPlan(String planName,
                             String userName,
                             Boolean isEnabled,
                             char[] plainPassword,
                             boolean retainPassword,
                             boolean clearRetainedPassword) {

        final SecurityMetadataPlan plan =
                SecurityMetadataPlan.createChangeUserPlan(
                    planIdGenerator, planName, this, userName, isEnabled,
                    plainPassword, retainPassword, clearRetainedPassword);
        register(plan);
        return plan;
    }

    /**
     * Remove a user with the specified name from the store.
     */
    @Override
    public synchronized SecurityMetadataPlan
        createDropUserPlan(String planName, String userName) {

        final SecurityMetadataPlan plan =
                SecurityMetadataPlan.createDropUserPlan(
                    planIdGenerator, planName, this, userName);
        register(plan);
        return plan;
    }

    @Override
    public synchronized RepairPlan createRepairPlan(String planName) {

        final RepairPlan plan = new RepairPlan(planIdGenerator, planName, this);
        register(plan);
        return plan;
    }

    /**
     * Submit a plan for asynchronous execution. If a previous plan is still
     * executing, we will currently throw an exception. In the future, plans
     * may be queued, but queuing would require policies and mechanism to
     * determine what should happen to the rest of the queue if a plan fails.
     * For example, should we "run the next plan, but only if the current
     * succeeds" or ".. regardless of if the current succeeds", etc.
     *
     * Plan execution can be repeated, in order to retry a plan.
     */
    @Override
    public PlanRun executePlan(Plan plan, boolean force) {

        /* For now, a BasicPlanner will only execute an AbstractPlan. */
        final AbstractPlan targetPlan;
        if (plan instanceof AbstractPlan) {
            targetPlan = (AbstractPlan) plan;
        } else {
            throw new NonfatalAssertionException
                ("Unknown Plan type: " + plan.getClass() +
                 " cannot be executed");
        }

        /* Check any preconditions for running the plan */
        targetPlan.validateStartOfRun();

        /* Check that the catalog's rules for running this plan are ok */
        catalog.validateStart(plan);
        PlanRun planRun = null;
        try {
            /*
             * Make sure we can get any plan-exclusion locks we need. Lock
             * before doing checks, to make sure that the topology does not
             * change.
             */
            plan.getCatalogLocks();

            /* Validate that this plan can run */
            plan.preExecuteCheck(force, logger);

            /*
             * Executing a plan equates to executing each of its tasks and
             * monitoring their state.  We'll kick off this process by running a
             * PlanExecutor in another thread.
             */
            planRun = targetPlan.startNewRun();
            final PlanExecutor planExec = new PlanExecutor(plannerAdmin, this,
                                                           targetPlan,
                                                           planRun, logger);

            final Future<Plan.State> future = executor.submit(planExec);

            /*
             * Note that Catalog.addPlanFuture guards against the possibility
             * that the execute thread has finished before the future is added
             * to the catalog.
             */
            catalog.addPlanFuture(targetPlan, future);
        } catch (RejectedExecutionException e) {
            final String problem =
                "Plan did not start, insufficient resources for " +
                "executing a plan";
            if (planRun != null) {
                plan.saveFailure(planRun, e, problem, logger);
            }
            planFinished(targetPlan);
            throw new OperationFaultException(problem, e);
        }

        return planRun;
    }

    /**
     * Used by the PlanExecutor to indicate that it's finished execution.
     */
    void planFinished(Plan plan) {
        catalog.clearLocks(plan.getId());
        catalog.clearPlan(plan);
    }

    @Override
    public PlannerAdmin getAdmin() {
        return plannerAdmin;
    }

    @Override
    public void lockElasticity(int planId, String planName) {
        catalog.lockElasticityChange(planId, planName);
    }

    @Override
    public void lockRN(int planId, String planName, RepNodeId rnId) {
        catalog.lockRN(planId, planName, rnId);
    }

    @Override
    public void lockShard(int planId, String planName, RepGroupId rgId) {
        catalog.lockShard(planId, planName, rgId);
    }

    /**
     * A collection of non-finished plans. It is used to enforce runtime
     * constraints such as:
     * - only one exclusive plan can run at a time
     * - rep nodes can't be targeted by more than one plan at a time.
     * - execution time locks taken. Locks must be acquire for each execution,
     * because the locks are transient and only last of the life of the
     * PlannerImpl.
     *
     * The catalog lets the Admin query plan status, and interrupt running
     * plans.
     * TODO: do we need the notion of exclusivity, if locks are now taken?
     */
    private class Catalog {
        private Plan currentExclusivePlan;
        private Plan currentExecutingPlan;
        private final Map<Integer, Future<Plan.State>> futures;
        private final Map<Integer, Plan> planMap;

        /*
         * Logical locks are used to govern which plans can run concurrently. A
         * single high level elasticity lock is used to ensure that only a
         * single elasticity plan can run at a time. Shard and RN locks
         * coordinate between concurrent elasticity and repair plans. Repair
         * plans are those that might move a single RN or migrate all the
         * components on a single SN.
         *
         * Locking a shard requires first checking for locks against any RNs
         * in the shard. The same applies in reverse; locking a RN is only
         * possible if there is no shard lock. For simplicity, and because this
         * does not need to be a performant action, all serialization is
         * accomplished simply be synchronizing on elasticityLock.
         */
        private final TopoLock elasticityLock;
        private final Map<RepNodeId, TopoLock> rnLocks;
        private final Map<RepGroupId, TopoLock> rgLocks;

        Catalog() {
            planMap = new HashMap<Integer, Plan>();
            futures = new HashMap<Integer, Future<Plan.State>>();

            elasticityLock = new TopoLock();
            rnLocks = new HashMap<RepNodeId, TopoLock>();
            rgLocks = new HashMap<RepGroupId, TopoLock>();
        }

        public void lockElasticityChange(int planId, String planName) {
            if (!elasticityLock.get(planId, planName)) {
                throw cantLock(planId, planName, elasticityLock.lockingPlanId,
                                   elasticityLock.lockingPlanName);
            }
        }

        /**
         * Check the shard locks before locking the RN.
         */
        public void lockRN(int planId, String planName, RepNodeId rnId) {
            synchronized (elasticityLock) {
                final TopoLock rgl =
                    rgLocks.get(new RepGroupId(rnId.getGroupId()));
                if (rgl != null) {
                    if (rgl.lockingPlanId != planId) {
                        throw cantLock(planId, planName, rgl.lockingPlanId,
                                       rgl.lockingPlanName);
                    }
                }

                TopoLock rnl = rnLocks.get(rnId);
                if (rnl == null) {
                    rnl = new TopoLock();
                    rnLocks.put(rnId, rnl);
                }
                if (!rnl.get(planId, planName)) {
                    throw cantLock(planId, planName, rnl.lockingPlanId,
                                   rnl.lockingPlanName);
                }
            }
        }

        private IllegalCommandException cantLock(int planId,
                                                 String planName,
                                                 int lockingId,
                                                 String lockingName) {
            return new IllegalCommandException
                ("Couldn't execute " + planId + "/" + planName + " because " +
                 lockingId + "/" + lockingName + " is running. " +
                 "Wait until that plan is finished or interrupted");
        }

        /**
         * Check the RN locks as well as the RN
         */
        public void lockShard(int planId, String planName, RepGroupId rgId) {
            synchronized (elasticityLock) {
                TopoLock rgl = rgLocks.get(rgId);
                if (rgl != null && rgl.locked) {
                    if (rgl.lockingPlanId == planId) {
                        return;
                    }
                    throw cantLock(planId, planName, rgl.lockingPlanId,
                                   rgl.lockingPlanName);
                }

                /* check RNs first */
                for (Map.Entry<RepNodeId, TopoLock> entry:
                         rnLocks.entrySet()) {
                    if (rgId.sameGroup(entry.getKey())) {
                        final TopoLock l = entry.getValue();
                        if ((l.lockingPlanId != planId) && l.locked) {
                            throw cantLock(planId, planName, l.lockingPlanId,
                                           l.lockingPlanName);
                        }
                    }
                }

                if (rgl == null) {
                    rgl = new TopoLock();
                    rgLocks.put(rgId, rgl);
                }

                rgl.get(planId, planName);
            }
        }

        public void clearLocks(int planId) {
            /* Remove all locks that pertain to this plan */
            synchronized (elasticityLock) {
                Iterator<TopoLock> iter = rnLocks.values().iterator();
                while (iter.hasNext()) {
                    final TopoLock tl = iter.next();
                    if (tl.lockingPlanId == planId) {
                        iter.remove();
                    }
                }

                iter = rgLocks.values().iterator();
                while (iter.hasNext()) {
                    final TopoLock tl = iter.next();
                    if (tl.lockingPlanId == planId) {
                        iter.remove();
                    }
                }
            }

            elasticityLock.releaseForPlanId(planId);
        }

        /**
         * Access should be synchronized by the caller.
         */
        private class TopoLock {
            boolean locked;
            int lockingPlanId;
            String lockingPlanName;

            boolean get(int planId, String planName) {
                if (locked && (lockingPlanId != planId)) {
                    return false;
                }
                locked = true;
                lockingPlanId = planId;
                lockingPlanName = planName;
                return true;
            }

            /**
             * Release the component if this plan owns it.
             */
            void releaseForPlanId(int planId) {
                if (!locked) {
                    return;
                }

                if (lockingPlanId == planId) {
                    locked = false;
                    lockingPlanId = 0;
                    lockingPlanName = null;
                }
            }
        }

        /**
         * Enforce exclusivity at plan creation time.
         *
         *             no pending or  1 or more pending   1 pending/approved
         *             approved       or approved non-ex   exclusive plan
         *              plans         plans
         *
         * New plan is   ok             ok                  throw exception
         * exclusive
         *
         * New plan is   ok             ok                  ok
         * not
         * exclusive
         */
        synchronized void addNewPlan(Plan plan) {

            /* No need to enforce exclusivity. */
            if (!plan.isExclusive()) {
                planMap.put(plan.getId(), plan);
                return;
            }

            if (currentExclusivePlan == null) {
                currentExclusivePlan = plan;
                planMap.put(plan.getId(), plan);
                return;
            }

            throw new IllegalCommandException
                (plan + " is an exclusive type plan, and cannot be created " +
                 " because " + currentExclusivePlan +
                 " is active. Consider canceling " +
                 currentExclusivePlan + ".");
        }

        synchronized void addPlanFuture(Plan plan, Future<Plan.State> future) {

            /*
             * There is the small possibility that the plan execution thread
             * will have executed and finished the plan before we save its
             * future. Check so that we don't needlessly add it.
             */
            if (!plan.getState().isTerminal()) {
                futures.put(plan.getId(), future);
            }
        }

        synchronized void validateStart(Plan plan) {

            /*
             * Plans that were created through Planner should be
             * registered. This is really an assertion check for internal
             * testing plans.
             */
            if (catalog.getPlan(plan.getId()) == null) {
                throw new NonfatalAssertionException
                    (plan + " must be registered.");
            }

            /* A non-exclusive plan has no restrictions on when it can run.*/
            if (!plan.isExclusive()) {
                return;
            }

            if (currentExecutingPlan != null) {
                if (currentExecutingPlan.equals(plan)) {
                    throw new IllegalCommandException
                        (plan + " is already running");
                }

                throw new IllegalCommandException
                    (currentExecutingPlan + " is running, can't start " + plan);
            }
            currentExecutingPlan = plan;
        }

        synchronized void clearPlan(Plan plan) {
            futures.remove(plan.getId());

            if (currentExecutingPlan == plan) {
                currentExecutingPlan = null;
            }

            if (!plan.getState().isTerminal()) {
                return;
            }

            planMap.remove(plan.getId());

            if (currentExclusivePlan == plan) {
                currentExclusivePlan = null;
            }
        }

        Plan getPlan(int planId) {
            return planMap.get(planId);
        }
    }

    @Override
    public Plan getCachedPlan(int planId) {
        return catalog.getPlan(planId);
    }

    private Plan getFromCatalog(int planId) {

        final Plan plan = getCachedPlan(planId);

        if (plan == null) {
            /*
             * Plan ids may be specified via the CLI, so a user specified
             * invalid id may result in this problem, and this should be an
             * IllegalCommandException.
             */
            throw new IllegalCommandException("Plan " + planId +
                                              " is not an active plan");
        }
        return plan;
    }

    /**
     */
    @Override
    public void approvePlan(int planId) {
        final Plan plan = getFromCatalog(planId);
        try {
            ((AbstractPlan) plan).requestApproval();
        } catch (IllegalStateException e) {

            /*
             * convert this to IllegalCommandException, since this is a
             * user initiated action.
             */
            throw new IllegalCommandException(e.getMessage());
        }
    }

    /**
     */
    @Override
    public void cancelPlan(int planId) {
        final AbstractPlan plan = (AbstractPlan) getFromCatalog(planId);
        try {
            plan.requestCancellation();
            planFinished(plan);
        } catch (IllegalStateException e) {

            /*
             * convert this to IllegalCommandException, since this is a
             * user initiated action.
             */
            throw new IllegalCommandException(e.getMessage());
        }
    }

    /**
     * Interrupt a RUNNING plan.
     */
    @Override
    public void interruptPlan(int planId) {
        final Plan plan = getFromCatalog(planId);
        final AbstractPlan aplan = (AbstractPlan) plan;
        if (aplan.cancelIfNotStarted()) {

            /*
             * If the plan isn't even running, just change state to CANCEL, no
             * need to do any interrupt processing.
             */
            return;
        }

        if (!(plan.getState().checkTransition
              (Plan.State.INTERRUPT_REQUESTED))) {
            throw new IllegalCommandException
                ("Can't interrupt plan " + plan + " in state " +
                 plan.getState());
        }

        logger.info("User requesting interrupt of " + plan);
        aplan.requestInterrupt();
    }
}
