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

package oracle.kv.impl.admin.plan.task;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.PlannerAdmin;
import oracle.kv.impl.admin.TopologyCheck;
import oracle.kv.impl.admin.TopologyCheck.Remedy;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams.RNHeapAndCacheSize;
import oracle.kv.impl.admin.plan.DeployTopoPlan;
import oracle.kv.impl.admin.plan.PortTracker;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.TopologyPrinter;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.persist.model.Persistent;
import com.sleepycat.je.rep.NodeType;

/**
 * Create and start a single RepNode on a particular Storage Node. This
 * requires:
 *
 * 1. adding the new RN to the topology
 * 2. adding appropriate param entries to the AdminDB for this RN.
 * 3. contacting the owning SN to invoke the RN creation.
 *
 * Note that since the Admin DB has the authoritative copy of the topology and
 * metadata, (1) and (2) must be done before the remote request to the SN is
 * made. The task must take care to be idempotent. Topology changes should not
 * be made needlessly, because unnecessary versions merely need pruning later.
 */

@Persistent
public class DeployNewRN extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private DeployTopoPlan plan;
    private StorageNodeId snId;
    private String snDescriptor;
    private String mountPoint;

    /*
     * Since the RepNodeId is only calculated when the task executes, this
     * field may be null. However, go to the effort to hang onto this
     * when it's available, because it's very useful for logging.
     */
    private RepNodeId displayRNId;

    /*
     * Only one of these fields will be set. If the RN is being made for a
     * brand new shard, the RepGroupId hasn't been allocated at the time when
     * the task is constructed. In that case, we use the planShardIdx. If
     * the RN is being added to a shard that already exists, the specified
     * shard field will be set.
     */
    private int planShardIdx;
    private RepGroupId specifiedShard;

    /* Hook to inject failures at different points in task execution */
    public static TestHook<String> FAULT_HOOK;

    /**
     * Creates a task for creating and starting a new RepNode for a brand
     * new shard, when we don't yet know the shard's id
     * @param mountPoint if null, put RN in the SN's root directory
     */
    public DeployNewRN(DeployTopoPlan plan,
                       StorageNodeId snId,
                       int planShardIdx,
                       String mountPoint) {

        super();
        this.planShardIdx = planShardIdx;
        init(plan, snId, mountPoint);
    }

    /**
     * Creates a task for creating and starting a new RepNode for a shard
     * that already exists and has a repGroupId.
     * @param mountPoint if null, put RN in the SN's root directory
     */
    public DeployNewRN(DeployTopoPlan plan,
                       StorageNodeId snId,
                       RepGroupId specifiedShard,
                       String mountPoint) {

        super();
        this.specifiedShard = specifiedShard;
        init(plan, snId, mountPoint);
    }

    private void init(DeployTopoPlan plan1,
                      StorageNodeId snId1,
                      String mountPoint1) {

        plan = plan1;
        mountPoint = mountPoint1;
        snId = snId1;

        /* A more descriptive label used for error messages, etc. */
        StorageNodeParams snp = plan1.getAdmin().getStorageNodeParams(snId1);
        snDescriptor = snp.displaySNIdAndHost();
    }

    /*
     * No-arg ctor for use by DPL.
     */
    DeployNewRN() {
    }

    /**
     * TODO: refactor change port tracker so it generates helper hosts and
     * works on a single SN. Correct now, just would be nicer to share the code.
     */
    private RepNodeParams makeRepNodeParams(Topology current,
                                            RepGroupId rgId,
                                            RepNodeId rnId) {
        /*
         * The RepNodeParams has everything needed to start up the new RepNode.
         */
        Parameters params = plan.getAdmin().getCurrentParameters();
        ParameterMap pMap = params.copyPolicies();

        /* Set JE HA host name */
        String haHostname = params.get(snId).getHAHostname();

        /* Find helper hosts for JE HA */
        PortTracker portTracker = new PortTracker(current, params, snId);
        int haPort = portTracker.getNextPort(snId);
        String otherHelpers = findHelperHosts(current.get(rgId), rnId, params);
        final NodeType nodeType = computeNodeType(current);
        String helperHosts;
        if (otherHelpers.length() == 0) {
            if (!nodeType.isElectable()) {
                throw new IllegalStateException(
                    "The self-electing node must be electable");
            }
            helperHosts = haHostname + ":" + haPort;
        } else {
            helperHosts = otherHelpers;
        }

        RepNodeParams rnp =
            new RepNodeParams(pMap, snId, rnId,
                              false /* disabled */,
                              haHostname, haPort, helperHosts,
                              mountPoint,
                              nodeType);
        /*
         * If the storage node has a memory setting, set an explicit JE heap
         * and cache size. The new RN has already been added to the topology,
         * so it will be accounted for by current.getHostedRepNodeIds().
         */
        StorageNodeParams snp = params.get(snId);
        int numRNsOnSN = current.getHostedRepNodeIds(snId).size();
        RNHeapAndCacheSize heapAndCache = snp.calculateRNHeapAndCache
                (pMap, numRNsOnSN, rnp.getRNCachePercent());
        long heapMB = heapAndCache.getHeapMB();
        long cacheBytes = heapAndCache.getCacheBytes();

        /*
         * If the storage node has a num cpus setting, set an explicit
         * -XX:ParallelGCThreads value
         */
        int gcThreads = snp.calcGCThreads();

        plan.getLogger().log
            (Level.INFO,
             "Creating {0} on {1} haPort={2}:{3} helpers={4} " +
             "storage directory={5} heapMB={6} cacheSize={7} " +
             "-XX:ParallelGCThreads={8}",
             new Object[] {rnId, snId, haHostname, haPort, helperHosts,
                           mountPoint,
                           (heapMB == 0) ? "unspecified" : heapMB,
                           (cacheBytes==0) ? "unspecified" : cacheBytes,
                           (gcThreads==0) ? "unspecified" : gcThreads});

        /*
         * Set the JVM heap, JE cache, and -XX:ParallelTCThreads.
         */
        rnp.setRNHeapAndJECache(heapAndCache);
        rnp.setParallelGCThreads(gcThreads);

        return rnp;
    }

    /**
     * Look at the current topology and parameters, as stored in the AdminDB,
     * and generate a set of helpers composed of all the hahost values for the
     * members of the group, other than the target RN.  Returns an empty string
     * if there are no other nodes to use as helpers.  In that case, the caller
     * should use the node's hostname and port to make it self-electing.
     */
    private String findHelperHosts(RepGroup shard,
                                   RepNodeId targetRNId,
                                   Parameters params) {

        StringBuilder helperHosts = new StringBuilder();

        for (RepNode rn : shard.getRepNodes()) {
            RepNodeId rId = rn.getResourceId();
            if (rId.equals(targetRNId)) {
                continue;
            }

            if (helperHosts.length() != 0) {
                helperHosts.append(",");
            }

            helperHosts.append(params.get(rId).getJENodeHostPort());
        }
        return helperHosts.toString();
    }

    /** Returns the node type for creating an RN in the specified SN. */
    private NodeType computeNodeType(final Topology current) {
        final Datacenter datacenter = current.getDatacenter(snId);
        final DatacenterType datacenterType = datacenter.getDatacenterType();
        switch (datacenterType) {
        case PRIMARY:
            return NodeType.ELECTABLE;
        case SECONDARY:
            return NodeType.SECONDARY;
        default:
            throw new AssertionError();
        }
    }

    @Override
    public State doWork()
        throws Exception {

        /* Create and save a topology and params that represent the new RN. */
        RepGroupId shardId = null;
        if (specifiedShard == null) {
            shardId = plan.getShardId(planShardIdx);
        } else {
            shardId = specifiedShard;
        }

        PlannerAdmin admin = plan.getAdmin();
        Topology current = admin.getCurrentTopology();

        RepGroup rg = current.get(shardId);
        if (rg == null) {
            /*
             * This is really an assert, intended to provide better debugging
             * information than the resulting NPE.
             */
            throw new IllegalStateException
            ("Expectedly can't find shard " + shardId + " current topology=" +
             TopologyPrinter.printTopology(current));
        }
        RepNode rn = null;
        RepNodeParams rnp = null;

        /*
         * If this shard already has a RN on this SN, then this task already
         * executed and this is a retry of the plan. We should use the RN
         * that is there. This assume we will never try to create the two RNs
         * from the same shard on the same SN.
         */
        for (RepNode existing : rg.getRepNodes()) {
            if (existing.getStorageNodeId().equals(snId)) {
                rn = existing;
                rnp = admin.getRepNodeParams(rn.getResourceId());
            }
        }

        if (rn == null) {
            rn = new RepNode(snId);
            rg.add(rn);
            displayRNId = rn.getResourceId();
            assert TestHookExecute.doHookIfSet(FAULT_HOOK,
                                               makeHookTag("1"));
            rnp =  makeRepNodeParams(current,
                                     rg.getResourceId(),
                                     rn.getResourceId());
            admin.saveTopoAndRNParam(current,
                                     plan.getDeployedInfo(),
                                     rnp, plan);
        } else {
            displayRNId = rn.getResourceId();
        }
        
        assert TestHookExecute.doHookIfSet(FAULT_HOOK, makeHookTag("2"));

        /*
         * Invoke the creation of the RN after the metadata is safely stored.
         * in the Admin DB.
         */
        LoginManager loginMgr = admin.getLoginManager();
        RegistryUtils regUtils = new RegistryUtils(current, loginMgr);
        StorageNodeAgentAPI sna = regUtils.getStorageNodeAgent(snId);

        if (rnp == null) {
            throw new IllegalStateException("RepNodeParams null for " + rn);
        }
        
        sna.createRepNode(rnp.getMap(), Utils.getMetadataSet(current, plan));

        /* Register this repNode with the monitor. */
        StorageNode sn = current.get(snId);
        admin.getMonitor().registerAgent(sn.getHostname(),
                                         sn.getRegistryPort(),
                                         rn.getResourceId());

        /*
         * At this point, we've succeeded. The user will have to rely on ping
         * and on monitoring to wait for the rep node to come up.
         */
        return State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    public String toString() {
        if (displayRNId == null) {
            return super.toString() + " on " +  snDescriptor;
        }
        return super.toString() + " " + displayRNId + " on " +
            snDescriptor;
    }

    @Override
    public String getName() {
        if (displayRNId == null) {
            return super.getName() + " on " +  snDescriptor;
        }
        return super.getName() + " " + displayRNId + " on " +  snDescriptor;
    }

    @Override
    public Runnable getCleanupJob() {
        return new Runnable() {
        @Override
        public void run(){
            boolean done = false;
            int numAttempts = 0;
            while (!done && !plan.cleanupInterrupted()) {
                try {
                    done = cleanupAllocation();
                    numAttempts++;
                } catch (Exception e) {
                    plan.getLogger().log
                        (Level.SEVERE,
                         "{0}: problem when cancelling deployment of RN {1}",
                         new Object[] {this, LoggerUtils.getStackTrace(e)});
                    /* 
                     * Don't try to continue with cleanup; a problem has
                     * occurred. Future, additional invocations of the plan
                     * will have to figure out the context and do cleanup.
                     */
                    throw new RuntimeException(e);
                }

                if (!done) {
                    /*
                     * Arbitrarily limit number of tries to 5. TODO: would be
                     * nicer if this was based on time.
                     */
                    if (numAttempts > 5) {
                        return;
                    }
                    
                    /*
                     * TODO: would be nicer to schedule a job, rather
                     * than sleep.
                     */
                    try {
                        Thread.sleep(AbstractTask.CLEANUP_RETRY_MILLIS);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }
        }
        };
    }

    private boolean cleanupAllocation() 
        throws RemoteException, NotBoundException {
        Logger logger = plan.getLogger();

        assert TestHookExecute.doHookIfSet(FAULT_HOOK,
                                           makeHookTag("cleanup"));
        /* RN wasn't created, nothing to do */
        if (displayRNId == null) {
            logger.info("DeployNewRN cleanup: RN not created."); 
            return true;
        }
        
        // TODO, get rid of cast
        Admin admin = (Admin)plan.getAdmin();
        TopologyCheck checker = 
            new TopologyCheck(logger,
                              admin.getCurrentTopology(),
                              admin.getCurrentParameters());

        Remedy remedy = checker.checkRNLocation(admin, snId, displayRNId, 
                                                true /* calledByDeployNewRN */,
                                                true /* mustReeanableRN */);
        logger.info("DeployNewRN cleanup: " + remedy);
        
        return checker.applyRemedy(remedy, plan, plan.getDeployedInfo(), null,
                                   null);
    }
    
    /**
     * For unit test support -- make a string that uniquely identifies when
     * this task executes on a given SN
     */
    private String makeHookTag(String pointName) {
        return "DeployNewRN/" + snId + "_pt" + pointName;
    }
}
