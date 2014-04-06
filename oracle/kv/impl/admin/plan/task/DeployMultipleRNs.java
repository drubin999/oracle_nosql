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

import java.util.HashSet;
import java.util.Set;

import oracle.kv.impl.admin.PlannerAdmin;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams.RNHeapAndCacheSize;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.persist.model.Persistent;

/**
 * Task for creating and starting all RepNodes which are housed on a particular
 * Storage Node.
 *
 * version 0: original.
 */
@Persistent(version=0)
public class DeployMultipleRNs extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    protected AbstractPlan plan;
    protected StorageNodeId snId;
    protected String snDescriptor;

    /**
     * Creates a task for creating and starting a new RepNode.
     */
    public DeployMultipleRNs(AbstractPlan plan,
                             StorageNodeId snId) {
        super();
        this.plan = plan;
        this.snId = snId;

        /* A more descriptive label used for error messages, etc. */
        StorageNodeParams snp = plan.getAdmin().getStorageNodeParams(snId);
        snDescriptor = snp.displaySNIdAndHost();
    }

    /*
     * No-arg ctor for use by DPL.
     */
    DeployMultipleRNs() {
    }

    private Set<RepNodeId> getTargets() {
        Parameters parameters = plan.getAdmin().getCurrentParameters();
        Set<RepNodeId> targetSet = new HashSet<RepNodeId>();

        for (RepNodeParams rnp: parameters.getRepNodeParams()) {
            if (rnp.getStorageNodeId().equals(snId)) {
                targetSet.add(rnp.getRepNodeId());
            }
        }
        return targetSet;
    }

    @Override
    public State doWork()
        throws Exception {
        PlannerAdmin admin = plan.getAdmin();

        /*
         * Attempt to deploy repNodes. Note that one or more repNodes may have
         * been previously deployed. The task is successful if the repNodes are
         * created, or are on their way to coming up.
         */
        Topology topo = plan.getAdmin().getCurrentTopology();
        ParameterMap policyMap = 
                plan.getAdmin().getCurrentParameters().copyPolicies();
        LoginManager loginMgr = admin.getLoginManager();
        RegistryUtils regUtils = new RegistryUtils(topo, loginMgr);
        StorageNodeAgentAPI sna = regUtils.getStorageNodeAgent(snId);
        StorageNodeParams snp = plan.getAdmin().getCurrentParameters().get(snId);
        int gcThreads = snp.calcGCThreads();
        Set<RepNodeId> targetRNIds = getTargets();
        int numRNsOnSN = targetRNIds.size();
        
        final Set<Metadata<? extends MetadataInfo>> metadataSet =
                                            Utils.getMetadataSet(topo, plan);
        
        for (RepNodeId rnId : targetRNIds) {
            RepNodeParams rnp = admin.getRepNodeParams(rnId);
            RNHeapAndCacheSize heapAndCache =
                snp.calculateRNHeapAndCache(policyMap,
                                            numRNsOnSN,
                                            rnp.getRNCachePercent());
            rnp.setRNHeapAndJECache(heapAndCache);
            rnp.setParallelGCThreads(gcThreads);
            sna.createRepNode(rnp.getMap(), metadataSet);

            /* Register this repNode with the monitor. */
            StorageNode sn = topo.get(snId);
            admin.getMonitor().registerAgent(sn.getHostname(),
                                             sn.getRegistryPort(),
                                             rnId);
        }

        /*
         * At this point, we've succeeded. The user will have to rely on ping
         * and on monitoring to wait for all the rep nodes to come up.
         */
        return State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    public void lockTopoComponents(Planner planner) {
        Set<RepNodeId> targets = getTargets();

        /*
         * Find the set of shard ids, we only want to get the shard lock
         * once, because the locking mechanism is fairly simplistic.
         */
        Set<RepGroupId> shards = new HashSet<RepGroupId>();
        for (RepNodeId rnId : targets) {
            shards.add(new RepGroupId(rnId.getGroupId()));
        }

        for (RepGroupId rgId : shards) {
            planner.lockShard(plan.getId(), plan.getName(), rgId);
        }
    }
}
