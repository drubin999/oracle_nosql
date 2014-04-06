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

import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import oracle.kv.impl.admin.PlannerAdmin;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams.RNHeapAndCacheSize;
import oracle.kv.impl.admin.plan.DeployTopoPlan;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.persist.model.Persistent;

/**
 * SNs that were previously over capacity and have lost an RN may now be able
 * to increase the per-RN memory settings. Check all the RNs on this SN to
 * see if its memory settings are optimal.
 */
@Persistent
public class CheckRNMemorySettings extends SingleJobTask {

    private static final long serialVersionUID = 1L;
    public static TestHook<Integer> FAULT_HOOK;

    private StorageNodeId snId;
    private DeployTopoPlan plan;

    public CheckRNMemorySettings(DeployTopoPlan plan,
                                 StorageNodeId snId) {
        super();
        this.snId = snId;
        this.plan = plan;
    }

    /*
     * No-arg ctor for use by DPL.
     */
    @SuppressWarnings("unused")
    private CheckRNMemorySettings() {
    }

    @Override
    public State doWork()
        throws Exception {

        PlannerAdmin admin = plan.getAdmin();
        Topology topo = admin.getCurrentTopology();
        Parameters parameters = admin.getCurrentParameters();
        ParameterMap policyMap = parameters.copyPolicies();
        Logger logger = plan.getLogger();

        Set<RepNodeParams> changed = new HashSet<RepNodeParams>();
        Set<RepNodeId> needsRestart = new HashSet<RepNodeId>();
        Set<RepNodeId> needsNotification = new HashSet<RepNodeId>();

        LoginManager loginMgr = admin.getLoginManager();
        RegistryUtils registry = new RegistryUtils(topo, loginMgr);
        StorageNodeAgentAPI sna = registry.getStorageNodeAgent(snId);

        /*
         * See if memory sizes for the RNs on this SN need to be recalculated,
         * as may be the case if the SN was over capacity and there has been
         * a change in the number of RNs on it.
         */
        Set<RepNodeId> rns = topo.getHostedRepNodeIds(snId);
        StorageNodeParams snp = parameters.get(snId);
        for (RepNodeId rnId : rns) {
            RepNodeParams rnp = parameters.get(rnId);
            RNHeapAndCacheSize heapAndCache =
                snp.calculateRNHeapAndCache(policyMap,
                                            rns.size(), /* num RNs on this RN */
                                            rnp.getRNCachePercent());

            long oldHeap = rnp.getMaxHeapMB();
            long oldCacheSize = rnp.getJECacheSize();
            rnp.setRNHeapAndJECache(heapAndCache);

            if (oldHeap != rnp.getMaxHeapMB()) {
                /* Compare the calculated heap to that in the AdminDB */
                logger.info("Heap size for " + rnId + " has changed from " +
                            oldHeap + "MB to " + rnp.getMaxHeapMB() + "MB");
                changed.add(rnp);
                needsRestart.add(rnId);
            } else if (oldCacheSize != rnp.getJECacheSize()) {
                /* Compare the calculated cache to that in the AdminDB */
                logger.info("Cache size for " + rnId + " has changed from " +
                            oldCacheSize + " to " + rnp.getJECacheSize());
                changed.add(rnp);
                needsNotification.add(rnId);
            } else {
                /*
                 * Do comparisons against what's in the remote SN config.
                 * There's the possibility that the AdminDB was previously
                 * updated and persisted, but that the SN did not hear about
                 * the change.
                 */
                RepNodeParams remoteRNP = readRemoteRepNodeParams(sna, rnId);
                long remoteHeap = remoteRNP.getMaxHeapMB();
                long remoteCacheSize = rnp.getJECacheSize();
                if (remoteHeap != rnp.getMaxHeapMB()) {
                    /* Compare the calculated heap to the remote SN config */
                    logger.info("Heap size for " + rnId +
                                " in " + snId + " has changed from " +
                                remoteHeap + "MB to " + rnp.getMaxHeapMB() +
                                "MB");
                    changed.add(rnp);
                    needsRestart.add(rnId);
                } else if (remoteCacheSize != rnp.getJECacheSize()) {
                    /* Compare the calculated cache to the remote SN config */
                    logger.info("Cache size for " + rnId + " in " + snId +
                                " has changed from " + remoteCacheSize +
                                " to " + rnp.getJECacheSize());
                    changed.add(rnp);
                    needsNotification.add(rnId);
                }
            }
        }

        /*
         * Update all params that have changed.
         * (a) Ask the Admin to write all the new RN params
         * (b) Ask the SNA to write the new RN configuration to the config files
         */
        for (RepNodeParams changedParams : changed) {
            admin.updateParams(changedParams);
            sna.newRepNodeParameters(changedParams.getMap());
        }

        /*
         * For RNs that have had changes that do not require reset, poke them
         * so they reread their configuration files.
         */
        for (RepNodeId rnId : needsNotification) {
            RepNodeAdminAPI rnAdmin = registry.getRepNodeAdmin(rnId);
            rnAdmin.newParameters();
        }

        /*
         * For param changes that require a process restart, bounce the
         * RN.
         */
        for (RepNodeId rnId : needsRestart) {
            Utils.stopRN(plan, snId, rnId);
            Utils.startRN(plan, snId, rnId);
            Utils.waitForRepNodeState(plan, rnId, ServiceStatus.RUNNING);
        }
        return Task.State.SUCCEEDED;
    }

    RepNodeParams readRemoteRepNodeParams(StorageNodeAgentAPI sna,
                                          RepNodeId rnId)
        throws RemoteException {
        LoadParameters lp = sna.getParams();
        ParameterMap rMap = lp.getMap(rnId.getFullName(),
                                      ParameterState.REPNODE_TYPE);
        return new RepNodeParams(rMap);
    }

    @Override
    public boolean continuePastError() {
        return false;
    }
}
