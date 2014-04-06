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

import java.util.List;

import oracle.kv.impl.admin.PlannerAdmin;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.DeploySNPlan;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.persist.model.Persistent;

/**
 * Deploy a new storage node.
 *
 * Note that we are saving the topology and params after the task successfully
 * creates and registers the SN. Most other plans save topology and params
 * before task execution, to make sure that the topology is consistent and
 * saved in the admin db before any kvstore component can access it. DeploySN
 * is a special case where it is safe to store the topology after execution
 * because there can be no earlier reference to the SN before the plan
 * finishes. This does mean that this task should be only be the only task used
 * in the plan, to ensure atomicity.
 */

@Persistent(version=1)
public class DeploySN extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private DeploySNPlan plan;

    /* True if this is the first SN to be deployed in the store. */
    private boolean isFirst;

    /**
     * Note that this task can only be used in the DeploySN plan, and only
     * one task can be executed within the plan, because of the post-task
     * execution save.
     */
    public DeploySN(DeploySNPlan plan, boolean isFirst) {
        super();
        this.plan = plan;
        this.isFirst = isFirst;
    }

    /*
     * No-arg ctor for use by DPL.
     */
    @SuppressWarnings("unused")
    private DeploySN() {
    }

    /**
     * Failure and interruption statuses are set by the PlanExecutor, to
     * generalize handling or exception cases.
     */
    @Override
    public State doWork()
        throws Exception {

        /* Check if this storage node is already running. */
        StorageNodeId snId = plan.getStorageNodeId();
        RegistryUtils trialRegistry =
            new RegistryUtils(plan.getTopology(),
                              plan.getAdmin().getLoginManager());
        StorageNodeAgentAPI sna;
        try {
            sna = trialRegistry.getStorageNodeAgent(snId);
            ServiceStatus serviceStatus = sna.ping().getServiceStatus();
            if (serviceStatus.isAlive()) {
                if (serviceStatus != ServiceStatus.WAITING_FOR_DEPLOY) {
                    /* This SNA is already up, nothing to do. */
                    plan.getLogger().info(this + ": SNA already deployed, " +
                                          "had status of " + serviceStatus);
                    return Task.State.SUCCEEDED;
                }
            }
        } catch (java.rmi.NotBoundException notbound) {
            /*
             * It's fine for if the SNA is not bound already. It means that
             * this is truly the first time the DeploySN task has run.
             */
        }

        StorageNodeParams inputSNP = plan.getInputStorageNodeParams();
        sna = RegistryUtils.getStorageNodeAgent(inputSNP.getHostname(),
                                                inputSNP.getRegistryPort(),
                                                GlobalParams.SNA_SERVICE_NAME,
                                                plan.getLoginManager());

        /*
         * Contact the StorageNodeAgent, register it, and get information about
         * the SNA params.
         */
        PlannerAdmin admin = plan.getAdmin();
        StorageNodeParams registrationParams = plan.getRegistrationParams();
        GlobalParams gp = admin.getParams().getGlobalParams();
        List<ParameterMap> snMaps =
            sna.register(gp.getMap(),
                         registrationParams.getMap(), isFirst);

        /*
         * If this is the first time the plan has been executed, save the
         * topology and params. If this is the first SN of the store, save
         * whether its address is loopback or not to make sure that this is
         * consistent across the store.  Any inconsistencies will result in an
         * exception from the register() call above.
         */
        StorageNodeAgent.RegisterReturnInfo rri = new
            StorageNodeAgent.RegisterReturnInfo(snMaps);
        registrationParams.setInstallationInfo(rri.getBootMap(),
                                               rri.getMountMap(),
                                               isFirst);
        if (plan.isFirstExecutionAttempt()) {
            if (isFirst) {
                gp.setIsLoopback(rri.getIsLoopback());
                admin.saveTopoAndParams(plan.getTopology(),
                                        plan.getDeployedInfo(),
                                        registrationParams,
                                        gp, plan);
            } else {
                admin.saveTopoAndParams(plan.getTopology(),
                                        plan.getDeployedInfo(),
                                        registrationParams,
                                        null, plan);
            }
        }

        /* Add this SNA to monitoring. */
        admin.getMonitor().registerAgent(registrationParams.getHostname(),
                                         registrationParams.getRegistryPort(),
                                         plan.getStorageNodeId());
        return State.SUCCEEDED;
    }

    @Override
    public String toString() {
        StorageNodeParams snp = plan.getInputStorageNodeParams();
        /*
         * Note that storage node id  may not be set in snp yet, so we should
         * not use StorageNodeParams.displaySNIdAndHost
         */
        return super.toString() +  " " +
            plan.getStorageNodeId() + "(" +
               snp.getHostname() + ":" + snp.getRegistryPort() + ")";
    }

    @Override
    public boolean continuePastError() {
        return false;
    }
}
