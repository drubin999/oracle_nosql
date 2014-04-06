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

import oracle.kv.impl.admin.NonfatalAssertionException;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.plan.PortTracker;
import oracle.kv.impl.admin.plan.TopologyPlan;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;

import com.sleepycat.persist.model.Persistent;

/**
 * Update parameters to disable all the services running on the target storage
 * node. This is purely a change to the admin database.We assume that the
 * target node is already down, and that there is no need to issue remote
 * requests to stop those services.
 *
 * Suppose we are migrating the services on SN1 -> SN20, which causes the
 * topology to change from version 5 -> 6, and suppose that succeeds. The basic
 * steps that occurred were:
 *
 *  1. create a new topo and params
 *  2. broadcast the topo changes
 *  3. ask the new SN to create the desired services.
 *
 * If we repeat this plan, the second plan execution will find that there are
 * no topology changes between what is desired and what is currently stored in
 * the admin db. We placidly accept this and continue nevertheless to do steps
 * 2 and 3, because we do not know whether the previous attempt was interrupted
 * between steps 1 and 2, or whether it succeeded. Because of that, if there
 * are no changes to the topology found, we will broadcast the entire topology
 * instead of just the delta.
 *
 * version 0: original.
 * version 1: Changed inheritance chain.
 */
@Persistent(version=1)
public class MigrateParamsAndTopo extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private StorageNodeId oldNode;
    private StorageNodeId newNode;
    private TopologyPlan plan;
    private int httpPort;

    /*
     * Note that the params and topology should be updated as a single
     * transaction to avoid any inconsistencies in the admin db's view of
     * the store.
     */
    private Set<RepNodeParams> changedRepNodeParams;
    private Set<AdminParams> changedAdminParams;

    public MigrateParamsAndTopo(TopologyPlan plan,
                                StorageNodeId oldNode,
                                StorageNodeId newNode,
                                int httpPort) {

        super();
        this.oldNode = oldNode;
        this.newNode = newNode;
        this.plan = plan;
        this.httpPort = httpPort;

        changedAdminParams = new HashSet<AdminParams>();
        changedRepNodeParams = new HashSet<RepNodeParams>();
    }

    /*
     * No-arg ctor for use by DPL.
     */
    @SuppressWarnings("unused")
    private MigrateParamsAndTopo() {
    }

    @Override
    public State doWork()
        throws Exception {
        Parameters parameters = plan.getAdmin().getCurrentParameters();
        PortTracker portTracker =
            new PortTracker(plan.getTopology(), parameters, newNode);

        /* Modify pertinent params. */
        transferParamsToNewNode(parameters, portTracker);

        /* Modify pertinent topology */
        transferTopoToNewNode();

        /*
         * Save topo and params to the administrative db to preserve a
         * consistent view of the change. Note that if this plan has been
         * retried it's possible that the topology created by this plan
         * has already been saved.
         */
        if (plan.isFirstExecutionAttempt()) {
            plan.getAdmin().saveTopoAndParams(plan.getTopology(),
                                              plan.getDeployedInfo(),
                                              changedRepNodeParams,
                                              changedAdminParams,
                                              plan);
        } else {
            plan.getAdmin().saveParams(changedRepNodeParams,
                                       changedAdminParams);
        }
        /* Send topology changes to all nodes.*/
        if (!Utils.broadcastTopoChangesToRNs(plan.getLogger(),
                                             plan.getTopology(),
                                            "replace " + oldNode + " with " +
                                             newNode,
                                             plan.getAdmin().getParams().
                                                        getAdminParams(),
                                             plan)) {
            return State.INTERRUPTED;
        }

        return Task.State.SUCCEEDED;
    }

    /**
     * Find all RepNodeParams and AdminParams for services on the old node,
     * and update them to refer to the new node.
     */
    private void transferParamsToNewNode(Parameters parameters,
                                         PortTracker portTracker) {
        /*
         * Find all params that still refer to the old SN. Move them to the
         * new SN, and set their HA hostport..
         */

        String newNodeHAHostname = parameters.get(newNode).getHAHostname();

        AdminId foundAdmin = null;
        for (AdminParams ap: parameters.getAdminParams()) {

            if (ap.getStorageNodeId().equals(oldNode)) {
                if (foundAdmin != null) {
                    /* Should only be one Admin on any SN. */
                    throw new NonfatalAssertionException
                        ("More than one admin service exists on " + oldNode +
                         ": " + foundAdmin + ", " + ap.getAdminId());
                }

                foundAdmin = ap.getAdminId();
                ap.setStorageNodeId(newNode);
                ap.setHttpPort(httpPort);
                int haPort = portTracker.getNextPort(newNode);

                // TODO: clean this up in the future so that setting the
                // ha hostnameport is consistent with the way other
                // fields are set.

                String nodeHostPort = newNodeHAHostname + ":" + haPort;
                plan.getLogger().info("transferring HA port for " +
                                      foundAdmin + " from " +
                                      ap.getNodeHostPort() + " to " +
                                      nodeHostPort);
                ap.setJEInfo(nodeHostPort,
                             findAdminHelpers(parameters, ap.getAdminId()));
                changedAdminParams.add(ap);
            }
        }

        for (RepNodeParams rnp: parameters.getRepNodeParams()) {

            if (rnp.getStorageNodeId().equals(oldNode)) {

                rnp.setStorageNodeId(newNode);
                int haPort = portTracker.getNextPort(newNode);

                // TODO: clean this up in the future so that setting the
                // ha hostnameport is consistent with the way other
                // fields are set.

                String nodeHostPort = newNodeHAHostname + ":" + haPort;
                plan.getLogger().info("transferring HA port for " +
                                      rnp.getRepNodeId() +
                                      " from " + rnp.getJENodeHostPort() + " to "
                                      + nodeHostPort);
                rnp.setJENodeHostPort(nodeHostPort);
                rnp.setJEHelperHosts
                   (findRNHelpers(parameters,rnp.getRepNodeId()));
                changedRepNodeParams.add(rnp);
            }
        }
    }

    /**
     * Generate helper hosts by appending all the nodeHostPort values for all
     * other members of this HA repGroup.
     */
    private String findRNHelpers(Parameters parameters, RepNodeId rnId) {

        Topology topo = plan.getTopology();
        RepNode targetRN = topo.get(rnId);
        RepGroup rg = topo.get(targetRN.getRepGroupId());

        StringBuilder helperHosts = new StringBuilder();
        for (RepNode rn : rg.getRepNodes()) {
            if (rn.getResourceId().equals(rnId)) {
                continue;
            }

            if (helperHosts.length() != 0) {
                helperHosts.append(",");
            }

            RepNodeParams peerParams = parameters.get(rn.getResourceId());
            helperHosts.append(peerParams.getJENodeHostPort());
        }
        return helperHosts.toString();
    }

    /**
     * Generate helper hosts by appending all the nodeHostPort values for all
     * other members of this HA repGroup.
     */
    private String findAdminHelpers(Parameters parameters, AdminId adId) {

        StringBuilder helperHosts = new StringBuilder();
        for (AdminParams otherParams : parameters.getAdminParams()) {
            if (otherParams.getAdminId().equals(adId)) {
                continue;
            }

            if (helperHosts.length() != 0) {
                helperHosts.append(",");
            }
            helperHosts.append(otherParams.getNodeHostPort());
        }

        return helperHosts.toString();
    }

    /**
     * Find all RepNodes that refer to the old node, and update the topology
     * to refer to the new node.Push the topology changes to all nodes
     * in the system.
     */
    private void transferTopoToNewNode() {

        Topology topo = plan.getTopology();

        for (RepGroup rg : topo.getRepGroupMap().getAll()) {
            for (RepNode rn : rg.getRepNodes()) {
                if (rn.getStorageNodeId().equals(oldNode)) {
                    RepNode updatedRN = new RepNode(newNode);
                    rg.update(rn.getResourceId(),updatedRN);
                }
            }
        }
    }

    @Override
    public boolean continuePastError() {
        return false;
    }
}
