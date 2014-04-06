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

import java.util.concurrent.atomic.AtomicInteger;

import oracle.kv.impl.admin.NonfatalAssertionException;
import oracle.kv.impl.admin.topo.TopologyCandidate;
import oracle.kv.impl.topo.Topology;

import com.sleepycat.persist.model.Persistent;

/**
 * An abstract class representing a plan that changes the topology of the
 * store.  Topology plans are exclusive - no other plans can run while a
 * topology plan is being run.
 *
 * Topologies are known to the client, repNodes, and admin. Of the three, the
 * admin database contains the authoritative, latest version. Clients, repNodes,
 * and admin replicas may have earlier, less up to date versions, but over time
 * the topology will circulate and all services should become up to date.
 *
 * A Topology plan first creates the new, desired topology, and then begins to
 * execute the appropriate actions to change the store to conform to that
 * topology. Since the admin database is the authoritative copy, the target
 * topology must be stored before being distributed to a client or repNode.
 * Because of this, before the plan successfully finishes, this latest topology
 * will contain services that may not yet exist.
 */
@Persistent
public abstract class TopologyPlan extends AbstractPlan {

    private static final long serialVersionUID = 1L;

    /**
     * The plan topology describes the desired final state of the kvstore after
     * the plan has been executed.
     */
    private Topology topology;

    private transient DeploymentInfo deploymentInfo;

    /**
     */
    TopologyPlan(AtomicInteger idGen,
                 String name,
                 Planner planner,
                 Topology topo) {
        super(idGen, name, planner);
        this.topology = topo;
    }

    /*
     * No-arg ctor for use by DPL.
     */
    protected TopologyPlan() {
    }

    @Override
    public boolean isExclusive() {
        return false;
    }

    public Topology getTopology() {
        return topology;
    }

    protected void setTopology(Topology t) {
        topology = t;
    }

    /**
     * A topology plan should only save its topology on the first plan
     * execution attempt. On subsequent attempts, the topology it is using
     * should be the same version as what is stored. No other topology should
     * have gotten in ahead of it, because topology plans are supposed to
     * be exclusive.
     */
    public boolean isFirstExecutionAttempt() {
        Topology current = getAdmin().getCurrentTopology();
        Topology createdByPlan = getTopology();
        if (current.getSequenceNumber() > createdByPlan.getSequenceNumber()) {
            throw new NonfatalAssertionException
                ("Unexpected error: the current topology version (" +
                 current.getSequenceNumber() + ") is greater than the " +
                 "topology version (" + createdByPlan.getSequenceNumber() +
                 ") used by " +  getName());
        }

        return (current.getSequenceNumber() <
                createdByPlan.getSequenceNumber());

    }

    @Override
    public DeploymentInfo getDeployedInfo() {
        return deploymentInfo;
    }

    @Override
    synchronized PlanRun startNewRun() {
        deploymentInfo = DeploymentInfo.makeDeploymentInfo
                (this, TopologyCandidate.NO_NAME);
        return super.startNewRun();
    }
    
    @Override
    void stripForDisplay() {
        topology = null;
    }
}
