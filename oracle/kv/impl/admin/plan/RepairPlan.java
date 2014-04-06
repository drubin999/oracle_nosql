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

import oracle.kv.impl.admin.plan.task.VerifyAndRepair;
import oracle.kv.impl.admin.topo.TopologyCandidate;

import com.sleepycat.persist.model.Persistent;

/**
 * A plan which takes issues reported by VerifyConfiguration.verifyTopology and
 * attempts to fix any reported problems.
 */
@Persistent(version=0)
public class RepairPlan extends AbstractPlan {

    private static final long serialVersionUID = 1L;

    private transient DeploymentInfo deploymentInfo;

    /* for DPL */
    RepairPlan() {
    }
    
    public RepairPlan(AtomicInteger idGen,
                      String planName,
                      Planner planner) {
        
        super(idGen, planName, planner);
        addTask(new VerifyAndRepair(this, false /* continuePastError */));
    }

    @Override
        public String getDefaultName() {
        return "Repair Topology";
    }

    @Override
        public boolean isExclusive() {
        return true;
    }

    @Override
    void preExecutionSave() {
        /* Nothing to do */
    }

    @Override
    void stripForDisplay() {
        /* Nothing to do */
    }

    @Override
    public void getCatalogLocks() {
        planner.lockElasticity(getId(), getName());
        getPerTaskLocks();
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
}