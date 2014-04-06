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

import com.sleepycat.persist.model.Persistent;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.TopologyCheck;
import oracle.kv.impl.admin.VerifyConfiguration;
import oracle.kv.impl.admin.VerifyResults;
import oracle.kv.impl.admin.plan.AbstractPlan;

@Persistent
public class VerifyAndRepair extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private AbstractPlan plan;
    private boolean shouldContinuePastError;
    
    /* For DPL */
    VerifyAndRepair() {
    }

    public VerifyAndRepair(AbstractPlan plan, boolean continuePastError) {
        this.plan = plan;
        this.shouldContinuePastError = continuePastError;
    }

    @Override
    public String getName() {
        return "VerifyAndRepair";
    }

    @Override
    public boolean continuePastError() {
        return shouldContinuePastError;
    }

    /**
     * Run a verify on the current configuration and then attempt to repair
     * any problems that are found.
     */
    @Override
    public State doWork() throws Exception {

        // TODO: get rid of PlannerAdmin
        Admin admin = (Admin)plan.getAdmin();
        final VerifyConfiguration checker =
            new VerifyConfiguration(admin,
                                    false, /* showProgress */
                                    true, /* listAll */
                                    plan.getLogger());
        checker.verifyTopology();
        VerifyResults results = checker.getResults();
        
        TopologyCheck topoCheck = checker.getTopoChecker();
        plan.getLogger().info("Found repairs: " + checker.getRepairs());
        topoCheck.applyRemedies(checker.getRepairs(), plan);
        topoCheck.repairInitialEmptyShards(results, plan, 
                                           plan.getDeployedInfo());

        return Task.State.SUCCEEDED;
    }
}
