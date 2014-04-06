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

import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;

import com.sleepycat.persist.model.Persistent;

/**
 * Monitors the state of a RepNode, blocking until a certain state has been
 * reached.
 *
 * version 0: original.
 * version 1: Changed inheritance chain.
 */
@Persistent(version=1)
public class WaitForRepNodeState extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    /**
     * The node that is to be monitored
     */
    private RepNodeId targetNodeId;

    /**
     * The state the node must be in before finishing this task
     */
    private ServiceStatus targetState;
    private AbstractPlan plan;

    public WaitForRepNodeState() {
    }

    /**
     * Creates a task that will block until a given RepNode has reached
     * a given state.
     *
     * @param desiredState the state to wait for
     */
    public WaitForRepNodeState(AbstractPlan plan,
                               RepNodeId targetNodeId,
                               ServiceStatus desiredState) {
        this.plan = plan;
        this.targetNodeId = targetNodeId;
        this.targetState = desiredState;
    }

    @Override
    public State doWork()
        throws Exception {

        return Utils.waitForRepNodeState(plan, targetNodeId, targetState);
    }

    @Override
    public String toString() {
       return super.toString() + " waits for " + targetNodeId + " to reach " +
           targetState + " state";
    }

    @Override
    public boolean continuePastError() {
        return true;
    }
}
