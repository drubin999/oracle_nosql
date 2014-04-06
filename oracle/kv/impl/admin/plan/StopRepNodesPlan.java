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

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.plan.task.StopRepNode;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;

import com.sleepycat.persist.model.Persistent;

/**
 * Stop the given set of RepNodes.
 */
@Persistent
public class StopRepNodesPlan extends AbstractPlan {

    private static final long serialVersionUID = 1L;

    private Set<RepNodeId> repNodeIds;

    StopRepNodesPlan(AtomicInteger idGen,
                     String name,
                     Planner planner,
                     Topology topology,
                     Set<RepNodeId> rnids) {

        super(idGen, name, planner);
        repNodeIds = rnids;

        for (RepNodeId rnid : rnids) {
            RepNode rn = topology.get(rnid);

            if (rn == null) {
                throw new IllegalCommandException
                    ("There is no RepNode with id " + rnid +
                     ". Please provide the id of an existing RepNode.");
            }

            addTask(new StopRepNode(this, rn.getStorageNodeId(),
                                    rnid, true));
        }
    }

    protected StopRepNodesPlan() {
    }

    @Override
    public boolean isExclusive() {
        return false;
    }

    public Set<RepNodeId> getRepNodeIds() {
       return repNodeIds;
    }

    @Override
    void preExecutionSave() {

        /* Nothing to save in advance. */
    }

    @Override
    public String getDefaultName() {
        return "Stop RepNodes";
    }

    @Override
    void stripForDisplay() {
        repNodeIds = null;
    }
}
