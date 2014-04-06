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

import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.plan.TopologyPlan;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;

import com.sleepycat.persist.model.Persistent;

/**
 * Task for creating and starting all RepNodes which are housed on a particular
 * Storage Node.
 * @Deprecated as of R2 in favor of DeployMultipleRNs
 * version 0: original
 * version 1: superclass changed inheritance chain.
 */
@Persistent(version=1)
public class DeployAllRepNodesOnSN extends DeployRepNode {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a task for creating and starting a new RepNode.
     * @Deprecated as of R2 in favor of DeployMultipleRNs
     */
    @Deprecated
    public DeployAllRepNodesOnSN(TopologyPlan plan,
                                 StorageNodeId snId) {
        super(plan, snId);
    }

    /*
     * No-arg ctor for use by DPL.
     */
    @SuppressWarnings("unused")
    private DeployAllRepNodesOnSN() {
    }

    @Override
    Set<RepNodeId> getTargets() {
        Parameters parameters = plan.getAdmin().getCurrentParameters();
        Set<RepNodeId> targetSet = new HashSet<RepNodeId>();

        for (RepNodeParams rnp: parameters.getRepNodeParams()) {
            if (rnp.getStorageNodeId().equals(snId)) {
                targetSet.add(rnp.getRepNodeId());
            }
        }
        return targetSet;
    }
}
