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

import java.util.Collections;
import java.util.Set;

import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;

import com.sleepycat.persist.model.Persistent;

/**
 * Task for creating and starting a single RepNode on a particular Storage Node.
 * @Deprecated as of R2 in favor of DeployNewRN
 * version 0: original
 * version 1: superclass changed inheritance chain.
 */
@Persistent(version=1)
    public class DeployOneRepNode extends DeployRepNode {

    private static final long serialVersionUID = 1L;

    private RepNodeId rnId;

    /**
     * Creates a task for creating and starting a new RepNode.
     * @Deprecated at of R2 in favor of DeployNewRN
     */
    @Deprecated
    public DeployOneRepNode(AbstractPlan plan,
                            StorageNodeId snId,
                            RepNodeId rnId) {
        super(plan, snId);
        this.rnId = rnId;
    }

    /*
     * No-arg ctor for use by DPL.
     */
    @SuppressWarnings("unused")
    private DeployOneRepNode() {
    }

    @Override
    Set<RepNodeId> getTargets() {
        return Collections.singleton(rnId);
    }

    /**
     * Return a more descriptive name which includes the target SN, so that the
     * user can relate errors to specific machines.
     */
    @Override
    public String getName() {
        return this.toString();
    }

    @Override
    public String toString() {
        return super.getName() + " of " + rnId + " on " +  snDescriptor;
    }
}
