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

import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodePool;

import com.sleepycat.persist.model.Persistent;

/**
 * A plan for deploying a new store.
 * Deprecated as of R2, replaced by DeployTopoPlan
 */
@Persistent(version=1)
public class DeployStorePlan extends TopologyPlan {

    private static final long serialVersionUID = 1L;

    /**
     * The initial arguments for the plan are preserved in order to display
     * them later.
     */
    private StorageNodePool targetPool;
    private int repFactor;
    private int numPartitions;

    /*
     * RepNodeParams created by the plan for deployment, saved before plan
     * execution.
     */
    @SuppressWarnings("unused")
    private Set<RepNodeParams> firstDeploymentRNPs;

    /*
     * No-arg ctor for use by DPL.
     */
    private DeployStorePlan() {
    }

    @Override
    public String getDefaultName() {
        return null;
    }

    @Override
    void preExecutionSave() {
    }

    public StorageNodePool getTargetPool() {
        return targetPool;
    }

    public int getRepFactor() {
        return repFactor;
    }

    public int getNumPartitions() {
        return numPartitions;
    }
}
