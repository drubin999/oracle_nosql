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

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.plan.DeployTopoPlan;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.Topology;

import com.sleepycat.persist.model.Persistent;

/**
 * A task for updating the replication factor of a datacenter
 */
@Persistent(version=1)
public class UpdateDatacenter extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private DatacenterId dcId;
    private int newRepFactor;
    private DeployTopoPlan plan;

    public UpdateDatacenter(DeployTopoPlan plan, DatacenterId dcId,
                            int newRepFactor) {
        super();
        this.plan = plan;
        this.dcId = dcId;
        this.newRepFactor = newRepFactor;
    }

    /*
     * No-arg ctor for use by DPL.
     */
    @SuppressWarnings("unused")
    private UpdateDatacenter() {
    }

    /*
     * Update the repfactor for this datacenter.
     */
    @Override
    public State doWork()
        throws Exception {

        Topology current = plan.getAdmin().getCurrentTopology();
        Datacenter currentdc = current.get(dcId);
        if (currentdc.getRepFactor() == newRepFactor) {
            /* Nothing to do */
            return Task.State.SUCCEEDED;
        }

        if (currentdc.getRepFactor() > newRepFactor) {
            throw new IllegalCommandException
                ("The proposed replication factor of " + newRepFactor +
                 "is less than the current replication factor of " +
                 currentdc.getRepFactor() +" for " + currentdc +
                 ". Oracle NoSQL Database doesn't yet " +
                 " support the ability to reduce replication factor" );
        }

        current.update(dcId,
                       Datacenter.newInstance(currentdc.getName(),
                                              newRepFactor,
                                              currentdc.getDatacenterType()));
        plan.getAdmin().saveTopo(current, plan.getDeployedInfo(), plan);
        return Task.State.SUCCEEDED;
    }

    @Override
    public String toString() {
        return super.toString() +  " zone=" + dcId;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }
}
