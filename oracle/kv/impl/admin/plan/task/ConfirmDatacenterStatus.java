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
import oracle.kv.impl.admin.plan.TopologyPlan;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.Topology;

import com.sleepycat.persist.model.Persistent;

/**
 * Check if a given datacenter is empty.
 */
@Persistent
public class ConfirmDatacenterStatus extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private DatacenterId targetId;
    private TopologyPlan plan;
    private String infoMsg;

    public ConfirmDatacenterStatus(TopologyPlan plan,
                                   DatacenterId targetId,
                                   String infoMsg) {
        super();
        this.targetId = targetId;
        this.plan = plan;
        this.infoMsg = infoMsg;
    }

    /*
     * No-arg ctor for use by DPL.
     */
    @SuppressWarnings("unused")
    private ConfirmDatacenterStatus() {
    }

    @Override
    public State doWork() throws Exception {

        /* Determine if the target datacenter is empty. */
        final Topology topo = plan.getTopology();

        for (StorageNode sn: topo.getSortedStorageNodes()) {
            if (targetId.equals(sn.getDatacenterId())) {
                throw new IllegalCommandException(infoMsg);
            }
        }
        return Task.State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }
}
