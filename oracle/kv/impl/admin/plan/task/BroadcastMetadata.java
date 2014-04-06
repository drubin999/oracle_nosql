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

import oracle.kv.impl.admin.PlannerAdmin;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.MetadataInfo;

import com.sleepycat.persist.model.Persistent;

/**
 * Broadcast the metadata on Admin to all RNs. This is typically done after any
 * change happens on the metadata on Admin, or the first time the topology is
 * deployed and each RN needs a new copy of the metadata.
 */
@Persistent
public class BroadcastMetadata <T extends Metadata<? extends MetadataInfo>>
                               extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    protected AbstractPlan plan;

    protected T md;

    public BroadcastMetadata(AbstractPlan plan, T md) {
        this.plan = plan;
        this.md = md;
    }

    /* No-arg ctor for DPL persistence */
    @SuppressWarnings("unused")
    private BroadcastMetadata() {
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    public State doWork() throws Exception {
        if (md != null) {
            final PlannerAdmin pa = plan.getAdmin();

            if (!Utils.broadcastMetadataChangesToRNs(
                plan.getLogger(), md, pa.getCurrentTopology(), toString(),
                pa.getParams().getAdminParams(), plan)) {

                return State.INTERRUPTED;
            }
        }
        return State.SUCCEEDED;
    }
}
