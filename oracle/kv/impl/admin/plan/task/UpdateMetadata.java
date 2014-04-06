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

import java.util.logging.Level;

import oracle.kv.impl.admin.PlannerAdmin;
import oracle.kv.impl.admin.plan.MetadataPlan;
import oracle.kv.impl.metadata.Metadata;

import com.sleepycat.persist.model.Persistent;

/**
 * Broadcasts metadata to all RNs. This task may be used standalone
 * or may be subclassed to provide additional functionality.
 */
@Persistent
public class UpdateMetadata<T extends Metadata<?>> extends SingleJobTask {
    private static final long serialVersionUID = 1L;

    protected /*final*/ MetadataPlan<T> plan;

    public UpdateMetadata(MetadataPlan<T> plan) {
        this.plan = plan;
    }

    /*
     * No-arg ctor for use by DPL.
     */
    @SuppressWarnings("unused")
    protected UpdateMetadata() {
    }

    /**
     * Gets the updated metadata to broadcast. The default implementation
     * calls plan.getMetadata(). If null is returned no broadcast is made and
     * the task ends with SUCCEEDED.
     *
     * @return the metadata to broadcast or null
     */
    protected T updateMetadata() {
        return plan.getMetadata();
    }

    @Override
    public State doWork() throws Exception {
        final T md = updateMetadata();

        if (md == null) {
            return State.SUCCEEDED;
        }
        plan.getLogger().log(Level.INFO, "About to broadcast {0}", md);

        final PlannerAdmin pa = plan.getAdmin();

        if (!Utils.broadcastMetadataChangesToRNs(plan.getLogger(),
                                                 md,
                                                 pa.getCurrentTopology(),
                                                 toString(),
                                                 pa.getParams().getAdminParams(),
                                                 plan)) {
            return State.INTERRUPTED;
        }
        return State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return true;
    }
}
