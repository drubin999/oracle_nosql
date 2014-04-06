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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import oracle.kv.impl.admin.PlannerAdmin;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.task.DestroyAdmin;
import oracle.kv.impl.admin.plan.task.EnsureAdminNotMaster;
import oracle.kv.impl.admin.plan.task.NewAdminParameters;
import oracle.kv.impl.admin.plan.task.RemoveAdminRefs;
import oracle.kv.impl.admin.plan.task.UpdateAdminHelperHost;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.StorageNodeId;

import com.sleepycat.persist.model.Persistent;

/**
 * A plan for removing an Admin replica.
 */
@Persistent(version=0)
public class RemoveAdminPlan extends AbstractPlan {

    private static final long serialVersionUID = 1L;

    @Deprecated
    AdminId victim;

    public RemoveAdminPlan(AtomicInteger idGen,
                           String name,
                           Planner planner,
                           DatacenterId dcid,
                           AdminId victim) {
        super(idGen, name, planner);

        final PlannerAdmin admin = planner.getAdmin();
        final Parameters parameters = admin.getCurrentParameters();

        Set<AdminId> victimIds = new HashSet<AdminId>();

        /*
         * Determine the Admins to remove based on the input parameters.
         * If both DatacenterId and AdminId are null, throw an
         * IllegalArgumentException. If AdminId is not null (regardless of
         * DatacenterId), then remove only the Admin with the specified
         * AdminId. Otherwise, remove all (and only) the Admins running
         * in the datacenter with the specified DatacenterId.
         */
        if (victim == null) {
            if (dcid != null) {
                victimIds =
                    parameters.getAdminIds(dcid, admin.getCurrentTopology());
            } else {
                throw new IllegalArgumentException(
                    "null specified for both DatacenterId and " +
                    "AdminId parameters");
            }
        } else {
            victimIds.add(victim);
        }
        for (AdminId victimId : victimIds) {

            final AdminParams ap = parameters.get(victimId);
            final StorageNodeId snid = ap.getStorageNodeId();

            /*
             * If running on the current victim, the first task will shut
             * down and restart that Admin, with the effect of transferring
             * mastership to a replica.  The plan will be interrupted, and
             * then must be re-executed on the new master.  On the second
             * execution, this task will be a no-op because we won't be
             * running on that victim, unless no replica was able to assume
             * mastership for some reason, and mastership has rebounded back
             * to the victim.
             */
            addTask(new EnsureAdminNotMaster(this, victimId));

            /*
             * Remove the Admin from all configurations.
             */
            addTask(new RemoveAdminRefs(this, victimId));

            /*
             * Update the helpers of the non-targeted Admins to exclude the
             * current Admin that was removed.
             */
            for (AdminParams oap : parameters.getAdminParams()) {

                final AdminId aid = oap.getAdminId();
                if (victimIds.contains(aid)) {
                    continue;
                }
                final StorageNodeParams osnp =
                    parameters.get(oap.getStorageNodeId());
                final String hostname = osnp.getHostname();
                final int registryPort = osnp.getRegistryPort();
                addTask(new UpdateAdminHelperHost(this, aid));
                addTask(
                    new NewAdminParameters(this, hostname, registryPort, aid));
            }

            /*
             * Finally, tell the SNA to shut down the Admin and remove it from
             * its local configuration.  This step is done last so that if the
             * SNA is unavailable, the Admin can still be removed from the
             * store.  If this step fails because the SNA is unavailable, the
             * operator can choose either to re-execute the plan when the SNA
             * returns to service; or, if the SNA is expected never to return,
             * to cancel the plan.
             *
             * Were such a plan to be canceled, and were the SNA then to
             * return, it would require manual editing to remove the Admin from
             * the SNA's local configuration.  No new plan to remove the Admin
             * can be created after the Admin's parameter record is removed
             * from the database, which happens in the RemoveAdminRefs task.
             */
            addTask(new DestroyAdmin(this, snid, victimId));
        }
    }

    @SuppressWarnings("unused")
    private RemoveAdminPlan() {
    }

    @Override
    public void preExecutionSave() {
    }

    @Override
    public boolean isExclusive() {
        return true;
    }

    @Override
    public String getDefaultName() {
        return "Remove Admin Replica";
    }

    @Override
    void stripForDisplay() {
        /* Nothing worth stripping */
    }
}
