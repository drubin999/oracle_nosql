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

import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.sleepycat.persist.model.Persistent;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.NonfatalAssertionException;
import oracle.kv.impl.admin.PlannerAdmin;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.task.NewAdminGlobalParameters;
import oracle.kv.impl.admin.plan.task.NewRNGlobalParameters;
import oracle.kv.impl.admin.plan.task.WriteNewGlobalParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.VersionUtil;

@Persistent
public class ChangeGlobalSecurityParamsPlan extends AbstractPlan {

    private static final long serialVersionUID = 1L;

    private static final KVVersion SECURITY_VERSION =
        KVVersion.R3_0; /* R3.0 Q1/2014 */

    private ParameterMap newParams = null;
    private Parameters currentParams;
    private static final Set<AdminId> allAdminIds = new HashSet<AdminId>();

    public ChangeGlobalSecurityParamsPlan(AtomicInteger idGenerator,
                                          String name,
                                          Planner planner,
                                          Topology topology,
                                          ParameterMap map) {
        super(idGenerator, name, planner);

        checkSecurityVersion();

        this.newParams = map;
        PlannerAdmin admin = planner.getAdmin();
        currentParams = admin.getCurrentParameters();
        allAdminIds.addAll(currentParams.getAdminIds());

        final ParameterMap filtered = newParams.readOnlyFilter().
            filter(EnumSet.of(ParameterState.Info.GLOBAL,
                              ParameterState.Info.SECURITY));

        final GlobalParams currentGlobalParams =
            currentParams.getGlobalParams();
        final boolean needsRestart =
            filtered.hasRestartRequiredDiff(currentGlobalParams.getMap());

        /* There should be no restart required */
        if (needsRestart) {
            throw new NonfatalAssertionException(
                "Parameter change would require an admin restart, which is " +
                "not supported.");
        }

        final List<StorageNodeId> snIds = topology.getStorageNodeIds();
        for (final StorageNodeId snId : snIds) {

            /*
             * First write the new global security parameters on all storage
             * nodes
             */
            addTask(new WriteNewGlobalParams(this, filtered, snId, false));

            addNewGlobalParametersTasks(snId, topology);
        }
    }

    /* Non-arg ctor for DPL */
    @SuppressWarnings("unused")
    private ChangeGlobalSecurityParamsPlan() {
    }

    /*
     * Add newGlobalParameter tasks for all components in the specified storage
     * node, including Admin and RepNode services
     */
    private void addNewGlobalParametersTasks(final StorageNodeId snId,
                                             final Topology topo) {

        final Set<RepNodeId> refreshRns = topo.getHostedRepNodeIds(snId);
        for (final RepNodeId rnid : refreshRns) {
            addTask(new NewRNGlobalParameters(this, rnid));
        }

        for (final AdminId aid : allAdminIds) {
            final StorageNodeId sidForAdmin =
                currentParams.get(aid).getStorageNodeId();
            if (sidForAdmin.equals(snId)) {
                final StorageNodeParams snp = currentParams.get(sidForAdmin);
                final String hostname = snp.getHostname();
                final int registryPort = snp.getRegistryPort();

                addTask(new NewAdminGlobalParameters(
                    this, hostname, registryPort, aid));
            }
        }
    }

    private void checkSecurityVersion() {

        /* Ensure all nodes in the store support security feature */
        final PlannerAdmin admin = planner.getAdmin();
        final KVVersion storeVersion = admin.getStoreVersion();

        if (VersionUtil.compareMinorVersion(
                storeVersion, SECURITY_VERSION) < 0) {
            throw new IllegalCommandException(
                "Cannot perform security metadata related operations when" +
                " not all nodes in the store support security feature." +
                " The highest version supported by all nodes is " +
                storeVersion.getNumericVersionString() +
                ", but security metadata operations require version "
                + SECURITY_VERSION.getNumericVersionString() + " or later.");
        }
    }

    @Override
    public String getDefaultName() {
        return "Change Global Security Params";
    }

    @Override
    public boolean isExclusive() {
        return false;
    }

    @Override
    void preExecutionSave() {
        /* Nothing to save before execution. */
    }

    @Override
    void stripForDisplay() {
        newParams = null;
        currentParams = null;
    }
}
