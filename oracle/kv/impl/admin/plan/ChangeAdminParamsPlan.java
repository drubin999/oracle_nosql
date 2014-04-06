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

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.PlannerAdmin;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.task.NewAdminParameters;
import oracle.kv.impl.admin.plan.task.StartAdmin;
import oracle.kv.impl.admin.plan.task.StopAdmin;
import oracle.kv.impl.admin.plan.task.WaitForAdminState;
import oracle.kv.impl.admin.plan.task.WriteNewAdminParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;

import com.sleepycat.persist.model.Persistent;

@Persistent
public class ChangeAdminParamsPlan extends AbstractPlan {
    private static final long serialVersionUID = 1L;

    private ParameterMap newParams;

    /*
     * An entry in the needsAction set is made by WriteNewAdminParams for the
     * AdminId associated with the task, if the task indeed modified the
     * parameters.
     *
     * Subsequent shutdown and restart tasks check the for the presence of
     * their associated AdminId to determine whether or not they need to take
     * action.
     */
    private transient Set<AdminId> needsAction = new HashSet<AdminId>();

    public ChangeAdminParamsPlan(AtomicInteger idGenerator,
                                 String name,
                                 Planner planner,
                                 AdminId givenId,
                                 ParameterMap map) {

        this(idGenerator, name, planner, givenId, null, null, map);
    }

    public ChangeAdminParamsPlan(AtomicInteger idGenerator,
                                 String name,
                                 Planner planner,
                                 AdminId givenId,
                                 DatacenterId dcid,
                                 Topology topology,
                                 ParameterMap map) {

        super(idGenerator, name, planner);

        newParams = map;
        validateParams(newParams);

        PlannerAdmin admin = planner.getAdmin();
        Parameters parameters = admin.getCurrentParameters();

        Set<AdminId> allAdmins;
        /* If the givenID is null, make a plan to change ALL admins. */
        if (givenId == null) {
            allAdmins = parameters.getAdminIds(dcid, topology);
        } else {
            allAdmins = new HashSet<AdminId>();
            allAdmins.add(givenId);
        }

        Set<AdminId> restartIds = new HashSet<AdminId>();

        ParameterMap filtered = newParams.readOnlyFilter();

        for (AdminId aid : allAdmins) {
            AdminParams current = parameters.get(aid);
            StorageNodeId snid = current.getStorageNodeId();
            StorageNodeParams snp = parameters.get(snid);
            String hostname = snp.getHostname();
            int registryPort = snp.getRegistryPort();
            addTask(new WriteNewAdminParams(this, filtered, aid, snid));

            if (filtered.hasRestartRequiredDiff(current.getMap())) {
                restartIds.add(aid);
            } else {
                addTask
                    (new NewAdminParameters(this, hostname, registryPort, aid));
            }
        }

        if (!restartIds.isEmpty()) {
            AdminId self = admin.getParams().getAdminParams().getAdminId();
            boolean restartSelf = false;

            for (AdminId aid : restartIds) {
                /* Check for this Admin instance. */
                if (aid.equals(self)) {
                    restartSelf = true;
                    continue; /* Don't restart self, yet. */
                }
                addRestartTasks(aid, parameters);
            }

            /* Do self last. */
            if (restartSelf) {
                addRestartTasks(self, parameters);
            }
        }
    }

    private void addRestartTasks(AdminId aid, Parameters parameters) {
        AdminParams current = parameters.get(aid);
        StorageNodeId snid = current.getStorageNodeId();
        addTask(new StopAdmin(this, snid, aid, false));
        addTask(new StartAdmin(this, snid, aid, false));
        addTask(new WaitForAdminState(this, snid, aid, ServiceStatus.RUNNING));
    }

    ChangeAdminParamsPlan() {
    }

    private void validateParams(ParameterMap map) {

        /* Check for incorrect JE params. */
        try {
            ParameterUtils pu = new ParameterUtils(map);
            pu.getEnvConfig();
            pu.getRepEnvConfig();
        } catch (Exception e) {
            throw new IllegalCommandException("Incorrect parameters: " +
                                              e.getMessage(), e);
        }
    }

    @Override
    public boolean isExclusive() {
        return false;
    }

    @Override
    public void preExecutionSave() {
       /* Nothing to save before execution. */
    }

    @Override
    public String getDefaultName() {
        return "Change Admin Params";
    }

    public AdminParams getNewParams() {
        return new AdminParams(newParams);
    }

    public Set<AdminId> getNeedsActionSet() {
        return needsAction;
    }

    public void interrupt() {
        planner.interruptPlan(getId());
    }

    @Override
    void stripForDisplay() {
        newParams = null;
    }
}
