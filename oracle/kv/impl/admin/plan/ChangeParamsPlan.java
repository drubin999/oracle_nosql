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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.plan.task.NewRepNodeParameters;
import oracle.kv.impl.admin.plan.task.StartRepNode;
import oracle.kv.impl.admin.plan.task.StopRepNode;
import oracle.kv.impl.admin.plan.task.WaitForRepNodeState;
import oracle.kv.impl.admin.plan.task.WriteNewParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;

import com.sleepycat.persist.model.Persistent;

@Persistent
public class ChangeParamsPlan extends AbstractPlan {

    private static final long serialVersionUID = 1L;

    protected ParameterMap newParams;
    public ChangeParamsPlan(AtomicInteger idGenerator,
                            String name,
                            Planner planner,
                            Topology topology,
                            Set<RepNodeId> rnids,
                            ParameterMap map) {

        super(idGenerator, name, planner);

        this.newParams = map;
        /* Do as much error checking as possible, before the plan is executed.*/
        validateParams(newParams);
        Set<RepNodeId> restartIds = new HashSet<RepNodeId>();

        /*
         * First write the new params on all nodes.
         */
        for (RepNodeId rnid : rnids) {
            StorageNodeId snid = topology.get(rnid).getStorageNodeId();
            ParameterMap filtered = newParams.readOnlyFilter();
            addTask(new WriteNewParams(this, filtered, rnid, snid, false));
            RepNodeParams current = planner.getAdmin().getRepNodeParams(rnid);

            /*
             * If restart is required put the Rep Node in a new set to be
             * handled below, otherwise, add a task to refresh parameters.
             */
            if (filtered.hasRestartRequiredDiff(current.getMap())) {
                restartIds.add(rnid);
            } else {
                addTask(new NewRepNodeParameters(this, rnid));
            }
        }

        if (!restartIds.isEmpty()) {
            List<RepNodeId> restart = sort(restartIds, topology);
            for (RepNodeId rnid : restart) {
                StorageNodeId snid = topology.get(rnid).getStorageNodeId();

                addTask(new StopRepNode(this, snid, rnid, false));
                addTask(new StartRepNode(this, snid, rnid, false));
                addTask(new WaitForRepNodeState(this,
                                                rnid,
                                                ServiceStatus.RUNNING));
            }
        }
    }

    protected List<RepNodeId> sort(Set<RepNodeId> ids,
    		@SuppressWarnings("unused") Topology topology) {
        List<RepNodeId> list = new ArrayList<RepNodeId>();
        for (RepNodeId id : ids) {
            list.add(id);
        }
        return list;
    }

    protected void validateParams(ParameterMap map) {

        /* Check for incorrect JE params. */
        try {
            ParameterUtils pu = new ParameterUtils(map);
            pu.getEnvConfig();
            pu.getRepEnvConfig(null, true);
        } catch (Exception e) {
            throw new IllegalCommandException("Incorrect parameters: " +
                                              e.getMessage(), e);
        }
    }

    /* DPL */
    protected ChangeParamsPlan() {
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
    public String getDefaultName() {
        return "Change RepNode Params";
    }

    public RepNodeParams getNewParams() {
        return new RepNodeParams(newParams);
    }

    @Override
    void stripForDisplay() {
        newParams = null;
    }
}
