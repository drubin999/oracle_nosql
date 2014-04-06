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

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.rep.RepNodeStatus;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.persist.model.Persistent;

/**
 * Change parameters store-wide for all RepNodes.
 *
 * The UI should filter based on parameters available for store-wide
 * modification but it's done here as well, just in case.
 *
 * Note that the sort method will only be called if there are restart-required
 * parameters to be modified.
 */
@Persistent
public class ChangeAllParamsPlan extends ChangeParamsPlan {

    private static final long serialVersionUID = 1L;

    public ChangeAllParamsPlan(AtomicInteger idGenerator,
                               String name,
                               Planner planner,
                               Topology topology,
                               DatacenterId dcid,
                               ParameterMap map,
                               Logger logger) {

        /* Filter params based on store-wide scope just in case */
        super(idGenerator, name, planner, topology,
              topology.getRepNodeIds(dcid),
              map.filter(ParameterState.Scope.STORE));
        this.logger = logger;
    }

    /**
     * Sort for optimal restart order.  This may change based on experience.
     * Current order:
     *  o restart replicas first, in no particular order
     *  o restart masters, in no particular order
     * This means that the sort just does a ping and separates replicas and
     * masters.
     */
    @Override
    protected List<RepNodeId> sort(Set<RepNodeId> ids, Topology topology) {
        int replicaIndex = 0;
        int masterIndex = ids.size() - 1;
        RegistryUtils ru = new RegistryUtils(topology, getLoginManager());
        RepNodeId[] list = new RepNodeId[ids.size()];
        for (RepNodeId id : ids) {
            if (isReplica(id, ru)) {
                list[replicaIndex++] = id;
            } else {
                list[masterIndex--] = id;
            }
        }
        return Arrays.asList(list);
    }

    /**
     * Override the validate method because the parameters provided will be a
     * partial set and ChangeParamsPlan.validate requires a full set.
     */
    @Override
    protected void validateParams(ParameterMap map) {
        return;
    }

    /**
     * Try to ping the service.  If it is not available, log the fact and add
     * the id to the end of the list.  If it's still not available when the
     * plan is executed the user will see that.
     */
    private boolean isReplica(RepNodeId id, RegistryUtils ru) {
        Exception e = null;
        try {
            RepNodeAdminAPI rna = ru.getRepNodeAdmin(id);
            RepNodeStatus status = rna.ping();
            return status.getReplicationState().isReplica();
        } catch (RemoteException re) {
            e = re;
        } catch (NotBoundException nbe) {
            e = nbe;
        }
        logger.warning("Could not reach node " + id +
                       " in ChangeAllParamsPlan: " + e);
        return false;
    }

    /* DPL */
    protected ChangeAllParamsPlan() {
    }
}
