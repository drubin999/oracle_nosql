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

import java.rmi.RemoteException;

import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.persist.model.Persistent;

/**
 * Tell the SN to delete this RN from its configuration file, and from its set
 * of managed processes.
 */
@Persistent
public class DeleteRepNode extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private StorageNodeId snId;
    private RepNodeId rnId;
    private AbstractPlan plan;

    public DeleteRepNode() {
    }

    public DeleteRepNode(AbstractPlan plan,
                         StorageNodeId snId,
                         RepNodeId rnId) {
        this.plan = plan;
        this.snId = snId;
        this.rnId = rnId;
    }

    @Override
    public State doWork()
        throws Exception {

        try {
            RegistryUtils registry =
                new RegistryUtils(plan.getAdmin().getCurrentTopology(),
                                  plan.getAdmin().getLoginManager());
            StorageNodeAgentAPI sna = registry.getStorageNodeAgent(snId);
            sna.destroyRepNode(rnId, true /* deleteData */);
            return State.SUCCEEDED;
            
        } catch (java.rmi.NotBoundException notbound) {
            plan.getLogger().info
                (snId + " cannot be contacted to delete " + rnId + ":" +
                 notbound);
        } catch (RemoteException e) {
            plan.getLogger().severe
                         ("Attempting to delete " + rnId + " from " + snId +
                          ": " +  e);
        }
        return State.ERROR;
    }

    @Override
    public String toString() {
        StorageNodeParams snp =
            (plan.getAdmin() != null ?
             plan.getAdmin().getStorageNodeParams(snId) : null);
        return super.toString() + " remove " + rnId + " from " +
            (snp != null ? snp.displaySNIdAndHost() : snId);
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    /* Lock the target RN */
    @Override
    public void lockTopoComponents(Planner planner) {
        planner.lockRN(plan.getId(), plan.getName(), rnId);        
    }
}
