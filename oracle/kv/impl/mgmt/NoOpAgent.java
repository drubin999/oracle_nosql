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

package oracle.kv.impl.mgmt;

import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.measurement.ProxiedServiceStatusChange;
import oracle.kv.impl.sna.ServiceManager;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.ServiceStatusTracker;

/**
 * A No-op version of the MgmtAgent.
 */
public class NoOpAgent implements MgmtAgent {

    @SuppressWarnings("unused")
    public NoOpAgent(StorageNodeAgent sna,
                    int pollingPort,
                    String trapHostName,
                    int trapPort,
                    ServiceStatusTracker tracker) {
    }

    @Override
    public void setSnaStatusTracker(ServiceStatusTracker snaStatusTracker) {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void proxiedStatusChange(ProxiedServiceStatusChange sc) {
    }

    @Override
    public void addRepNode(RepNodeParams rnp, ServiceManager mgr) {
    }

    @Override
    public void removeRepNode(RepNodeId rnid) {
    }

    @Override
    public boolean checkParametersEqual(int pollp, String traph, int trapp) {
        return true;
    }

    @Override
    public void addAdmin(AdminParams ap, ServiceManager mgr) throws Exception {
    }

    @Override
    public void removeAdmin() {
    }
}
