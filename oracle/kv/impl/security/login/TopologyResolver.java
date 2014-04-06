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

package oracle.kv.impl.security.login;

import java.util.List;

import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;

/**
 * TopologyResolver defines an abstract interface used to resolve resource id
 * values.
 */
public interface TopologyResolver {

    /**
     * A class describing a StorageNode component.
     */
    public class SNInfo {
        private final String hostname;
        private final int registryPort;
        private final StorageNodeId snId;

        public SNInfo(String hostname, int registryPort, StorageNodeId snId) {
            this.hostname = hostname;
            this.registryPort = registryPort;
            this.snId = snId;
        }

        public String getHostname() {
            return hostname;
        }

        public int getRegistryPort() {
            return registryPort;
        }

        public StorageNodeId getStorageNodeId() {
            return snId;
        }
    }

    /**
     * Given a resource id, determine the storage node on which is resides.
     *
     * @param rid a ResourceID, which should generally be one of
     *    StorageNodeId, RepNodeId or AdminId
     * @return the SNInfo corresponding to the resource id.  If the
     *    StorageNode cannot be determined or if the resource id is not an
     *    id that equates to a StorageNode component, return null.
     */
    SNInfo getStorageNode(ResourceId rid);

    /**
     * Return a list of known RepNodeIds in the store. This is used to allow
     * an admin to use RepNode services to validate persistent tokens.
     *
     * @param maxReturn a limit on the number of RepNodes to return
     * @return a list or RepNodeIds if the resolver has access to topology
     *   information, or null otherwise
     */
    List<RepNodeId> listRepNodeIds(int maxReturn);
}
