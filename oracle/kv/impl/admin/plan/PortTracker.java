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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.PortRange;

/**
 * A utility class for assigning HA ports to rep nodes.  Determines the next
 * available HA port on a given node, based on ports currently in use and the
 * defined range of HA ports.
 */
public class PortTracker {

    /* Free ports for each SN. */
    private final Map<StorageNodeId, List<HAPort>> freePorts;
    private final Parameters parameters;

    public PortTracker(Topology topology,
                       Parameters parameters,
                       List<StorageNodeId> targetSNs) {

        this.parameters = parameters;
        /* Create a map that holds the unused HA ports. */
        freePorts = new HashMap<StorageNodeId, List<HAPort>>();

        /* Initialize the map with all the available HA ports. */
        for (StorageNodeId snId : targetSNs) {
            StorageNodeParams snp = parameters.get(snId);
            List<HAPort> configured = new ArrayList<HAPort>();
            List<Integer> r = PortRange.getRange(snp.getHAPortRange());

            final int firstPort = r.get(0);
            final int lastPort = r.get(1);
            for (int i = firstPort; i <= lastPort; i++)  {
                configured.add(new HAPort(i));
            }
            freePorts.put(snId, configured);
        }

        /* Remove the ports that are in use. */
        for (RepGroup rg : topology.getRepGroupMap().getAll()) {
            for (RepNode rn: rg.getRepNodes()) {
                RepNodeParams rnp = parameters.get(rn.getResourceId());
                if (rnp == null) {

                    /*
                     * This RN is in the target topology, but is not yet fully
                     * deployed, and does not have a params instant. It's not
                     * consuming any ports, so we can skip it.
                     */
                    continue;
                }

                int inUsePort = rnp.getHAPort();
                StorageNodeId rnSN = rn.getStorageNodeId();

                if (targetSNs.contains(rnSN)) {

                    StorageNodeParams snp = parameters.get(rnSN);

                    /*
                     * Take the port in use out of the free port set. Assert
                     * that it was there in the first place.
                     */
                    boolean removed =
                        freePorts.get(rnSN).remove(new HAPort(inUsePort));
                    assert removed : "Port " + inUsePort +
                        " was used in repNode " + rn + " but was not in " +
                        " available port list(" + snp.getHAPortRange() +
                        ") for" + rnSN;
                }
            }
        }

        for (AdminId aid : parameters.getAdminIds()) {
            AdminParams ap = parameters.get(aid);
            if (ap.getNodeHostPort() == null) {
                /* Some parameters may not yet be initialized. */
                continue;
            }

            int inUsePort =
                Integer.parseInt(ap.getNodeHostPort().split(":")[1]);
            StorageNodeId apSN = ap.getStorageNodeId();

            if (targetSNs.contains(apSN)) {

                StorageNodeParams snp = parameters.get(apSN);

                boolean removed =
                    freePorts.get(apSN).remove(new HAPort(inUsePort));
                assert removed : "Port " + inUsePort +
                    " was used in Admin " + aid + " but was not in " +
                    " available port list(" + snp.getHAPortRange() +
                    ") for" + apSN;
            }
        }
    }

    /**
     * Constructor used when there is only a single storage node of interest,
     * rather than a pool.
     */
    public PortTracker(Topology topology,
                       Parameters parameters,
                       StorageNodeId target) {
        this(topology, parameters, Collections.singletonList(target));
    }

    /**
     * Get the next available HA port.
     */
    public int getNextPort(StorageNodeId targetSNId) {

        List<HAPort> available = freePorts.get(targetSNId);
        if (available.size() == 0) {
            StorageNodeParams snp = parameters.get(targetSNId);

            throw new IllegalCommandException("Storage node " + targetSNId  +
                                              " with HAPortRange of " +
                                              snp.getHAPortRange() +
                                              " does not have any available" +
                                              " HA ports left.");
        }
        int nextPort = available.get(0).value;
        available.remove(0);
        return nextPort;
    }

    /*
     * Note that we represent ports as a class, rather than just an Integer in
     * these maps, because otherwise the List.remove(Object),
     * List.remove(Integer) methods get confused.
     */
    private class HAPort{
        final int value;
        HAPort(int value) {
            this.value = value;
        }

        /*
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
            final int prime = 31;
            return prime * value;
        }

        /* (
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof HAPort)) {
                return false;
            }
            HAPort other = (HAPort) obj;
            if (value != other.value) {
                return false;
            }
            return true;
        }
    }
}

