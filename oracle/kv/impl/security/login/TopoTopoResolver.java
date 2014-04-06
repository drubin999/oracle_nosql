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

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import oracle.kv.impl.api.TopologyManager;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;

/**
 * TopoTopoResolver provides an implementation of TopologyResolver that
 * resolves based on a Topology object.
 */
public class TopoTopoResolver implements TopologyResolver {

    private final TopoHandle topoHandle;
    private final SNInfo localSNInfo;
    private final Logger logger;

    /**
     * Creates a TopologyResolver.
     * @param topoHandle a handle to access a topology object
     * @param localSNInfo our local SN Info - may be null if we are still
     * in bootstrap mode
     */
    public TopoTopoResolver(TopoHandle topoHandle,
                            SNInfo localSNInfo,
                            Logger logger) {
        this.topoHandle = topoHandle;
        this.localSNInfo = localSNInfo;
        this.logger = logger;
    }

    public interface TopoHandle {
        Topology getTopology();
    }

    public static class TopoTopoHandle implements TopoHandle {

        private volatile Topology topo;

        public TopoTopoHandle(Topology initialTopo) {
            topo = initialTopo;
        }

        @Override
        public Topology getTopology() {
            return topo;
        }

        public void setTopology(Topology newTopo) {
            this.topo = newTopo;
        }
    }

    public static class TopoMgrTopoHandle implements TopoHandle {

        private volatile TopologyManager topoMgr;

        public TopoMgrTopoHandle(TopologyManager topoMgr) {
            this.topoMgr = topoMgr;
        }

        @Override
        public Topology getTopology() {
            return topoMgr.getTopology();
        }

        public void setTopoMgr(TopologyManager newTopoMgr) {
            this.topoMgr = newTopoMgr;
        }
    }

    /**
     * Resolve a ResourceID to its SNInfo.
     */
    @Override
    public SNInfo getStorageNode(ResourceId target) {

        if (localSNInfo != null &&
            target instanceof StorageNodeId &&
            target.equals(localSNInfo.getStorageNodeId())) {

            logger.fine("TopoTopoResolver resolved target from localSNInfo");

            return localSNInfo;
        }

        final Topology topo = topoHandle.getTopology();
        if (topo == null) {
            logger.info("TopoTopoResolver unable to resolve target: " +
                        target + " without topology");
            return null;
        }

        final Topology.Component<?> comp = topo.get(target);
        if (comp == null) {
            return null;
        }

        if (comp instanceof RepNode) {
            final RepNode rn = (RepNode) comp;
            final StorageNode sn = topo.get(rn.getStorageNodeId());

            if (sn == null) {
                /* only an invalid topology could break this */
                throw new IllegalStateException("corrupted Topology");
            }

            return new SNInfo(sn.getHostname(), sn.getRegistryPort(),
                              sn.getStorageNodeId());
        } else if (comp instanceof StorageNode) {
            final StorageNode sn = (StorageNode) comp;
            return new SNInfo(sn.getHostname(), sn.getRegistryPort(),
                              sn.getStorageNodeId());
        } else {
            return null;
        }
    }

    @Override
    public List<RepNodeId> listRepNodeIds(int maxReturn) {
        final Topology topo = topoHandle.getTopology();
        if (topo == null) {
            return null;
        }

        final List<RepNodeId> rnList = new ArrayList<RepNodeId>();
        for (RepNodeId rnId : topo.getRepNodeIds()) {
            if (rnList.size() >= maxReturn) {
                break;
            }
            rnList.add(rnId);
        }

        return rnList;
    }
}
