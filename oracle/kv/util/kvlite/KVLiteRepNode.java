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

package oracle.kv.util.kvlite;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.Partition;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;

import com.sleepycat.je.rep.NodeType;

/**
 * See KVLite.
 * This class creates a standalone store with only a RepNode, no admin.
 */
public class KVLiteRepNode {

    private final StorageNodeAgentAPI sna;
    private final String kvstore;
    private Topology topo;
    private final BootstrapParams bp;

    public KVLiteRepNode(String kvstore,
                         StorageNodeAgentAPI sna,
                         BootstrapParams bp,
                         int numPartitions) {
        this.bp = bp;
        this.sna = sna;
        this.kvstore = kvstore;
        createTopology(numPartitions);
    }

    public void run()
        throws Exception {

        registerSNA();
        createRepNode();
        TestStatus.setActive(true);
    }

    private void registerSNA()
        throws Exception {

        StorageNodeParams snp = new StorageNodeParams
            (new StorageNodeId(1), bp.getHostname(),
             bp.getRegistryPort(), "");
        GlobalParams gp = new GlobalParams(kvstore);

        /**
         * This will kill any bootstrap admin that was started.
         */
        sna.register(gp.getMap(), snp.getMap(), false);
    }

    private void createTopology(int numPartitions) {

        topo = new Topology(kvstore);

        /**
         * Use kvstore name as Datacenter. It will have a repFactor of 1.
         */
        Datacenter dc =
            Datacenter.newInstance(kvstore, 1, DatacenterType.PRIMARY);
        topo.add(dc);

        /**
         * Now a StorageNode.
         */
        StorageNode sn =
            new StorageNode(dc, bp.getHostname(), bp.getRegistryPort());
        topo.add(sn);

        /**
         * Now RepGroup, RepNode.  NOTE: need to add RG to Topo before adding
         * RNs to RG.
         */
        RepGroup rg = new RepGroup();
        topo.add(rg);
        RepNode rn = new RepNode(sn.getResourceId());
        rg.add(rn);

        /**
         * Partition for the RepGroup.
         */
        for (int i = 0; i < numPartitions; i++) {
            topo.add(new Partition(rg));
        }

        rn = topo.get(new RepNodeId(1, 1));
    }

    private RepNodeParams createRepNodeParams() {

        String host = bp.getHostname();
        int nodeHAPort = bp.getRegistryPort() + 1;
        String mountPoint = null;
        List<String> mountPoints = bp.getMountPoints();
        if (mountPoints != null && mountPoints.size() > 0) {
            mountPoint = mountPoints.get(0);
        }

        RepNodeParams rnp = new RepNodeParams
            (new StorageNodeId(1), new RepNodeId(1, 1),
             false, /* disabled */
             host, nodeHAPort,
             host, nodeHAPort /* helper*/,
             mountPoint, NodeType.ELECTABLE);
        return rnp;
    }

    private void createRepNode()
        throws Exception {

        RepNodeParams rnp = createRepNodeParams();
        final Set<Metadata<? extends MetadataInfo>> metadataSet =
                new HashSet<Metadata<? extends MetadataInfo>>(1);
        metadataSet.add(topo);
        sna.createRepNode(rnp.getMap(), metadataSet);
    }
}
