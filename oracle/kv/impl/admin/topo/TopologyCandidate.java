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

package oracle.kv.impl.admin.topo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import oracle.kv.impl.admin.param.DatacenterParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.TopologyPrinter;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

/**
 * The topology candidate packages:
 * - a user provided name
 * - a target topology that is a prospective layout for the store
 * - information about how we arrived at the target topology, used to illuminate
 * the decision process for the user, and as a debugging aid.
 *
 * Candidates are generated and updated in response to user actions. The
 * "topology create" command makes the initial candidate, while commands such
 * as "topology redistribute" modify a candidate. Candidates are only
 * blueprints for potential store layouts. A topology that is actually deployed
 * is represented by a RealizedTopology instance.
 */

@Entity
 public class TopologyCandidate implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * NO_NAME is used for situations where we need compatibility with R1,
     * which is before candidates existed, or for topology changing commands
     * that do not require a user created candidate topology. For example, the
     * migrate-sn command changes the topology, but since the parameters to the
     * command adequately describe the change, that command doesn't require
     * that the user supply a candidate,
     */
    public static final String NO_NAME = "none";

    @PrimaryKey
    private String candidateName;

    private Topology topology;

    /*
     * Mount point assignments are made when the topology is built. Mount
     * points are not known by the Topology class, so we store the assignments
     * here, as a sort of proxy for the RepNodeParams that will be created
     * when the topology is deployed.
     */
    private Map<RepNodeId, String> mountPointAssignments;

    /* For debugging/auditing */
    private Map<DatacenterId,Integer> maxShards;
    private DatacenterId smallestDC;
    private List<String> auditTrail;

    /* For DPL */
    @SuppressWarnings("unused")
    private TopologyCandidate() {
    }

    public TopologyCandidate(String name, Topology topoCopy) {
        this.candidateName = name;
        topology = topoCopy;
        mountPointAssignments = new HashMap<RepNodeId, String>();
        maxShards = new HashMap<DatacenterId,Integer>();
        auditTrail = new ArrayList<String>();
        log("starting number of shards: " +  topoCopy.getRepGroupMap().size());
        log("number of partitions: " + topoCopy.getPartitionMap().size());
    }

    public Topology getTopology() {
       return topology;
    }

    public void resetTopology(Topology topo) {
        topology = topo;
    }

    public String getName() {
       return candidateName;
    }

    public void setShardsPerDC(DatacenterId dcId, int numShards) {
        maxShards.put(dcId, numShards);
    }

    public void setSmallestDC(DatacenterId dcId) {
        smallestDC = dcId;
    }

    String getSmallestDCName(Parameters params) {
        if (smallestDC != null) {
            return smallestDC + "/" + params.get(smallestDC).getName();
        }
        return null;
    }

    /**
     * Display the candidate. If verbose is specified, show the fields meant
     * for auditing/debug use.
     */
    public String display() {
        StringBuilder sb = new StringBuilder();
        sb.append("name= ").append(candidateName);
        sb.append("\nzones= ").append(getDatacenters());
        sb.append("\n");
        sb.append(TopologyPrinter.printTopology(this, null, false));

        return sb.toString();
    }

    public Set<DatacenterId> getDatacenters() {
        Set<DatacenterId> dcIds = new HashSet<DatacenterId>();
        for (Datacenter dc : topology.getDatacenterMap().getAll()) {
            dcIds.add(dc.getResourceId());
        }
        return dcIds;
    }

    public String showDatacenters(Parameters params) {

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<DatacenterId,Integer> entry : maxShards.entrySet()) {
            DatacenterId dcId = entry.getKey();
            DatacenterParams dcp = params.get(dcId);
            sb.append(dcId).append("/").append(dcp.getName());
            sb.append(" maximum shards= ").append(entry.getValue());
            sb.append("\n");
        }
        return sb.toString();
    }

    public String showAudit() {
        StringBuilder sb = new StringBuilder();
        sb.append("[Audit]\n");
        for(String s : auditTrail) {
            sb.append(s).append("\n");
        }
        return sb.toString();
    }

    /**
     * Save some information about the building of the topology, for audit/
     * debugging assistance later.
     */
    public void log(String s) {
        auditTrail.add(s);
    }

    public void saveMountPoint(RepNodeId rnId, String mountPoint) {
        mountPointAssignments.put(rnId, mountPoint);
    }

    public String getMountPoint(RepNodeId rnId) {
        return mountPointAssignments.get(rnId);
    }

    public Map<RepNodeId, String> getAllMountPoints() {
        return mountPointAssignments;
    }
}
