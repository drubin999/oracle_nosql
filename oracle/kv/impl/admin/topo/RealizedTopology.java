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

import java.text.DateFormat;
import java.util.Date;

import oracle.kv.impl.admin.plan.DeploymentInfo;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.FormatUtils;
import oracle.kv.impl.util.TopologyPrinter;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

/**
 * A RealizedTopology is created as a byproduct when a topology changing plan 
 * executes. It's a snapshot of the changes created by the plan.
 */

@Entity
public class RealizedTopology {
    
    @PrimaryKey
    private Long deployStartMillis;

    /**
     * Some, but not all RealizedTopologies are created by plans that take a 
     * TopologyCandidate as a parameter. If one has been provided to the plan,
     * it's saved here as part of the audit trail.
     */
    private String candidateName;
    private String planName;
    private int planId;
    private Topology topology;

    /* For DPL */
    @SuppressWarnings("unused")
    private RealizedTopology() {
    }

    /**
     * The very first initial empty topology 
     */
    public RealizedTopology(String storeName) {
        topology = new Topology(storeName);
        candidateName = TopologyCandidate.NO_NAME;
        planName = Plan.NO_NAME;
        planId = 0;
        setStartTime();
    }

    public RealizedTopology(Topology topo, DeploymentInfo info) {
        deployStartMillis = info.getDeployStartTime();
        topology = topo;
        this.planName = info.getPlanName();
        this.planId = info.getPlanId();
        this.candidateName = info.getCandidateName();
    }

    /**
     * TODO: since the start time is the primary key, we need to guarantee
     * that new realizedTopologies always ascend. If there is some clock skew
     * between nodes in the Admin replication group, there is the possibility
     * that the start millis is <= to the last saved RealizedTopology.
     */
    public void setStartTime() {
        deployStartMillis = System.currentTimeMillis();
    }

    public long getStartTime() {
        return deployStartMillis;
    }

    public Topology getTopology() {
       return topology;
    }

    public void setTopology(Topology topo) {
       topology = topo;
    }

    /**
     * @param concise if true, do not display the whole topology.
     */
    public String display(boolean concise) {
        StringBuilder sb = new StringBuilder();
        sb.append("deployTime=");
        DateFormat fm = FormatUtils.getDateTimeAndTimeZoneFormatter();
        sb.append(fm.format(new Date(deployStartMillis)));
        sb.append(" plan=").append(planId).append("/").append(planName);
        sb.append(" candidate=").append(candidateName);
        if (!concise) {
            sb.append('\n');
            sb.append(TopologyPrinter.printTopology(topology));
        }
        return sb.toString();
    }
}