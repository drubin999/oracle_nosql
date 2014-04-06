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

import oracle.kv.impl.admin.topo.TopologyCandidate;

/**
 * A struct to package together information that goes into the RealizedTopology.
 * A DeploymentInfo instance is meant to be a transient field in the plan that
 * deploys a topology. The instance is valid only during the time when the plan
 * is actively executing, and is initialized at the beginning of a plan 
 * execution run.
 */
public class DeploymentInfo {

    private long deployStartTime;

    private final String planName;
    private final int planId;
    private final String candidateName;

    /**
     * The preferred way to obtain a DeploymentInfo, rather than using the
     * constructor, to ensure that the DeployInfo's start time is properly 
     * validated. The start time is used as a key for topology storage, and 
     * must be unique and ascending.
     */
    public static DeploymentInfo makeDeploymentInfo(AbstractPlan plan,
                                                    String candidateName) {
        DeploymentInfo info = new DeploymentInfo(plan.getName(),
                                                 plan.getId(),
                                                 candidateName);
        long validatedTime =  plan.getAdmin().validateStartTime
            (info.getDeployStartTime());
        info.setDeployStartTime(validatedTime);
        return info;
    }

    /** 
     * Used when a plan is not available, either because we're starting up
     * the Admin service, or it's a unit test situation.
     */
    public static DeploymentInfo makeStartupDeploymentInfo() {
        return new DeploymentInfo(Plan.NO_NAME, 0, TopologyCandidate.NO_NAME);
    }

    /**
     * Used when setting up an initial empty topology. Care must be taken to
     * ensure that the deploy start time is an ascending, unique value, so this
     * constructor is private, and the caller should use the factory method
     * makeDeploymentInfo();
     */
    private DeploymentInfo(String planName, int planId, String candidateName) {
        this.planName = planName;
        this.planId = planId;
        this.candidateName = candidateName;
        deployStartTime = System.currentTimeMillis();
    }

    public long getDeployStartTime() {
        return deployStartTime;
    }

    public void setDeployStartTime(long validatedTime) {
        deployStartTime = validatedTime;
    }
    
    public String getPlanName() {
        return planName;
    }

    public int getPlanId() {
        return planId;
    }

    public String getCandidateName() {
        return candidateName;
    }
}