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

import java.io.Serializable;

import oracle.kv.impl.admin.plan.Plan.State;
import oracle.kv.impl.measurement.Measurement;
import oracle.kv.impl.monitor.Metrics;
import oracle.kv.impl.util.FormatUtils;

/**
 * Information about a change in plan status, for monitoring.
 */
public class PlanStateChange implements Measurement, Serializable {
    private static final long serialVersionUID = 1L;
   
    private final int planId;
    private final String planName;
    private final Plan.State status;
    private final long time;
    private final int attemptNumber;
    private final boolean needsAlert;
    private final String msg;
    
    public PlanStateChange(int planId,
                           String planName,
                           State state,
                           int attemptNumber,
                           String msg) {
        this.planId = planId;
        this.planName = planName;
        this.status = state;
        time = System.currentTimeMillis();
        this.attemptNumber = attemptNumber;
        this.msg = msg;
        needsAlert = (status == Plan.State.ERROR);
    }

    @Override
    public long getStart() {
        return time;
    }
    
    @Override
    public long getEnd() {
        return time;
    }
    @Override
    public int getId() {
        return Metrics.PLAN_STATE.getId();
    }  

    /**
     * @return the planId
     */
    public int getPlanId() {
        return planId;
    }

    /**
     * @return the status
     */
    public Plan.State getStatus() {
        return status;
    }

    /**
     * @return the time
     */
    public long getTime() {
        return time;
    }

    /**
     * @return the attemptNumber
     */
    public int getAttemptNumber() {
        return attemptNumber;
    }

    /**
     * @return the needsAlert
     */
    public boolean isNeedsAlert() {
        return needsAlert;
    }

    /**
     * @return the msg
     */
    public String getMsg() {
        return msg;
    }

    /* 
     */
    @Override
    public String toString() {
        String show = "PlanStateChange [id=" + planId +
            " name=" + planName +
            " state=" + status + 
            " at " + FormatUtils.formatDateAndTime(time) + 
            " numAttempts=" + attemptNumber;
        if (needsAlert) {
            show += " needsAlert=true";
        }

        if (msg != null) {
            show += " : " +  msg;
        } 
        
        show +="]";
        return show;
    }   
}
