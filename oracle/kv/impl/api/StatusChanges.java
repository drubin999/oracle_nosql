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

package oracle.kv.impl.api;

import java.io.Serializable;

import oracle.kv.impl.topo.RepNodeId;

import com.sleepycat.je.rep.ReplicatedEnvironment;

/**
 * The status updates that are returned as part of a response.
 *
 * The status update information is expected to be a small number of bytes
 * in the typical case and cheap to compute so that it does not add
 * significantly to the overhead of a request/response.
 */
public class StatusChanges implements Serializable {

    private static final long serialVersionUID = 1L;

    /* The state at the *responding node* as contained in the Response */
    private final ReplicatedEnvironment.State state;

    /* The masterId, it's non-null if the node is currently active, that is
     * it's a master or replica.
     */
    private final RepNodeId masterId;

    /*
     * Note that we are using time order events. Revisit if this turns out to
     * be an issue and we should be using heavier weight group-wise sequenced
     * election proposal numbers instead.
     */
    private final long statusTime;

    public StatusChanges(ReplicatedEnvironment.State state,
                         RepNodeId masterId,
                         long sequenceNum) {
        super();
        this.state = state;
        this.masterId = masterId;
        this.statusTime = sequenceNum;
    }

    public ReplicatedEnvironment.State getState() {
        return state;
    }

    /**
     * Returns the master RN as known to the responding RN
     *
     * @return the unique id associated with the master RN
     */
    public RepNodeId getCurrentMaster() {
        return masterId;
    }

    /**
     * Returns the time associated with the status transition at the responding
     * node.
     */
    public long getStatusTime() {
        return statusTime;
    }
}
