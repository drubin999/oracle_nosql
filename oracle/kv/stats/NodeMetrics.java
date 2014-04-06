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

package oracle.kv.stats;

import java.io.Serializable;

import oracle.kv.KVStore;

/**
 * The metrics associated with a node in the KVS.
 */
public interface NodeMetrics extends Serializable {

    /**
     * Returns the internal name associated with the node. It's unique across
     * the KVStore.
     */
    public String getNodeName();

    /**
     * Returns the zone that hosts the node.
     *
     * @deprecated replaced by {@link #getZoneName}
     */
    @Deprecated
    public String getDataCenterName();

    /**
     * Returns the zone that hosts the node.
     */
    public String getZoneName();

    /**
     * Returns true is the node is currently active, that is, it's reachable
     * and can service requests.
     */
    public boolean isActive();

    /**
     * Returns true if the node is currently a master.
     */
    public boolean isMaster();

    /**
     * Returns the number of requests that were concurrently active for this
     * node at this KVS client.
     */
    public int getMaxActiveRequestCount();

    /**
     * Returns the total number of requests processed by the node.
     */
    public long getRequestCount();

    /**
     * Returns the number of requests that were tried at this node but did not
     * result in a successful response.
     */
    public long getFailedRequestCount();

    /**
     * Returns the trailing average latency (in ms) over all requests made to
     * this node.
     * <p>
     * Note that since this is a trailing average it's not cleared when the
     * statistics are cleared via the {@link KVStore#getStats(boolean)} method.
     */
    public int getAvLatencyMs();
}
