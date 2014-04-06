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

package oracle.kv.impl.monitor;

import oracle.kv.impl.topo.ResourceId;

/**
 * An interface to specify a class that holds a reference to the Monitor.
 * Ordinarily it is the Admin that keeps this reference, but in testing it is
 * useful to create a proxy for the Admin that doesn't carry all Admin's
 * baggage along with it.
 */

public interface MonitorKeeper {
    /**
     * Get the instance of Monitor associated with the implementing class.
     */
    Monitor getMonitor();

    /**
     * Return the latency ceiling associated with the given RepNode.
     */
    int getLatencyCeiling(ResourceId rnid);

    /**
     * Return the throughput floor associated with the given RepNode.
     */
    int getThroughputFloor(ResourceId rnid);

    /**
     * Return the threshold to apply to the average commit lag computed
     * from the total commit lag and the number of commit log records
     * replayed by the given RepNode, as reported by the JE backend.
     */
    long getCommitLagThreshold(ResourceId rnid);
}

