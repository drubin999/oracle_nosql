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

import java.util.concurrent.atomic.AtomicInteger;

import oracle.kv.impl.topo.Topology;

import com.sleepycat.persist.model.Persistent;

/**
 * Stop all RepNodes in the store.
 */
@Persistent
public class StopAllRepNodesPlan extends StopRepNodesPlan {

    private static final long serialVersionUID = 1L;

    StopAllRepNodesPlan(AtomicInteger idGen, 
                        String name,
                        Planner planner,
                        Topology topology) {
        super(idGen, name, planner, topology, topology.getRepNodeIds());
    }

    /* DPL */
    protected StopAllRepNodesPlan() {
    }

    @Override
    public boolean isExclusive() {
        /* 
         * Doesn't really make sense to do anything administrative if we're 
         * trying to shut down the store.
         */
        return true;
    }
}
