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

package oracle.kv.impl.monitor.views;

import java.util.ArrayList;
import java.util.List;

import oracle.kv.impl.admin.plan.PlanStateChange;
import oracle.kv.impl.monitor.Tracker;
import oracle.kv.impl.monitor.ViewListener;
import oracle.kv.impl.topo.ResourceId;

/**
 * Tracks plan state changes.  The UI uses this to dynamically update a listing
 * of plan states.
 */
public class PlanStateChangeTracker
    extends Tracker<PlanStateChange> implements ViewListener<PlanStateChange> {

    private final static int PRUNE_FREQUENCY = 40;

    private final List<EventHolder<PlanStateChange>> queue;
    private int newInfoCounter = 0;

    public PlanStateChangeTracker() {
        super();
        queue = new ArrayList<EventHolder<PlanStateChange>>();
    }

    private void prune() {
        long interesting = getEarliestInterestingTimeStamp();

        while (!queue.isEmpty()) {
            EventHolder<PlanStateChange> psc = queue.get(0);
            if (psc.getSyntheticTimestamp() > interesting) {
                /* Stop if we've reached the earliest interesting timestamp. */
                break;
            }
            queue.remove(0);
        }
    }

    @Override
    public void newInfo(ResourceId rId, PlanStateChange psc) {
        synchronized (this) {
            if (newInfoCounter++ % PRUNE_FREQUENCY == 0) {
                prune();
            }

            long syntheticTimestamp = getSyntheticTimestamp(psc.getTime());

            queue.add
                (new EventHolder<PlanStateChange>
                 (syntheticTimestamp, psc,
                 false /* Plans states are never recordable. */));
        }
        notifyListeners();
    }

    /**
     * Get a list of events that have occurred since the given time.
     */
    @Override
    public synchronized
        RetrievedEvents<PlanStateChange> retrieveNewEvents(long since) {

        List<EventHolder<PlanStateChange>> values =
            new ArrayList<EventHolder<PlanStateChange>>();

        long syntheticStampOfLastRecord = since;
        for (EventHolder<PlanStateChange> psc : queue) {
            if (psc.getSyntheticTimestamp() > since) {
                values.add(psc);
                syntheticStampOfLastRecord = psc.getSyntheticTimestamp();
            }
        }

        return new RetrievedEvents<PlanStateChange>(syntheticStampOfLastRecord,
                                                    values);
    }
}
