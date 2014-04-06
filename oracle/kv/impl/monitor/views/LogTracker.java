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
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import oracle.kv.impl.monitor.Tracker;
import oracle.kv.impl.monitor.ViewListener;
import oracle.kv.impl.topo.ResourceId;

/**
 * Tracks logging events as java.util.LogRecords.
 */

public class LogTracker extends Tracker<LogRecord>
    implements ViewListener<LogRecord> {

    static final int PRUNE_FREQUENCY = 40; /* Run the pruner modulo this. */
    static final int QUEUE_MAX = 5000;

    private final List<EventHolder<LogRecord>> queue;
    private int newInfoCounter = 0;
    private final Logger logger;

    public LogTracker(Logger logger) {
        super();
        this.logger = logger;
        queue = new ArrayList<EventHolder<LogRecord>>();
    }

    private void prune() {
        long interesting = getEarliestInterestingTimeStamp();

        while (!queue.isEmpty()) {
            if (queue.size() < 40) {

                /*
                 * Leave a few log records in the queue, so that a new client
                 * can display some recent history.
                 */
                break;
            }

            EventHolder<LogRecord> lr = queue.get(0);
            if (lr.getSyntheticTimestamp() > interesting) {
                /* Stop if we've reached the earliest interesting timestamp. */
                break;
            }
            queue.remove(0);
        }

        /*
         * Guarantee that the queue is never larger than a certain max number
         * of entries. If that number is exceeded, then we discard guarantees
         * about keeping messages until all listeners have indicated that they
         * are no longer interesting.
         */
        int currentSize = queue.size();
        if (currentSize > QUEUE_MAX) {
            for (int i = 0; i < (currentSize - QUEUE_MAX); i++) {
                queue.remove(0);
            }
            logger.severe("Log queue size=" + currentSize +
                          " exceeds maximum of " + QUEUE_MAX +
                          ", was pruned;" +
                          " some messages prior to this one were lost.");
        }
    }

    @Override
    public void newInfo(ResourceId rId, LogRecord lr) {
        synchronized (this) {
            if (newInfoCounter++ % PRUNE_FREQUENCY == 0) {
                prune();
            }

            long syntheticTimestamp = getSyntheticTimestamp(lr.getMillis());

            queue.add(new EventHolder<LogRecord>
                      (syntheticTimestamp, lr,
                       lr.getLevel() == Level.SEVERE /* Such are recordable*/));
        }
        /*
         * If we are still holding the monitor after exiting the synchronized
         * block, it means that newInfo has been called recursively via prune's
         * call to logger.severe, above.  If that is the case, then we'll
         * decline to call notifyListeners, because a listener receiving a
         * notification may cause a deadlock.  In any case, notifyListeners
         * will be called when the first newInfo frame regains control.
         */
        if (! Thread.holdsLock(this)) {
            notifyListeners();
        }
    }

    /**
     * Get a list of events that have occurred since the given time.
     */
    @Override
    public synchronized
        RetrievedEvents<LogRecord> retrieveNewEvents(long since) {

        List<EventHolder<LogRecord>> values =
            new ArrayList<EventHolder<LogRecord>>();

        long syntheticStampOfLastRecord = since;
        for (EventHolder<LogRecord> lr : queue) {
            if (lr.getSyntheticTimestamp() > since) {
                values.add(lr);
                syntheticStampOfLastRecord = lr.getSyntheticTimestamp();
            }
        }

        return
            new RetrievedEvents<LogRecord>(syntheticStampOfLastRecord, values);
    }
}
