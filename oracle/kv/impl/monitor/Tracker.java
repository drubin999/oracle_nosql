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

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

/**
 * A Tracker stores incoming monitor data and makes it available to a set of
 * listeners. These listeners may be remote to the AdminService/Monitor and
 * will independently request data. The Tracker is responsible for storing
 * enough of a backlog of data so each listener will see a complete stream of
 * data.
 * 
 * Trackers are related to ViewListeners. Classes that extend Tracker generally
 * implement ViewListeners. However, there are ViewListeners that do not
 * extend tracker, because they are meant for simpler, local, single threaded
 * access.
 */
public abstract class Tracker<T> {

    protected long lastTimestampGiven = 0;
    private final List<TrackerListener> listeners;

    protected Tracker() {
        listeners = new ArrayList<TrackerListener>();
    }

    protected List<TrackerListener> getListeners() {
        return listeners;
    }

    public synchronized void registerListener(TrackerListener tl) {
        listeners.add(tl);
    }

    public synchronized void removeListener(TrackerListener tl) {
        listeners.remove(tl);
    }

    protected void notifyListeners() {

        /*
         * Copy the list because the method notifyOfNewEvents may remove an
         * item from the original list by calling removeListener, resulting in
         * the list's being modified while we're iterating over it.
         */
        List<TrackerListener> myListeners;
        synchronized (this) {
            myListeners = new ArrayList<TrackerListener>(listeners);
        }

        /*
         * Don't hold the monitor when notifying the listeners.
         */
        for (TrackerListener tl : myListeners) {
            try {
                tl.notifyOfNewEvents();
            } catch (RemoteException re) {
                /* There's a problem with this listener; just get rid of it. */
                listeners.remove(tl);
            }
        }
    }

    protected synchronized long getEarliestInterestingTimeStamp() {

        long interesting = lastTimestampGiven;

        for (TrackerListener tl : listeners) {
            long candidate;
            try {
                candidate = tl.getInterestingTime();
            } catch (RemoteException re) {
                listeners.remove(tl);
                continue;
            }

            if (candidate < interesting) {
                interesting = candidate;
            }
        }
        return interesting;
    }

    protected long getSyntheticTimestamp(long naturalTimestamp) {

        long syntheticTimestamp = naturalTimestamp;

        if (lastTimestampGiven < syntheticTimestamp) {
            lastTimestampGiven = syntheticTimestamp;
        } else {
            lastTimestampGiven++;
            syntheticTimestamp = lastTimestampGiven;
        }
        return syntheticTimestamp;
    }
    
    /**
     * Get a list of events that have occurred since the given time.
     */
    abstract public RetrievedEvents<T> retrieveNewEvents(long since);

    /**
     * When deciding, for the purpose of retrieveNewEvents, whether a given log
     * record is later or earlier than another, we use the synthetic timestamp
     * in EventHolder, which is guaranteed to be different from any other log
     * record's timestamp.  When creating such a timestamp, we examine the
     * unique timestamp of the most recently created record.  If that value is
     * less than the natural timestamp of the new record, we use the new
     * record's natural timestamp as its unique timestamp.  Otherwise, we
     * increment the most recently given unique timestamp, and use that as a
     * synthetic timestamp for the new record.  This guarantees that no log
     * records will be lost, when the client asks for log records added since
     * the timestamp of the last record previously delivered, even if new
     * records have natural timestamps that are less than or equal to that of
     * the last previously delivered record.  Since the synthetic timestamps
     * are near in value to the natural timestamps, if not identical, it also
     * works to ask for events since a particular natural timestamp, without
     * reference to the last record delivered.
     */
    public static class EventHolder<T> implements Serializable {
        private static final long serialVersionUID = 1L;

        private long syntheticTimestamp;
        private T event;
        boolean recordable;

        public EventHolder(long syntheticTimestamp, T event,
                           boolean recordable) {
            this.syntheticTimestamp = syntheticTimestamp;
            this.event = event;
            this.recordable = recordable;
        }

        public long getSyntheticTimestamp() {
            return syntheticTimestamp;
        }

        public T getEvent() {
            return event;
        }

        public boolean isRecordable() {
            return recordable;
        }
    }

    /**
     * We have to communicate the synthetic timestamp of the last retrieved
     * event to the caller, which will (optionally) use this timestamp as the
     * "since" value when requesting the next set of updates.  We use an object
     * of this class to return this value along with the list of LogRecords
     * retrieved.
     */
    public static class RetrievedEvents<T> implements Serializable {
        private static final long serialVersionUID = 1L;

        private long syntheticTimestampOfLastEvent;
        private List<EventHolder<T>> events;

        public RetrievedEvents(long syntheticStampOfLastEvent,
                               List<EventHolder<T>> events) {
            this.syntheticTimestampOfLastEvent = syntheticStampOfLastEvent;
            this.events = events;
        }

        public long getLastSyntheticTimestamp() {
            return syntheticTimestampOfLastEvent;
        }

        public int size() {
            return events.size();
        }

        public List<T> getEvents() {
            List<T> values = new ArrayList<T>();
            for (EventHolder<T> se : events) {
                values.add(se.getEvent());
            }
            return values;
        }

        public List<EventHolder<T>> getRecordableEvents() {
            List<EventHolder<T>> values = new ArrayList<EventHolder<T>>();
            for (EventHolder<T> se : events) {
                if (se.isRecordable()) {
                    values.add(se);
                }
            }
            return values;
        }
    }
}
