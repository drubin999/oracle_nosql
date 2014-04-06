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

package oracle.kv.impl.admin.criticalevent;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.logging.LogRecord;

import oracle.kv.impl.admin.NonfatalAssertionException;
import oracle.kv.impl.monitor.views.PerfEvent;
import oracle.kv.impl.monitor.views.ServiceChange;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.LogFormatter;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.Transaction;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.KeyField;
import com.sleepycat.persist.model.Persistent;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class CriticalEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    public static enum EventType {
        ALL(""), STAT("S"), PERF("P"), LOG("L");

        /* This string is used as part of the database key. */
        private String internalValue;

        private EventType(String internalValue) {
            this.internalValue = internalValue;
        }

        public String toInternalString() {
            return internalValue;
        }

        public static EventType fromInternalString(String s) {
            EventType value = ALL;
            for (final EventType et : EnumSet.allOf(EventType.class)) {
                if (et.toInternalString().equals(s)) {
                    value = et;
                }
            }
            return value;
        }
    }

    @PrimaryKey
    EventKey key;

    private byte[] serializedEvent;

    private static EventFormatter ef = new EventFormatter();

    private CriticalEvent(EventKey key, Object event) {

        this.key = key;

        try {
            ByteArrayOutputStream bastream = new ByteArrayOutputStream() ;
            ObjectOutputStream oostream = new ObjectOutputStream(bastream);
            oostream.writeObject(event);
            oostream.close();
            this.serializedEvent = bastream.toByteArray();
        } catch (IOException ioe) {
            throw new IllegalStateException
                ("IOException while serializing event", ioe);
        }
    }

    public CriticalEvent(long timestamp, ServiceChange event) {
        this(new EventKey(timestamp, EventType.STAT), event);
    }

    public CriticalEvent(long timestamp, PerfEvent event) {
        this(new EventKey(timestamp, EventType.PERF), event);
    }

    public CriticalEvent(long timestamp, LogRecord event) {
        this(new EventKey(timestamp, EventType.LOG), event);
    }

    public CriticalEvent() {
    }

    public EventType getEventType() {
        return EventType.fromInternalString(key.getCategory());
    }

    public long getSyntheticTimestamp() {
        return key.getSyntheticTimestamp();
    }

    @Override
    public String toString() {
        EventType t = getEventType();

        String s = key.toString() + " ";

        switch (t) {
        case STAT:
            return s + ef.format(getStatusEvent());
        case LOG:
            return s + ef.format(getLogEvent());
        case PERF:
            return s + ef.format(getPerfEvent());
        default:
            assert false;
        }
        return null;
    }

    public String getDetailString() {
        EventType t = getEventType();

        String s = key.toString() + " ";

        switch (t) {
        case STAT:
            return s + ef.formatDetail(getStatusEvent());
        case LOG:
            return s + ef.formatDetail(getLogEvent());
        case PERF:
            return s + ef.formatDetail(getPerfEvent());
        default:
            assert false;
        }
        return null;
    }

    public ServiceChange getStatusEvent() {
        if (getEventType() != EventType.STAT) {
            throw new NonfatalAssertionException
                ("Attempt to get wrong event type");
        }

        return (ServiceChange) decodeSerializedEvent();
    }

    /** Returns a formatted string for a status event. */
    public static String formatStatusEvent(final ServiceChange statusEvent) {
        return ef.format(statusEvent);
    }

    public PerfEvent getPerfEvent() {
        if (getEventType() != EventType.PERF) {
            throw new NonfatalAssertionException
                ("Attempt to get wrong event type");
        }

        return (PerfEvent) decodeSerializedEvent();
    }

    public LogRecord getLogEvent() {
        if (getEventType() != EventType.LOG) {
            throw new NonfatalAssertionException
                ("Attempt to get wrong event type");
        }

        return (LogRecord) decodeSerializedEvent();
    }

    /** Returns a formatted string for a log record. */
    public static String formatLogEvent(final LogRecord logRecord) {
        return ef.format(logRecord);
    }

    public void persist(EntityStore estore, Transaction txn) {
        final PrimaryIndex<EventKey, CriticalEvent> pi =
            estore.getPrimaryIndex(EventKey.class, CriticalEvent.class);

        pi.put(txn, this);
    }

    /**
     * Retrieve a list of events matching the given criteria.
     */
    public static List<CriticalEvent> fetch
        (EntityStore estore, Transaction txn, long startTime, long endTime,
         CriticalEvent.EventType type) {

        final PrimaryIndex<EventKey, CriticalEvent> pi =
            estore.getPrimaryIndex(EventKey.class, CriticalEvent.class);

        EventKey startKey = new EventKey(startTime, type);
        EventKey endKey = new EventKey(endTime == 0 ? Long.MAX_VALUE : endTime,
                                       type);

        List<CriticalEvent> events = new ArrayList<CriticalEvent>();

        EntityCursor<CriticalEvent> eventCursor =
            pi.entities(txn, startKey, true, endKey, false,
                        CursorConfig.READ_UNCOMMITTED);

        try {
            for (CriticalEvent ev : eventCursor) {
                if (type == EventType.ALL || type.equals(ev.getEventType())) {
                    events.add(ev);
                }
            }
        } finally {
            eventCursor.close();
        }

        return events;
    }

    /**
     * Expire older events from the persistent store.
     */
    public static void ageStore
        (EntityStore estore, Transaction txn, long pruningAge) {

        final PrimaryIndex<EventKey, CriticalEvent> pi =
            estore.getPrimaryIndex(EventKey.class, CriticalEvent.class);

        long expiry = new Date().getTime() - pruningAge;

        EventKey startKey = new EventKey(0L, EventType.ALL);
        EventKey endKey = new EventKey(expiry, EventType.ALL);

        EntityCursor<CriticalEvent> eventCursor =
            pi.entities(txn, startKey, true, endKey, true,
                        CursorConfig.READ_UNCOMMITTED);

        try {
            for (CriticalEvent ev = eventCursor.first();
                 ev != null;
                 ev = eventCursor.next()) {

                eventCursor.delete();
            }
        } finally {
            eventCursor.close();
        }
    }

    /**
     * Retrieve a single event from the database, using the given key.
     */
    public static CriticalEvent fetch
        (EntityStore estore, Transaction txn, String eventId) {

        final PrimaryIndex<EventKey, CriticalEvent> pi =
            estore.getPrimaryIndex(EventKey.class, CriticalEvent.class);

        return pi.get(txn, EventKey.fromString(eventId),
                      LockMode.READ_UNCOMMITTED);
    }

    public EventKey getKey() {
        return key;
    }

    private Object decodeSerializedEvent() {

        Object o = null;

        try {
            ObjectInputStream oistream = new ObjectInputStream
                (new ByteArrayInputStream(serializedEvent));
            o = oistream.readObject();
            oistream.close();
        } catch (Exception e) {
            throw new IllegalStateException
                ("Trouble deserializing an event record.", e);
        }

        return o;
    }

    @Persistent
    public static class EventKey implements Serializable {

        private static final long serialVersionUID = 1L;

        @KeyField(1)
        long syntheticTimestamp;

        @KeyField(2)
        String category;

        public EventKey(long syntheticTimeStamp, EventType category) {
            this.syntheticTimestamp = syntheticTimeStamp;
            this.category = category.toInternalString();
        }

        public EventKey() {
        }

        public long getSyntheticTimestamp() {
            return syntheticTimestamp;
        }

        public String getCategory() {
            return category;
        }

        /**
         * The String representation of the key is the timestamp with
         * a single letter appended to indicate the category.
         *
         * We stringify the synthetic timestamp as a compact string which is
         * really a base 36 number.  The digits of these numbers comprise the
         * numerals 0-9 and the letters A-Z, however we use lowercase o to
         * distinguish it from zero and capital L to distinguish it from 1.
         *
         */
        @Override
        public String toString() {
            return encode(syntheticTimestamp) + category;
        }

        /**
         * Reconstitute an EventKey from its toString representation.
         */
        public static EventKey fromString(String s) {
            int len = s.length();

            /* The final char in the string represents the category. */
            String catInitial = s.substring(len - 1, len).toUpperCase();
            EventType type = EventType.fromInternalString(catInitial);

            String tstr = s.substring(0, len - 1);
            long tval = decode(tstr);
            return new EventKey(tval, type);
        }

        private static final char[] ENCODING_DIGITS ={
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b',
            'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'L', 'm', 'n',
            'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z' };

        private static final char[] DECODING_DIGITS ={
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B',
            'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N',
            'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z' };

        private static final int BASE = ENCODING_DIGITS.length;

        private static String encode(long value) {
            StringBuilder sb = new StringBuilder();

            if (value == 0) {
                sb.append("0");
            }

            while (value != 0) {
                int remainder = (int)(value % BASE);
                sb.insert(0, ENCODING_DIGITS[remainder]);
                value /= BASE;
            }

            return sb.toString();
        }

        /**
         * When decoding, we treat upper and lowercase letters as the same
         * digit.
         */
        private static long decode(String s) {
            long value = 0;
            long place = 1;

            char[] c = s.toUpperCase().toCharArray();

            for (int i = c.length - 1; i >= 0; i--) {
                value =
                    value + Arrays.binarySearch(DECODING_DIGITS, c[i]) * place;
                place *= BASE;
            }

            return value;
        }
    }

    /**
     * Our private version of LogFormatter.
     */
    private static class EventFormatter extends LogFormatter {

        public EventFormatter() {
            super(null);
        }

        @Override
        public String format(LogRecord record) {
            return format(record, false);
        }

        public String formatDetail(LogRecord record) {
            return format(record, true);
        }

        private String format(LogRecord record, boolean detail) {
            StringBuilder sb = new StringBuilder("LOG  ");
            sb.append(getDate(record.getMillis()));
            sb.append(" ");
            sb.append(record.getLevel().getLocalizedName());
            sb.append(" ");
            String formattedMessage = formatMessage(record);
            /* For the short format we only want a single line of the message */
            if (detail == false) {
                int n = formattedMessage.indexOf('\n');
                if (n != -1) {
                    formattedMessage = formattedMessage.substring(0, n);
                }
            }
            sb.append(formattedMessage);
            Throwable t = record.getThrown();
            if (t != null && detail) {
                sb.append(LoggerUtils.getStackTrace(t));
            }
            return sb.toString();
        }

        public String format(ServiceChange record) {
            StringBuilder sb = new StringBuilder("STAT ");
            sb.append(getDate(record.getChangeTime()));
            sb.append(" ");
            ResourceId target = record.getTarget();
            sb.append(target.toString());
            sb.append(" ");
            sb.append(record.getStatus().toString());
            sb.append(" sev");
            sb.append(Integer.toString(record.getSeverity()));
            ResourceId reporter = record.getReporter();
            if (! target.equals(reporter)) {
                sb.append(" ");
                sb.append("(reported by " + reporter.toString() + ")");
            }
            return sb.toString();
        }

        public String formatDetail(ServiceChange record) {
            /* The detail string is the same as the short string. Right? */
            return format(record);
        }

        /**
         * Used by CLI 'show events -type perf'. See ShowCommand.ShowEvents.
         */
        public String format(PerfEvent record) {
            StringBuilder sb = new StringBuilder("PERF ");
            sb.append(getDate(record.getChangeTime()));
            sb.append(" ");
            ResourceId target = record.getResourceId();
            sb.append(target.toString());
            sb.append(" ");
            sb.append(record.getSingleInt());
            sb.append(" ");
            sb.append("replicaLagMs=" + record.getCommitLagMs());
            sb.append(" ");
            sb.append(record.getMultiInt());
            return sb.toString();
        }

        /**
         * Used by CLI 'show events -id <eventId>'. See ShowCommand.ShowEvents.
         */
        public String formatDetail(PerfEvent record) {
            StringBuilder sb = new StringBuilder("PERF ");
            sb.append(record.getColumnFormatted());
            return sb.toString();
        }
    }
}
