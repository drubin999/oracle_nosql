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

package oracle.kv.impl.util;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * A simple logger used to limit the rate at which messages related to a
 * specific object are logged to at most once within the configured time
 * period. The effect of the rate limited logging is to sample log messages
 * associated with the object.
 *
 * This type of logging is suitable for informational messages about the state
 * of some entity that may persist over some extended period of time, e.g. a
 * repeated problem communicating with a specific node, where the nature of the
 * problem may change over time.
 *
 * @param <T> the type of the object associated with the log message.
 */
public class RateLimitingLogger<T> {
    /**
     * Contains the objects that had messages last logged for them and the
     * associated time that it was last logged.
     */
    private final Map<T, Long> logEvents;

    /**
     *  The log message sampling period.
     */
    private final int logSamplePeriodMs;

    /* The number of log messages that were actually written. */
    private long limitedMessageCount = 0;

    private final Logger logger;

    /**
     * Constructs a configured RateLimitingLoggerInstance.
     *
     * @param logSamplePeriodMs used to compute the max rate of
     *         1 message/logSamplePeriodMs
     * @param maxObjects the max number of MRU objects to track
     *
     * @param logger the rate limited messages are written to this logger
     */
    @SuppressWarnings("serial")
    public RateLimitingLogger(final int logSamplePeriodMs,
                              final int maxObjects,
                              final Logger logger) {

        this.logSamplePeriodMs = logSamplePeriodMs;
        this.logger = logger;

        logEvents = new LinkedHashMap<T,Long>(9) {
            @Override
            protected boolean
            removeEldestEntry(Map.Entry<T, Long> eldest) {

              return size() > maxObjects;
            }
          };
    }

    /* For testing */
    synchronized long getLimitedMessageCount() {
        return limitedMessageCount;
    }


    /* For testing */
    int getMapSize() {
        return logEvents.size();
    }

    /**
     * Logs the message, if one has not already been logged for the object
     * in the current time interval.
     *
     * @param object the object associated with the log message
     *
     * @param level the level to be used for logging
     *
     * @param string the log message string
     */
    public synchronized void log(T object, Level level, String string) {

        if (object == null) {
            logger.log(level, string);
            return;
        }

        final Long timeMs = logEvents.get(object);

        final long now = System.currentTimeMillis();
        if ((timeMs == null) ||
            (now > (timeMs + logSamplePeriodMs))) {
            limitedMessageCount++;
            logEvents.put(object, now);
            logger.log(level, string);
        }
    }
}
