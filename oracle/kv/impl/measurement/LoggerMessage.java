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

package oracle.kv.impl.measurement;

import java.io.Serializable;
import java.util.logging.LogRecord;

import oracle.kv.impl.monitor.Metrics;

/**
 * A wrapper for a java.util.logging message issued at a service, which
 * is forwarded to the AdminService to display in the store-wide consolidated
 * view.
 */
public class LoggerMessage implements Measurement, Serializable {

    // TODO: Is it too heavyweight to send the LogRecord? An alternative is to
    // send the message level, timestamp and string.

    private static final long serialVersionUID = 1L;
    private final LogRecord logRecord;

    public LoggerMessage(LogRecord logRecord) {
        this.logRecord = logRecord;
    }

    @Override
    public int getId() {
        return Metrics.LOG_MSG.getId();
    }

    @Override
    public String toString() {
        return logRecord.getMessage();
    }

    public LogRecord getLogRecord() {
        return logRecord;
    }

    @Override
    public long getStart() {
        return 0;
    }

    @Override
    public long getEnd() {
        return 0;
    }
}