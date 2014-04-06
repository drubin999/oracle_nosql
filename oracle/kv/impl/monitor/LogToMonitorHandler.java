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

import java.util.logging.Handler;
import java.util.logging.Level;

import oracle.kv.impl.util.server.LoggerUtils;

/**
 * Sends logging output to the Monitor.
 */
public abstract class LogToMonitorHandler extends Handler {

    public LogToMonitorHandler() {
        Level level = null;
        String propertyName = getClass().getName() + ".level";
        String levelProperty = LoggerUtils.getLoggerProperty(propertyName);

        if (levelProperty == null) {
            level = Level.ALL;
        } else {
            level = Level.parse(levelProperty);
        }
        setLevel(level);
    }

    /**
     * When a handler is closed, we want to remove it from any associated
     * loggers, so that a handler will be created again if the logger is
     * ressurected.
     */
    @Override
    public void close()  {
        LoggerUtils.removeHandler(this);
    }

    @Override
    public void flush() {
        /* Nothing to do. */
    }
}