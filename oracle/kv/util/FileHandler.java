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

package oracle.kv.util;

import java.io.IOException;
import java.util.logging.Level;

import oracle.kv.impl.util.server.LoggerUtils;

/**
 * KVStore instances of java.util.logging.Logger are configured to use this
 * implementation of java.util.logging.ConsoleHandler. By default, the
 * handler's level is {@link Level#INFO}. To enable the console output, use the
 * standard java.util.logging.LogManager configuration to set the desired
 * level:
 * <pre>
 * oracle.kv.util.FileHandler.level=FINE
 * </pre>
 */
public class FileHandler extends java.util.logging.FileHandler {

    int limit;
    int count;

    /*
     * Using a KV specific handler lets us enable and disable output for
     * all kvstore component.
     */
    public FileHandler(String pattern, int limit, int count, boolean append)
        throws IOException, SecurityException {

        super(pattern, limit, count, append);

        this.limit = limit;
        this.count = count;
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

    public int getCount() {
        return count;
    }

    public int getLimit() {
        return limit;
    }

    /**
     * When a handler is closed, we want to remove it from any associated
     * loggers, so that a handler will be created again if the logger is
     * ressurected.
     */
    @Override
    public synchronized void close() {
        super.close();
        LoggerUtils.removeHandler(this);
    }
}

