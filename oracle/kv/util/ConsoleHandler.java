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

import java.util.logging.Level;

import oracle.kv.impl.util.CommonLoggerUtils;

/**
 * KVStore instances of java.util.logging.Logger are configured to use this
 * implementation of java.util.logging.ConsoleHandler. By default, the
 * handler's level is {@link Level#OFF}. To enable the console output for a

 * KVStore component, as it would be seen on the console of the local node, use
 * the standard java.util.logging.LogManager configuration to set the desired
 * level:
 * <pre>
 * oracle.kv.util.ConsoleHandler.level=ALL
 * </pre>
 * See oracle.kv.util.StoreConsoleHandler for the store wide consolidated
 * logging output as it would be seen on the AdminService.
 */
public class ConsoleHandler extends java.util.logging.ConsoleHandler {

    /*
     * Using a KV specific handler lets us enable and disable output for all
     * kvstore component.
     */
    public ConsoleHandler() {
        super();

        Level level = null;
        String propertyName = getClass().getName() + ".level";

        String levelProperty =
            CommonLoggerUtils.getLoggerProperty(propertyName);
        if (levelProperty == null) {
            level = Level.OFF;
        } else {
            level = Level.parse(levelProperty);
        }

        setLevel(level);
    }
}

