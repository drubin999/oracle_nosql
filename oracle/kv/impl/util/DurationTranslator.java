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

import java.util.concurrent.TimeUnit;

/**
 * In KVStore, all duration values should be expressed as milliseconds, for
 * uniformity. Durations can be expressed by the user, via the GUI and
 * XML configuration file, as a two part value and time unit. Time units are
 * defined by @{link java.util.concurrent.TimeUnit}. For example, if the
 * property sheet contains:
 *
 * &lt;property name="fooTimeoutValue" value="9"&gt;
 * &lt;property name="fooTimeoutUnit" value="SECONDS"&gt;
 *
 * Then DurationTranslator.translate(propertySheet, "fooTimeout",
 * "fooTimeoutUnit") will return 9000
 */
public class DurationTranslator {

    /**
     * Takes a property sheet with two configuration properties, where one
     * is a time value, and the other is a time unit, and
     * translates it into milliseconds
     */
    public static long translate(long duration,
                                 String timeUnitPropName) {

        TimeUnit unit = TimeUnit.valueOf(timeUnitPropName);
        return unit.toMillis(duration);
    }
}
