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

package oracle.kv.impl.param;

import java.util.concurrent.TimeUnit;

import com.sleepycat.persist.model.Persistent;

/**
 * String format is "long Unit"
 * Accepted units are valid TimeUnit strings, as well as NS, US, MS, S, MIN, H.
 * All units are case-insensitive.  The separator for the two tokens can be:
 * " ", "-", "_".  More could possibly be added.
 *
 * Valid examples:
 *   "1 SECONDS"
 *   "10 minutes"
 *   "1_ns"
 *   "500 ms"
 * Invalid:
 *   "1s", "10 sec", etc.
 *
 */
@Persistent
public class DurationParameter extends Parameter {

    private static final long serialVersionUID = 1L;
    /**
     * Allow various formats (N is Long, TU is TimeUnit):
     *  "N TU", "N_TU", "N-TU"
     */
    private static final String[] allowedRegex = {"\\s+", "-", "_"};

    private TimeUnit unit;
    private long amount;

    /* For DPL */
    public DurationParameter() {
    }

    public DurationParameter(String name, String value) {
        super(name);
        parseDuration(value);
    }

    public DurationParameter(String name, TimeUnit unit, long amount) {
        super(name);
        this.unit = unit;
        this.amount = amount;
    }

    /**
     * Compare based on absolute value, not the unit.
     */
    @Override
    public boolean equals(Parameter other) {
        if (other == null || !(other instanceof DurationParameter)) {
            return false;
        }
        if (toMillis() == ((DurationParameter) other).toMillis()) {
            return true;
        }
        return false;
    }

    public long getAmount() {
        return amount;
    }

    public TimeUnit getUnit() {
        return unit;
    }

    public long toMillis() {
        return unit.toMillis(amount);
    }

    public void setMillis(long millis) {
        unit = TimeUnit.MILLISECONDS;
        amount = millis;
    }

    private void parseDuration(String value) {
        for (String regex : allowedRegex) {
            String[] tokens = value.split(regex);
            if (tokens.length == 2) {
                amount = Long.parseLong(tokens[0]);
                try {
                    unit = TimeUnit.valueOf(tokens[1].toUpperCase
                                            (java.util.Locale.ENGLISH));
                    return;
                } catch (IllegalArgumentException e) {
                    /**
                     * Try IEEE unit values (see below).
                     */
                    try {
                        unit = IEEEUnit.unit(tokens[1]);
                        return;
                    } catch (IllegalArgumentException e1) {
                        /* fall through */
                    }
                }
            }
        }
        throw new IllegalArgumentException
            ("Invalid duration format: " + value);
    }

    public String asString(Character separator) {
        return Long.toString(amount) + separator + unit.toString();
    }

    @Override
    public String asString() {
        return asString(' ');
    }

    @Override
    public ParameterState.Type getType() {
        return ParameterState.Type.DURATION;
    }

    /**
     * Allow (case-insensitive) ns, us, ms, s, min, h as units.
     */
    private enum IEEEUnit {
        NS() {
            @Override
			TimeUnit getUnit() {
                return TimeUnit.NANOSECONDS;
            }
        },

        US() {
            @Override
			TimeUnit getUnit() {
                return TimeUnit.MICROSECONDS;
            }
        },

        MS() {
            @Override
			TimeUnit getUnit() {
                return TimeUnit.MILLISECONDS;
            }
        },

        S() {
            @Override
			TimeUnit getUnit() {
                return TimeUnit.SECONDS;
            }
        },

        MIN() {
            @Override
			TimeUnit getUnit() {
                return TimeUnit.MINUTES;
            }
        },

        H() {
            @Override
			TimeUnit getUnit() {
                return TimeUnit.HOURS;
            }
        };

        abstract TimeUnit getUnit();

        static TimeUnit unit(String value) {
            return valueOf(value.toUpperCase(java.util.Locale.ENGLISH)).
                getUnit();
        }
    }
}
