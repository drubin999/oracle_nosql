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

/**
 * Utility class that permits a "poll based" waiting for a condition.
 */
public abstract class PollCondition {

    private final int checkPeriodMs;
    private final int timeoutMs;

    public PollCondition(int checkPeriodMs,
                         int timeoutMs) {
        super();
        assert checkPeriodMs <= timeoutMs;
        this.checkPeriodMs = checkPeriodMs;
        this.timeoutMs = timeoutMs;
    }

    protected abstract boolean condition();

    public boolean await() {

        if (condition()) {
            return true;
        }

        final long timeLimit = System.currentTimeMillis() + timeoutMs;
        do {
            try {
                Thread.sleep(checkPeriodMs);
            } catch (InterruptedException e) {
                return false;
            }
            if (condition()) {
                return true;
            }
        } while (System.currentTimeMillis() < timeLimit);

        return false;
    }
}