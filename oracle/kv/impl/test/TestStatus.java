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

package oracle.kv.impl.test;

/**
 * Used to determine whether the code is operating in a test environment. It's
 * the responsibility of the individual test to set the state. In unit tests,
 * this is typically done in the setUp method.
 */
public class TestStatus {

    private static boolean isActive = false;
    private static boolean isWriteNoSyncAllowed = false;
    private static boolean manyRNs = false;

    /**
     * Indicates whether the code is running in a test environment. Must be
     * set explicitly by a test.
     */
    public static void setActive(boolean isActive) {
        TestStatus.isActive = isActive;
    }

    /**
     * Return true if the stats was set to be active via setActive() to
     * indicate that we are running in a test environment.
     */
    public static boolean isActive() {
        return isActive;
    }

    /**
     * Whether write-no-sync durability is allowed in unit tests.  This should
     * only be set by tests, never in production mode.
     */
    public static void setWriteNoSyncAllowed(boolean isWriteNoSyncAllowed) {
        TestStatus.isWriteNoSyncAllowed = isWriteNoSyncAllowed;
    }

    /**
     * If false, the production durability level should be used.  If true, we
     * have the option of reducing sync durability to write-no-sync in cases
     * where this seems appropriate, in order to speed up unit tests.
     */
    public static boolean isWriteNoSyncAllowed() {
        return isWriteNoSyncAllowed;
    }

    /**
     * Indicates whether the test will instantiate many RNs on a single machine,
     * thereby in danger of running out of swap and heap when the default 
     * sizings are used.
     */
    public static void setManyRNs(boolean many) {
        TestStatus.manyRNs = many;
    }

    public static boolean manyRNs() {
        return manyRNs;
    }
}
