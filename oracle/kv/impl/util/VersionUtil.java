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

import oracle.kv.KVVersion;

/**
 * Version utility methods.
 *
 * @see oracle.kv.KVVersion
 */
public class VersionUtil {

    /* Prevent construction */
    private VersionUtil() {}

    /**
     * Compares the two specified version, ignoring the patch number.
     *
     * Returns -1 if v1 is an earlier minor version than v2.
     * Returns 0 if v1 is the same minor version as v2.
     * Returns 1 if v1 is a later minor version than v2..
     *
     * In the comparison all numeric fields except patch are compared.
     */
    public static int compareMinorVersion(KVVersion v1, KVVersion v2) {
        /* Zero out the patch # of the two versions and compare */
        return new KVVersion(v1.getOracleMajor(),
                             v1.getOracleMinor(),
                             v1.getMajor(),
                             v1.getMinor(),
                             0, null).
            compareTo(new KVVersion(v2.getOracleMajor(),
                                    v2.getOracleMinor(),
                                    v2.getMajor(),
                                    v2.getMinor(),
                                    0, null));
    }

    /**
     * Checks whether the specified software version can be upgraded to the
     * current version and throws IllegalStateException if it cannot. The
     * previous version must be greater than or equal to the prerequisite and
     * must not be a newer minor version.
     *
     * @param previousVersion the previous software version to check
     *
     * @throws IllegalStateException if the check fails
     */
    public static void checkUpgrade(KVVersion previousVersion) {
        checkUpgrade(previousVersion,
                     KVVersion.PREREQUISITE_VERSION,
                     KVVersion.CURRENT_VERSION);
    }

    /**
     * Checks whether the specified software version can be upgraded to the
     * specified target version and throws IllegalStateException if it cannot.
     * the previous version must be greater than or equal to the target's
     * prerequisite and must not be a newer minor version.
     *
     * @param previousVersion the previous software version to check
     * @param prerequisiteVersion the target's prerequisite
     * @param targetVersion the target version
     */
    public static void checkUpgrade(KVVersion previousVersion,
                                    KVVersion prerequisiteVersion,
                                    KVVersion targetVersion) {

        if (previousVersion.compareTo(prerequisiteVersion) < 0) {
            throw new IllegalStateException(
                    "The previous software version " +
                    previousVersion.getNumericVersionString() +
                    " does not satisfy the prerequisite for " +
                    targetVersion.getNumericVersionString() +
                    " which requires version " +
                    prerequisiteVersion.getNumericVersionString() +
                    " or later.");
        }

        /*
         * Candidate is at least the prerequisite, check that it is not
         * a newer minor version.
         */
        if (compareMinorVersion(previousVersion, targetVersion) > 0) {
            throw new IllegalStateException(
                    "The previous software version " +
                    previousVersion.getNumericVersionString() +
                    " is a newer minor version than " +
                    targetVersion.getNumericVersionString());
        }
    }
}
