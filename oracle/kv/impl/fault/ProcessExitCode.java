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

package oracle.kv.impl.fault;

/**
 * The enumeration of process exit codes used to communicate between a process
 * and its handler, e.g. the SNA, some shell script, etc.
 * <p>
 * Process exit codes must be in the range [0-255] and must not be one of the
 * following: 1-2, 64-113 (C/C++ standard), 126 - 165, and 255 since they are
 * reserved and have special meaning.
 */
public enum ProcessExitCode {

    RESTART() {

        @Override
        public short getValue() {
            return 200;
        }

        @Override
        public boolean needsRestart() {
            return true;
        }
    },

    NO_RESTART{

        @Override
        public short getValue() {
            return 201;
        }

        @Override
        public boolean needsRestart() {
            return false;
        }
    },

    /*
     * It's a variant of RESTART indicating that the process needs to be
     * started due to an OOME. It's a distinct OOME so that the SNA can log
     * this fact because the managed service can't.
     */
    RESTART_OOME {
        @Override
        public short getValue() {
            return 202;
        }

        @Override
        public boolean needsRestart() {
            return true;
        }
    };

    /**
     * Returns the numeric value associated with the process exit code.
     */
    public abstract short getValue();

    public abstract boolean needsRestart();

    /**
     * Returns true if the process exit code indicates that the process needs
     * to be restarted, or if the exit code is not one of the know exit codes
     * from the above enumeration.
     */
    static public boolean needsRestart(int exitCode) {
        for (ProcessExitCode v : ProcessExitCode.values()) {
            if (exitCode == v.getValue()) {
                return v.needsRestart();
            }
        }

        /*
         * Some unknown exitCode. Opt for availability, restart the process.
         * If its a recurring error, the SNA will eventually shut down the
         * managed service.
         */
        return true;
    }
}
