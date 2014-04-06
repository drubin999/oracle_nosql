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
 * A configurable service is the equivalent of a "process" in the KV Store,
 * except that it does not need to have a one-to-one relationship with a JVM.
 * Multiple services may be started within a single JVM.
 *
 * Description of ServiceStatus enumeration:
 *
 *                    generated by
 * enum               the service?  description
 * ----               ------------  -----------
 * STARTING           yes           Service is coming up, if a RepNode it may
 *                                  be doing recovery/syncup
 * RUNNING            yes           Service is running normally
 * STOPPING           yes           Service is stopping.  If a RepNode it may
 *                                  be performing a checkpoint.  If SNA it may
 *                                  be shutting down managed services
 * WAITING_FOR_DEPLOY yes           If SNA, waiting to be registered.  RepNode
 *                                  is waiting for a configure() call when it
 *                                  is being created for the first time
 * STOPPED            maybe*        an intentional, clean shutdown
 * ERROR_RESTARTING   maybe*        Service is in an error state and restart
 *                                  will be attempted
 * ERROR_NO_RESTART   maybe*        Service is in an error state and will not
 *                                  be automatically restarted.  Administrative
 *                                  intervention is required
 * UNREACHABLE        no            Service is not reachable and it should be.
 *                                  This status is only generated by ping or
 *                                  the Admin
 *
 * Values initialized with "true" are also treated as alerts.  Terminal state
 * comes from isTerminalState(), below.
 *
 * "maybe*" indicates that the service may call its ServiceStatusTracker with
 * the state change but the SNA will also generate it on behalf of the service
 * when it detects the state via shutdown or process failure.  This ensures
 * that alerts are not lost.
 *
 */
public interface ConfigurableService {

    /*
     * Each type of service status has a severity and alert level associated
     * with it, which gives the UI a hint of what action to take for this
     * status.
     */
    public enum ServiceStatus {
        /* needsAlert, isTerminal, severity, isAlive. */
        STARTING(false, false, 1, true),
        WAITING_FOR_DEPLOY(false, false, 1, true),
        RUNNING(false, false, 1, true),
        STOPPING(false, true, 1, false),
        STOPPED(false, true, 1, false),
        ERROR_RESTARTING(true, true, 2, true),
        ERROR_NO_RESTART(true, true, 2, false),
        UNREACHABLE(true, true, 2, false);

        private final boolean needsAlert;
        private final boolean isTerminal;
        private final int severity;
        private final boolean isAlive;
        /**
         * NeedsAlert and severity are used for monitoring.
         */
        ServiceStatus(boolean needsAlert,
                      boolean isTerminal,
                      int severity,
                      boolean isAlive) {
            this.needsAlert = needsAlert;
            this.isTerminal = isTerminal;
            this.severity = severity;
            this.isAlive = isAlive;
        }

        public boolean needsAlert() {
            return needsAlert;
        }

        public int getSeverity() {
            return severity;
        }

        /**
         * Returns true, if this is a process terminal state.
         */
        public boolean isTerminalState() {
            return isTerminal;
        }

        public boolean isAlive() {
            return isAlive;
        }
    }

    public void start();

    public void stop(boolean force);

}
