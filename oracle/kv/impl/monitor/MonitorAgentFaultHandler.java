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

import java.util.logging.Logger;

import oracle.kv.impl.fault.ClientAccessException;
import oracle.kv.impl.fault.InternalFaultException;
import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.fault.ProcessFaultHandler;

/**
 * Monitor agents should never cause process exit. Faults should be recorded
 * as well as possible, although an error found from monitoring may indicate
 * a general error in the monitoring system.
 */
public class MonitorAgentFaultHandler extends ProcessFaultHandler {

    public MonitorAgentFaultHandler(Logger logger) {
        super(logger, ProcessExitCode.RESTART);
    }

    /**
     * Do nothing, don't shutdown.
     */
    @Override
    protected void queueShutdownInternal(Throwable fault,
                                         ProcessExitCode exitCode) {
        /* do nothing. */
    }

    /**
     * Wrap it inside an AdminFaultException, if it isn't already an
     * InternalFaultException. The fault is an InternalFaultException when it
     * originated at a different service, and is just passed through.
     */
    @Override
    protected RuntimeException getThrowException(RuntimeException fault) {
        if (fault instanceof InternalFaultException) {
            return fault;
        }
        if (fault instanceof ClientAccessException) {
            return ((ClientAccessException)fault).getCause();
        }
        return new MonitorAgentFaultException(fault);
    }

    /**
     * Monitor agents should never shuts down, so all exit codes are eaten in
     * this method. If this is a problem with a true exit code, we do log
     * the problem.
     */
    @Override
    public ProcessExitCode getExitCode(RuntimeException fault,
                                       ProcessExitCode  exitCode) {

        /* This is a pass-through exception, which was logged elsewhere */
        if (fault instanceof InternalFaultException) {
            return null;
        }

        /*
         * Report the error, but don't return an exit code. We don't want the
         * process to exit, but we do want to leave some information about the
         * problem. Also write this to stderr, in case the logging system has
         * failed, say due to out of disk,
         */
        String msg =
            "Exception encountered. but process will remain active: " + fault;
        logger.severe(msg);
        System.err.println(msg);
        return null;
    }

    private static class MonitorAgentFaultException
        extends InternalFaultException {
        private static final long serialVersionUID = 1L;

        MonitorAgentFaultException(Throwable cause) {
            super(cause);
        }
    }
}
