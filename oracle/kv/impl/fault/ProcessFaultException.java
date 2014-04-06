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
 * Informs a process level handler that the process can recover from the fault
 * by being restarted.
 *
 * <p>
 * The handler for this exception is expected to exit with the
 * {@link ProcessExitCode#RESTART} exit code. The SNA or a shell script which
 * takes appropriate action based upon the processes's exit code ten restarts
 * the process.
 * <p>
 * Since the fault handling design requires that this exception be used
 * entirely within a process, it's not Serializable.
 *
 * @see SystemFaultException
 */
@SuppressWarnings("serial")
public class ProcessFaultException extends RuntimeException {

    /**
     * Constructor to wrap a fault and indicate that it's restartable.
     *
     * @param msg a message further explaining the error
     * @param fault the exception being wrapped
     */
    public ProcessFaultException(String msg, RuntimeException fault) {
        super(msg, fault);
        assert fault != null;
    }

    @Override
    public RuntimeException getCause() {
        return (RuntimeException)super.getCause();
    }

    /**
     * The process exit code indicating that the process must be restarted.
     *
     * @return the process exit code
     */
    public ProcessExitCode getExitCode() {
        return ProcessExitCode.RESTART;
    }
}
