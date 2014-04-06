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

package oracle.kv;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Used to indicate an error condition that cannot normally be handled by the
 * caller of the method, except by retrying the operation.
 *
 * <p>When the error occurred remotely and was due to an internally defined
 * server exception, the {@link #getCause} method will return null rather than
 * returning the internal exception, and the remote stack trace will not be a
 * part of the stack trace of this exception.  This is because the internal
 * exception class is not present in the client library.</p>
 *
 * <p>For logging, testing and debugging purposes, the full textual stack trace
 * of the remote exception, including any nested cause exceptions, is available
 * using the {@link #getRemoteStackTrace} method, and the remote (or local)
 * exception's class name is returned by {@link #getFaultClassName}.  The
 * {@link #toString} and {@link #printStackTrace} methods output the fault
 * class name and remote stack trace.</p>
 *
 * <p>When the error occurred remotely, it will have already been logged and
 * reported on a remote KVStore node and will be available to administrators.
 * However, to correlate client and server errors and to make error information
 * easily accessible on the client, it is good practice to also log the error
 * locally.  Errors that originated locally are not automatically logged and
 * available to administrators, and the client application is responsible for
 * reporting them.  See {@link #wasLoggedRemotely}.</p>
 */
public class FaultException extends RuntimeException {

    /*
     * TODO: Expand fault information?
     * + Indication of "write may have completed" when we can't tell because
     *   of a marshalling error.
     * + Indicate that retry is probably unproductive, for example, when TTL is
     *   exceeded.
     */

    private static final long serialVersionUID = 1L;

    private final boolean occurredRemotely;
    private final String faultClassName;
    private final String remoteStackTrace;

    /**
     * For internal use only.
     * @hidden
     */
    public FaultException(String msg, boolean isRemote) {
        this(msg, null, isRemote);
    }

    /**
     * For internal use only.
     * @hidden
     */
    public FaultException(Throwable cause, boolean isRemote) {
        this(cause.getMessage(), cause, isRemote);
    }

    /**
     * For internal use only.
     * @hidden
     */
    public FaultException(String msg, Throwable cause, boolean isRemote) {
        super(msg + " (" +
              KVVersion.CURRENT_VERSION.getNumericVersionString() + ")",
              isRemote ? null : cause);
        occurredRemotely = isRemote;
        if (isRemote) {
            final StringWriter sw = new StringWriter(500);

            /* Save textual remote stack trace. */
            if (cause == null) {
                new RuntimeException().printStackTrace(new PrintWriter(sw));
                faultClassName = this.getClass().getName();
            } else {
                cause.printStackTrace(new PrintWriter(sw));
                /* Fault class name is remote exception class name. */
                faultClassName = cause.getClass().getName();
            }
            remoteStackTrace = sw.toString();
        } else {
            /* There is no remote stack trace. */
            remoteStackTrace = null;
            /* Use most meaningful local exception class name. */
            if (cause != null && this.getClass() == FaultException.class) {
                faultClassName = cause.getClass().getName();
            } else {
                faultClassName = this.getClass().getName();
            }
        }
    }

    /**
     * Returns whether the exception was previously logged remotely.
     *
     * <p>When the error occurred remotely, it will have already been logged
     * and reported on a remote KVStore node and will be available to
     * administrators.  However, to correlate client and server errors and to
     * make error information easily accessible on the client, it is good
     * practice to also log the error locally.  Errors that originated locally
     * are not automatically logged and available to administrators, and the
     * client application is responsible for reporting them.</p>
     */
    public boolean wasLoggedRemotely() {
        return occurredRemotely;
    }

    /**
     * Returns the name of the class associated with the original fault
     * exception, or the name of the local exception class if the error
     * occurred locally. When the error occurred locally and this exception is
     * a simple wrapper, the class name of the wrapped exception is returned.
     * This method exists primarily for logging, testing and debugging.
     */
    public String getFaultClassName() {
        return faultClassName;
    }

    /**
     * Returns the textual stack trace associated with the remote fault
     * exception, or null if the error occurred locally. This method exists
     * primarily for logging, testing and debugging.
     */
    public String getRemoteStackTrace() {
        return remoteStackTrace;
    }

    /**
     * Returns a description of the fault that includes the standard
     * <code>Throwable</code> description (class name and message), followed by
     * the fault class name, followed by the remote stack trace (if any).  This
     * method exists primarily for logging, testing and debugging.
     */
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(1000);
        sb.append(super.toString());
        sb.append("\nFault class name: ");
        sb.append(faultClassName);
        if (remoteStackTrace != null) {
            sb.append("\nRemote stack trace: ");
            sb.append(remoteStackTrace);
        }
        return sb.toString();
    }
}
