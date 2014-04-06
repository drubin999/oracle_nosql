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

/**
 * Thrown when a request cannot be processed because the configured timeout
 * interval is exceeded.
 *
 * <p>The default timeout interval (specified by {@link
 * KVStoreConfig#getRequestTimeout}) is five seconds, and this exception should
 * rarely be thrown.</p>
 *
 * <p>Note that the durability of an update operation is uncertain if it
 * results in a {@link RequestTimeoutException} being thrown. The changes
 * requested by the update may or may not have been committed to the master or
 * propagated to one or more replicas. Applications may want to retry the
 * update operation if it is idempotent, or perform read operations to
 * determine the outcome of the previous update.</p>
 *
 * <p>Note also that if the consistency specified for a read operation
 * is {@link Consistency#NONE_REQUIRED_NO_MASTER}, then this exception
 * will be thrown if the operation is attempted when the only node
 * available is the Master.</p>
 *
 * <p>Depending on the nature of the application, when this exception is thrown
 * the client may wish to
 * <ul>
 * <li>retry the operation,</li>
 * <li>fall back to using a larger timeout interval, and resume using the
 * original timeout interval at a later time, or</li>
 * <li>give up and report an error at a higher level.</li>
 * </ul>
 * </p>
 */
public class RequestTimeoutException extends FaultException {

    private static final long serialVersionUID = 1L;

    private volatile int timeoutMs;

    /**
     * For internal use only.
     * @hidden
     */
    public RequestTimeoutException(int timeoutMs,
                                   String msg,
                                   Exception cause,
                                   boolean isRemote) {
        super(msg, cause, isRemote);
        this.timeoutMs = timeoutMs;
    }

    @Override
    public String getMessage() {
        if (timeoutMs == 0) {
            return super.getMessage();
        }
        return super.getMessage() + " Timeout: " + timeoutMs + "ms";
    }

    /**
     * Returns the timeout that was in effect for the operation.
     */
    public int getTimeoutMs() {
        return timeoutMs;
    }

    /**
     * Sets the timeout that was in effect for the operation.
     */
    public void setTimeoutMs(int timeoutMs) {
        this.timeoutMs = timeoutMs;
    }
}
