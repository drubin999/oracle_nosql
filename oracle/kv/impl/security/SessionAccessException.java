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
package oracle.kv.impl.security;

import oracle.kv.FaultException;

/**
 * Exception thrown when session data is inaccessible. The session data may
 * be non-existent or it may exist but simply cannot be accessed due to an
 * operational failure.
 */
public class SessionAccessException extends FaultException {

    private static final long serialVersionUID = 1L;

    /*
     * When false, this indicates that the error applies to the credentials
     * provided by the authentication context that was current when an operation
     * was performed, and should be considered when the dispatch mechanism
     * decides whether to retry the operation. When true, this indicates that
     * the exception refers to authentication credentials passed explicitly in
     * operation arguments, and the exception is intended to signal a return
     * result directly to the caller. A value of true is normally specified
     * when the exception is thrown, and a new exception is thrown when caught
     * in the context of authentication credentials checking.
     */
    private final boolean isReturnSignal;

    public SessionAccessException(String message) {
        super(message, true);
        this.isReturnSignal = true;
    }

    public SessionAccessException(Throwable cause,
                                  boolean isReturnSignal) {
        super(cause, true);
        this.isReturnSignal = isReturnSignal;
    }

    /**
     * Indicates whether the SessionAccessException applies to the session
     * associated with the caller of a method or to a session argument to the
     * method call.
     * @return true if the exception applies to a method argument and false
     * otherwise.
     */
    public boolean getIsReturnSignal() {
        return isReturnSignal;
    }
}
