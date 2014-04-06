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

import javax.security.auth.Subject;

import oracle.kv.AuthenticationRequiredException;
import oracle.kv.UnauthorizedException;

/**
 * An interface defining the required support behavior for checking
 * access rights for proxy object access.
 */
public interface AccessChecker {

    /**
     * Given a auth context object, locate a Subject object that describes
     * the requestor.
     *
     * @param authCtx the security context, which might be null, that
     *        identifies the requestor
     * @return a Subject representing the caller, or null if there is
     *         no user context.
     * @throw SessionAccessException
     */
    Subject identifyRequestor(AuthContext authCtx)
        throws SessionAccessException;


    /**
     * Check that a pending invocation of a method is valid.
     *
     * @param execCtx an object describing the context in which the security
     *        check is being made.
     * @param opCtx an object describing the operation  in which the security
     *        check is being made.
     * @throw AuthenticationRequiredException if security checking is
     *        in force and the security context does not contain a
     *        valid identity.
     * @throw UnauthorizedException if security checking is in force and
     *        the identity indicated by the security context does not
     *        have the necessary access rights to make the call.
     * @throw SessionAccessException
     */
    void checkAccess(ExecutionContext execCtx, OperationContext opCtx)
        throws AuthenticationRequiredException, UnauthorizedException,
               SessionAccessException;

}
