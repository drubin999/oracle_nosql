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

import java.security.Principal;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import javax.security.auth.Subject;

import oracle.kv.AuthenticationRequiredException;
import oracle.kv.UnauthorizedException;
import oracle.kv.impl.security.login.LoginToken;
import oracle.kv.impl.security.login.TokenVerifier;

/**
 * Standard implementation of AccessChecker.
 */
public class AccessCheckerImpl implements AccessChecker {
    private final TokenVerifier verifier;
    private volatile Logger logger;

    /**
     * Construct a AccessChecker that uses the provided TokenVerifier
     * to validate method calls.
     */
    public AccessCheckerImpl(TokenVerifier verifier, Logger logger) {
        this.verifier = verifier;
        this.logger = logger;
    }

    /**
     * Log a message describing an access error.
     * @param msg a general message describing the cause of the error
     * @param execCtx the ExecutionContext that encountered the error
     * @param opCtx the OperationContext that was being attempted
     */
    private void logError(String msg,
                          ExecutionContext execCtx,
                          OperationContext opCtx) {
        if (execCtx.requestorContext() != null &&
            execCtx.requestorContext().getClientHost() != null) {
            logger.info(msg + " : " +
                        "client host: " + execCtx.requestorHost() +
                        ", auth host: " +
                        execCtx.requestorContext().getClientHost() +
                        ": " + opCtx.describe());
        } else {
            logger.info(msg + " : " +
                        "client host: " + execCtx.requestorHost() +
                        ": " + opCtx.describe());
        }
    }

    /**
     * Updates the logger used by this instance.  The logger must always be
     * non-null but it may be changed.
     */
    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    /**
     * Identifies the requestor of an operation.
     * @param context the identifying context provided by the caller.
     *   This is null allowable.
     * @return a Subject object if the identity could be determined,
     *   or else null.
     * @throw SessionAccessException
     */
    @Override
    public Subject identifyRequestor(AuthContext context)
        throws SessionAccessException {

        if (context == null) {
            return null;
        }

        final LoginToken token = context.getLoginToken();

        if (token == null) {
            return null;
        }

        try {
            return verifier.verifyToken(token);
        } catch (SessionAccessException sae) {
            /*
             * rethrow indicating that the access exception applies to the
             * token supplied with the AuthContext.
             */
            throw new SessionAccessException(sae,
                                             false /* isReturnSignal */);
        }
    }

    /**
     * Check the authorization of the requestor against the requirements
     * of the operation.
     */
    @Override
    public void checkAccess(ExecutionContext execCtx, OperationContext opCtx)
        throws AuthenticationRequiredException, UnauthorizedException {

        final List<KVStoreRolePrincipal> roles = opCtx.getRequiredRoles();

        if (roles.size() == 0) {
            /*
             * subject could be null here, either because token was null
             * or because it couldn't be validated, but since there are
             * no authentication requirements, we don't worry about it
             * here.
             */
            return;
        }

        final Subject subject = execCtx.requestorSubject();

        if (subject == null) {
            final AuthContext secCtx = execCtx.requestorContext();
            if (secCtx == null || secCtx.getLoginToken() == null) {
                logError("Attempt to call method without authentication",
                         execCtx, opCtx);
                throw new AuthenticationRequiredException(
                    "Authentication required for access",
                    false /* isReturnSignal */);
            }

            /* Because we had a token, it must have been invalid */
            logError("Attempt to call method with invalid authentication",
                     execCtx, opCtx);
            throw new AuthenticationRequiredException(
                "Authentication required for access",
                false /* isReturnSignal */);
        }

        final Set<Principal> principals = subject.getPrincipals();
        for (KVStoreRolePrincipal princ : roles) {
            if (!principals.contains(princ)) {
                logError("Insufficient access rights",
                         execCtx, opCtx);
                throw new UnauthorizedException(
                    "Insufficient access rights granted");
            }
        }
    }
}
