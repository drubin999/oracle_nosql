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

package oracle.kv.impl.security.login;

import javax.security.auth.Subject;

import oracle.kv.impl.security.SessionAccessException;

/**
 * Defines the interface for management of login sessions.
 * <p>
 * A login session is a record of a user authentication operation to a
 * KVStore instance that tracks the identity of the authenticated user, the
 * authorized capabilities for that user, plus information about the lifetime
 * of the session.  When an operation is requested by an application, it sends
 * along a Login Token, which includes an ID value for the session.  This allows
 * the component that is processing the request to determine whether the request
 * is authorized.  If the session id doesn't identify an active session, or if
 * the authorized user capabilities for the session don't allow the action to
 * be performed, the request is rejected.
 * <p>
 * Sessions normally have a limited duration of validity, which is referred to
 * as the session lifetime. Some SessionManagers may support an unlimited
 * lifetime for sessions, which is signaled by a lifetime of 0.  Unless action
 * is taken to extend the lifetime of a session (through the
 * updateSessionExpiration method), an action expires at the end of its lifetime
 * and will normally become inaccessible.
 */

public interface SessionManager {

    /**
     * Creates a new Session.
     * @param subject a Subject that should be associated with the session.
     * Only KVStoreUserPrincipal and KVStoreRolePrincipal principals are
     * tracked as part of the session.
     * @param clientHost the host from which the session access originated
     * @param expireTime the time at which the session should expire,
     * expressed in the same time units and time base as
     * System.currentTimeMillis().  If set to 0L, the session does not expire,
     * but not all SessionManagers support non-expiring sessions.
     * @return a LoginSession object if the session creation was successful,
     * or null if unsuccessful
     * @throws SessionAccessException if unable to create the session due
     * to an operational problem
     * @throws IllegalArgumentException if the expireTime argument is invalid
     * for the SessionManager implementation
     */
    LoginSession createSession(
        Subject subject, String clientHost, long expireTime)
        throws SessionAccessException, IllegalArgumentException;

    /**
     * Look up a Session by SessionId.
     *
     * @param sessionId the Id of the session to be located
     * @return the login session if found and not expired, or else null
     * @throws SessionAccessException if unable to look up the session due
     * to an operational problem
     */
    LoginSession lookupSession(LoginSession.Id sessionId)
        throws SessionAccessException;

    /**
     * Update the expiration time associated with a session.
     * @param sessionId the id of the session to be updated
     * @param expireTime the new expire time to assign
     * @return the updated session object if the update was successful,
     *   or else null
     * @throws SessionAccessException if unable to look up the session due
     * to an operational problem
     * @throws IllegalArgumentException if the expireTime argument is invalid
     * for the SessionManager implementation
     */
    LoginSession updateSessionExpiration(LoginSession.Id sessionId,
                                         long expireTime)
        throws SessionAccessException, IllegalArgumentException;

    /**
     * Log out the specified session.
     * @param sessionId the id of the session to be logged out
     * @throws SessionAccessException if unable to look up the session due
     * to an operational problem
     */
    void logoutSession(LoginSession.Id sessionId)
        throws SessionAccessException;

}
