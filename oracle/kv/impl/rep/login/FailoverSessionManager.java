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

package oracle.kv.impl.rep.login;

import java.util.logging.Logger;

import javax.security.auth.Subject;

import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.security.login.LoginSession;
import oracle.kv.impl.security.login.SessionManager;

/**
 * FailoverSessionManager uses a KVSessionManager as its primary storage
 * location for session storage, but will fail over to an in-memory
 * session manager if the KVSessionManager encounters faults.
 */
public class FailoverSessionManager implements SessionManager {

    public static final byte[] MEMORY_PREFIX = { 0 };
    public static final byte[] PERSISTENT_PREFIX = { 0x01 };

    private final Logger logger;

    private final KVSessionManager kvSessMgr;

    private final SessionManager tableSessMgr;

    /**
     * Creates a FailoverSessionManager.
     */
    public FailoverSessionManager(KVSessionManager kvSessMgr,
                                  SessionManager tableSessMgr,
                                  Logger logger) {

        this.kvSessMgr = kvSessMgr;
        this.tableSessMgr = tableSessMgr;
        this.logger = logger;
    }

    /**
     * Creates a new Session.
     */
    @Override
    public LoginSession createSession(
        Subject subject, String clientHost, long expireTime)
    {

        try {
            final LoginSession session =
                kvSessMgr.createSession(subject, clientHost, expireTime);

            /*
             * The persistent session manager might fail to create the
             * session.  If so, we fail over to the in-memory session
             * manager.
             */
            if (session != null) {
                return session;
            }
            logger.info("Persistent session manager failed to create " +
                        "a new session.");
        } catch (SessionAccessException sae) {

            /*
             * Some sort of failure on persistent access.  We'll fall
             * back to in-memory access.
             */
            logger.info("Persistent session manager encountered " +
                        " exception: " + sae);
        }

        return tableSessMgr.createSession(subject, clientHost, expireTime);
    }

    /**
     * Look up a Session by SessionId.
     * @return the login session if found, or else null
     */
    @Override
    public LoginSession lookupSession(LoginSession.Id sessionId)
        throws SessionAccessException {

        if (sessionId.beginsWith(PERSISTENT_PREFIX)) {
            return kvSessMgr.lookupSession(sessionId);
        }
        return tableSessMgr.lookupSession(sessionId);
    }

    /**
     * Log out the specified session.
     */
    @Override
    public LoginSession updateSessionExpiration(LoginSession.Id sessionId,
                                                long newExpireTime)
        throws SessionAccessException {

        if (sessionId.beginsWith(PERSISTENT_PREFIX)) {
            return kvSessMgr.updateSessionExpiration(sessionId,
                                                     newExpireTime);
        }
        return tableSessMgr.updateSessionExpiration(sessionId, newExpireTime);
    }

    /**
     * Log out the specified session.
     */
    @Override
    public void logoutSession(LoginSession.Id sessionId)
        throws SessionAccessException {

        if (sessionId.beginsWith(PERSISTENT_PREFIX)) {
            kvSessMgr.logoutSession(sessionId);
        } else {
            tableSessMgr.logoutSession(sessionId);
        }
    }
}
