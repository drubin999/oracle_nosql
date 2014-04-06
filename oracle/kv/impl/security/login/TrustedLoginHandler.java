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

import java.security.Principal;
import java.util.HashSet;
import java.util.Set;

import javax.security.auth.Subject;

import oracle.kv.AuthenticationRequiredException;
import oracle.kv.impl.security.KVStoreRolePrincipal;
import oracle.kv.impl.security.login.SessionId.IdScope;
import oracle.kv.impl.topo.ResourceId;

/**
 * Provides login capabilities for infrastructure components.
 * This serves as the implementation layer behind TrustedLoginImpl.
 */
public class TrustedLoginHandler {

    /**
     * The number of login sessions that this can manage, but default.
     * This is used in the case of an unregistered SNA, so this is probably
     * total overkill.
     */
    private static final int CAPACITY = 10000;

    private static final int SESSION_ID_RANDOM_BYTES = 16;

    /* Our session manager */
    private LoginTable sessMgr;

    /* The identifier for the owning component */
    private ResourceId ownerId;

    /* If true, the ownerId can only be interpreted locally */
    private final boolean localId;

    /*
     * The lifetime for newly created sessions, in ms.  The value 0L means
     * that sessions do not expire.
     */
    private volatile long sessionLifetime;

    /**
     * Constructor for use by the unregistered SNA.
     */
    public TrustedLoginHandler(ResourceId ownerId, boolean localId) {
        this(ownerId, localId, 0L, CAPACITY);
    }

    /**
     * Common constructor for use by the registered SNA and the unregistered
     * SNA constructor.
     */
    public TrustedLoginHandler(ResourceId ownerId,
                               boolean localId,
                               long sessionLifeTime,
                               int sessionLimit) {
        this.ownerId = ownerId;
        this.localId = localId;
        this.sessionLifetime = sessionLifeTime;
        this.sessMgr =
            new LoginTable(sessionLimit,
                           new byte[0],
                           SESSION_ID_RANDOM_BYTES);
    }

    /**
     * Obtain a login token that identifies the caller as an infrastructure
     * component when accessing the RMI interfaces of this component.
     *
     * @return a login result
     */
    LoginResult loginInternal(String clientHost) {

        final long expireTime = (sessionLifetime == 0L) ? 0L :
            System.currentTimeMillis() + sessionLifetime;
        final LoginSession session =
            sessMgr.createSession(makeInternalSubject(), clientHost,
                                  expireTime);

        return new LoginResult(
            new LoginToken(new SessionId(
                               session.getId().getValue(),
                               localId ? IdScope.LOCAL : IdScope.STORE,
                               ownerId),
                           session.getExpireTime()));
    }

    /**
     * Check an existing LoginToken for validity.  This is intended for use
     * with locally generated tokens.
     *
     * @return a Subject describing the user, or null if not valid
     */
    Subject validateLoginToken(LoginToken loginToken) {

        if (loginToken == null) {
            return null;
        }

        final LoginSession session =
            sessMgr.lookupSession(new LoginSession.Id(
                                      loginToken.getSessionId().getIdValue()));
        if (session == null || session.isExpired()) {
            return null;
        }
        return session.getSubject();
    }

    /**
     * Log out the login token.  The LoginToken will no longer be usable for
     * accessing secure interfaces.
     *
     * @throws AuthenticationRequiredException if the login token is not valid,
     *         or is already logged out.
     */
    void logout(LoginToken loginToken) {

        if (loginToken == null) {
            throw new AuthenticationRequiredException(
                "LoginToken is null",
                true /* isReturnSignal */);
        }

        final LoginSession session =
            sessMgr.lookupSession(new LoginSession.Id(
                                      loginToken.getSessionId().getIdValue()));
        if (session == null || session.isExpired()) {
            throw new AuthenticationRequiredException(
                "session is not valid",
                true /* isReturnSignal */);
        }
        sessMgr.logoutSession(session.getId());
    }

    /**
     * Update the session limits.
     */
    public boolean updateSessionLimit(final int newLimit) {
        return sessMgr.updateSessionLimit(newLimit);
    }

    /**
     * Update the session lifetime.
     */
    public boolean updateSessionLifetime(final long newLifetime) {
        if (newLifetime == sessionLifetime) {
            return false;
        }
        sessionLifetime = newLifetime;
        return true;
    }

    private Subject makeInternalSubject() {

        final Set<Principal> internalPrincipals = new HashSet<Principal>();
        internalPrincipals.add(KVStoreRolePrincipal.INTERNAL);
        internalPrincipals.add(KVStoreRolePrincipal.AUTHENTICATED);
        final Set<Object> publicCreds = new HashSet<Object>();
        final Set<Object> privateCreds = new HashSet<Object>();
        return new Subject(true, internalPrincipals, publicCreds, privateCreds);
    }
}
