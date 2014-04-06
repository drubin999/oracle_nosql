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

import java.util.logging.Logger;

import javax.security.auth.Subject;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.AuthenticationRequiredException;
import oracle.kv.LoginCredentials;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.security.ProxyCredentials;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.security.UserVerifier;
import oracle.kv.impl.security.login.SessionId.IdScope;
import oracle.kv.impl.topo.ResourceId;

/**
 * Provides logic for authentication of logins against a user database.
 */

public class UserLoginHandler {

    /**
     * Number of bytes in new session id values.
     */
    public static final int SESSION_ID_RANDOM_BYTES = 16;

    /* Default account lockout checking interval in seconds */
    private static final int DEF_ACCT_ERR_LCK_INT = 10 * 60;

    /* Default account lockout checking threshold count */
    private static final int DEF_ACCT_ERR_LCK_CNT = 10;

    /* Default account lockout period in seconds */
    private static final int DEF_ACCT_ERR_LCK_TMO = 10 * 60;

    /* Identifies the component that is hosting this instance */
    private volatile ResourceId ownerId;

    /*
     * If true, indicates that the ownerId is locally generated, and is not
     * resolvable by other storage nodes.
     */
    private volatile boolean localOwnerId;

    /* The underlying session manager object */
    private final SessionManager sessMgr;

    /* An object for translating user identities into Subject descriptions */
    private final UserVerifier userVerifier;

    /*
     * The configured lifetime value for newly created sessions, in ms.
     * See SessionManager for a description of session lifetime.
     */
    private volatile long sessionLifetime;

    /*
     * The configuration setting indicating whether a client may request that
     * their session be extended.
     */
    private volatile boolean allowExtension;

    protected final Logger logger;

    /*
     * Tracks login errors, and prevents login when an excessive number of
     * login attempts have failed.
     */
    private volatile LoginErrorTracker errorTracker;

    /**
     * Configuration for the login handler.
     */
    public static class LoginConfig {
        /* session lifetime in ms */
        private long sessionLifetime;
        /* whether to allow sessions to be extended */
        private boolean allowExtension;
        /* account error lockout interval in ms */
        private long acctErrLockoutInt;
        /* account error threshold for lockout */
        private int acctErrLockoutCnt;
        /* account error lockout timeout in ms */
        private long acctErrLockoutTMO;

        public LoginConfig() {
        }

        public LoginConfig setSessionLifetime(long lifetimeMs) {
            this.sessionLifetime = lifetimeMs;
            return this;
        }

        public LoginConfig setAllowExtension(boolean allowExtend) {
            this.allowExtension = allowExtend;
            return this;
        }

        public LoginConfig setAcctErrLockoutInt(long lockoutIntervalMs) {
            this.acctErrLockoutInt = lockoutIntervalMs;
            return this;
        }

        public LoginConfig setAcctErrLockoutCnt(int lockoutCount) {
            this.acctErrLockoutCnt = lockoutCount;
            return this;
        }

        public LoginConfig setAcctErrLockoutTMO(long lockoutTMOMs) {
            this.acctErrLockoutTMO = lockoutTMOMs;
            return this;
        }

        @Override
        public LoginConfig clone() {
            final LoginConfig dup = new LoginConfig();

            dup.sessionLifetime = sessionLifetime;
            dup.allowExtension = allowExtension;
            dup.acctErrLockoutInt = acctErrLockoutInt;
            dup.acctErrLockoutCnt = acctErrLockoutCnt;
            dup.acctErrLockoutTMO = acctErrLockoutTMO;
            return dup;
        }

        /**
         * Try to populate a LoginConfig using the values in the specified
         * GlobalParams object. If the GlobalParams is null, a LoginConfig with
         * all zero values is returned.
         *
         * @param gp GlobalParam object
         */
        public static LoginConfig buildLoginConfig(final GlobalParams gp) {
            final LoginConfig config = new LoginConfig();
            if (gp != null) {
                final long sessionLifetimeInMillis =
                    gp.getSessionTimeoutUnit().toMillis(gp.getSessionTimeout());
                final int acctErrLockoutTMOInSeconds = (int)
                    gp.getAcctErrLockoutTimeoutUnit().toMillis(
                        gp.getAcctErrLockoutTimeout());
                final int acctErrLockoutIntInSeconds = (int)
                    gp.getAcctErrLockoutThrIntUnit().toMillis(
                        gp.getAcctErrLockoutThrInt());

                config.setAcctErrLockoutCnt(gp.getAcctErrLockoutThrCount()).
                       setSessionLifetime(sessionLifetimeInMillis).
                       setAllowExtension(gp.getSessionExtendAllow()).
                       setAcctErrLockoutTMO(acctErrLockoutTMOInSeconds).
                       setAcctErrLockoutInt(acctErrLockoutIntInSeconds);
            }
            return config;
        }
    }

    public UserLoginHandler(ResourceId ownerId,
                            boolean localOwnerId,
                            UserVerifier userVerifier,
                            SessionManager sessionManager,
                            LoginConfig loginConfig,
                            Logger logger) {
        this.logger = logger;
        this.ownerId = ownerId;
        this.localOwnerId = localOwnerId;
        this.sessMgr = sessionManager;
        this.userVerifier = userVerifier;
        this.sessionLifetime = loginConfig.sessionLifetime;
        this.allowExtension = loginConfig.allowExtension;
        this.errorTracker = makeErrorTracker(loginConfig);
    }

    private LoginErrorTracker makeErrorTracker(final LoginConfig loginConfig) {
        return
            new LoginErrorTracker(
                (loginConfig.acctErrLockoutInt == 0 ?
                 DEF_ACCT_ERR_LCK_INT : loginConfig.acctErrLockoutInt),
                (loginConfig.acctErrLockoutCnt == 0 ?
                 DEF_ACCT_ERR_LCK_CNT : loginConfig.acctErrLockoutCnt),
                (loginConfig.acctErrLockoutTMO == 0 ?
                 DEF_ACCT_ERR_LCK_TMO : loginConfig.acctErrLockoutTMO),
                logger);
    }

    /**
     * Log a user into the database.
     * @param creds the credential object used for login.  The actual type
     *        of the object will vary depending on login mechanism.
     * @param clientHost the host from which the client request was received,
     *        if this is a forwarded login request.
     * @return a LoginResult
     * @throw AuthenticationFailureException if the LoginCredentials are
     *        not valid
     */
    public LoginResult login(LoginCredentials creds, String clientHost)
         throws AuthenticationFailureException {

        if (creds instanceof ProxyCredentials) {
            /* ProxyCredentials can be used only through proxyLogin() */
            throw new AuthenticationFailureException (
                "Invalid use of ProxyCredentials.");
        }

        return loginInternal(creds, clientHost);
    }

    /**
     * Log a user into the database, without password checking.
     *
     * @param creds the credential object used for login, which identifies.
     *        the user to be logged in.
     * @param clientHost the host from which the client request was received,
     *        if this is a forwarded login request.
     * @return a LoginResult
     * @throw AuthenticationFailureException if the LoginCredentials are
     *        not valid
     */
    public LoginResult proxyLogin(ProxyCredentials creds, String clientHost)
         throws AuthenticationFailureException {

        return loginInternal(creds, clientHost);
    }

    /**
     * Request that a login token be replaced with a new token that has a later
     * expiration than that of the original token.  Depending on system policy,
     * this might not be allowed.  This is controlled by the
     * sessionTokenExpiration security configuration parameter.
     * TODO: link to discussion of system policy
     *
     * @return null if the LoginToken is not valid or if session extension is
     *          not allowed - otherwise, return a new LoginToken.  The
     *          returned LoginToken will use the same session id as the
     *          original token, but the client should use the new LoginToken
     *          for subsequent requests.
     * @throw SessionAccessException if an operational failure prevents
     *   completion of the operation
     */
    public LoginToken requestSessionExtension(LoginToken loginToken)
        throws SessionAccessException {

        if (loginToken == null) {
            return null;
        }

        if (!allowExtension) {
            logger.fine("Session extend not allowed");
            return null;
        }

        final LoginSession session =
            sessMgr.lookupSession(
                new LoginSession.Id(
                    loginToken.getSessionId().getIdValue()));


        if (session == null || session.isExpired()) {
            logger.info("Session " + loginToken.getSessionId().hashId() +
                        ": extend failed due to expiration");

            return null;
        }

        long newExpire;
        if (sessionLifetime == 0L) {
            newExpire = 0L;
        } else {
            newExpire = System.currentTimeMillis() + sessionLifetime;
        }

        logger.info("Session extend allowed");

        final LoginSession newSession =
            sessMgr.updateSessionExpiration(session.getId(), newExpire);

        if (newSession == null) {
            logger.info("Session " + session.getId().hashId() +
                        ": update failed");
            return null;
        }

        final SessionId sid = (newSession.isPersistent()) ?
            new SessionId(newSession.getId().getValue()) :
            new SessionId(newSession.getId().getValue(),
                          getScope(), ownerId);

        return new LoginToken(sid, newSession.getExpireTime());
    }

    /**
     * Check an existing LoginToken for validity.
     * @return a Subject describing the user, or null if not valid
     * @throw SessionAccessException
     */
    public Subject validateLoginToken(LoginToken loginToken)
        throws SessionAccessException {

        if (loginToken == null) {
            return null;
        }

        final LoginSession session =
            sessMgr.lookupSession(new LoginSession.Id(
                                      loginToken.getSessionId().getIdValue()));

        if (session == null || session.isExpired()) {
            return null;
        }

        return userVerifier.verifyUser(session.getSubject());
    }

    /**
     * Log out the login token.  The LoginToken will no longer be usable for
     * accessing secure object interfaces.  If the session is already logged
     * out, this is treated as a a successful operation.  If the LoginToken
     * is not recognized, this may be because it was logged out earlier and
     * flushed from memory, and so this case will also be treated as successful.
     * If the LoginToken is recognized, but the session
     * cannot be modified because the session-containing shard is not
     * writable,
     */
    public void logout(LoginToken loginToken)
        throws AuthenticationRequiredException, SessionAccessException {

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

    protected LoginResult createLoginSession(Subject subject,
                                             String clientHost) {

        final long expireTime =
            (sessionLifetime != 0L) ?
            (System.currentTimeMillis() + sessionLifetime) :
            0;

        final LoginSession session =
            sessMgr.createSession(subject, clientHost, expireTime);

        final SessionId sid = (session.isPersistent()) ?
            new SessionId(session.getId().getValue()) :
            new SessionId(session.getId().getValue(), getScope(), ownerId);
        return new LoginResult(
            new LoginToken(sid, session.getExpireTime()));
    }

    protected IdScope getScope() {
        return localOwnerId ? IdScope.LOCAL : IdScope.STORE;
    }

    /**
     * Update the global session and login config.
     */
    public void updateConfig(final LoginConfig config) {
        this.sessionLifetime = config.sessionLifetime;
        this.allowExtension = config.allowExtension;
        this.errorTracker = makeErrorTracker(config);
    }

    /**
     * Log a user into the database.
     *
     * @param creds the credential object used for login.  The actual type
     *        of the object will vary depending on login mechanism.
     * @param clientHost the host from which the client request was received,
     *        if this is a forwarded login request.
     * @return a LoginResult
     * @throw AuthenticationFailureException if the LoginCredentials are
     *        not valid
     */
    private LoginResult loginInternal(LoginCredentials creds, String clientHost)
         throws AuthenticationFailureException {

         Subject subject = null;

         if (creds == null) {
             throw new AuthenticationFailureException (
                 "No credentials provided.");
         }

         if (errorTracker.isAccountLocked(creds.getUsername(), clientHost)) {
             throw new AuthenticationFailureException (
                 "User account is locked.");
         }

         subject = userVerifier.verifyUser(creds);

         if (subject == null) {
             errorTracker.noteLoginError(creds.getUsername(), clientHost);
             logger.info("Failed login attempt by user " + creds.getUsername());
             throw new AuthenticationFailureException ("Authentication failed");
         }
         errorTracker.noteLoginSuccess(creds.getUsername(), clientHost);

         logger.info("Successful login by user " + creds.getUsername());

         return createLoginSession(subject, clientHost);
     }

    /**
     * Changes the ownerId for this handler.  When changed, it is assumed
     * that the id is no longer a local id.  This is used when an Admin is
     * deployed.
     */
    public void updateOwner(final ResourceId id) {
        this.ownerId = id;
        this.localOwnerId = false;
    }

    public ResourceId getOwnerId() {
        return this.ownerId;
    }
}
