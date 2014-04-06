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

import java.rmi.RemoteException;

import javax.security.auth.Subject;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.AuthenticationRequiredException;
import oracle.kv.LoginCredentials;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.ProxyCredentials;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.util.registry.VersionedRemote;

/**
 * An RMI interface that provides user login capabilities for the KVStore
 * database.  KVUserLogin is implemented and exported by SNA, RepNode and Admin
 * components in a storage node with an InterfaceType of LOGIN.
 *
 * @since 3.0
 */

public interface UserLogin extends VersionedRemote {
    /**
     * Log a user into the database.  A login may be persistent, in which
     * case it is can be used on any rep node, and does not depend upon the
     * node that served the token being up for others to make use of the token,
     * or it can be non-persistent, in which case it remains valid only while
     * the serving component is up.  For the non-persistent case, it can be
     * either local or non-local.  In the case of local tokens, it is valid
     * only for use on the SN that performed the login. Non-local tokens may
     * be used on SNs other than the one that originated it.  Note that a
     * non-persistent token is quicker to process and requires fewer resources,
     * but is less resilient in the case of a component restart and less
     * efficient and reliable for logging out if the login token is used with
     * multiple components.  If the login is authenticated but the
     * login cannot be made persistent, the login is treated as successful
     * and the returned login token is noted as non-persistent.
     *
     * @param creds the credential object used for login.  The actual type
     *        of the object will vary depending on login mechanism.
     * @return a LoginResult
     * @throw AuthenticationFailureException if the LoginCredentials are
     *        not valid
     */
     LoginResult login(LoginCredentials creds, short serialVersion)
         throws AuthenticationFailureException, RemoteException;

    /**
     * Attempt a login on behalf of a user into the database.  If a
     * user logs in to an admin but subsequently issues a command that requires
     * access to a RepNode, the admin uses this interface to establish a usable
     * LoginToken.  The caller must provide a security context of internal.
     * This is not supported by the SNA.
     *
     * @return a LoginResult, provided that the login did not fail.
     * @throw AuthenticationFailureException if the credentials are not valid
     * @throw AuthenticationRequiredException if the component does not support
     *        proxy login
     */
     LoginResult proxyLogin(ProxyCredentials creds,
                            AuthContext authContext,
                            short serialVersion)
         throws AuthenticationFailureException,
                AuthenticationRequiredException,
                RemoteException;

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
     * @throw SessionAccessException if unable to access the session associated
     * with the token due to an infrastructure malfunction
     */
    LoginToken requestSessionExtension(LoginToken loginToken,
                                       short serialVersion)
        throws SessionAccessException, RemoteException;

    /**
     * Check an existing LoginToken for validity.
     * @return a Subject describing the user, or null, if the token is not valid
     * @throw SessionAccessException if unable to resolve the token due to
     *  an infrastructure malfunction.
     */
    Subject validateLoginToken(LoginToken loginToken,
                               AuthContext authCtx,
                               short serialVersion)
        throws SessionAccessException, RemoteException;

    /**
     * Log out the login token.  The LoginToken will no longer be usable for
     * accessing secure object interfaces.  If the session is already logged
     * out, this is treated as a a successful operation.  If the LoginToken
     * is not recognized, this may be because it was logged out earlier and
     * flushed from memory, and so this case will also be treated as successful.
     * @throws AuthenticationRequiredException if the loginToken does not
     * identified a logged-in session
     * @throws SessionAccessException if unable to access the underlying
     * session due to an infrastructure malfunction
     */
    void logout(LoginToken loginToken, short serialVersion)
        throws RemoteException, AuthenticationRequiredException,
               SessionAccessException;
}
