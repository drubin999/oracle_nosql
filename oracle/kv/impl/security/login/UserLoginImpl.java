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
import java.rmi.server.RemoteServer;
import java.rmi.server.ServerNotActiveException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.security.auth.Subject;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.AuthenticationRequiredException;
import oracle.kv.KVSecurityException;
import oracle.kv.LoginCredentials;
import oracle.kv.impl.fault.ClientAccessException;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.KVStoreRole;
import oracle.kv.impl.security.ProxyCredentials;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.security.annotations.PublicMethod;
import oracle.kv.impl.security.annotations.SecureAPI;
import oracle.kv.impl.security.annotations.SecureAutoMethod;
import oracle.kv.impl.util.registry.VersionedRemoteImpl;


/**
 * An implementation of the UserLogin interface.
 */

@SecureAPI
public class UserLoginImpl extends VersionedRemoteImpl implements UserLogin {

    /* The object that manages user logins */
    private final UserLoginHandler loginHandler;

    /* The fault handler for this process */
    private final ProcessFaultHandler faultHandler;

    private final Logger logger;

    public UserLoginImpl(ProcessFaultHandler faultHandler,
                         UserLoginHandler loginHandler,
                         Logger logger) {

        this.loginHandler = loginHandler;
        this.faultHandler = faultHandler;
        this.logger = logger;
    }

    /**
     * Log a user into the database.
     * @see UserLogin#login
     */
    @Override
    @PublicMethod
    public LoginResult login(final LoginCredentials creds,
                             final short serialVersion)
        throws AuthenticationFailureException, RemoteException {

        return faultHandler.execute(
            new ProcessFaultHandler.SimpleOperation<LoginResult>() {

                @Override
                public LoginResult execute() {

                    try {
                        return loginHandler.login(creds, getClientHost());
                    } catch (KVSecurityException kvse) {
                        throw new ClientAccessException(kvse);
                    }
                }
            });
     }

    /**
     * Log another user into the database.
     * @see UserLogin#login
     */
    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public LoginResult proxyLogin(final ProxyCredentials creds,
                                  final AuthContext authContext,
                                  final short serialVersion)
        throws AuthenticationFailureException, SessionAccessException,
               RemoteException {

        return faultHandler.execute(
            new ProcessFaultHandler.SimpleOperation<LoginResult>() {

                @Override
                public LoginResult execute() {
                    try {
                        return loginHandler.proxyLogin(
                            creds, authContext.getClientHost());
                    } catch (KVSecurityException kvse) {
                        throw new ClientAccessException(kvse);
                    }
                }
            });
     }

    /**
     * Request that a login token be replaced with a new token that has a later
     * expiration.
     * @see UserLogin#requestSessionExtension
     */
    @Override
    @PublicMethod
    public LoginToken requestSessionExtension(final LoginToken loginToken,
                                              final short serialVersion)
        throws SessionAccessException, RemoteException {

        return faultHandler.execute(
            new ProcessFaultHandler.SimpleOperation<LoginToken>() {

                @Override
                public LoginToken execute() {
                    try {
                        return loginHandler.requestSessionExtension(loginToken);
                    } catch (KVSecurityException kvse) {
                        throw new ClientAccessException(kvse);
                    } catch (SessionAccessException sae) {
                        throw sae;
                    }
                }
            });
    }

    /**
     * Check an existing LoginToken for validity.
     * @see UserLogin#validateLoginToken
     */
    @Override
    @SecureAutoMethod(roles = { KVStoreRole.INTERNAL })
    public Subject validateLoginToken(final LoginToken loginToken,
                                      final AuthContext authCtx,
                                      final short serialVersion)
        throws SessionAccessException, RemoteException {

        return faultHandler.execute(
            new ProcessFaultHandler.SimpleOperation<Subject>() {

                @Override
                public Subject execute() {
                    try {
                        return loginHandler.validateLoginToken(loginToken);
                    } catch (KVSecurityException kvse) {
                        throw new ClientAccessException(kvse);
                    }
                }
            });
    }

    /**
     * Log out the login token.
     * @see UserLogin#logout
     */
    @Override
    @PublicMethod
    public void logout(final LoginToken loginToken, final short serialVersion)
        throws AuthenticationRequiredException, SessionAccessException,
               RemoteException {

        faultHandler.execute(
            new ProcessFaultHandler.SimpleProcedure() {

                @Override
                public void execute() {
                    try {
                        loginHandler.logout(loginToken);
                    } catch (KVSecurityException kvse) {
                        throw new ClientAccessException(kvse);
                    } catch (SessionAccessException sae) {
                        throw sae;
                    }
                }
            });
    }

    private String getClientHost() {
        try {
            return RemoteServer.getClientHost();
        } catch (ServerNotActiveException snae) {
            logger.log(Level.SEVERE,
                       "RemoteServer.getClientHost failed: ({0})",
                       snae.getMessage());
            return null;
        }
    }
}
