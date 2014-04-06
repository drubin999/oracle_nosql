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

import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.LoginCredentials;
import oracle.kv.impl.topo.ResourceId.ResourceType;
import oracle.kv.impl.util.HostPort;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * AdminLoginManager is an implementation of LoginManager that is intended to
 * manage a login to a KVStore Admin.  Much of the implementation is shared
 * with RepNodeLoginManager through the common UserLoginManager base class.
 */

public class AdminLoginManager extends UserLoginManager {

    /**
     * Creates an instance with no login handle.  Before using it, it should
     * be inititalized.  This can be done with a call to initialize or with
     * a call to bootstrap.
     */
    public AdminLoginManager(String username, boolean autoRenew) {
        super(username, autoRenew);
    }

    /**
     * Initializes the login manager for use.
     *
     * @param token a LoginToken for use in accessing the admin interface
     * @param registryHost the host from which the token originated
     * @param registryPort the port from which the token originated
     */
    public void initialize(LoginToken token,
                           String registryHost,
                           int registryPort) {
        init(new AdminLoginHandle(token, registryHost, registryPort));
    }

    /**
     * Performs the initial login.
     *
     * @return true if we were able to authenticate to the admin, or false
     * if a communication problem prevented it.
     * @throws AuthenticationFailureException if the login credentials are
     * not valid
     */
    public boolean bootstrap(String registryHost,
                             int registryPort,
                             LoginCredentials loginCreds)
        throws AuthenticationFailureException {

        return bootstrap(new String[] { registryHost + ":" + registryPort },
                         loginCreds);
    }

    /**
     * Performs the initial login.
     *
     * @return true if we were able to authenticate to the admin, or false
     * if a communication problem prevented it.
     * @throws AuthenticationFailureException if the login credentials are
     * not valid
     * @throws IllegalArgumentException if the registryHostPorts array is
     * null or if any of the contained Strings are null, or do not contain
     * a valid host:port string.
     */
    public boolean bootstrap(String[] registryHostPorts,
                             LoginCredentials loginCreds)
        throws AuthenticationFailureException {

        final HostPort[] hostPorts = HostPort.parse(registryHostPorts);

        for (HostPort hostPort : hostPorts) {

            final String registryHost = hostPort.hostname();
            final int registryPort = hostPort.port();

            try {
                final UserLoginAPI loginAPI =
                    RegistryUtils.getAdminLogin(registryHost, registryPort,
                                                (LoginManager) null);

                final LoginResult login = loginAPI.login(loginCreds);
                if (login.getLoginToken() == null) {
                    continue;
                }

                final LoginHandle loginHandle =
                    new AdminLoginHandle(login.getLoginToken(),
                                         registryHost, registryPort);
                init(loginHandle);
                return true;
            } catch (NotBoundException nbe) {
                /* Try the next option, if any */
                continue;
            } catch (RemoteException re) {
                /* Try the next option, if any */
                continue;
            }
        }

        return false;
    }

    /**
     * AdminLoginHandle is intended specifically for use within the
     * AdminLoginManager class.
     */
    private static final class AdminLoginHandle
        extends AbstractUserLoginHandle {

        private final String hostname;
        private final int registryPort;

        private AdminLoginHandle(LoginToken loginToken,
                                 String hostname,
                                 int registryPort) {
            super(loginToken);
            this.hostname = hostname;
            this.registryPort = registryPort;
        }

        @Override
        protected UserLoginAPI getLoginAPI()
            throws RemoteException {

            try {
                return RegistryUtils.getAdminLogin(hostname, registryPort,
                                                   (LoginManager) null);
            } catch (NotBoundException nbe) {
                throw new RemoteException(
                    "login interface not bound", nbe);
            }
        }

        /**
         * Report whether this login handle supports authentication to the
         * specified type of resource.
         */
        @Override
        public boolean isUsable(ResourceType rtype) {
            return rtype.equals(ResourceType.ADMIN);
        }
    }
}
