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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import oracle.kv.AuthenticationRequiredException;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.security.login.TopologyResolver.SNInfo;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.ResourceId.ResourceType;
import oracle.kv.impl.util.HostPort;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * LoginManager implementation for KVStore internals.  The implementation of
 * InternalLoginManager differs from UserLoginManager-derived classes in that
 * the InternalLoginManager tracks login handles by StorageNode address, rather
 * than having a single login handle that is used for all nodes.
 */
public class InternalLoginManager implements LoginManager {

    /* A map of SN address to LoginHandle */
    private final ConcurrentHashMap<HostPort, LoginHandle> loginMap;

    /*
     * A possibly null topology resolver.  When null, logins by resource id
     * are not allowed.
     */
    private final TopologyResolver topoResolver;

    /**
     * Creates a login manager for use by SN components. It is initialized
     * with an empty set of logins.
     *
     * @param topoResolver an instance of TopologyResolver that is used to
     * identify the StorageNode that houses a resource id. This is may be
     * null if no calls to get a login based on ResourceId will be made.
     */
    public InternalLoginManager(TopologyResolver topoResolver) {
        this.loginMap = new ConcurrentHashMap<HostPort, LoginHandle>();
        this.topoResolver = topoResolver;
    }

    /**
     * Returns the login associated with the login manager.  For an internal
     * login manager, there is no username.
     */
    @Override
    public String getUsername() {
        return null;
    }

    /**
     * Get a login appropriate for the specified target.
     */
    @Override
    public LoginHandle getHandle(HostPort target,
                                 ResourceType rtype) {

        /* See if we already have a LoginToken for the target in our map */
        LoginHandle loginHandle = loginMap.get(target);
        if (loginHandle != null) {
            return loginHandle;
        }

        /* Not yet in our map - create an entry */
        loginHandle = new InternalLoginHandle(null, /* login token */
                                              target.hostname(),
                                              target.port());
        try {
            loginHandle.renewToken(null);
        } catch (SessionAccessException sae) {
            /* Probably not helpful, but caller will be notified on access */
            return loginHandle;
        }

        final LoginHandle prevHandle =
            loginMap.putIfAbsent(target, loginHandle);

        /*
         * If someone else already got that info, discard our
         * information and use what is already there.
         */
        if (prevHandle == null) {
            return loginHandle;
        }

        try {
            loginHandle.logoutToken();
        } catch (SessionAccessException sae) /* CHECKSTYLE:OFF */{
            /* Unlikely, but if it occurs, we'll ignore this */
        } /* CHECKSTYLE:ON */
        return prevHandle;
    }

    /**
     * Get a login appropriate for the specified target resource.
     */
    @Override
    public LoginHandle getHandle(ResourceId target) {

        if (topoResolver == null) {
            throw new UnsupportedOperationException(
                "Cannot get internal login via ResourceId without a " +
                "topology resolver");
        }

        final SNInfo sn = topoResolver.getStorageNode(target);
        if (sn == null) {
            return null;
        }

        final HostPort snTarget =
            new HostPort(sn.getHostname(), sn.getRegistryPort());

        return getHandle(snTarget, target.getType());
    }

    /**
     * Log out all entries.
     */
    @Override
    public void logout() {
        final Set<Map.Entry<HostPort, LoginHandle>> logins =
            loginMap.entrySet();

        /*
         * As a future enhancement, consider implementing this with an
         * Executor service.
         */
        for (Map.Entry<HostPort, LoginHandle> tokenEntry : logins) {
            try {
                logout(tokenEntry.getKey(), tokenEntry.getValue());
            } catch (SessionAccessException sae) /* CHECKSTYLE:OFF */ {
                /* ignore */
            } /* CHECKSTYLE:ON */
        }
    }

    /**
     * Attempt to logout the specified info at the specified target.
     * return true if we were successful.
     */
    private boolean logout(HostPort target, LoginHandle loginHandle)
        throws SessionAccessException {

        if (loginHandle == null) {
            return true;
        }

        try {
            loginHandle.logoutToken();
            loginMap.remove(target, loginHandle);
            return true;

        } catch (AuthenticationRequiredException are) {
            /* swallow this - we're already logged out */
            return true;
        }
    }

    /**
     * LoginHandle implementation for internals.  Unlike user-based login
     * handles, internal login handles are able to "renew" a login into
     * existence as the credentials are provided by the SSL implementation.
     */
    private class InternalLoginHandle extends LoginHandle {
        private final String hostname;
        private final int registryPort;

        public InternalLoginHandle(LoginToken loginToken,
                                   String hostname,
                                   int registryPort) {

            super(loginToken);
            this.hostname = hostname;
            this.registryPort = registryPort;
        }

        /**
         * @see LoginHandle#renewToken
         */
        @Override
        public LoginToken renewToken(LoginToken prevToken)
            throws SessionAccessException {

            final LoginToken currToken = getLoginToken();
            if (prevToken != currToken) {
                return currToken;
            }
            try {
                final TrustedLoginAPI tlAPI = getLoginAPI();
                final LoginResult result = tlAPI.loginInternal();

                if (result.getLoginToken() == null) {
                    throw new RemoteException("Unable to get login access");
                }

                final LoginToken logoutToken =
                    updateLoginToken(currToken, result.getLoginToken()) ?
                    currToken : result.getLoginToken();

                if (logoutToken != null) {
                    try {
                        tlAPI.logout(logoutToken);
                    } catch (AuthenticationRequiredException are)
                        /* CHECKSTYLE:OFF */{
                        /* Already logged out */
                    } /* CHECKSTYLE:ON */
                }

                return getLoginToken();
            } catch (RemoteException re) {
                throw new SessionAccessException(re,
                                                 false /* isReturnSignal */);
            }
        }

        @Override
        public void logoutToken()
            throws SessionAccessException {
            logoutToken(true);
        }

        private void logoutToken(boolean retry)
            throws SessionAccessException {

            final LoginToken logoutToken = getLoginToken();
            if (logoutToken != null) {
                try {
                    getLoginAPI().logout(logoutToken);
                } catch (AuthenticationRequiredException are)
                    /* CHECKSTYLE:OFF */ {
                    /* Already logged out - ignore this */
                } /* CHECKSTYLE:ON */ catch (RemoteException re) {
                    /* retry once */
                    if (retry) {
                        logoutToken(false);
                    }
                }
            }
        }

        private TrustedLoginAPI getLoginAPI()
            throws RemoteException {

            try {
                return RegistryUtils.getStorageNodeAgentLogin(hostname,
                                                              registryPort);
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
            return (rtype.equals(ResourceType.REP_NODE) ||
                    rtype.equals(ResourceType.ADMIN) ||
                    rtype.equals(ResourceType.STORAGE_NODE));
        }
    }
}
