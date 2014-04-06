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

import java.rmi.AccessException;
import java.rmi.ConnectIOException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.Arrays;
import java.util.List;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.AuthenticationRequiredException;
import oracle.kv.FaultException;
import oracle.kv.KVStoreException;
import oracle.kv.LoginCredentials;
import oracle.kv.impl.api.TopologyManager;
import oracle.kv.impl.fault.EnvironmentTimeoutException;
import oracle.kv.impl.fault.InternalFaultException;
import oracle.kv.impl.security.ClientProxyCredentials;
import oracle.kv.impl.security.ProxyCredentials;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.ResourceId.ResourceType;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.HostPort;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * This is an implementation of a UserLoginManager that is appropriate to use
 * when communicating with a RepNode, which is the primary client use case.
 *
 * TODO: consider handling the case that we are unable to fully logout due
 * to no ability to write for persistent tokens.
 */
public class RepNodeLoginManager extends UserLoginManager {

    private TopologyManager topoManager;

    /**
     * Creates a basic instance.  Prior to using the login manager it must
     * be bootstrapped, using a call to the bootstrap() method, and typically
     * followed by a call to setTopology().
     */
    public RepNodeLoginManager(String username, boolean autoRenew) {
        super(username, autoRenew);
    }

    /**
     * The bootstap method must be called to perform the initial login
     * operation before we have Topology information.  This will set up a
     * login token that can be used to get the Topology information.  Once
     * Topology acquisition is done, the owner should call the
     * setTopology method on this class.
     *
     * @param expectedStoreName if not null, specifies the name of the kvstore
     *   instance that we are looking for
     * @throws KVStoreException if no registry host could be found that
     *   supports the login or if an error occured while attempting the
     *   login.
     * @throws AuthenticationFailureException if the login credentials
     *   are not accepted
     */
    public void bootstrap(String[] registryHostPorts,
                          LoginCredentials loginCreds,
                          String expectedStoreName)
        throws KVStoreException, AuthenticationFailureException  {

        /*
         * The structure of this method was lifted from the
         * TopologyLocator.getInitialTopology() method. It would be nice
         * to share code, but there are differences that make factoring
         * tricky.
         */

        Exception cause = null;

        /*
         * If we run across a kvStore of the wrong name, we refuse to talk
         * to it.  Make a note if we do see one that is wrong.
         */
        String wrongStoreName = null;

        final HostPort[] hostPorts = HostPort.parse(registryHostPorts);

        for (final HostPort hostPort : hostPorts) {

            final String registryHostname = hostPort.hostname();
            final int registryPort = hostPort.port();

            try {
                final Registry snRegistry =
                    RegistryUtils.getRegistry(registryHostname, registryPort,
                                              expectedStoreName);

                for (String serviceName : snRegistry.list()) {

                    try {

                        /*
                         * Skip things that don't look like RepNodes (this is
                         * for the client).
                         */
                        final String svcStoreName =
                            RegistryUtils.isRepNodeLogin(serviceName);

                        if (svcStoreName != null) {
                            if (expectedStoreName != null &&
                                !expectedStoreName.equals(svcStoreName)) {

                                wrongStoreName = svcStoreName;
                                continue;
                            }
                            final Remote stub = snRegistry.lookup(serviceName);
                            if (stub instanceof UserLogin) {

                                final HostPort loginTarget =
                                    new HostPort(registryHostname,
                                                 registryPort);

                                if (bootstrapLogin((UserLogin) stub,
                                                   loginCreds,
                                                   loginTarget)) {
                                    break;
                                }
                            }
                        }
                    } catch (SessionAccessException e) {
                        cause = e;
                    } catch (AccessException e) {
                        cause = e;
                    } catch (ConnectIOException e) {
                        cause = e;
                    } catch (NotBoundException e) {
                        /*
                         * Should not happen since we are iterating over a
                         * bound list.
                         */
                        cause = e;
                    } catch (InternalFaultException e) {
                        /*
                         * Be robust even in the presence of internal faults.
                         * Keep trying with other functioning nodes.
                         */
                        if (cause == null) {
                            /*
                             * Preserve non-fault exception as the reason
                             * if one's already present.
                             */
                            cause = e;
                        }
                    } catch (FaultException fe) {
                        /*
                         * Be robust in the presence of environment timeout
                         * cases. Keep trying with other functioning nodes.
                         */
                        if (fe.getFaultClassName().equals(
                                EnvironmentTimeoutException.class.getName())) {
                            if (cause == null) {
                                /*
                                 * Preserve non-fault exception as the reason
                                 * if one's already present.
                                 */
                                cause = fe;
                            }
                        } else {
                            throw fe;
                        }
                    }
                }

                /*
                 * Once we have an initial login handle, we can stop
                 * visiting the helper hosts.
                 */
                if (getLoginHandle() != null) {
                    break;
                }

            } catch (RemoteException e) {
                cause = e;
            }
        }

        /*
         * Hopefully we were able to get a login handle.  If not, it could be
         * because there were no RepNodes active at any of the hosts, or
         * because none of the RepNodes is running a LoginManager.  If the
         * latter is true, it's probably the case that the SN is not secure,
         * and the credentials are pointless.  We might want to indicate that
         * somehow, but we'll need to signal that later.  If the former is
         * true, or if we are somehow in a broken state where no LoginManager
         * is running yet and security is enabled, an
         * AuthenticationRequiredRequiredException will be signaled later.
         */

        if (getLoginHandle() == null) {
            if (wrongStoreName != null) {
                throw new KVStoreException(
                    "Could not establish an initial login from: " +
                    Arrays.toString(registryHostPorts) +
                    " - ignored non-matching store name " + wrongStoreName,
                    cause);
            }
            throw new KVStoreException(
                "Could not establish an initial login from: " +
                Arrays.toString(registryHostPorts), cause);
        }
    }

    /**
     * Register the TopologyManager to allow proper availability to
     * provided.
     */
    public void setTopology(TopologyManager topoMgr) {
        this.topoManager = topoMgr;
        final LoginHandle currentHandle = getLoginHandle();
        if (currentHandle == null) {
            /* Presumably there will be a login attempt to follow */
            return;
        }

        /* Replace the current login handle with a new one */
        final LoginHandle liveLoginHandle =
            new LiveRNLoginHandle(currentHandle.getLoginToken());
        init(liveLoginHandle);
    }

    /**
     * Re-login using the supplied credentials.
     * A topology must be in effect.
     * @throws AuthenticationRequiredException if no node could be contacted
     *    that was capable of processing the login request
     * @throws AuthenticationFailureException if the credentials are invalid
     */
    public synchronized void login(LoginCredentials creds)
        throws AuthenticationRequiredException,
               AuthenticationFailureException {

        if (topoManager == null) {
            throw new IllegalStateException("Not properly initialized");
        }

        Exception cause = null;

        final Topology topo = topoManager.getLocalTopology();
        final RegistryUtils ru = new RegistryUtils(topo, (LoginManager) null);
        for (final RepGroup rg : topo.getRepGroupMap().getAll()) {
            for (RepNode rn : rg.getRepNodes()) {

                try {
                    final UserLoginAPI rnLogin =
                        ru.getRepNodeLogin(rn.getResourceId());
                    final LoginResult result = rnLogin.login(creds);
                    if (result.getLoginToken() != null) {
                        final LoginHandle loginHandle =
                            new LiveRNLoginHandle(result.getLoginToken());
                        init(loginHandle);
                        return;
                    }
                } catch (AuthenticationFailureException e) {
                    /* Only one chance on this */
                    throw e;
                } catch (AccessException e) {
                    cause = e;
                } catch (NotBoundException e) {
                    /*
                     * Should not happen since we are iterating over a
                     * bound list.
                     */
                    cause = e;
                } catch (InternalFaultException e) {
                    /*
                     * Be robust even in the presence of internal faults.
                     * Keep trying with other functioning nodes.
                     */
                    if (cause == null) {
                        /*
                         * Preserve non-fault exception as the reason
                         * if one's already present.
                         */
                        cause = e;
                    }
                } catch (FaultException fe) {
                    /*
                     * Be robust in the presence of environment timeout cases.
                     * Keep trying with other functioning nodes.
                     */
                    if (fe.getFaultClassName().equals(
                        EnvironmentTimeoutException.class.getName())) {
                        if (cause == null) {
                            /*
                             * Preserve non-fault exception as the reason
                             * if one's already present.
                             */
                            cause = fe;
                        }
                    } else {
                        throw fe;
                    }
                } catch (RemoteException re) {
                    if (cause == null) {
                        /*
                         * Preserve non-fault exception as the reason
                         * if one's already present.
                         */
                        cause = re;
                    }
                }
            }
        }

        throw new AuthenticationRequiredException(cause,
                                                  false /* isReturnSignal */);
    }

    /**
     * Attempts to log in to a RepNode.
     *
     * @param userLogin an RMI stub interface to a UserLogin
     * @param loginCreds proxy login credentials containing the user
     * identity to log in as.
     * @param loginTarget The SNA trusted login identity to which we are
     * attempting the login.
     * @return the login login result
     * @throws RemoteException
     * @throws AuthenticationFailureException
     */
    private boolean bootstrapLogin(UserLogin userLogin,
                                   LoginCredentials loginCreds,
                                   HostPort loginTarget)
        throws RemoteException, AuthenticationFailureException,
               SessionAccessException {

        final UserLoginAPI ulAPI = UserLoginAPI.wrap(userLogin);
        final LoginResult loginResult;

        if (loginCreds instanceof ClientProxyCredentials) {

            /*
             * A KVStore internal component is logging into the store on
             * behalf of a previously authenticated user. Get a login on
             * their behalf using the proxyLogin method.
             */
            loginResult =
                proxyBootstrapLogin(userLogin,
                                    (ClientProxyCredentials) loginCreds,
                                    loginTarget);
        } else {
            loginResult = ulAPI.login(loginCreds);
        }

        if (loginResult.getLoginToken() != null) {
            final LoginHandle loginHandle =
                new BSRNLoginHandle(loginResult.getLoginToken(), ulAPI);
            init(loginHandle);
            return true;
        }

        return false;
    }

    /**
     * Attempts a proxyLogin to the specified login interface.
     *
     * @param userLogin an RMI stub interface to a UserLogin
     * @param loginCreds proxy login credentials containing the user
     * identity to log in as and a LoginManager that authenticates us
     * as a KVStore internal entity.
     * @param loginTarget The SNA trusted login identity to which we are
     * attempting the login.
     * @return the login login result
     * @throws RemoteException
     */
    private LoginResult proxyBootstrapLogin(UserLogin userLogin,
                                            ClientProxyCredentials loginCreds,
                                            HostPort loginTarget)
        throws RemoteException, SessionAccessException {

        final UserLoginAPI localAPI =
            UserLoginAPI.wrap(userLogin,
                              loginCreds.getInternalManager().
                              getHandle(loginTarget, ResourceType.REP_NODE));

        return localAPI.proxyLogin(
            new ProxyCredentials(loginCreds.getUser()));
    }

    /**
     * Implementation of LoginHandle for RepNode login after bootstrap has
     * been completed and we have a topology.
     */
    class LiveRNLoginHandle extends AbstractUserLoginHandle {

        public LiveRNLoginHandle(LoginToken loginToken) {
            super(loginToken);
        }

        /**
         * Get a UserLoginAPI appropriate for the current LoginToken.
         */
        @Override
        protected UserLoginAPI getLoginAPI()
            throws RemoteException {

            final LoginToken token = getLoginToken();
            if (token == null) {
                return null;
            }
            final SessionId sessId = token.getSessionId();
            final String storename =
                topoManager.getTopology().getKVStoreName();
            final Topology topo = topoManager.getTopology();

            if (sessId.getIdValueScope() == SessionId.IdScope.PERSISTENT) {
                final List<RepNode> repNodes = topo.getSortedRepNodes();

                /*
                 * Very simple logic for now. Later, think about datacenter
                 * choice, shards, retries, etc.
                 */
                RemoteException toThrow = null;
                for (RepNode rn : repNodes) {
                    final StorageNodeId snid = rn.getStorageNodeId();
                    final StorageNode sn = topo.get(snid);
                    try {
                        return
                            RegistryUtils.getRepNodeLogin(
                                storename, sn.getHostname(),
                                sn.getRegistryPort(), rn.getResourceId(),
                                (LoginManager) null);
                    } catch (RemoteException re) {
                        if (toThrow == null) {
                            toThrow = re;
                        }
                    } catch (NotBoundException nbe) /* CHECKSTYLE:OFF */ {
                    } /* CHECKSTYLE:ON */
                }

                if (toThrow != null) {
                    throw toThrow;
                }

                throw new RemoteException("No RepNode available");
            }

            /* Non-persistent case */
            final ResourceId rid = sessId.getAllocator();
            if (!(rid instanceof RepNodeId)) {
                throw new IllegalStateException("Expected a RepNodeId");
            }

            final RepNodeId rnid = (RepNodeId) rid;
            final RepGroup rg = topo.get(new RepGroupId(rnid.getGroupId()));
            final RepNode rn = rg.get(rnid);
            if (rn == null) {
                throw new IllegalStateException(
                    "Missing RepNode with id " + rnid + " in topology");
            }
            final StorageNodeId snid = rn.getStorageNodeId();
            final StorageNode sn = topo.get(snid);
            try {
                return
                    RegistryUtils.getRepNodeLogin(
                        storename, sn.getHostname(),
                        sn.getRegistryPort(), rn.getResourceId(),
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
            return (rtype.equals(ResourceType.REP_NODE) ||
                    rtype.equals(ResourceType.ADMIN));
        }
    }

    /**
     * The RepNodeLoginManager "Bootstrap" login handle. We use this upon the
     * initial login, prior to having Topology available.  It isn't very
     * durable vis. reconnections, but isn't expected to be long-lived.
     */
    static class BSRNLoginHandle extends AbstractUserLoginHandle {
        private UserLoginAPI loginAPI;

        public BSRNLoginHandle(LoginToken loginToken, UserLoginAPI loginAPI) {
            super(loginToken);
            this.loginAPI = loginAPI;
        }

        /**
         * Get a UserLoginAPI appropriate for the current LoginToken.
         */
        @Override
        protected UserLoginAPI getLoginAPI()
            throws RemoteException {

            return loginAPI;
        }

        /**
         * Report whether this login handle supports authentication to the
         * specified type of resource.
         */
        @Override
        public boolean isUsable(ResourceType rtype) {
            return (rtype.equals(ResourceType.REP_NODE) ||
                    rtype.equals(ResourceType.ADMIN));
        }
    }
}
