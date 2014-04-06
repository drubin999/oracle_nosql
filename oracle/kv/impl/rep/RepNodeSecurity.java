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

package oracle.kv.impl.rep;

import java.util.logging.Logger;
import javax.security.auth.Subject;

import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.api.RequestDispatcher;
import oracle.kv.impl.api.TopologyManager;
import oracle.kv.impl.rep.login.FailoverSessionManager;
import oracle.kv.impl.rep.login.KVSessionManager;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.AccessChecker;
import oracle.kv.impl.security.AccessCheckerImpl;
import oracle.kv.impl.security.ExecutionContext;
import oracle.kv.impl.security.KVStoreUserPrincipal;
import oracle.kv.impl.security.ProxyCredentials;
import oracle.kv.impl.security.UserVerifier;
import oracle.kv.impl.security.login.InternalLoginManager;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.login.LoginUpdater.GlobalParamsUpdater;
import oracle.kv.impl.security.login.LoginUpdater.ServiceParamsUpdater;
import oracle.kv.impl.security.login.TokenResolverImpl;
import oracle.kv.impl.security.login.TokenVerifier;
import oracle.kv.impl.security.login.TopologyResolver.SNInfo;
import oracle.kv.impl.security.login.TopoTopoResolver;
import oracle.kv.impl.security.login.TopoTopoResolver.TopoMgrTopoHandle;
import oracle.kv.impl.security.login.UserLoginHandler;
import oracle.kv.impl.security.metadata.KVStoreUser;
import oracle.kv.impl.security.metadata.SecurityMetadata;

/**
 * This is the security management portion of the RepNode. It constructs and
 * houses the AccessCheck implementation, etc.
 */
public class RepNodeSecurity implements GlobalParamsUpdater,
                                        ServiceParamsUpdater {

    private final RepNodeService repNodeService;
    private final AccessChecker accessChecker;
    private final TokenResolverImpl tokenResolver;
    private final TopoMgrTopoHandle topoMgrHandle;
    private final TopoTopoResolver topoResolver;
    private final InternalLoginManager loginMgr;
    private final String storeName;
    private final TokenVerifier tokenVerifier;
    private final Logger logger;
    private final UserVerifier userVerifier;
    private KVSessionManager kvSessionManager;

    /**
     * Constructor
     */
    public RepNodeSecurity(RepNodeService rnService, Logger logger) {
        this.repNodeService = rnService;
        this.logger = logger;
        final RepNodeService.Params params = rnService.getParams();
        final SecurityParams secParams = params.getSecurityParams();
        this.storeName = params.getGlobalParams().getKVStoreName();
        this.kvSessionManager = null;

        if (secParams.isSecure()) {
            this.userVerifier = new RepNodeUserVerifier();
            final StorageNodeParams snParams = params.getStorageNodeParams();
            final String hostname = snParams.getHostname();
            final int registryPort = snParams.getRegistryPort();

            this.topoMgrHandle = new TopoMgrTopoHandle(null);
            final SNInfo localSNInfo = new SNInfo(hostname, registryPort,
                                                  rnService.getStorageNodeId());
            this.topoResolver = new TopoTopoResolver(topoMgrHandle,
                                                     localSNInfo, logger);
            this.loginMgr = new InternalLoginManager(topoResolver);
            this.tokenResolver = 
                new TokenResolverImpl(hostname, registryPort, storeName,
                                      topoResolver, loginMgr, logger);

            final RepNodeParams rp = rnService.getRepNodeParams();
            final int tokenCacheCapacity = rp.getLoginCacheSize();

            final GlobalParams gp = rnService.getParams().getGlobalParams();
            final long tokenCacheEntryLifetime =
                gp.getLoginCacheTimeoutUnit().toMillis(
                    gp.getLoginCacheTimeout());
            final TokenVerifier.CacheConfig tokenCacheConfig =
                new TokenVerifier.CacheConfig(tokenCacheCapacity,
                                              tokenCacheEntryLifetime);

            tokenVerifier =
                new TokenVerifier(tokenCacheConfig, tokenResolver);
            this.accessChecker = new AccessCheckerImpl(tokenVerifier, logger);
        } else {
            this.userVerifier = null;
            this.accessChecker = null;
            this.tokenResolver = null;
            this.topoMgrHandle = null;
            this.topoResolver = null;
            this.loginMgr = null;
            this.tokenVerifier = null;
        }
    }

    /**
     * Updates the security configuration to use the request dispatcher.
     * There is an inter-dependency between these two classes, so we delay
     * the use of the dispatcher until both instances are available.
     */
    void setDispatcher(RequestDispatcher dispatcher) {
        if (tokenResolver != null) {
            this.kvSessionManager =
                new KVSessionManager(dispatcher,
                                     repNodeService.getRepNodeParams(),
                                     loginMgr,
                                     storeName, 
                                     FailoverSessionManager.PERSISTENT_PREFIX,
                                     UserLoginHandler.SESSION_ID_RANDOM_BYTES,
                                     userVerifier,
                                     logger);
            tokenResolver.setPersistentResolver(kvSessionManager);
        }
    }

    /**
     * Enable the security functions.  This is dependent on topology startup.
     */
    void startup() {
        if (kvSessionManager != null) {
            kvSessionManager.start();
        }
    }

    /**
     * Stop the use of persistent store by security functions.
     */
    public void stop() {
        if (kvSessionManager != null) {
            kvSessionManager.stop();
        }
    }

    public AccessChecker getAccessChecker() {
        return accessChecker;
    }

    public LoginManager getLoginManager() {
        return loginMgr;
    }

    public KVSessionManager getKVSessionManager() {
        return kvSessionManager;
    }

    public UserVerifier getUserVerifier() {
        return userVerifier;
    }

    void setTopologyManager(TopologyManager topoMgr) {
        if (topoMgrHandle != null) {
            topoMgrHandle.setTopoMgr(topoMgr);
        }
    }

    @Override
    public void newServiceParameters(ParameterMap map) {
        if (tokenVerifier == null) {
            return;
        }
        final RepNodeParams rp = new RepNodeParams(map);
        final int newCapacity = rp.getLoginCacheSize();

        /* Update the loginCacheSize if a new value is specified */
        if (tokenVerifier.updateLoginCacheSize(newCapacity)) {
            logger.info(String.format(
                "RNSecurity: loginCacheSize has been updated to %d",
                newCapacity));
        }
    }

    @Override
    public void newGlobalParameters(ParameterMap map) {
        if (tokenVerifier == null) {
            return;
        }

        final GlobalParams gp = new GlobalParams(map);
        final long newLifeTime =
            gp.getLoginCacheTimeoutUnit().toMillis(gp.getLoginCacheTimeout());

        /* Update the loginCacheTimeout if a new value is specified */
        if (tokenVerifier.updateLoginCacheTimeout(newLifeTime)) {
            logger.info(String.format(
                "RNecurity: loginCacheTimeout has been updated to %d ms",
                newLifeTime));
        }
    }

    private class RepNodeUserVerifier implements UserVerifier {

        /**
         * Verify that the login credentials are valid and return a subject that
         * identifies the user.  If ProxyCredentials are passed, no validation
         * is performed, so the caller must limit use of ProxyCredentials to
         * INTERNAL roles.
         */
        @Override
        public Subject verifyUser(LoginCredentials creds) {

            final SecurityMetadata secMd = repNodeService.getSecurityMetadata();

            if (secMd == null) {
                logger.info(
                    "Unable to verify user credentials with no security " +
                    "metadata available");
                return null;
            }

            final KVStoreUser user = secMd.getUser(creds.getUsername());
            if (user == null) {
                logger.info("User password credentials are not valid");
                return null;
            }

            if (creds instanceof PasswordCredentials) {
                final PasswordCredentials pwCreds = (PasswordCredentials) creds;

                if (!user.verifyPassword(pwCreds.getPassword())) {
                    logger.info("User password credentials are not valid");
                    return null;
                }
            } else if (!(creds instanceof ProxyCredentials)) {
                logger.info(
                    "Encountered unsupported login credentials of type " +
                    creds.getClass());
                return null;
            }

            return user.makeKVSubject();
        }

        /**
         * Verify that the Subject is valid.
         */
        @Override
        public Subject verifyUser(Subject subj) {

            final KVStoreUserPrincipal userPrinc =
                ExecutionContext.getSubjectUserPrincipal(subj);
            if (userPrinc == null) {
                return null;
            }

            final SecurityMetadata secMd = repNodeService.getSecurityMetadata();
            if (secMd == null) {
                logger.info(
                    "Unable to verify user with no security metadata " +
                    "available");
                return null;
            }

            final KVStoreUser user = secMd.getUser(userPrinc.getName());

            if (user == null || !user.isEnabled()) {
                logger.info(
                    "User " + userPrinc.getName() + " is not valid");
                return null;
            }

            return subj;
        }
    }
}
