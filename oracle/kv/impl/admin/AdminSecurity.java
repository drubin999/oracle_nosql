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

package oracle.kv.impl.admin;

import java.util.logging.Logger;

import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.AccessChecker;
import oracle.kv.impl.security.AccessCheckerImpl;
import oracle.kv.impl.security.login.InternalLoginManager;
import oracle.kv.impl.security.login.LoginUpdater.GlobalParamsUpdater;
import oracle.kv.impl.security.login.LoginUpdater.ServiceParamsUpdater;
import oracle.kv.impl.security.login.ParamTopoResolver;
import oracle.kv.impl.security.login.ParamTopoResolver.ParamsHandle;
import oracle.kv.impl.security.login.TokenResolverImpl;
import oracle.kv.impl.security.login.TokenVerifier;

/**
 * This is the security management portion of the Admin. It constructs and
 * houses the AccessCheck implementation, etc.
 */
public class AdminSecurity implements GlobalParamsUpdater,
                                      ServiceParamsUpdater {

    private final AdminService adminService;
    private final AccessCheckerImpl accessChecker;
    private final TokenResolverImpl tokenResolver;
    private final AdminParamsHandle paramsHandle;
    private final ParamTopoResolver topoResolver;
    private final TokenVerifier tokenVerifier;
    private Logger logger;
    /* not final because it can change when configured */
    private InternalLoginManager loginMgr;

    /**
     * Constructor
     */
    public AdminSecurity(AdminService adminService, Logger logger) {

        this.logger = logger;
        this.adminService = adminService;
        final AdminServiceParams params = adminService.getParams();
        final SecurityParams secParams = params.getSecurityParams();
        final String storeName = params.getGlobalParams().getKVStoreName();

        if (secParams.isSecure()) {
            final StorageNodeParams snParams = params.getStorageNodeParams();
            final String hostname = snParams.getHostname();
            final int registryPort = snParams.getRegistryPort();

            this.paramsHandle = new AdminParamsHandle();
            this.topoResolver = new ParamTopoResolver(paramsHandle, logger);
            this.loginMgr = new InternalLoginManager(topoResolver);
            this.tokenResolver = new TokenResolverImpl(hostname, registryPort,
                                                       storeName, topoResolver,
                                                       loginMgr, logger);

            final AdminParams ap = params.getAdminParams();
            final int tokenCacheCapacity = ap.getLoginCacheSize();

            final GlobalParams gp = params.getGlobalParams();
            final long tokenCacheEntryLifetime =
                gp.getLoginCacheTimeoutUnit().toMillis(
                    gp.getLoginCacheTimeout());
            final TokenVerifier.CacheConfig  tokenCacheConfig =
                new TokenVerifier.CacheConfig(tokenCacheCapacity,
                                              tokenCacheEntryLifetime);
            this.tokenVerifier =
                new TokenVerifier(tokenCacheConfig, tokenResolver);
            this.accessChecker = new AccessCheckerImpl(tokenVerifier, logger);
        } else {
            paramsHandle = null;
            topoResolver = null;
            tokenResolver = null;
            accessChecker = null;
            loginMgr = null;
            tokenVerifier = null;
        }
    }

    /**
     * For access by AdminService when a configure() operation is performed
     */
    void configure(String storeName) {
        if (loginMgr == null) {
            return;
        }
        loginMgr.logout();
        loginMgr = new InternalLoginManager(topoResolver);
        logger = adminService.getLogger();
        topoResolver.setLogger(logger);
        tokenResolver.setLogger(logger);
        tokenResolver.setStoreName(storeName);
        accessChecker.setLogger(logger);
    }

    public AccessChecker getAccessChecker() {
        return accessChecker;
    }

    public InternalLoginManager getLoginManager() {
        return loginMgr;
    }

    private class AdminParamsHandle implements ParamsHandle {
        @Override
        public Parameters getParameters() {

            Admin admin = adminService.getAdmin();
            if (admin == null) {
                return null;
            }

            return admin.getCurrentParameters();
        }
    }

    @Override
    public void newServiceParameters(ParameterMap map) {
        if (tokenVerifier == null) {
            return;
        }
        final AdminParams ap = new AdminParams(map);
        final int newCapacity = ap.getLoginCacheSize();

        /* Update the loginCacheSize if a new value is specified */
        if (tokenVerifier.updateLoginCacheSize(newCapacity)) {
            logger.info(String.format(
                "AdminSecurity: loginCacheSize has been updated to %d",
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
                "AdminSecurity: loginCacheTimeout has been updated to %d ms",
                newLifeTime));
        }
    }
}
