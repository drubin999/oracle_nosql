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

package oracle.kv.impl.sna;

import java.util.List;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.AccessChecker;
import oracle.kv.impl.security.AccessCheckerImpl;
import oracle.kv.impl.security.login.InternalLoginManager;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.login.LoginUpdater.GlobalParamsUpdater;
import oracle.kv.impl.security.login.LoginUpdater.ServiceParamsUpdater;
import oracle.kv.impl.security.login.TokenResolverImpl;
import oracle.kv.impl.security.login.TokenVerifier;
import oracle.kv.impl.security.login.TopologyResolver;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;

/**
 * This is the security management portion of the SNA. It constructs and
 * houses the AccessCheck implementation, etc.
 */
public class SNASecurity implements GlobalParamsUpdater,
                                    ServiceParamsUpdater {

    @SuppressWarnings("unused")
    private final StorageNodeAgent sna;
    private final AccessChecker accessChecker;
    private final TokenResolverImpl tokenResolver;
    private final InternalLoginManager loginMgr;
    private final TokenVerifier tokenVerifier;
    private final Logger logger;

    /**
     * Creates the security repository for the SNA.
     */
    public SNASecurity(StorageNodeAgent sna,
                       BootstrapParams bp,
                       SecurityParams sp,
                       GlobalParams gp,
                       StorageNodeParams snp,
                       Logger logger) {

        this.sna = sna;
        this.logger = logger;

        if (sp.isSecure()) {
            final String hostname = bp.getHostname();
            final int registryPort = bp.getRegistryPort();

            final String storeName = bp.getStoreName();
            final StorageNodeId snid =
                (storeName == null) ? null : new StorageNodeId(bp.getId());

            final TopologyResolver topoResolver =
                (storeName == null) ? null :
                new SNATopoResolver(
                    new TopologyResolver.SNInfo(hostname, registryPort, snid));

            this.loginMgr = new InternalLoginManager(null);
            this.tokenResolver =
                new TokenResolverImpl(hostname, registryPort, storeName,
                                      topoResolver, loginMgr, logger);

            final StorageNodeParams newSNp =
                (snp == null) ?
                new StorageNodeParams(sna.getHostname(), sna.getRegistryPort(),
                                      null) :
                snp;
            final int tokenCacheCapacity = newSNp.getLoginCacheSize();

            final GlobalParams newGp =
                (gp == null) ? new GlobalParams(sna.getStoreName()) : gp;
            final long tokenCacheEntryLifetime =
                newGp.getLoginCacheTimeoutUnit().toMillis(
                    newGp.getLoginCacheTimeout());
            final TokenVerifier.CacheConfig  tokenCacheConfig =
                new TokenVerifier.CacheConfig(tokenCacheCapacity,
                                              tokenCacheEntryLifetime);


            tokenVerifier = new TokenVerifier(tokenCacheConfig, tokenResolver);

            this.accessChecker = new AccessCheckerImpl(tokenVerifier, logger);
        } else {
            tokenResolver = null;
            accessChecker = null;
            loginMgr = null;
            tokenVerifier = null;
        }
    }

    public AccessChecker getAccessChecker() {
        return accessChecker;
    }

    public LoginManager getLoginManager() {
        return loginMgr;
    }

    @Override
    public void newServiceParameters(ParameterMap map) {
        if (tokenVerifier == null) {
            return;
        }
        final StorageNodeParams snp = new StorageNodeParams(map);
        final int newCapacity = snp.getLoginCacheSize();

        /* Update the loginCacheSize if a new value is specified */
        if (tokenVerifier.updateLoginCacheSize(newCapacity)) {
            logger.info(String.format(
                "SNASecurity: loginCacheSize has been updated to %d",
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
                "SNASecurity: loginCacheTimeout has been updated to %d ms",
                newLifeTime));
        }
    }

    private final class SNATopoResolver implements TopologyResolver {

        final SNInfo localSNInfo;

        private SNATopoResolver(SNInfo snInfo) {
            this.localSNInfo = snInfo;
        }

        private SNATopoResolver() {
            localSNInfo = null;
        }

        @Override
        public SNInfo getStorageNode(ResourceId rid) {
            if (localSNInfo != null &&
                rid instanceof StorageNodeId &&
                ((StorageNodeId) rid).getStorageNodeId() ==
                localSNInfo.getStorageNodeId().getStorageNodeId()) {

                return localSNInfo;
            }

            return  null;
        }

        @Override
        public List<RepNodeId> listRepNodeIds(
            @SuppressWarnings("unused") int maxRNs) {
            return null;
        }
    }
}
