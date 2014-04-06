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

package oracle.kv.impl.rep.login;

import static
    oracle.kv.impl.security.login.UserLoginHandler.SESSION_ID_RANDOM_BYTES;
import static oracle.kv.impl.util.registry.RegistryUtils.InterfaceType.LOGIN;

import java.rmi.RemoteException;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.rep.RepNodeSecurity;
import oracle.kv.impl.rep.RepNodeService;
import oracle.kv.impl.rep.RepNodeServiceFaultHandler;
import oracle.kv.impl.security.ConfigurationException;
import oracle.kv.impl.security.SecureProxy;
import oracle.kv.impl.security.UserVerifier;
import oracle.kv.impl.security.login.LoginTable;
import oracle.kv.impl.security.login.LoginUpdater.GlobalParamsUpdater;
import oracle.kv.impl.security.login.LoginUpdater.ServiceParamsUpdater;
import oracle.kv.impl.security.login.UserLogin;
import oracle.kv.impl.security.login.UserLoginHandler;
import oracle.kv.impl.security.login.UserLoginHandler.LoginConfig;
import oracle.kv.impl.security.login.UserLoginImpl;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RMISocketPolicy;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryPair;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * RepNodeLoginService provides the user login service for the RepNode.
 */
public class RepNodeLoginService implements GlobalParamsUpdater,
                                            ServiceParamsUpdater {

    /**
     *  The repNode being served
     */
    private final RepNodeService repNodeService;

    /**
     * The fault handler associated with the service.
     */
    private final RepNodeServiceFaultHandler faultHandler;

    /**
     * The login module for the RepNode
     */
    private final UserLogin userLogin;

    /**
     * The login module for the RepNode wrapped by the SecureProxy
     */
    private UserLogin exportableUL;

    private final Logger logger;

    private final UserLoginHandler loginHandler;

    private LoginTable tableSessionMgr;

    /**
     * Creates a RepNodeLoginService to provide login capabilities for
     * the RepNode.
     */
    public RepNodeLoginService(RepNodeService repNodeService) {

        this.repNodeService = repNodeService;
        logger = LoggerUtils.getLogger(this.getClass(),
                                       repNodeService.getParams());

        if (repNodeService.getParams().getSecurityParams().isSecure()) {
            faultHandler =
                new RepNodeServiceFaultHandler(repNodeService,
                                               logger,
                                               ProcessExitCode.RESTART);
            loginHandler =
                makeLoginHandler(repNodeService.getRepNodeSecurity().
                                 getKVSessionManager());
            userLogin = new UserLoginImpl(faultHandler, loginHandler, logger);
        } else {
            faultHandler = null;
            userLogin = null;
            loginHandler = null;
        }
    }

    public UserLogin getUserLogin() {
        return userLogin;
    }

    private UserLoginHandler makeLoginHandler(KVSessionManager kvSessMgr) {
        final UserVerifier verifier =
            repNodeService.getRepNodeSecurity().getUserVerifier();
        final RepNodeId rnId = repNodeService.getRepNodeId();

        /* Populated loginConfig from GlobalParameters */
        final GlobalParams gp = repNodeService.getParams().getGlobalParams();
        final LoginConfig loginConfig = LoginConfig.buildLoginConfig(gp);

        /* For use when no writable shard can be found for login */
        final RepNodeParams rp = repNodeService.getRepNodeParams();
        tableSessionMgr = new LoginTable(rp.getSessionLimit(),
                                         new byte[0],
                                         SESSION_ID_RANDOM_BYTES);

        final FailoverSessionManager failSessMgr =
            new FailoverSessionManager(kvSessMgr, tableSessionMgr, logger);

        final UserLoginHandler ulh =
            new UserLoginHandler(rnId, false, verifier, failSessMgr,
                                 loginConfig, logger);

        return ulh;
    }

    /**
     * Starts up the login service. The UserLogin is bound in the registry.
     */
    public void startup()
        throws RemoteException {

        if (userLogin == null) {
            logger.info("No RN Login configured. ");
            return;
        }

        final RepNodeParams rnp = repNodeService.getRepNodeParams();
        final String kvsName =
                repNodeService.getParams().getGlobalParams().getKVStoreName();

        final String csfName =
            ClientSocketFactory.factoryName(
                kvsName, RepNodeId.getPrefix(), LOGIN.interfaceName());

        final StorageNodeParams snp =
            repNodeService.getParams().getStorageNodeParams();

        final RMISocketPolicy rmiPolicy = repNodeService.getParams().
            getSecurityParams().getRMISocketPolicy();
        final SocketFactoryPair sfp =
            rnp.getLoginSFP(rmiPolicy, snp.getServicePortRange(), csfName);

        initExportableUL();

        logger.info("Starting RN Login. " +
                    " Server socket factory:" + sfp.getServerFactory() +
                    " Client socket connect factory: " +
                    sfp.getClientFactory());

        repNodeService.rebind(exportableUL, LOGIN,
                              sfp.getClientFactory(),
                              sfp.getServerFactory());
        logger.info("Starting UserLogin");
    }

    /**
     * Unbind the login service in the registry.
     */
    public void stop() throws RemoteException {
        if (userLogin != null) {
            repNodeService.unbind(exportableUL, LOGIN);
        }
    }

    private void initExportableUL() {
        final RepNodeSecurity rnSecurity = repNodeService.getRepNodeSecurity();
        try {
            exportableUL =
                SecureProxy.create(userLogin,
                                   rnSecurity.getAccessChecker(),
                                   faultHandler);
            logger.info(
                "Successfully created secure proxy for the user login");
        } catch (ConfigurationException ce) {
            throw new IllegalStateException("Unabled to create proxy", ce);
        }
    }

    @Override
    public void newServiceParameters(ParameterMap map) {
        if (tableSessionMgr == null) {
            return;
        }
        final int newLimit =
            map.getOrDefault(ParameterState.COMMON_SESSION_LIMIT).asInt();
        if (tableSessionMgr.updateSessionLimit(newLimit)) {
            logger.info(
                "SessionLimit for LoginTable has been updated with " +
                newLimit);
        }
    }

    @Override
    public void newGlobalParameters(ParameterMap map) {
        if (loginHandler == null) {
            return;
        }
        final GlobalParams gp = new GlobalParams(map);
        final LoginConfig config = LoginConfig.buildLoginConfig(gp);
        loginHandler.updateConfig(config);
        logger.info(
            "Config for UserLoginHandler has been updated with GlobalParams:" +
            gp.getMap()
        );
    }
}
