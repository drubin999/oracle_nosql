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
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.login.LoginUpdater.GlobalParamsUpdater;
import oracle.kv.impl.security.login.LoginUpdater.ServiceParamsUpdater;
import oracle.kv.impl.security.login.UserLogin;
import oracle.kv.impl.security.login.UserLoginHandler.LoginConfig;
import oracle.kv.impl.security.login.UserLoginImpl;
import oracle.kv.impl.topo.AdminId;

/**
 * Implements the login service for the Admin. The LoginService uses the admin
 * database as its source for security metadata. 
 */
public class LoginService implements GlobalParamsUpdater,
                                     ServiceParamsUpdater  {
    private final AdminService aservice;

    private final UserLogin userLogin;
    private final AdminLoginHandler loginHandler;

    /**
     * Creates a LoginService instance to operate on behalf of the specified
     * AdminService.
     */
    public LoginService(AdminService aservice) {
        this.aservice = aservice;

        loginHandler = AdminLoginHandler.create(aservice);

        userLogin = new UserLoginImpl(aservice.getFaultHandler(),
                                      loginHandler,
                                      aservice.getLogger());
    }

    public UserLogin getUserLogin() {
        return userLogin;
    }

    @Override
    public void newServiceParameters(ParameterMap map) {
        if (loginHandler == null) {
            return;
        }
        final Logger logger = aservice.getLogger();
        final int newLimit =
            map.getOrDefault(ParameterState.COMMON_SESSION_LIMIT).asInt();
        if (loginHandler.updateSessionLimit(newLimit)) {
            logger.info(
                "SessionLimit for AdminLoginHandler has been updated " +
                "with " + newLimit);
        }

        /* Update the ownerId of loginHandler once the admin is deployed */
        final AdminId adminId = new AdminParams(map).getAdminId();
        if (!loginHandler.getOwnerId().equals(adminId)) {
            loginHandler.updateOwner(adminId);
            logger.info("Owner of login handler has been updated with " +
                        adminId);
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
        final Logger logger = aservice.getLogger();
        logger.info( "Config for AdminLoginHandler has been updated with " +
                     "GlobalParams:" + gp.getMap());
    }
}
