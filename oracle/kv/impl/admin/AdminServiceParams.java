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

import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.admin.param.StorageNodeParams;

/**
 * A convenience class to package all the parameter components used by the
 * Admin service.
 */
public class AdminServiceParams {

    private volatile SecurityParams securityParams;
    private volatile GlobalParams globalParams;
    private final StorageNodeParams storageNodeParams;
    private volatile AdminParams adminParams;

    public AdminServiceParams(SecurityParams securityParams,
                              GlobalParams globalParams,
                              StorageNodeParams storageNodeParams,
                              AdminParams adminParams) {
        super();
        this.securityParams = securityParams;
        this.globalParams = globalParams;
        this.storageNodeParams = storageNodeParams;
        this.adminParams = adminParams;
    }

    public SecurityParams getSecurityParams() {
        return securityParams;
    }

    public GlobalParams getGlobalParams() {
        return globalParams;
    }

    public StorageNodeParams getStorageNodeParams() {
        return storageNodeParams;
    }

    public AdminParams getAdminParams() {
        return adminParams;
    }

    public void setAdminParams(AdminParams newParams) {
        adminParams = newParams;
    }

    public void setGlobalParams(GlobalParams newParams) {
        globalParams = newParams;
    }

    public void setSecurityParams(SecurityParams newParams) {
        securityParams = newParams;
    }
}
