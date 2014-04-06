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

package oracle.kv.impl.security;

import oracle.kv.impl.security.login.LoginManager;

/**
 * A set of login credentials that allow a KVStore component to log in to a
 * KVStore on behalf of a user. These are processed locally on the client
 * side. That is, this class isn't actually sent over the wire. Instead,
 * a ProxyCredentials instance is substituted for the proxyLogin call and the
 * internalManager is used locally to enable the RepNodeLoginManager to have
 * access to server authentication. Serialization of this class should not
 * be relied upon, as the associated LoginManager is not included in the
 * serialized form.
 */
public class ClientProxyCredentials extends ProxyCredentials {

    private static final long serialVersionUID = 1L;

    /*
     * Used on the "client" side of the server-server connection to supply the
     * login token needed to authorize the user of the proxyLogin operation.
     */
    private final transient LoginManager internalManager;

    /**
     * Create a Credentials object that enables proxyLogin operations.
     */
    public ClientProxyCredentials(KVStoreUserPrincipal user,
                                  LoginManager internalManager) {
        super(user);
        this.internalManager = internalManager;
    }

    /**
     * Returns the internal login manager.
     */
    public LoginManager getInternalManager() {
        return internalManager;
    }
}
