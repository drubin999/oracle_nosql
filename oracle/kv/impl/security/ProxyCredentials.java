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

import java.io.Serializable;

import oracle.kv.LoginCredentials;

/**
 * A set of login credentials that allow a KVStore component to log in to a
 * KVStore on behalf of a user. These are processed locally on the client
 * side.
 */
public class ProxyCredentials implements LoginCredentials, Serializable {

    private static final long serialVersionUID = 1L;

    private final KVStoreUserPrincipal user;

    public ProxyCredentials(KVStoreUserPrincipal user) {
        this.user = user;
    }

    /**
     * Identify the user owning the credentials.
     *
     * @return the name of the user for which the credentials belong.
     */
    @Override
    public String getUsername() {
        return user.getName();
    }

    /**
     * Returns the user to be logged in.
     */
    public KVStoreUserPrincipal getUser() {
        return user;
    }
}
