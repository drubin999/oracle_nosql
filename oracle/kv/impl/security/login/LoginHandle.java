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

import java.util.concurrent.atomic.AtomicReference;

import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.topo.ResourceId.ResourceType;

/**
 * LoginHandle defines the interface by which RMI interface APIs acquire
 * LoginTokens for called methods.
 */
public abstract class LoginHandle {
    private final AtomicReference<LoginToken> loginToken;

    protected LoginHandle(LoginToken loginToken) {
        this.loginToken = new AtomicReference<LoginToken>(loginToken);
    }

    /**
     * Get the current LoginToken value.
     */
    public LoginToken getLoginToken() {
        return loginToken.get();
    }

    /**
     * Attempt to update the LoginToken to be a new value.
     * If the current token is not the same identity as the old token,
     * the update is not performed.
     * @return true if the update was performed.
     */
    protected boolean updateLoginToken(LoginToken oldToken,
                                       LoginToken newToken) {
        return loginToken.compareAndSet(oldToken, newToken);
    }

    /**
     * Attempt to renew the token to a later expiration time.
     * Returns null if unable to renew.
     */
    public abstract LoginToken renewToken(LoginToken currToken)
        throws SessionAccessException;

    /**
     * Logout the session associated with the login token.
     */
    public abstract void logoutToken()
        throws SessionAccessException;

    /**
     * Report whether this login handle supports authentication to the
     * specified resouce type.
     */
    public abstract boolean isUsable(ResourceType rtype);
}
