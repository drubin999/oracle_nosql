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

import java.rmi.RemoteException;

import javax.security.auth.Subject;

import oracle.kv.AuthenticationRequiredException;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.util.registry.RemoteAPI;

/**
 * An RMI interface that provides login capabilities for infrastructure
 * components.  KVTrustedLogin is implemented and exported by RepNode, Admin
 * and SNA components in a storage node with an InterfaceType of TRUSTED_LOGIN.
 * This is provided only over an SSL interface that requires client
 * authentication or that includes some other connection-level authentication
 * phase.
 */
public final class TrustedLoginAPI extends RemoteAPI {

    final TrustedLogin remote;

    private TrustedLoginAPI(TrustedLogin remote)
        throws RemoteException {

        super(remote);
        this.remote = remote;
    }

    public static TrustedLoginAPI wrap(TrustedLogin remote)
        throws RemoteException {

        return new TrustedLoginAPI(remote);
    }

    /**
     * Obtain a login token that identifies the caller as an infrastructure
     * component when accessing the RMI interfaces of this component.
     *
     * @return a login result
     */
    public LoginResult loginInternal()
        throws RemoteException {

        return remote.loginInternal(getSerialVersion());
    }

    /**
     * Check an existing LoginToken for validity.
     * @return a Subject describing the user
     * @throw AuthenticationRequiredException if the token is not valid
     */
    public Subject validateLoginToken(LoginToken loginToken)
        throws AuthenticationRequiredException, SessionAccessException,
               RemoteException {

        return remote.validateLoginToken(loginToken, getSerialVersion());
    }

    /**
     * Log out the login token.  The LoginToken will no longer be usable for
     * accessing secure object interfaces.  If the session is already logged
     * out, this is treated as a a successful operation.  If the LoginToken
     * is not recognized, this may be because it was logged out earlier and
     * flushed from memory, and so this case will also be treated as successful.
     */
    public void logout(LoginToken loginToken)
        throws AuthenticationRequiredException, SessionAccessException,
               RemoteException {

        remote.logout(loginToken, getSerialVersion());
    }
}
