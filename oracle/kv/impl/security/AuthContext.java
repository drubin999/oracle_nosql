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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

import oracle.kv.impl.security.login.LoginToken;
import oracle.kv.impl.util.FastExternalizable;

/**
 * AuthContext captures security information for an RMI method call.
 */
public class AuthContext implements Serializable, FastExternalizable {

    private static final long serialVersionUID = 1;

    private static final int HAS_TOKEN           = 0x1;
    private static final int HAS_FORWARDER_TOKEN = 0x2;
    private static final int HAS_CLIENT_HOST     = 0x4;

    /* The primary login token */
    private LoginToken loginToken;

    /* The login token for the forwarding entity */
    private LoginToken forwarderToken;

    /* The client host for the loginToken, as reported by the forwarder */
    private String clientHost;

    /**
     * Create a AuthContext for an operation being initiated from the
     * original client.
     */
    public AuthContext(LoginToken loginToken) {
        this.loginToken = loginToken;
        this.forwarderToken = null;
        this.clientHost = null;
    }

    /**
     * Create a AuthContext for an operation being forwarded by an SN
     * component to another component on behalf of the original client.
     */
    public AuthContext(LoginToken loginToken,
                       LoginToken forwarderToken,
                       String clientHost) {
        this.loginToken = loginToken;
        this.forwarderToken = forwarderToken;
        this.clientHost = clientHost;
    }

    /* for FastExternalizable */
    public AuthContext(ObjectInput in, short serialVersion)
        throws IOException {

        final int flags = in.readByte();
        if ((flags & HAS_TOKEN) != 0) {
            loginToken = new LoginToken(in, serialVersion);
        } else {
            loginToken = null;
        }

        if ((flags & HAS_FORWARDER_TOKEN) != 0) {
            forwarderToken = new LoginToken(in, serialVersion);
        } else {
            forwarderToken = null;
        }

        if ((flags & HAS_CLIENT_HOST) != 0) {
            clientHost = in.readUTF();
        } else {
            clientHost = null;
        }
    }

    @Override
    public void writeFastExternal(ObjectOutput out, short serialVersion)
        throws IOException {

        int flags = 0;
        if (loginToken != null) {
            flags |= HAS_TOKEN;
        }
        if (forwarderToken != null) {
            flags |= HAS_FORWARDER_TOKEN;
        }
        if (clientHost != null) {
            flags |= HAS_CLIENT_HOST;
        }

        out.writeByte((byte) flags);

        if (loginToken != null) {
            loginToken.writeFastExternal(out, serialVersion);
        }
        if (forwarderToken != null) {
            forwarderToken.writeFastExternal(out, serialVersion);
        }
        if (clientHost != null) {
            out.writeUTF(clientHost);
        }
    }

    /**
     * Get the login token for the originating requester.
     */
    public LoginToken getLoginToken() {
        return loginToken;
    }

    /*
     * The ForwarderLoginToken is populated for forwarded requests.
     * It is needed to allow the clientHost to be specified.
     */
    public LoginToken getForwarderLoginToken() {
        return forwarderToken;
    }

    /**
     * The host that originated a forwarded request, for audit logging.
     */
    public String getClientHost() {
        return clientHost;
    }
}
