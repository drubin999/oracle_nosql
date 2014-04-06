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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

import oracle.kv.impl.util.FastExternalizable;

/**
 * LoginResult is the result of a login operation.  It currently contains only
 * a single field, but it is expected that later versions will expand on this.
 */
public class LoginResult implements Serializable, FastExternalizable {

    private static final long serialVersionUID = 1;

    private LoginToken loginToken;

    /**
     * Constructor.
     */
    public LoginResult(LoginToken loginToken) {
        this.loginToken = loginToken;
    }

    /* for FastExternalizable */
    public LoginResult(ObjectInput in, short serialVersion)
        throws IOException {

        final boolean hasToken = in.readBoolean();
        if (hasToken) {
            loginToken = new LoginToken(in, serialVersion);
        } else {
            loginToken = null;
        }
    }

    @Override
    public void writeFastExternal(ObjectOutput out, short serialVersion)
        throws IOException {

        if (loginToken == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            loginToken.writeFastExternal(out, serialVersion);
        }
    }

    public LoginToken getLoginToken() {
        return loginToken;
    }
}
