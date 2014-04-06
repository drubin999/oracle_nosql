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

package oracle.kv.impl.security.ssl;

import java.security.Principal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;

/**
 * SSL socket authentication implementation based on certificate DN
 * pattern match.
 */
class SSLPatternMatcher {

    private final Pattern pattern;

    /**
     * Construct an pattern matcher that will verify peers based on a
     * match of the Distinguished Name in the peer certificate to the
     * configured pattern.
     *
     * @param regexPattern a string that conforms to Java regular expression
     *    format rules
     */
    SSLPatternMatcher(String regexPattern) {
        this.pattern = Pattern.compile(regexPattern);
    }

    boolean verifyPeer(SSLSession sslSession) {
        Principal principal = null;
        try {
            principal = sslSession.getPeerPrincipal();
        } catch (SSLPeerUnverifiedException pue) {
            return false;
        }

        if (principal != null) {
            if (principal instanceof X500Principal) {
                final X500Principal x500Principal = (X500Principal) principal;
                final String name =
                    x500Principal.getName(X500Principal.RFC1779);
                final Matcher m = pattern.matcher(name);
                if (m.matches()) {
                    return true;
                }
            }
        }
        return false;
    }


}
