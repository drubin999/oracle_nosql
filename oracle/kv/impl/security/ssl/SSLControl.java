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

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

import com.sleepycat.je.rep.net.SSLAuthenticator;

/**
 * SSL policy control information.
 */
public class SSLControl {

    private final SSLParameters sslParameters;
    private final SSLContext sslContext;
    private final SSLAuthenticator sslAuthenticator;
    private final HostnameVerifier sslHostVerifier;

    public SSLControl(SSLParameters sslParameters,
                      SSLContext sslContext,
                      HostnameVerifier sslHostVerifier,
                      SSLAuthenticator sslAuthenticator) {
        this.sslParameters = sslParameters;
        this.sslContext = sslContext;
        this.sslHostVerifier = sslHostVerifier;
        this.sslAuthenticator = sslAuthenticator;
    }

    public SSLParameters sslParameters() {
        return this.sslParameters;
    }

    public SSLContext sslContext() {
        return this.sslContext;
    }

    public SSLAuthenticator peerAuthenticator() {
        return this.sslAuthenticator;
    }

    public HostnameVerifier hostVerifier() {
        return this.sslHostVerifier;
    }

    public void applySSLParameters(SSLSocket sslSocket) {
        if (sslParameters != null) {
            /* Apply sslParameter-selected policies */
            if (sslParameters.getCipherSuites() != null) {
                sslSocket.setEnabledCipherSuites(
                    sslParameters.getCipherSuites());
            }

            if (sslParameters.getProtocols() != null) {
                sslSocket.setEnabledProtocols(
                    sslParameters.getProtocols());
            }

            /* These are only applicable to the server side */
            if (sslParameters.getNeedClientAuth()) {
                sslSocket.setNeedClientAuth(true);
            }
        }
    }

    /*
     * Override hashCode() and equals() to give us a better chance to
     * reduce socket usage.
     */
    @Override
    public int hashCode() {
        int result = 17;
        if (sslParameters != null) {
            result = result * 31 + sslParameters.hashCode();
        }
        if (sslContext != null) {
            result = result * 31 + sslContext.hashCode();
        }
        if (sslAuthenticator != null) {
            result = result * 31 + sslAuthenticator.hashCode();
        }
        return result;
    }

    @Override
    public boolean equals(Object obj) {

        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        final SSLControl other = (SSLControl) obj;

        if (sslParameters != other.sslParameters) {
            return false;
        }

        if (sslContext != other.sslContext) {
            return false;
        }

        if (sslAuthenticator != other.sslAuthenticator) {
            return false;
        }

        return true;
    }
}
