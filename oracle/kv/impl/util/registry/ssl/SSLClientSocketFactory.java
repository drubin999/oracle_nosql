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

package oracle.kv.impl.util.registry.ssl;

import java.io.IOException;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;

import oracle.kv.KVStoreConfig;
import oracle.kv.impl.security.ssl.SSLControl;
import oracle.kv.impl.util.registry.ClientSocketFactory;

/**
 * An implementation of RMIClientSocketFactory that permits configuration of
 * the following Socket timeouts:
 * <ol>
 * <li>Connection timeout</li>
 * <li>Read timeout</li>
 * </ol>
 * These are set to allow clients to become aware of possible network problems
 * in a timely manner.
 * <p>
 * CSFs with the appropriate timeouts for a registry are specified on the
 * client side.
 * <p>
 * CSFs for service requests (unrelated to the registry) have default values
 * provided by the server that can be overridden by the client as below:
 * <ol>
 * <li>Server side timeout parameters are set via the KVS admin as policy
 * parameters</li>
 * <li>Client side timeout parameters are set via {@link KVStoreConfig}. When
 * present, they override the parameters set at the server level.</li>
 * </ol>
 * <p>
 * This class leverages the underlying ClientSocketFactory class, which
 * performs all of the heavy lifting for socket timeout handling.
 */
public class SSLClientSocketFactory
    extends ClientSocketFactory {

    public enum Use {
        USER,
        TRUSTED
    };

    private static final long serialVersionUID = 1L;

    /* The KVStore instance with which this is associated */
    private final String kvStoreName;

    /* The intended use for this factory */
    private final Use clientUse;

    /* SSLControl for use when clientUse = TRUSTED */
    private static SSLControl trustedSSLControl;

    /*
     * SSLControl for use when clientUse = USER and either there is no known
     * store context, or there is no SSLControl associated with the store
     * context.
     */
    private static SSLControl defaultUserSSLControl;

    /* Map of use KVStore name to SSLControl */
    private static final Map<String, SSLControl> userSSLControlMap =
        new ConcurrentHashMap<String, SSLControl>();

    /**
     * Creates the client socket factory for use by a Client.
     *
     * @param name the factory name
     *
     * @param connectTimeoutMs the connect timeout. A zero value denotes an
     *                          infinite timeout
     * @param readTimeoutMs the read timeout associated with the connection.
     *                       A zero value denotes an infinite timeout
     * @param kvStoreName the name of the KVStore instance
     */
    public SSLClientSocketFactory(String name,
                                  int connectTimeoutMs,
                                  int readTimeoutMs,
                                  String kvStoreName) {
        super(name, connectTimeoutMs, readTimeoutMs);
        this.kvStoreName = kvStoreName;
        this.clientUse = Use.USER;
    }

    /**
     * Creates a client socket factory for client access.  This is a
     * convenience version that supplies the use as USER.  Currently used only
     * for testing.
     *
     * @param name the factory name
     *
     * @param connectTimeoutMs the connect timeout. A zero value denotes an
     *                          infinite timeout
     * @param readTimeoutMs the read timeout associated with the connection.
     *                       A zero value denotes an infinite timeout
     */
    public SSLClientSocketFactory(String name,
                                  int connectTimeoutMs,
                                  int readTimeoutMs) {
        this(name, connectTimeoutMs, readTimeoutMs, Use.USER);
    }

    /**
     * Creates the client socket factory within the server.
     *
     * @param name the factory name
     *
     * @param connectTimeoutMs the connect timeout. A zero value denotes an
     *                          infinite timeout
     * @param readTimeoutMs the read timeout associated with the connection.
     *                       A zero value denotes an infinite timeout
     * @param clientUse The intended use for the client socekt factory
     */
    public SSLClientSocketFactory(String name,
                                   int connectTimeoutMs,
                                   int readTimeoutMs,
                                   Use clientUse) {
        super(name, connectTimeoutMs, readTimeoutMs);
        this.kvStoreName = null;
        this.clientUse = clientUse;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result +
            ((kvStoreName == null) ? 0 : kvStoreName.hashCode()) +
            ((clientUse == null) ? 0 : clientUse.hashCode());
        return result;
    }

    @Override
    public String toString() {
        return "<SSLClientSocketFactory" +
                " name=" + name +
                " id=" + this.hashCode() +
                " connectMs=" + connectTimeoutMs +
                " readMs=" + readTimeoutMs +
                " kvStoreName=" + kvStoreName +
                " clientUse=" + clientUse +
                ">";
    }

    @Override
    public boolean equals(Object obj) {

        if (!super.equals(obj)) {
            return false;
        }

        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof SSLClientSocketFactory)) {
            return false;
        }
        if (!super.equals(obj)) {
            return false;
        }

        final SSLClientSocketFactory other = (SSLClientSocketFactory) obj;

        if (clientUse == null) {
            if (other.clientUse != null) {
                return false;
            }
        } else if (!clientUse.equals(other.clientUse)) {
            return false;
        }

        if (kvStoreName == null) {
            if (other.kvStoreName != null) {
                return false;
            }
        } else if (!kvStoreName.equals(other.kvStoreName)) {
            return false;
        }
        return true;
    }

    /**
     * @see java.rmi.server.RMIClientSocketFactory#createSocket
     */
    @Override
    public Socket createSocket(String host, int port)
        throws java.net.UnknownHostException, IOException {

        final Socket sock = createTimeoutSocket(host, port);

        try {
            SSLContext context = null;
            SSLControl control = null;
            if (Use.TRUSTED.equals(clientUse)) {
                control = trustedSSLControl;
                context = control.sslContext();
            } else {
                if (kvStoreName != null) {
                    /* First look for a store-specific SSLControl */
                    control = userSSLControlMap.get(kvStoreName);
                }
                /*
                 * If no store-specific SSLControl exists consider a default
                 * SSLControl.
                 */
                if (control == null && defaultUserSSLControl != null) {
                    control = defaultUserSSLControl;
                }
                if (control == null) {
                    context = SSLContext.getDefault();
                } else {
                    context = control.sslContext();
                }
            }

            final SSLSocket sslSock = (SSLSocket)
                context.getSocketFactory().createSocket(
                    sock, host, port, true); /* autoclose */

            if (control != null) {
                control.applySSLParameters(sslSock);
            }

            /* Start the handshake.  This is completed synchronously */
            sslSock.startHandshake();
            if (control != null && control.hostVerifier() != null) {
                if (!control.hostVerifier().
                    verify(host, sslSock.getSession())) {
                    throw new IOException(
                        "SSL connection to host " + host + " is not valid.");
                }
            }
            return sslSock;
        } catch (NoSuchAlgorithmException nsae) {
            /*
             * This is very unlikely to occur in a reasonably configured system
             */
            throw new IOException("Unknown algorithm", nsae);
        }
    }

    /**
     * Set the Trusted SSLControl
     */
    static void setTrustedControl(SSLControl trustedControl) {
        trustedSSLControl = trustedControl;
    }

    /**
     * Set the User SSLControl
     */
    static void setUserControl(SSLControl userControl, String storeContext) {
        if (storeContext != null) {
            userSSLControlMap.put(storeContext, userControl);
        }
        /* Always set the default */
        defaultUserSSLControl = userControl;
    }
}

