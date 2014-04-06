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

import static oracle.kv.impl.util.registry.ssl.SSLClientSocketFactory.Use;

import oracle.kv.impl.security.ssl.SSLControl;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RMISocketPolicy;
import oracle.kv.impl.util.registry.ServerSocketFactory;

/**
 * An RMISocketPolicy implementation is responsible for producing
 * socket factories for RMI.
 */
public class SSLSocketPolicy implements RMISocketPolicy {

    private SSLControl serverSSLControl;
    private SSLControl clientSSLControl;

    /**
     * Create an SSLSocketPolicy.
     */
    public SSLSocketPolicy(SSLControl serverSSLControl,
                           SSLControl clientSSLControl) {
        this.serverSSLControl = serverSSLControl;
        this.clientSSLControl = clientSSLControl;
    }

    /**
     * Prepare for use as standard client policy.
     */
    @Override
    public void prepareClient(String storeContext) {
        /*
         * The client socket factory picks up configuration from its environment
         * because it may be serialized and sent to another process.  We use the
         * client factories locally as well, so set the default context to
         * match our requirements.
         */
        if (isTrusted()) {
            SSLClientSocketFactory.setTrustedControl(clientSSLControl);
        } else {
            SSLClientSocketFactory.setUserControl(clientSSLControl,
                                                  storeContext);
        }
    }

    /**
     * Create a SocketFactoryPair appropriate for creation of an RMI registry.
     */
    @Override
    public SocketFactoryPair getRegistryPair(SocketFactoryArgs args) {
        final ServerSocketFactory ssf =
            SSLServerSocketFactory.create(serverSSLControl,
                                          args.getSsfName(),
                                          args.getSsfBacklog(),
                                          args.getSsfPortRange());
        final ClientSocketFactory csf = null;

        return new SocketFactoryPair(ssf, csf);
    }

    /**
     * Return a Client socket factory for appropriate for registry
     * access by the client.
     */
    @Override
    public ClientSocketFactory getRegistryCSF(SocketFactoryArgs args) {
        return new SSLClientSocketFactory(args.getCsfName(),
                                          args.getCsfConnectTimeout(),
                                          args.getCsfReadTimeout(),
                                          args.getKvStoreName());
    }

    /**
     * Create a SocketFactoryPair appropriate for exporting an object over RMI.
     */
    @Override
    public SocketFactoryPair getBindPair(SocketFactoryArgs args) {
        final ServerSocketFactory ssf =
            SSLServerSocketFactory.create(serverSSLControl,
                                          args.getSsfName(),
                                          args.getSsfBacklog(),
                                          args.getSsfPortRange());

        ClientSocketFactory csf = null;
        if (args.getCsfName() != null) {
            if (isTrusted()) {
                csf = new SSLClientSocketFactory(args.getCsfName(),
                                                 args.getCsfConnectTimeout(),
                                                 args.getCsfReadTimeout(),
                                                 Use.TRUSTED);
            } else {
                csf = new SSLClientSocketFactory(args.getCsfName(),
                                                 args.getCsfConnectTimeout(),
                                                 args.getCsfReadTimeout(),
                                                 args.getKvStoreName());
            }
        }

        return new SocketFactoryPair(ssf, csf);
    }

    /**
     * Report whether the SSF/CSF pairing can be optionally dropped without
     * impacting correct behavior.  I.e., is the policy simply providing tuning
     * parameters?
     */
    @Override
    public boolean isPolicyOptional() {
        return false;
    }

    /**
     * Reports whether the policy allows a server to be able to "trust" an
     * incoming client connection.
     */
    @Override
    public boolean isTrustCapable() {
        return isTrusted();
    }

    /**
     * An SSL socket policy is a trusted policy if it includes a client
     * authenticator in the server side of the configuration.
     */
    private boolean isTrusted() {
        return serverSSLControl != null &&
            serverSSLControl.peerAuthenticator() != null;
    }

}

