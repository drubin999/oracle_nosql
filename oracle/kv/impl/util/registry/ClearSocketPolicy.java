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

package oracle.kv.impl.util.registry;

import oracle.kv.impl.mgmt.jmx.JmxAgent;

/**
 * Provides an implementation of RMISocketPolicy that transmits information
 * "in the clear", with no encryption.
 */
public class ClearSocketPolicy implements RMISocketPolicy {

    public ClearSocketPolicy() {
    }

    /**
     * Prepare for use as standard client policy.
     */
    @Override
    public void prepareClient(@SuppressWarnings("unused") String storeContext) {
        /* No action needed */
    }

    /**
     * Registry creation sockets factories.
     */
    @Override
    public SocketFactoryPair getRegistryPair(SocketFactoryArgs args) {
        final ServerSocketFactory ssf =
            (args.getSsfName() == null) ? null :
            ClearServerSocketFactory.create(args.getSsfName(),
                                            args.getSsfBacklog(),
                                            args.getSsfPortRange());

        /* Any CSF args specified are ignored */
        final ClientSocketFactory csf = null;

        return new SocketFactoryPair(ssf, csf);
    }

    /*
     * Return a Client socket factory appropriate for registry access by the
     * client.
     */
    @Override
    public ClientSocketFactory getRegistryCSF(SocketFactoryArgs args) {
        /*
         * Until we get to the next upgrade release boundary beyond 3.0,
         * return ClientSocketFactory.
         */
        return new ClearClientSocketFactory(args.getCsfName(),
                                            args.getCsfConnectTimeout(),
                                            args.getCsfReadTimeout());
    }

    /**
     * Standard RMI export socket factories.
     */
    @Override
    public SocketFactoryPair getBindPair(SocketFactoryArgs args) {
        if (args.getSsfPortRange() == null ||

            /*
             * When exporting for JMX access, don't supply a CSF.  JMX clients
             * probably won't have our client library available, so they'll
             * need to use the Java-provided CSF class.
             */
            JmxAgent.JMX_SSF_NAME.equals(args.getSsfName())) {
            return new SocketFactoryPair(null, null);
        }

        final ServerSocketFactory ssf =
            ClearServerSocketFactory.create(args.getSsfName(),
                                            args.getSsfBacklog(),
                                            args.getSsfPortRange());

        ClientSocketFactory csf = null;

        if (args.getCsfName() != null &&
            args.getUseCsf() && !ClientSocketFactory.isDisabled()) {
            /*
             * Although it would be good to use ClearClientSocketFactory here,
             * the R2 -> R3 upgrade clients might not have ClientSocketFactory
             * available, so we continue to use ClientSocketFactory for
             * purposes of compatibility.
             *
             * TODO: convert to ClearClientSocketFactory at the next upgrade
             * version boundary.
             */
            csf = new ClientSocketFactory(args.getCsfName(),
                                          args.getCsfConnectTimeout(),
                                          args.getCsfReadTimeout());
        }
        return new SocketFactoryPair(ssf, csf);
    }

    /**
     * Report whether the SSF/CSF pairing can be optionally dropped without
     * impact correct behavior.  I.e., is the policy simply providing tuning
     * parameters?
     */
    @Override
    public boolean isPolicyOptional() {
        return true;
    }

    /**
     * Reports whether the policy allows a server to be able to "trust" an
     * incoming client connection.
     */
    @Override
    public boolean isTrustCapable() {
        return false;
    }
}
