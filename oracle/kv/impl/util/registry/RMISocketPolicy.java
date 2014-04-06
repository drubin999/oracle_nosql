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

/**
 * An RMISocketPolicy implementation is responsible for producing socket
 * factories for RMI. The policy generally produces client and server pairs.
 */
public interface RMISocketPolicy {

    /**
     * A matched pair of socket factories - one for the client and one for
     * the server.
     */
    public static final class SocketFactoryPair {
        private final ServerSocketFactory serverFactory;
        private final ClientSocketFactory clientFactory;

        public SocketFactoryPair(ServerSocketFactory serverFactory,
                                 ClientSocketFactory clientFactory) {
            this.serverFactory = serverFactory;
            this.clientFactory = clientFactory;
        }

        public ClientSocketFactory getClientFactory() {
            return clientFactory;
        }

        public ServerSocketFactory getServerFactory() {
            return serverFactory;
        }
    }

    /**
     * A mechanism for expressing socket and CSF creation options.
     */
    public static final class SocketFactoryArgs {
        private String ssfName;
        private int ssfBacklog;
        private String ssfPortRange;
        private String csfName;
        private int csfConnectTimeoutMs;
        private int csfReadTimeoutMs;
        private String kvStoreName;
        private boolean useCsf = true;

        public SocketFactoryArgs() {
        }

        public String getSsfName() {
            return ssfName;
        }

        public SocketFactoryArgs setSsfName(String newSsfName) {
            this.ssfName = newSsfName;
            return this;
        }

        public int getSsfBacklog() {
            return ssfBacklog;
        }

        public SocketFactoryArgs setSsfBacklog(int newSsfBacklog) {
            this.ssfBacklog = newSsfBacklog;
            return this;
        }

        public String getSsfPortRange() {
            return ssfPortRange;
        }

        public SocketFactoryArgs setSsfPortRange(String newSsfPortRange) {
            this.ssfPortRange = newSsfPortRange;
            return this;
        }

        public String getCsfName() {
            return csfName;
        }

        public SocketFactoryArgs setCsfName(String newCsfName) {
            this.csfName = newCsfName;
            return this;
        }

        public int getCsfConnectTimeout() {
            return csfConnectTimeoutMs;
        }

        public SocketFactoryArgs setCsfConnectTimeout(
            int newCsfConnectTimeoutMs) {

            this.csfConnectTimeoutMs = newCsfConnectTimeoutMs;
            return this;
        }

        public int getCsfReadTimeout() {
            return csfReadTimeoutMs;
        }

        public SocketFactoryArgs setCsfReadTimeout(int newCsfReadTimeoutMs) {
            this.csfReadTimeoutMs = newCsfReadTimeoutMs;
            return this;
        }

        public String getKvStoreName() {
            return kvStoreName;
        }

        public SocketFactoryArgs setKvStoreName(String newKvStoreName) {
            this.kvStoreName = newKvStoreName;
            return this;
        }

        public boolean getUseCsf() {
            return useCsf;
        }

        /**
         * Set the preference as to whether the server should create a client
         * socket factory.  This is available for CSFs but not for SSFs
         * because CSFs may be configured through parameters in addition
         * to ClientSocketFactory.setIsDisabled(), but SSFs rely only on
         * ServerSocketFactory.setIsDisabled() for configuration.
         */
        public SocketFactoryArgs setUseCsf(boolean newUseCsf) {
            this.useCsf = newUseCsf;
            return this;
        }
    }

    /**
     * Prepare for use as the standard client policy within an SN component.
     * This is expected to install any state needed to be picked up by
     * client socket factories sent from the server.
     *
     * @param storeContext a null-allowable string that indicates a specific
     *   KVStore name that is being prepared. If null, the prepare is
     *   performed only for unqualified RMI access.
     */
    void prepareClient(String storeContext);

    /**
     * Return a Server/Client pair of socket factories appropriate
     * for registry creation.
     */
    SocketFactoryPair getRegistryPair(SocketFactoryArgs args);

    /*
     * Return a Client socket factory for appropriate for registry
     * access by the client
     */
    ClientSocketFactory getRegistryCSF(SocketFactoryArgs args);

    /**
     * Return a Server/Client pair of socket factories appropriate
     * for untrusted object binding.
     */
    SocketFactoryPair getBindPair(SocketFactoryArgs args);

    /**
     * Report whether the SSF/CSF pairing can be optionally dropped without
     * impact correct behavior.  I.e., is the policy simply providing tuning
     * parameters?
     * TBD: can this go away now that we don't need the TestStatus-dependent
     * factory skipping logic?
     */
    boolean isPolicyOptional();

    /**
     * Reports whether the policy allows a server to be able to "trust" an
     * incoming client connection.
     */
    boolean isTrustCapable();
}
