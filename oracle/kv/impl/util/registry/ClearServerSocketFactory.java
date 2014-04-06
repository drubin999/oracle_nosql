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

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;

import oracle.kv.impl.util.PortRange;

/**
 * The server socket factory used by the SN and RNs to allocate any available
 * free port. It permits control of the server connect backlog.
 */
public class ClearServerSocketFactory extends ServerSocketFactory {

    /**
     * Create a server socket factory which yields socket connections with
     * the specified backlog. The name can be null to permit sharing of ports
     * across similarly configured services; we do not currently use this
     * feature.
     *
     * @param name the (possibly null) name associated with the SSF
     * @param backlog the backlog associated with the server socket. A value
     * of zero means use the java default value.
     * @param startPort the start of the port range
     * @param endPort the end of the port range. Both end points are inclusive.
     * A zero start and end port is used to denote an unconstrained allocation
     * of ports as defined by the method
     * {@link ServerSocketFactory#isUnconstrained()}.
     */
    @SuppressWarnings("javadoc")
    public ClearServerSocketFactory(String name,
                                    int backlog,
                                    int startPort,
                                    int endPort) {
        super(name, backlog, startPort, endPort);
    }

    @Override
    public String toString() {
        return "<ClearServerSocketFactory" +
               " name=" + name +
               " backlog=" + backlog +
               " port range=" + startPort + "," + endPort + ">";
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
        return (obj instanceof ClearServerSocketFactory);
    }

    /**
     * Creates a server socket. Port selection is accomplished as follows:
     *
     * 1) If a specific non-zero port is specified, it's created using just
     * that port.
     *
     * 2) If the port range is unconstrained, that is, isUnconstrained is true
     * then any available port is used.
     *
     * 3) Otherwise, a port from within the specified port range is allocated
     * and IOException is thrown if all ports in that range are busy.
     */
    @Override
    public ServerSocket createServerSocket(int port) throws IOException {

        return commonCreateServerSocket(port);
    }

    /**
     * A trival implementation of prepareServerSocket, since we don't require
     * its functionality.
     */
    @Override
    public ServerSocket prepareServerSocket() {
        return null;
    }

    /**
     * A trival implementation of prepareServerSocket, since we don't require
     * its functionality.
     */
    @Override
    public void discardServerSocket(ServerSocket ss) {
        throw new UnsupportedOperationException(
            "discardServerSocket is not supported by this implementation.");
    }

    /**
     * Factory method to configure SSF appropriately.
     *
     * @return an SSF or null if the factory has been disabled
     */
    public static ClearServerSocketFactory create(String name,
                                                  int backlog,
                                                  String portRange) {

        if (ServerSocketFactory.isDisabled()) {
            return null;
        }

        if (PortRange.isUnconstrained(portRange)) {
            return new ClearServerSocketFactory(name, backlog, 0, 0);
        }

        final List<Integer> range = PortRange.getRange(portRange);
        return new ClearServerSocketFactory(name, backlog,
                                            range.get(0), range.get(1));
    }

    @Override
    protected ServerSocket instantiateServerSocket(int port)
        throws IOException {

        return new ServerSocket(port);
    }

    @Override
    protected ServerSocket instantiateServerSocket(int port, int backlog1)
        throws IOException {

        return new ServerSocket(port, backlog1);
    }
}
