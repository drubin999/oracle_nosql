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

package oracle.kv.impl.util;

import static oracle.kv.impl.util.TopologyLocator.HOST_PORT_SEPARATOR;

/**
 * Host:Port-tuple and parsing.
 */
public class HostPort {
    private final String hostname;
    private final int port;

    public HostPort(final String hostname, final int port) {
        this.hostname = hostname;
        this.port = port;
    }

    public String hostname() {
        return hostname;
    }

    public int port() {
        return port;
    }

    /**
     * Parses a host:port into a HostPort object.
     *
     * @throws IllegalArgumentException if the input string is null or
     * does not conform to expected format.
     */
    public static HostPort parse(String hostPort) {
        if (hostPort == null || hostPort.length() == 0) {
            throw new IllegalArgumentException
                ("Null or empty host and port pair: " + hostPort);
        }

        final int portStartIndex = hostPort.indexOf(HOST_PORT_SEPARATOR);
        if (portStartIndex <= 0 ||
            portStartIndex == hostPort.length() - 1) {

            throw new IllegalArgumentException
                ("Missing or illegal port separator char: " + hostPort);
        }

        final String registryHostname =
            hostPort.substring(0, portStartIndex);

        final int registryPort;
        try {
            registryPort =
                Integer.parseInt(hostPort.substring(portStartIndex + 1));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException
                ("Illegal number format for port: " + hostPort + " " + e);
        }

        return new HostPort(registryHostname, registryPort);
    }

    /**
     * Parse an array of host:port strings into an array of HostPort.
     *
     * @throws IllegalArgumentException if hostPorts is null or if any of
     * the contained hostPort strings are null, or otherwise do not contain
     * a valid host:port combination.
     */
    public static HostPort[] parse(String[] hostPorts) {
        final HostPort[] hps = new HostPort[hostPorts.length];

        for (int i = 0; i < hostPorts.length; i++) {
            hps[i] = HostPort.parse(hostPorts[i]);
        }

        return hps;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        if (other == null || !(other instanceof HostPort)) {
            return false;
        }

        final HostPort otherHostPort = (HostPort) other;
        return (hostname.equals(otherHostPort.hostname) &&
                port == otherHostPort.port);
    }

    @Override
    public int hashCode() {
        return hostname.hashCode() + port * 31;
    }
}
