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
import java.rmi.server.RMIServerSocketFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * The server socket factory used by the SN and RNs to allocate any available
 * free port. It permits control of the server connect backlog.
 */
public abstract class ServerSocketFactory
    implements RMIServerSocketFactory {

    /* A count of the allocated sockets. */
    private final AtomicInteger socketCount = new AtomicInteger(0);

    /*
     * The name associated with the SSF. It's used in the equals method to
     * ensure that each service is associated with a different port and
     * custom backlog.
     */
    protected final String name;

    /* The backlog associated with server sockets */
    protected final int backlog;

    /* The port range. */
    protected final int startPort;
    protected final int endPort;

    /*
     * The port at which the range scan will resume upon the next call to
     * createServerSocket.
     */
    private int currentPort;

    /* The size based upon the above range. */
    private final int rangeSize;

    /* A logger for connection-related logging. */
    protected Logger connectionLogger;

    private static boolean disabled = false;

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
     * of ports as defined by the method {@link #isUnconstrained()}.
     */
    protected ServerSocketFactory(String name,
                                  int backlog,
                                  int startPort,
                                  int endPort) {
        super();

        if (endPort < startPort) {
            throw new IllegalArgumentException("End port " + endPort +
                                               " must be >= startPort " +
                                               startPort);
        }
        this.name = name;
        this.backlog = backlog;
        this.startPort = startPort;
        this.endPort = endPort;
        this.currentPort = startPort;
        /* The range is inclusive. */
        rangeSize = isUnconstrained() ?
            Integer.MAX_VALUE :
            (endPort - startPort) + 1;
    }

    /**
     * Returns the number of sockets that have been allocated so far.
     */
    public int getSocketCount() {
        return socketCount.get();
    }

    public static boolean isDisabled() {
        return disabled;
    }

    public static void setDisabled(boolean disable) {
        disabled = disable;
    }

    public int getBacklog() {
        return backlog;
    }

    public void setConnectionLogger(Logger logger) {
        this.connectionLogger = logger;
    }

    /**
     * Some implementations may require that the server socket be created
     * with a port number that we can specify in a call to
     * UnicasteRemoteObject.exportObject, but where we don't actually have
     * a fixed port number.  This call allows the factory to pre-create a
     * ServerSocket using whatever port number constraints it has been
     * configured with, so that we have a known port available to us.
     * In a subsequent call to createServerSocket that specifies the port
     * number on which that server socket is listening, the implementation
     * should consume the server socket.  In the event of an error occurring
     * during the export operation, the caller must call discardServerSocket
     * to signal that the server socket is to be discarded.
     */
    public abstract ServerSocket prepareServerSocket()
        throws IOException;

    /**
     * Signals that a server socket previously created by prepareServerSocket
     * will not be used, and should be closed and discarded.  Any IOExceptions
     * that occur during socket closing should be suppressed.
     */
    public abstract void discardServerSocket(ServerSocket ss);

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + backlog;
        result = prime * result + endPort;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + startPort;
        return result;
    }

    /**
     * Do common equality checking.  Equality requires:
     * 1) obj is non-null
     * 2) obj is an instance of a ServerSocketFactory-derived class
     * 3) the following properties are equals() against obj
     *     - name
     *     - backlog
     *     - startPort
     *     - endPort
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof ServerSocketFactory)) {
            return false;
        }
        final ServerSocketFactory other = (ServerSocketFactory) obj;
        if (backlog != other.backlog) {
            return false;
        }
        if (startPort != other.startPort) {
            return false;
        }
        if (endPort != other.endPort) {
            return false;
        }
        if (name == null) {
            if (other.name != null) {
                return false;
            }
        } else if (!name.equals(other.name)) {
            return false;
        }
        return true;
    }

    /**
     * Returns true if the port range is unconstrained.
     */
    protected boolean isUnconstrained() {
        return (startPort == 0) && (endPort == 0);
    }

    protected ServerSocket commonCreateServerSocket(int port)
        throws IOException {

        if ((port != 0) || isUnconstrained()) {
            return createServerSocketInternal(port);
        }

        /* Select a port from within the free range */
        for (int portCount = rangeSize; portCount > 0; portCount--) {
            try {
                return createServerSocketInternal(currentPort);
            } catch (IOException ioe) /* CHECKSTYLE:OFF */ {
                /* Port in use or unusable, continue the scan. */
            } /* CHECKSTYLE:ON */ finally {
                if (currentPort == endPort) {
                    currentPort = startPort;
                } else {
                    currentPort++;
                }
            }
        }

        throw new IOException("No free ports in the range: " +
                              startPort + "," + endPort);
     }

    /**
     * Instantiate a ServerSocket using the specified port and default
     * backlog.
     */
    protected abstract ServerSocket instantiateServerSocket(int port)
        throws IOException;

    /**
     * Instantiate a ServerSocket using the specified port and the specified
     * backlog.
     */
    protected abstract ServerSocket instantiateServerSocket(int port,
                                                            int useBacklog)
        throws IOException;

    private ServerSocket createServerSocketInternal(int port)
        throws IOException {

        final ServerSocket so = (backlog > 0) ?
            instantiateServerSocket(port, backlog) :
            /* use the default value. */
            instantiateServerSocket(port);

        socketCount.incrementAndGet();

        return so;
    }
}
