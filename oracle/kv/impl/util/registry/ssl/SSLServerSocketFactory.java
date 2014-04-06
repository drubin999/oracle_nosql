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
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLSocket;

import oracle.kv.impl.security.ssl.SSLControl;
import oracle.kv.impl.util.PortRange;
import oracle.kv.impl.util.registry.ServerSocketFactory;

/**
 * This implementation of ServerSocketFactory provides SSL protocol support.
 */
public class SSLServerSocketFactory extends ServerSocketFactory {

    /* Limit on number of handshake threads to allow */
    private static final int HANDSHAKE_THREAD_MAX = 10;

    /* Limit on number of entries in handshake queue to allow */
    private static final int HANDSHAKE_QUEUE_MAX = 10;

    /* The handshake executor thread keepalive time */
    private static final long KEEPALIVE_MS = 10000L;

    /* Handshake thread counter, for numbering threads */
    private static final AtomicInteger hsThreadCounter = new AtomicInteger(0);

    /*
     * Map used to support the prepareServerSocket functionality.
     */
    private final Map<Integer, ServerSocket> pendingSocketMap;

    /* SSLControl used to set policy for SSL */
    private final SSLControl sslControl;

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
     * {@link oracle.kv.impl.util.registry.ServerSocketFactory#isUnconstrained}.
     */
    public SSLServerSocketFactory(SSLControl sslControl,
                                  String name,
                                  int backlog,
                                  int startPort,
                                  int endPort) {
        super(name, backlog, startPort, endPort);

        if (sslControl == null) {
            throw new IllegalArgumentException("sslControl may not be null");
        }
        this.sslControl = sslControl;
        pendingSocketMap = new HashMap<Integer, ServerSocket>();
    }

    @Override
    public String toString() {
        return "<SSLServerSocketFactory" +
               " name=" + name +
               " backlog=" + backlog +
               " port range=" + startPort + "," + endPort +
               " ssl control = " + sslControl + ">";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + sslControl.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
        if (!(obj instanceof SSLServerSocketFactory)) {
            return false;
        }
        final SSLServerSocketFactory other = (SSLServerSocketFactory) obj;
        if (!sslControl.equals(other.sslControl)) {
            return false;
        }
        return true;
    }

    class SSLInternalServerSocket extends ServerSocket {

        private final ThreadPoolExecutor handshakeExecutor;
        private final Thread rawAcceptor;
        private final BlockingQueue<AcceptEvent> acceptEvents;
        private final BlockingQueue<Runnable> handshakeExecutionQueue;
        private final int listenPort;

        SSLInternalServerSocket(int port) throws IOException {
            this(port, 0);
        }

        SSLInternalServerSocket(int port, int backlog) throws IOException {
            super(port, backlog);

            listenPort = getLocalPort();

            if (doBackgroundAccept()) {
                /*
                 * Set up the threads needed to asynchronously process accepts
                 */
                handshakeExecutionQueue =
                    new LinkedBlockingQueue<Runnable>(HANDSHAKE_QUEUE_MAX);
                handshakeExecutor =
                    new ThreadPoolExecutor(1, /* corePoolSize */
                                           HANDSHAKE_THREAD_MAX,
                                           KEEPALIVE_MS,
                                           TimeUnit.MILLISECONDS,
                                           handshakeExecutionQueue,
                                           new HandshakeThreadFactory());
                acceptEvents = new LinkedBlockingQueue<AcceptEvent>();

                final String acceptorName = "SSLAccept-" + listenPort;
                rawAcceptor = new Thread(new RawAcceptor(), acceptorName);
                rawAcceptor.setDaemon(true);
                rawAcceptor.start();
            } else {
                /*
                 * We either have no need for background handshake processing
                 * or we don't want it enabled.
                 */
                handshakeExecutionQueue = null;
                handshakeExecutor = null;
                acceptEvents = null;
                rawAcceptor = null;
            }
        }

        @Override
        public Socket accept() throws IOException {
            if (acceptEvents != null) {
                /* Handshake processing is begin done on background thread */
                while (true) {
                    try {
                        final AcceptEvent event = acceptEvents.take();
                        return event.yieldSocket();
                    } catch (InterruptedException ie) {
                        if (connectionLogger != null) {
                            connectionLogger.info(
                                "Interrupted while waiting for acceptEvent");
                        }
                    }
                }
            }

            /* Handshake processing is done inline, if needed */
            while (true) {
                final SSLSocket sslSocket = acceptSocket();
                if (authenticateNewSocket(sslSocket)) {
                    return sslSocket;
                }
            }
        }

        /**
         * Give a newly accepted socket, perform any validations that are
         * needed, and if acceptable, return true.  If there is a problem
         * with the socket, log any errors, close the socket and return false.
         */
        private boolean authenticateNewSocket(SSLSocket sslSocket) {
            if (sslControl.peerAuthenticator() == null) {
                return true;
            }

            try {
                /* blocks until completion on initial call */
                sslSocket.startHandshake();

                if (sslControl.peerAuthenticator().
                    isTrusted(sslSocket.getSession())) {
                    return true;
                }

                if (connectionLogger != null) {
                    connectionLogger.info(
                        "Rejecting client connection");
                }
                forceCloseSocket(sslSocket);
            } catch (IOException ioe) {
                if (connectionLogger != null) {
                    connectionLogger.info(
                        "error while handshaking: " + ioe);
                }
                forceCloseSocket(sslSocket);
            }

            return false;
        }

        private SSLSocket acceptSocket() throws IOException {
            final Socket socket = super.accept();

            /* Construct an SSL socket on top of our Timeout Socket */
            final SSLSocket sslSocket = (SSLSocket)
                sslControl.sslContext().getSocketFactory().createSocket(
                    socket,
                    /*
                     * Maybe it would be nice to add a call to getHostName()
                     * here, but that can timeout - don't let this happen in
                     * accept().  Just call toString() on the address.
                     */
                    socket.getInetAddress().toString(),
                    socket.getPort(), true);

            /* By definition we are not using client mode */
            sslSocket.setUseClientMode(false);

            sslControl.applySSLParameters(sslSocket);

            return sslSocket;
        }

        private void forceCloseSocket(Socket socket) {
            try {
                socket.close();
            } catch (IOException ioe) {
                if (connectionLogger != null) {
                    connectionLogger.info(
                        "Exception closing socket: " + ioe);
                }
            }
        }

        private void queueAcceptedSocket(SSLSocket newSocket) {
            try {
                acceptEvents.put(new SocketEvent(newSocket));
            } catch (InterruptedException ie) {
                if (connectionLogger != null) {
                    connectionLogger.info(
                        "Interrupted while queueing new socket - " +
                        "discarding:" + ie);
                }
                forceCloseSocket(newSocket);
            }
        }

        /**
         * The RawAcceptor class performs the raw accept() operation on the
         * server socket.  Accepted sockets are put on a queue to be processed
         * in the background.
         */
        private final class RawAcceptor implements Runnable {
            @Override
            public void run() {
                while (true) {
                    SSLSocket sslSocket = null;
                    try {
                        sslSocket = acceptSocket();
                    } catch (IOException ioe) {

                        /*
                         * An IOException indicates a shutdown of the server
                         * socket, which is our cue to quit.
                         */
                        try {
                            acceptEvents.put(new ExceptionEvent(ioe));
                        } catch (InterruptedException ie) {

                            /*
                             * TODO: retry logic to allow insertion to succeed?
                             */
                            if (connectionLogger != null) {
                                connectionLogger.info(
                                    "Unable to queue ExecptionEvent " +
                                    "while terminating");
                            }
                        }
                        handshakeExecutor.shutdown();
                        if (connectionLogger != null) {
                            connectionLogger.info(
                                "Queued shutdown event for port " +
                                listenPort);
                        }
                        return;
                    }

                    if  (sslSocket != null) {
                        if (sslControl.peerAuthenticator() != null) {
                            /*
                             * The SSLControl object wants to do SSL certificate
                             * verification, and we need to wait until the
                             * handshake is complete for that to happen.  Put
                             * the socket on the handshake queue.
                             */
                            try {
                                handshakeExecutor.execute(
                                    new HandshakeAndVerify(sslSocket));
                            } catch (RejectedExecutionException ie) {
                                if (connectionLogger != null) {
                                    connectionLogger.info(
                                        "Unable to queue socket for " +
                                        "verification - interrupted.");
                                }
                                forceCloseSocket(sslSocket);
                            }
                        } else {
                            /*
                             * No SSLCertificate verification is required.
                             * The SSL handshake still needs to be done, and
                             * that can take a while to complete, but that will
                             * happen in the RMI processing thread.
                             */
                            queueAcceptedSocket(sslSocket);
                        }
                    }
                }
            }

            /**
             * Simple class to wait for the the SSL handshake to complete,
             * and then apply SSL certificate authentication.
             */
            private final class HandshakeAndVerify implements Runnable {
                private SSLSocket sslSocket;
                private HandshakeAndVerify(SSLSocket sslSocket) {
                    this.sslSocket = sslSocket;
                }

                @Override
                public void run() {
                    if (authenticateNewSocket(sslSocket)) {
                        queueAcceptedSocket(sslSocket);
                    }
                }
            }
        }

        /* Base class of accept events */
        private abstract class AcceptEvent {
            abstract Socket yieldSocket() throws IOException;
        }

        /* Accept event yielding a socket */
        private class SocketEvent extends AcceptEvent {
            private final Socket socket;

            SocketEvent(Socket socket) {
                this.socket = socket;
            }

            @Override
            Socket yieldSocket() throws IOException {
                return socket;
            }
        }

        /* Accept event yielding an exception */
        private class ExceptionEvent extends AcceptEvent {
            private final IOException ioe;

            ExceptionEvent(IOException ioe) {
                this.ioe = ioe;
            }

            @Override
            Socket yieldSocket() throws IOException {
                throw ioe;
            }
        }

        private class HandshakeThreadFactory implements ThreadFactory {
            @Override
            public Thread newThread(Runnable r) {
                final String tName = "SSLHandshake-" + listenPort + "-" +
                    hsThreadCounter.getAndIncrement();
                final Thread t = new Thread(r, tName);
                t.setDaemon(true);
                return t;
            }
        }

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

        if (port != 0) {
            final ServerSocket ss = retrievePendingSocket(port);
            if (ss != null) {
                return ss;
            }
        }

        return commonCreateServerSocket(port);
    }

    /**
     * Prepares a server socket for eventual use by createServerSocket()
     * if the server socket will required background handshake support.
     */
    @Override
    public synchronized ServerSocket prepareServerSocket()
        throws IOException {

        /*
         * The peer authentication mechanism requires a prepared server
         * socket. Otherwise, no need to use this.
         */
        if (!doBackgroundAccept()) {
            return null;
        }

        final ServerSocket ss = createServerSocket(0);
        pendingSocketMap.put(ss.getLocalPort(), ss);
        return ss;
    }

    /**
     * Discards a server socket created by prepareServerSocket, but which
     * will not be used.
     */
    @Override
    public synchronized void discardServerSocket(ServerSocket ss) {
        /*
         * Don't rely on being able to look up the server socket by its
         * listen port, just in case it somehow got closed already.
         */
        final Set<Map.Entry<Integer, ServerSocket>> entries =
            pendingSocketMap.entrySet();
        for (Map.Entry<Integer, ServerSocket> entry : entries) {
            if (entry.getValue() == ss) {
                pendingSocketMap.remove(entry.getKey());
                break;
            }
        }

        try {
            ss.close();
        } catch (IOException ioe) /* CHECKSTYLE:OFF */ {
            /* Squash this */
        } /* CHECKSTYLE:ON */
    }

    /**
     * Factory method to configure SSF appropriately.
     *
     * @return an SSF or null if the factory has been disabled
     */
    public static SSLServerSocketFactory create(SSLControl sslControl,
                                                String name,
                                                int backlog,
                                                String portRange) {

        /*
         * NB: The ServerSocketFactory.isDisabled() value is not used here, as
         * SSL functionality requires socket factories.
         */

        if (portRange == null || PortRange.isUnconstrained(portRange)) {
            return new SSLServerSocketFactory(sslControl, name, backlog, 0, 0);
        }

        final List<Integer> range = PortRange.getRange(portRange);
        return new SSLServerSocketFactory(sslControl, name, backlog,
                                          range.get(0), range.get(1));
    }

    private boolean doBackgroundAccept() {
        return sslControl.peerAuthenticator() != null;
    }

    /**
     * Attempt to locate a previously created server socket.
     */
    private synchronized ServerSocket retrievePendingSocket(int port) {

        return pendingSocketMap.remove(port);
    }

    @Override
    protected ServerSocket instantiateServerSocket(int port)
        throws IOException {

        return new SSLInternalServerSocket(port);
    }

    @Override
    protected ServerSocket instantiateServerSocket(int port, int backlog1)
        throws IOException {

        return new SSLInternalServerSocket(port, backlog1);
    }

}
