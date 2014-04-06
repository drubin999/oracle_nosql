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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * TimeoutSocket provides an efficient implementation of socket read timeouts.
 * The more obvious approach is to have the ClientSocketFactory.createSocket
 * method create a vanilla java.net.Socket, and specify timeouts with
 * Socket.setSoTimeout(). This turned out to be cpu intensive, because the
 * implementation uses a poll system call to enforce the timeout.
 * <p>
 * Instead, the TimeoutSocket provides an isActive() method that lets a timer
 * thread query the socket to see if a read has taken too long. If the socket
 * has a pending read, but has been inactive for longer than the timeout
 * period, the socket is forcibly closed. Timeout monitoring is only in effect
 * if this socket is properly registered with the
 * ClientSocketFactor.TimeoutTask.
 *
 * @see ClientSocketFactory
 */
public class TimeoutSocket extends Socket {

    /*
     * Denotes read response activity associated with the socket. It's set each
     * time a read operation is successfully completed on the socket's input
     * stream.
     */
    private volatile boolean readActivity;

    /*
     * Tracks the number of pending read operations.  Timeouts only take effect
     * if a read operation is underway: the socket will be left open if it is
     * idle.
     */
    private final AtomicInteger pendingReads = new AtomicInteger(0);

    /*
     * The timeout associated with the socket. A value of zero indicates no
     * timeout.
     */
    private volatile int timeoutMs;

    /* The "time" of the last check for read activity on the socket. */
    private long lastCheckMs = 0L;

    /** Set to true if the socket has been closed due to a timeout. */
    private volatile boolean closedTimeout;

    /**
     * The caller should register this socket with a TimeoutTask so it's
     * monitored for inactivity. See ClientSocketFactory.TimeoutTask.register
     */
    public TimeoutSocket(int timeoutMs) {
        this.timeoutMs = timeoutMs;
        readActivity = true;
    }

    /**
     * Used to modify the timeout associated with the socket.
     *
     * @param timeoutMs the new timeout value
     */
    public void setTimeoutMs(int timeoutMs) {
        this.timeoutMs = timeoutMs;
        /* Ensure that the next tick resets the time and counter. */
        readActivity = true;
    }

    @Override
    public InputStream getInputStream()
        throws IOException {

        return new TimeoutInputStream(super.getInputStream());
    }

    /**
     * All reads done with this socket with are wrapped so we can keep track of
     * whether there have been any reads. Timeouts are only enforced during
     * reads.
     */
    private class TimeoutInputStream extends FilterInputStream {
        private int bytesRead;

        TimeoutInputStream(InputStream in) {
            super(in);
        }

        @Override
        public int read()
            throws IOException {

            pendingReads.incrementAndGet();
            try {
                synchronized (this) {
                    final int result = super.read();
                    if (result > 0) {
                        readActivity = true;
                        bytesRead += result;
                    }
                    return result;
                }
            } catch (SocketException e) {
                checkClosedTimeout();
                throw e;
            } finally {
                pendingReads.decrementAndGet();
            }
        }

        /**
         * Throw SocketTimeoutException if the socket has been closed due to a
         * timeout.
         */
        private void checkClosedTimeout()
            throws SocketTimeoutException {

            if (closedTimeout) {
                final int br;
                synchronized (this) {
                    br = bytesRead;
                }
                final SocketTimeoutException e = new SocketTimeoutException(
                    "Read interrupted after stream transferred " + br +
                    " bytes because inactive socket " + TimeoutSocket.this +
                    " timed out and was forcibly closed, timeout: " +
                    timeoutMs + " ms");
                e.bytesTransferred = br;
                throw e;
            }
        }

        @Override
        public int read(byte[] b)
            throws IOException {

            pendingReads.incrementAndGet();
            try {
                synchronized (this) {
                    final int result = super.read(b);
                     if (result > 0) {
                         readActivity = true;
                         bytesRead += result;
                     }
                     return result;
                }
            } catch (SocketException e) {
                checkClosedTimeout();
                throw e;
            } finally {
                pendingReads.decrementAndGet();
            }
        }

        @Override
        public int read(byte[] b, int off, int len)
            throws IOException {

            pendingReads.incrementAndGet();
            try {
                synchronized (this) {
                    final int result = super.read(b, off, len);
                    if (result > 0) {
                        readActivity = true;
                        bytesRead += result;
                    }
                    return result;
                }
            } catch (SocketException e) {
                checkClosedTimeout();
                throw e;
            } finally {
                pendingReads.decrementAndGet();
            }
        }
    }

    @Override
    public synchronized void close()
        throws IOException {

        super.close();
        readActivity = false;
    }

    private void resetActivityCounter(long timeMs) {
        lastCheckMs = timeMs;
        readActivity = false;
    }

    /**
     * Method invoked by the timeout thread to check on the socket on a
     * periodic basis. Note that the time that is passed in is a "pseudo" time
     * that is only meaningful for calculating time differences.
     *
     * @param timeMs the pseudo time
     * @param logger is provided if possible. There may be times when the
     * logger is not available, mainly in a test setting where multiple
     * services are being created within a single process, and those services
     * are being opened and closed, so check if the logger is null.
     *
     * @return true if the socket is active, false if it isn't and has been
     * closed
     */
    public boolean isActive(long timeMs, Logger logger) {

        if (isClosed()) {
            /* The socket is closed */
            return false;
        }

        if (!isConnected()) {
            /* Not yet connected, wait for it to be connected. */
            return true;
        }

        if (readActivity) {
            resetActivityCounter(timeMs);
            return true;
        }

        if ((timeoutMs == 0) ||
            /* Don't timeout if the socket is idle */
            pendingReads.get() == 0 ||
            (timeMs - lastCheckMs) < timeoutMs) {
            return true;
        }

        /*
         * No activity, force the socket closed thus generating an
         * AsynchronousCloseException in the read/write threads. Note that when
         * multiple services are instantiated in a single process, which only
         * happens in test cases, because there is a single static logger in
         * the ClientServiceLibrary, the provided logger may come from a
         * different service, and the component id prefix on the logging
         * message may be confusing. We toString() the socket so the message is
         * as clear as possible about the target socket.
         */
        final String message = "Inactive socket " + this +
            " timed out and was forcibly closed, timeout: " + timeoutMs +
            " ms";

        if (logger == null) {
            System.err.println(message);
        } else {
            logger.info(message);
        }

        try {
            closedTimeout = true;
            close();
        } catch (IOException e) {
            if (logger != null) {
                logger.log(Level.FINEST, "Exception on close", e);
            }
        }
        return false;
    }
}
