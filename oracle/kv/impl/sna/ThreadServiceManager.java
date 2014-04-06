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

package oracle.kv.impl.sna;

/**
 * Implementation of ServiceManager that uses threads.
 */
public class ThreadServiceManager extends ServiceManager implements Runnable {

    private Thread thread;

    public ThreadServiceManager(StorageNodeAgent sna,
                                ManagedService service) {
        super(sna, service);
    }

    /**
     * This method must call the service as if it were invoked in a separate
     * process, using main().
     */
    @Override
    public void run() {
        String[] args = service.createArgs();
        ManagedService.main(args);
        logger.info("Thread returning for service " + service.getServiceName());
    }

    @Override
    public void start()
        throws Exception {

        thread = new Thread(this);
        thread.start();
        logger.fine("ThreadServiceManager started thread");
        notifyStarted();
    }

    /**
     * For threads stop() is the same as waitFor().  Interrupt() may not
     * work but it should.
     */
    @Override
    public void stop() {
        thread.interrupt();
        waitFor(0);
    }

    @Override
    public void waitFor(int millis) {
        try {
            thread.join(millis);
            if (thread.isAlive()) {
                logger.info("Thread join timed out for " +
                            service.getServiceName() + ", timeout millis: " +
                            millis);
            } else {
                logger.info("Joined thread for " + service.getServiceName());
            }
        } catch (InterruptedException ie) {
            logger.info("Thread wait for service was interrupted");
        }
    }

    /**
     * Nothing to do for threads -- they won't restart.
     */
    @Override
    public void dontRestart() {
    }

    /**
     * With threads it is hard to know for sure, but if we have a thread then
     * we will assume true.
     */
    @Override
    public boolean isRunning() {
        return thread != null;
    }
}
