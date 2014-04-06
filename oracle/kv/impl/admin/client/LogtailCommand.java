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

package oracle.kv.impl.admin.client;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.logging.LogRecord;

import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.monitor.Tracker;
import oracle.kv.impl.monitor.TrackerListenerImpl;
import oracle.kv.impl.util.LogFormatter;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellException;

class LogtailCommand extends ShellCommand {
    private RemoteException threadException = null;
    private boolean logTailThreadGo;
    private LogListener lh;
    CommandServiceAPI cs;
    CommandShell cmd;

    LogtailCommand() {
        super("logtail", 4);
    }

    @Override
    public String execute(String[] args, Shell shell)
        throws ShellException {

        if (args.length != 1) {
            shell.badArgCount(this);
        }
        cmd = (CommandShell) shell;
        cs = cmd.getAdmin();
        try {
            shell.getOutput().println("(Press <enter> to interrupt.)");
            /*
             * Run the tail in a separate thread, so that the current thread
             * can terminate it when required.
             */
            Thread t = new Thread("logtail") {
                @Override
                public void run() {
                    try {
                        logtailWorker();
                    } catch (RemoteException e) {
                        threadException = e;
                    }
                }
            };

            threadException = null;
            logTailThreadGo = true;
            t.start();

            /* Wait for the user to type <enter>. */
            shell.getInput().readLine();
            synchronized (this) {
                logTailThreadGo = false;
                t.interrupt();
            }
            t.join();
            if (threadException != null) {
                return "Exception from logtail: " +
                    threadException.getMessage();
            }
        } catch (RemoteException re) {
            cmd.noAdmin(re);
        } catch (IOException ioe) {
            return "Exception reading input during logtail";
        } catch (InterruptedException ignored) {
        }
        return "";
    }

    @Override
    public String getCommandDescription() {
        return "Monitors the store-wide log file until interrupted by an " +
               "\"enter\"" + eolt + "keypress.";
    }

    private static class LogListener extends TrackerListenerImpl {

        private static final long serialVersionUID = 1L;

        Object lockObject;

        LogListener(long interestingTime, Object lockObject)
            throws RemoteException {

            super(interestingTime);
            this.lockObject = lockObject;
        }

        @Override
                public void notifyOfNewEvents() {
            synchronized (lockObject) {
                lockObject.notify();
            }
        }
    }

    private void logtailWorker()
        throws RemoteException {

        long logSince = 0;

        lh = new LogListener(logSince, this);
        cs.registerLogTrackerListener(lh);
        LogFormatter lf = new LogFormatter(null);

        boolean registered = true;
        try {
            while (true) {
                Tracker.RetrievedEvents<LogRecord> logEventsContainer;
                List<LogRecord> logEvents;

                synchronized (this) {
                    while (true) {
                        /* If we're exiting, stop the listener. */
                        if (logTailThreadGo == false) {
                            try {
                                cs.removeLogTrackerListener(lh);
                                registered = false;
                            } finally {
                                UnicastRemoteObject.unexportObject(lh, true);
                                lh = null;
                            }
                            return;
                        }

                        logEventsContainer = cs.getLogSince(logSince);
                        logEvents = logEventsContainer.getEvents();
                        if (logEvents.size() != 0) {
                            break;
                        }
                        /* Wait for LogListener.onNewEventsPresent. */
                        try {
                            this.wait();
                        } catch (InterruptedException e) {
                        }
                    }
                    /*
                     * Remember the timestamp of the last record we retrieved;
                     * this will become the "since" argument in the next
                     * request.
                     */
                    logSince =
                        logEventsContainer.getLastSyntheticTimestamp();
                    /*
                     * Move the interesting time up, so older items can be
                     * deleted.
                     */
                    lh.setInterestingTime(logSince);
                }

                /* Print the records we got. */

                for (LogRecord lr : logEvents) {
                    cmd.getOutput().print(lf.format(lr));
                }
            }
        } finally {

            /*
             * Guard against exception paths that don't unregister. Failing
             * to unregister the listener will make the log queue continue to
             * grow.
             */
            if (registered) {
                cs.removeLogTrackerListener(lh);
            }
        }
    }
}
