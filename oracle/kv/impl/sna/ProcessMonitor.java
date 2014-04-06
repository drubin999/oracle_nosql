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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.util.CommonLoggerUtils;

/**
 * The class used to create, manage and restart processes.
 *
 * An instance of this object is created for each process to be managed.  The
 * caller is responsible for the command to be executed, setting a restart
 * count, and the initial creation of the process, for example:
 *
 *    List<String> command = new ArrayList<String>();
 *    command.add("/bin/sleep");
 *    command.add("1");
 *    ProcessMonitor monitor = new ProcessMonitor(command, -1);
 *    monitor.startProcess();
 *
 * Internal objects and threads are used to monitor and restart the process if
 * desired.  This will happen if the process terminates normally or abnormally.
 * A restart count of -1 means restart indefinitely.
 *
 * Note: exit/restart of a managed process will not change the command line
 * arguments used for starting it.  Those are cached in this object.  The major
 * thing affected is JVMParams such as arguments to Java or logging
 * configuration.  This behavior could be changed but it's simple at this time.
 *
 * The caller can explicitly stop the process which will not result in restart.
 *
 * If an excessive number of process restarts are detected the managed service
 * will eventually be permanently stopped.  The algorithm, which may need
 * tuning over time is:
 *   If there have been more than RESTART_MAX restarts in a period of
 *   RESTART_MILLIS milliseconds, stop restarting.
 */
public class ProcessMonitor {
    private final ReentrantLock lock;
    private Logger logger;
    private List<String> command;
    private ArrayList<Long> restarts;
    private String serviceName;
    private MonitorThread monitorThread;
    private IOThread ioThread;
    private int restartCount;
    private ProcessState state;
    private Process process;
    private int totalRestarts;
    private int exitCode;
    protected StringBuilder startupBuffer;

    /**
     * Start with 5 restarts in under 60 seconds as a problem.  TODO: tune this
     * to maybe have both short- and long-term triggers.  A long-term trigger
     * could just log a warning that something's wrong (e.g. 1 restart/hour
     * isn't horrible but probably means something is up).
     */
    private static final int RESTART_RESET=30;
    private static final int RESTART_MAX=5;
    private static final long RESTART_MILLIS=60*1000;

    enum ProcessState {
        DOWN, RUNNING, STOPPING
            }

    public ProcessMonitor(List<String> command,
                          int restartCount,
                          String serviceName,
                          Logger logger) {
        this.lock = new ReentrantLock();
        this.restartCount = restartCount;
        this.logger = logger;
        this.command = command;
        this.serviceName = serviceName;
        this.process = null;
        this.monitorThread = null;
        this.ioThread = null;
        this.state = ProcessState.DOWN;
        this.totalRestarts = 0;
        this.restarts = new ArrayList<Long>();
        this.exitCode = 0;
    }

    public void reset(List<String> newCommand,
                      String newServiceName) {
        this.command = newCommand;
        this.serviceName = newServiceName;
    }

    public void dontRestart() {
        lock.lock();
        restartCount = 0;
        lock.unlock();
    }

    public boolean canRestart() {
        return (restartCount != 0);
    }

    public boolean isRunning() {
        return (state != ProcessState.DOWN);
    }

    public int getExitCode() {
        return exitCode;
    }

    public void resetLogger(Logger logger1) {
        this.logger = logger1;
    }

    private void logFine(String msg) {
        if (logger != null) {
            logger.fine(serviceName + ": ProcessMonitor: " + msg);
        }
    }

    private void logInfo(String msg) {
        if (logger != null) {
            logger.info(serviceName + ": ProcessMonitor: " + msg);
        }
    }

    private void logSevere(String msg) {
        if (logger != null) {
            logger.severe(serviceName + ": ProcessMonitor: " + msg);
        }
    }

    protected void afterStart() {
        /* no-op */
    }

    protected void onRestart() {
        /* no-op */
    }

    protected void onExit(@SuppressWarnings("unused")
    			          int exitStatus) {
        /* no-op */
    }

    public void startProcess()
        throws IOException {

        lock.lock();
        try {
            if (state == ProcessState.DOWN) {
                ProcessBuilder builder = new ProcessBuilder(command);
                builder.redirectErrorStream(true);
                process = builder.start();
                state = ProcessState.RUNNING;
                logInfo("startProcess");
                ioThread = new IOThread("SNA.io." + serviceName);
                ioThread.start();
                monitorThread =
                    new MonitorThread("SNA.monitor." + serviceName, true);
                monitorThread.start();
            }
        } finally {
            lock.unlock();
        }
        afterStart();
    }

    /**
     * Stop a running process.  The process may still be running or it may have
     * exited; this method cleans up the object and threads in both cases.
     * This is complicated by the need to synchronize access from both the
     * MonitorThread and the owning thread.  If called by the owning thread the
     * process will not be restarted by the MonitorThread.  That case is
     * simpler.  Trickier races occur if the MonitorThread is in the middle of
     * stopping/restarting the process and the owning thread calls.  There is
     * one window where the lock allows the owning thread in.  If that occurs
     * the restartCount will have been set to 0 causing the MonitorThread to
     * simply exit if it is not interrupted.
     */
    public void stopProcess(boolean isMonitor)
        throws InterruptedException {

        if (!isMonitor) {
            logInfo("stopProcess");
        }
        lock.lock();

        /* Setting restartCount to 0 ensures that the process won't restart. */
        if (!isMonitor) {
            restartCount = 0;
        }

        /**
         * If the owning thread is stopping the process and the monitor calls,
         * just return.  The monitor will eventually exit.  If the monitor is
         * stopping the process and the owning thread calls, continue on so
         * that the process is not restarted and the monitor is killed.
         */
        if (state == ProcessState.DOWN ||
            (state != ProcessState.RUNNING && isMonitor)) {
            /* The process is already stopping or down. */
            lock.unlock();
            return;
        }

        state = ProcessState.STOPPING;
        if (process != null) {
            process.destroy();
            /* don't null the process object until after joins, below */
        }
        lock.unlock();
        /* The lock is unlocked, allowing threads to exit. */
        try {

            /**
             * No lock is held but the only threads that will set monitorThread
             * and ioThread to null are the object owner and the MonitorThread
             * itself, and the Thread.join() synchronizes that race.
             */
            if (monitorThread != null && !isMonitor) {
                monitorThread.join();
                monitorThread = null;
            }

            if (ioThread != null) {
                ioThread.join();
                ioThread = null;
            }
        } catch (InterruptedException e) {
            logInfo("Exception in stopProcess");
            if (Thread.interrupted() && isMonitor) {
                /* Rethrow if MonitorThread was interrupted. */
                throw e;
            }
        }
        lock.lock();
        state = ProcessState.DOWN;
        process = null;
        lock.unlock();
    }

    public void restartProcess()
        throws IOException {

        logInfo("restartProcess called, totalRestarts is " + totalRestarts +
                ", restartCount is " + restartCount);

        lock.lock();
        try {
            if (restartCount != 0) {
                /* This will lock recursively. */
                startProcess();
                restarts.add(System.currentTimeMillis());
                ++totalRestarts;
                if (restartCount > 0) {
                    --restartCount;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Wait for the managed process to exit to synchronize shutdown.  This is
     * called by the ProcessServiceManager when a service is shut down cleanly.
     */
    public boolean waitProcess(long millis)
        throws InterruptedException {

        boolean retval = true;

        if (monitorThread != null) {
            logFine("waiting for MonitorThread");
            monitorThread.join(millis);
            if (monitorThread.isAlive()) {
                retval = false;
            }
            monitorThread = null;
        }

        if (ioThread != null && retval == true) {
            logFine("waiting for IOThread");
            ioThread.join(millis);
            if (ioThread.isAlive()) {
                retval = false;
            }
            ioThread = null;
        }
        return retval;
    }

    /**
     * Forcibly terminate the process.  This should only be called if an
     * organized stop fails.
     */
    public void destroyProcess() {
        lock.lock();
        Process p = process;
        lock.unlock();
        if (p != null) {
            p.destroy();
        }
    }

    /**
     * The class responsible for monitoring a Process and restarting it on
     * exit.  A new instance of this thread/object is created on each restart.
     */
    class MonitorThread extends Thread {
        private final boolean useExitCode;

        private MonitorThread(String name, boolean useExitCode) {
            super(name);
            this.useExitCode = useExitCode;
        }

        /**
         * Should the process be restarted? Default is yes.
         */
        private boolean okToRestart(int exitStatus) {
            if (useExitCode &&
                ! ProcessExitCode.needsRestart(exitStatus)) {
                logInfo("exit code:" + exitStatus);
                return false;
            }

            logInfo("Process restart requested; exit code:" +
                    exitStatus +
                    ((exitStatus == ProcessExitCode.RESTART_OOME.getValue()) ?
                       " Process experienced an OOME." : ""));
            if (restartCount == 0) {
                logInfo("restart count is 0");
                return false;
            }
            return checkExcessiveRestarts();
        }

        /**
         * Determine if there have been too many restarts of this process
         * based on algorithm vs number.
         *
         * Return true if it is OK to restart (not too many restarts).
         * Return false (bad) if there have been too many restarts.
         */
        private boolean checkExcessiveRestarts() {
            boolean ret = true;
            if (restarts.size() >= RESTART_MAX) {
                long last = restarts.get(restarts.size()-1);
                long first = restarts.get(restarts.size()-RESTART_MAX);
                if (last - first < RESTART_MILLIS) {
                    logSevere("excessive restarts (" + restarts +
                              "), disabling service");
                    dontRestart();
                    ret = false;
                }
            }
            if (restarts.size() >= RESTART_RESET) {
                /* Reset the list to avoid unbound growth */
                restarts = new ArrayList<Long>();
            }
            return ret;
        }

        @Override
        public void run() {
            try {
                assert (process != null);
                exitCode = process.waitFor();
                logInfo("exited, exit code: " + exitCode);

                /**
                 * Let the IOThread exit -- it may have useful things to say if
                 * the exit occurred during startup.
                 */
                if (ioThread != null) {
                    ioThread.join();
                    ioThread = null;
                }

                /**
                 * If the process was explicitly stopped the stop and restart
                 * will be no-ops because the process state will be DOWN and
                 * the restart count will have been zeroed.
                 */
                stopProcess(true);

                /**
                 * Restart or not?  Default is to restart.
                 * TODO: manufacture ERROR_* ServiceStatus for the service
                 */
                if (okToRestart(exitCode)) {
                    onRestart();
                    restartProcess();
                } else {
                    onExit(exitCode);
                    logInfo("not restarting");
                }
            } catch (Exception e) {
                String msg = "Unexpected exception in MonitorThread: " +
                    e + CommonLoggerUtils.getStackTrace(e);
                logSevere(msg);
            }
        }
    }

    /**
     * The class responsible for handling output from a managed Process.  A new
     * instance of this thread/object is created on each restart.
     */
    class IOThread extends Thread {
        public IOThread(String name) {
            super(name);
        }

        @Override
        public void run() {
            try {
                boolean startupOK = false;

                /**
                 * Small delay to start to give the process a chance to do
                 * something.
                 */
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ignored) {}

                /**
                 * Process may have already been stopped and nulled.
                 */
                startupBuffer = new StringBuilder(512);
                logFine("IOThread initializing startup buffer");
                BufferedReader reader = null;
                lock.lock();
                if (process != null) {
                    reader = new BufferedReader
                        (new InputStreamReader(process.getInputStream()));
                } else {
                    logInfo("IOthread: no process, exiting");
                    lock.unlock();
                    return;
                }
                lock.unlock();
                for (String line = reader.readLine();
                     line != null;
                     line = reader.readLine()) {

                    /**
                     * Logging output in the new process should only be
                     * generated by the SNA.  Once the real service takes over
                     * it will log to its own files.
                     */
                    logInfo(line);

                    if (line.contains(ManagedService.STARTUP_OK)) {
                        startupOK = true;
                        startupBuffer = null;
                        logFine("IOThread clearing startup buffer");
                    }
                    if (!startupOK) {
                        startupBuffer.append("\n" + line);
                    }
                }
            } catch (Exception e) {
                logInfo("IOThread exception: " +
                        e.getMessage());
            }
            logInfo("IOThread exiting");
        }

        void closeInput()
            throws IOException {

            /* Provoke an IO exception to cause the thread to exit. */
            process.getInputStream().close();
        }
    }
}
