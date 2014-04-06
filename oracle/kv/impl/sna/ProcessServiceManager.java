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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import oracle.kv.impl.measurement.ProxiedServiceStatusChange;
import oracle.kv.impl.mgmt.MgmtAgent;
import oracle.kv.impl.monitor.AgentRepository;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;

/**
 * Implementation of ServiceManager that uses processes.
 */
public class ProcessServiceManager extends ServiceManager {
    private AgentRepository agentRepository;
    private ProcessMonitor monitor;
    private final MgmtAgent mgmtAgent;
    private static String pathToJava;
    private final StorageNodeAgent sna;

    public ProcessServiceManager(StorageNodeAgent sna,
                                 ManagedService service) {
        super(sna, service);
        this.sna = sna;
        this.mgmtAgent = sna.getMgmtAgent();
        this.agentRepository = null;
        monitor = null;
        registered(sna);
    }

    class ServiceProcessMonitor extends ProcessMonitor {
        private final ServiceManager mgr;

        public ServiceProcessMonitor(ServiceManager mgr,
                                     List<String> command) {
            super(command, -1, service.getServiceName(), logger);
            this.mgr = mgr;
        }

        private void generateStatusChange(ServiceStatus status) {
            if (agentRepository != null) {
                ResourceId rid = getService().getResourceId();
                ProxiedServiceStatusChange sc =
                    new ProxiedServiceStatusChange(rid, status);
                agentRepository.add(sc);
                mgmtAgent.proxiedStatusChange(sc);
            }
        }

        @Override
        protected void onExit(int exitCode) {

            service.setStartupBuffer(startupBuffer);
            final ResourceId rId = service.getResourceId();
            if (rId instanceof RepNodeId) {
                if (sna.getMasterBalanceManager() != null) {
                    sna.getMasterBalanceManager().noteExit((RepNodeId)rId);
                }
            }

            /**
             * Generate a status change for the service.  If the exit code is 0
             * proxy a STOPPED state because that happens on admin-generated
             * shutdown and may not have been picked up by the monitor.
             */
            if (exitCode != 0) {
                generateStatusChange(ServiceStatus.ERROR_NO_RESTART);
            } else {
                generateStatusChange(ServiceStatus.STOPPED);
            }
        }

        @Override
        protected void onRestart() {

            final ResourceId rId = service.getResourceId();
            if (rId instanceof RepNodeId) {
               sna.getMasterBalanceManager().noteExit((RepNodeId)rId);
            }

            generateStatusChange(ServiceStatus.ERROR_RESTARTING);
            service.resetHandles();
            service.resetParameters(false);
            if (service.resetOnRestart()) {
                mgr.reset();
            }
        }

        @Override
        protected void afterStart() {
            mgr.notifyStarted();
        }
    }

    @Override
    public void registered(StorageNodeAgent sna1) {
        if (agentRepository == null) {
            agentRepository = (sna1.getMonitorAgent() != null) ?
                sna1.getMonitorAgent().getAgentRepository() : null;
        }
    }

    @Override
    public void start()
        throws Exception {

        List<String> command = createExecArgs();
        if (logger != null) {
            logger.info("Executing process with arguments: " + command);
        }
        monitor = new ServiceProcessMonitor(this, command);
        monitor.startProcess();
    }

    /**
     * Terminate managed process with prejudice.
     */
    @Override
    public void stop() {

        try {
            if (monitor != null) {
                monitor.stopProcess(false);
            }
        } catch (InterruptedException ie) {
        } finally {
            monitor.destroyProcess();
            try {
                monitor.waitProcess(0);
            } catch (InterruptedException ignored) {
                /* nothing to do if this was interrupted */
            }
        }
    }

    @Override
    public void waitFor(int millis) {

        try {
            if (monitor != null) {
                if (!monitor.waitProcess(millis)) {
                    logger.info("Service did not exiting cleanly in " + millis
                                + " milliseconds, it will be killed");
                    stop();
                }
            }
        } catch (InterruptedException ie) {
        }
    }

    @Override
    public void dontRestart() {

        if (monitor != null) {
            monitor.dontRestart();
        }
    }

    @Override
    public boolean isRunning() {
        if (monitor != null) {
            return monitor.isRunning();
        }
        return false;
    }

    /**
     * Reset ProcessMonitor command using the current service info.
     */
    @Override
    public void reset() {
        List<String> command = createExecArgs();
        monitor.reset(command, service.getServiceName());
    }

    /**
     * Force is OK with processes.
     */
    @Override
    public boolean forceOK(boolean force) {
        return force;
    }

    @Override
    public void resetLogger(Logger logger1) {
        this.logger = logger1;
        monitor.resetLogger(logger);
    }

    /**
     * Terminate managed process without any bookkeeping -- this simulates
     * a random exit.  The process should be restarted by the ProcessMonitor.
     */
    void destroy() {
        if (monitor != null) {
            monitor.destroyProcess();
            try {
                monitor.waitProcess(0);
            } catch (InterruptedException ignored) {
                /* nothing to do if this was interrupted */
            }
        }
    }

    /**
     * Tiny test class to test exec of java.
     */
    public static class TestJavaExec {
        public static final int EXITCODE = 75;
        public static void main(String args[]) {
            System.exit(EXITCODE);
        }
    }

    /**
     * Try exec'ing "java.home"/bin/java.  If it works, use it.  If not,
     * return "java" and rely on the PATH environment variable.
     */
    private String findJava() {
        String home = System.getProperty("java.home");
        String cp = System.getProperty("java.class.path");
        String path = home + File.separator + "bin" +
            File.separator + "java";
        String execString = path;
        if (cp != null) {
            execString = execString + " -cp " + cp;
        }
        execString = execString + " " + getClass().getName() + "$TestJavaExec";
        try {
            Process process = Runtime.getRuntime().exec(execString);
            process.waitFor();
            if (process.exitValue() == TestJavaExec.EXITCODE) {
                return path;
            }
        } catch (Exception e) {
            logger.info("Unable to exec test process: " +
                        execString + ", exception: " + e);
        }
        return "java";
    }

    /**
     * Determine the path to the JVM used to execute this process and use it
     * for execution of new JVMs.  Test it out with a tiny program.
     */
    private synchronized String getPathToJava() {
        if (pathToJava == null) {
            pathToJava = findJava();
            logger.info("Using java program: " + pathToJava +
                        " to execute managed processes");
        }
        return pathToJava;
    }

    /**
     * TODO: think about inferred arguments that could be added based on
     * heap size.  E.g.:
     * < 4G -d32
     * > 4G -d64 if available, -XX:+UseCompressedOops
     * > 32G -- they should just figure it out.
     */
    private void addJavaMiscArgs(List<String> command,
                                 String jvmArgs) {

        String miscParams = "";
        /* A service may add its own default arguments */
        if (service.getDefaultJavaArgs(jvmArgs) != null) {
            miscParams = service.getDefaultJavaArgs(jvmArgs);
        }
        if (jvmArgs != null) {
            miscParams += " " + jvmArgs;
        }
        if (miscParams.length() != 0) {
            String[] args = miscParams.trim().split("\\s+");
            for (String arg : args) {

                /**
                 * Replace leading/trailing quotes that may have ended up in
                 * the command.  These will cause problems.
                 */
                arg = arg.replaceAll("^\"|\"$", "");
                arg = arg.replaceAll("^\'|\'$", "");
                command.add(arg);
            }
        }
    }

    private void addJavaExtraArgs(List<String> command) {
        final String jvmExtraArgs =
            System.getProperty("oracle.kv.jvm.extraargs");
        if (jvmExtraArgs != null) {
            for (String arg : splitExtraArgs(jvmExtraArgs)) {
                command.add(arg);
            }
            command.add("-Doracle.kv.jvm.extraargs=" + jvmExtraArgs);
        }
    }

    private static String[] splitExtraArgs(String extraArgs) {
        return extraArgs.split(";");
    }

    private void addLoggingArgs(List<String> command,
                                String loggingConfig) {

        if (loggingConfig == null || loggingConfig.length() == 0) {
            return;
        }
        String logConfigFile = service.createLoggingConfigFile(loggingConfig);
        if (logConfigFile != null) {
            command.add("-Djava.util.logging.config.file=" + logConfigFile);
        }
    }

    private boolean addAssertions(List<String> command) {
        command.add("-ea");
	return true;
    }

    private List<String> createExecArgs() {

        List<String> command = new ArrayList<String>();

        String customStartupPrefix = sna.getCustomProcessStartupPrefix();
        if ((customStartupPrefix != null) && !customStartupPrefix.isEmpty()){
            command.add(customStartupPrefix);
        }

        command.add(getPathToJava());
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));

        /**
         * If Java arguments and/or logging configuration are present, add
         * them.
         */
        String jvmArgs = service.getJVMArgs();
        String loggingConfig = service.getLoggingConfig();
        addJavaMiscArgs(command, jvmArgs);
        addJavaExtraArgs(command);
	assert(addAssertions(command));
        if (loggingConfig != null) {
            /*
             * Logging configuration.  This will create the logging config file.
             */
            addLoggingArgs(command, loggingConfig);
        } else {
            String logConfig =
                System.getProperty("java.util.logging.config.file");
            if (logConfig != null) {
                command.add("-Djava.util.logging.config.file=" + logConfig);
            }
        }
        return service.addExecArgs(command);
    }

}
