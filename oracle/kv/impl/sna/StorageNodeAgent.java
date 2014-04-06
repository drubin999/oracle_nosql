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
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.mgmt.MgmtAgent;
import oracle.kv.impl.mgmt.MgmtAgentFactory;
import oracle.kv.impl.mgmt.jmx.JmxAgent;
import oracle.kv.impl.monitor.AgentRepository;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterTracker;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.security.ConfigurationException;
import oracle.kv.impl.security.SecureProxy;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.login.LoginUpdater;
import oracle.kv.impl.security.login.TrustedLoginHandler;
import oracle.kv.impl.security.login.TrustedLoginImpl;
import oracle.kv.impl.sna.masterBalance.MasterBalanceManager;
import oracle.kv.impl.sna.masterBalance.MasterBalanceManager.SNInfo;
import oracle.kv.impl.sna.masterBalance.MasterBalanceManagerInterface;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.FileUtils;
import oracle.kv.impl.util.ServiceStatusTracker;
import oracle.kv.impl.util.ServiceUtils;
import oracle.kv.impl.util.VersionUtil;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RMISocketPolicy;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryPair;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.registry.RegistryUtils.InterfaceType;
import oracle.kv.impl.util.registry.ServerSocketFactory;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * The class that does the work of the Storage Node Agent (SNA).  It is
 * mostly controlled by StorageNodeAgentImpl.
 */
public final class StorageNodeAgent {

    /* External commands, for "java -jar" usage. */
    public static final String START_COMMAND_NAME = "start";
    public static final String START_COMMAND_DESC =
        "starts StorageNodeAgent (and if configured, store) in kvroot";
    public static final String STOP_COMMAND_NAME = "stop";
    public static final String STOP_COMMAND_DESC =
        "stops StorageNodeAgent and services related to kvroot";
    public static final String RESTART_COMMAND_NAME = "restart";
    public static final String RESTART_COMMAND_DESC =
        "combines stop and start commands into one";
    public static final String CONFIG_FLAG = "-config";
    public static final String COMMAND_ARGS =
        CommandParser.getRootUsage() + " " +
        CommandParser.optional(CONFIG_FLAG + " <bootstrapFileName>");
    public static final String DEFAULT_CONFIG_FILE = "config.xml";
    public static final String DEFAULT_SECURITY_DIR = "security";
    public static final String DEFAULT_SECURITY_FILE = "security.xml";

    /*
     * Additional hidden args. The -shutdown flag is added by "java -jar" when
     * stop and restart commands are used.
     */
    public static final String SHUTDOWN_FLAG = "-shutdown";
    public static final String THREADS_FLAG = "-threads";
    public static final String LINK_COMMAND = "ln";

    private final StorageNodeAgentImpl snai;
    private StorageNodeAgentInterface exportableSnaif;
    /*
     * Many of these members are technically final once set but they cannot be
     * set in the constructor.
     */
    private String bootstrapDir;
    private String bootstrapFile;
    private File securityDir;
    private String securityConfigFile;
    private BootstrapParams bp;
    private SecurityParams sp;
    private File kvRoot;
    private File snRoot;
    private File kvConfigPath;
    private Registry registry;
    private String snaName;
    private StorageNodeId snid;
    private int serviceWaitMillis;
    private int repnodeWaitSecs;
    private int maxLink;
    private int linkExecWaitSecs;
    private boolean isWindows;
    private Boolean isLoopback;
    private boolean isVerbose;

    /* SNP Information cached for reporting to mgmt. */
    int capacity;
    int logFileLimit;
    int logFileCount;
    int numCPUs;
    int memoryMB;
    String mountPointsString;

    /**
     * The service status associated with the SNA. Note that only the following
     * subset of states: STARTING, WAITING_FOR_DEPLOY, and RUNNING are
     * currently relevant to the SNA.
     */
    private boolean createBootstrapAdmin;
    private ServiceStatusTracker statusTracker;
    private boolean useThreads;
    private Logger logger;
    private final Map<String, ServiceManager> repNodeServices;
    private ServiceManager adminService;
    private MonitorAgentImpl monitorAgent;
    private MgmtAgent mgmtAgent;
    private TrustedLoginImpl trustedLogin;
    private SNASecurity snaSecurity;
    private final ParameterTracker snParameterTracker;
    private final ParameterTracker globalParameterTracker;

    /* The master Balance manager component in the SNA. */
    private MasterBalanceManagerInterface masterBalanceManager;

    private String customProcessStartupPrefix;
    /**
     * Test only.  restart*Hook exists to "fake" exiting the process at various
     * times to test how things go when it is restarted.  It works by creating
     * a new SNA and shutting down the running one.
     */
    private TestHook<StorageNodeAgent> restartRNHook;
    private TestHook<StorageNodeAgent> restartAdminHook;
    private TestHook<StorageNodeAgent> stopRNHook;

    /* Hook to inject failures at different points in SN execution */
    public static TestHook<Integer> FAULT_HOOK;

    /**
     * A constructor that allows the caller to indicate that the bootstrap
     * admin service should or should not be started.
     */
    StorageNodeAgent(StorageNodeAgentImpl snai, boolean createBootstrapAdmin) {

        this.snai = snai;
        bootstrapDir = null;
        bootstrapFile = null;
        securityDir = null;
        securityConfigFile = null;
        kvRoot = null;
        snRoot = null;
        kvConfigPath = null;
        registry = null;
        snaName = GlobalParams.SNA_SERVICE_NAME;
        snid = new StorageNodeId(0);
        logger = null;
        statusTracker = null;
        mgmtAgent = null;
        useThreads = false;
        this.createBootstrapAdmin = createBootstrapAdmin;
        isLoopback = null;
        snParameterTracker = new ParameterTracker();
        globalParameterTracker = new ParameterTracker();

        repNodeServices = new HashMap<String, ServiceManager>();
        adminService = null;
        final String os = System.getProperty("os.name");
        if (os.indexOf("Windows") != -1) {
            isWindows = true;
        } else {
            isWindows = false;
        }
    }

    MasterBalanceManagerInterface getMasterBalanceManager() {
        return masterBalanceManager;
    }

    void setRNTestHook(TestHook<StorageNodeAgent> hook) {
        restartRNHook = hook;
    }

    void setStopRNTestHook(TestHook<StorageNodeAgent> hook) {
        stopRNHook = hook;
    }

    void setAdminTestHook(TestHook<StorageNodeAgent> hook) {
        restartAdminHook = hook;
    }

    /**
     * For testing.
     */
    void setRepNodeWaitSecs(int seconds) {
        repnodeWaitSecs = seconds;
    }

    class SNAParser extends CommandParser {
        private boolean shutdown;

        public SNAParser(String[] args) {
            super(args);
            shutdown = false;
        }

        public boolean getShutdown() {
            return shutdown;
        }

        @Override
        protected void verifyArgs() {
            if (getRootDir() == null) {
                missingArg(ROOT_FLAG);
            } else {
                File rtDir = new File(getRootDir());
                if (!rtDir.isDirectory()) {
                    System.err.println
                        ("Root directory " + rootDir + " does not exist or " +
                         "is not a directory");
                    System.exit(2);
                }
            }
            if (bootstrapFile == null) {
                bootstrapFile = DEFAULT_CONFIG_FILE;
            }
            if (securityConfigFile == null) {
                securityConfigFile = DEFAULT_SECURITY_FILE;
            }
            isVerbose = getVerbose();
        }

        @Override
        protected boolean checkArg(String arg) {
            if (arg.equals(CONFIG_FLAG)) {
                bootstrapFile = nextArg(arg);
                return true;
            }
            if (arg.equals(SHUTDOWN_FLAG)) {
                shutdown = true;
                return true;
            }
            if (arg.equals(THREADS_FLAG)) {
                useThreads = true;
                return true;
            }
            return false;
        }

        @Override
        public void usage(String errorMsg) {
            if (errorMsg != null) {
                System.err.println(errorMsg);
            }
            System.err.println(KVSTORE_USAGE_PREFIX + " <" +
                               START_COMMAND_NAME + " | " +
                               STOP_COMMAND_NAME + " | " +
                               RESTART_COMMAND_NAME + ">\n\t" +
                               COMMAND_ARGS);

            // TODO: why not System.exit?
            throw new IllegalArgumentException
                ("Could not parse Storage Node Agent arguments");
        }
    }

    boolean parseArgs(String args[]) {

        SNAParser parser = new SNAParser(args);
        parser.parseArgs();
        bootstrapDir = parser.getRootDir();

        if (parser.getShutdown()) {
            return true;
        }
        return false;
    }

    /**
     * Using the bootstrap config file, attempt to stop the SNA
     * process that it using it.
     */
    boolean stopRunningAgent() {
        final File configPath = new File(bootstrapDir, bootstrapFile);
        bp = ConfigUtils.getBootstrapParams(configPath, logger);

        String relSecurityDir = bp.getSecurityDir();
        if (relSecurityDir != null) {
            securityDir = new File(bootstrapDir, relSecurityDir);

            final File securityConfigPath = new File(securityDir,
                                                     securityConfigFile);
            if (securityConfigPath.exists()) {
                sp = ConfigUtils.getSecurityParams(securityConfigPath, logger);
            } else {
                System.err.println(
                    "Configuration declares that security should be " +
                    "present, but it was not found at " +
                    securityConfigPath);
                return false;
            }
        } else {
            securityDir = null;
            sp = SecurityParams.makeDefault();
        }
        sp.initRMISocketPolicies();

        /*
         * TODO: We probably should be calling snp.setRegistryCSF if
         * it is available to us.  However, it only affects whether we use
         * the default timeouts or configured timeouts, so it's not
         * critical.
         */
        BootstrapParams.initRegistryCSF(sp);

        snaSecurity = new SNASecurity(this, bp, sp, null /* gp */,
                                      null /* snp */, logger);

        StorageNodeAgentAPI snai1 = null;
        try {
            if (bp.getStoreName() != null) {
                StorageNodeId snid1 = new StorageNodeId(bp.getId());
                String bn =
                    RegistryUtils.bindingName(bp.getStoreName(),
                                              snid1.getFullName(),
                                              RegistryUtils.InterfaceType.MAIN);
                snai1 = RegistryUtils.getStorageNodeAgent
                    (bp.getHostname(), bp.getRegistryPort(), bn,
                     getLoginManager());
            } else {
                snai1 = RegistryUtils.getStorageNodeAgent
                    (bp.getHostname(), bp.getRegistryPort(),
                     GlobalParams.SNA_SERVICE_NAME, getLoginManager());
            }
            snai1.shutdown(true, false);
            return true;
        } catch (RemoteException re) {
            System.err.println("Exception shutting down Storage Node Agent: " +
                               re.getMessage());
        } catch (NotBoundException nbe) {
            System.err.println("Unable to contact Storage Node Agent: " +
                               nbe.getMessage());
        }
        return false;
    }

    /**
     * Start an instance of the Storage Node Agent.  It needs to know the
     * startup directory and configuration file.  The configuration file must
     * have this information as well:
     * 1.  Is the SNA registered or not (kvName is set)
     * 2.  Initial port for RMI and default service name
     * 3.  KV root directory
     */
    void start()
        throws RemoteException {

        /**
         * Get a bootstrap logger and initialize status.
         */
        logger = LoggerUtils.getBootstrapLogger
            (bootstrapDir, FileNames.BOOTSTRAP_SNA_LOG, snaName);
        statusTracker = new ServiceStatusTracker(logger);
        statusTracker.update(ServiceStatus.STARTING);

        final File configPath = new File(bootstrapDir, bootstrapFile);
        logger.info("Starting, configuration file: " + configPath);
        bp = ConfigUtils.getBootstrapParams(configPath, logger);

        String relSecDir = bp.getSecurityDir();
        if (relSecDir != null) {
            securityDir = new File(bootstrapDir, relSecDir);

            final File securityConfigPath = new File(securityDir,
                                                     securityConfigFile);
            if (securityConfigPath.exists()) {
                logger.info("Loading security configuration: " +
                            securityConfigPath);
                sp = ConfigUtils.getSecurityParams(securityConfigPath, logger);
            } else {
                securityDir = null;
                logger.log(Level.SEVERE,
                           "Configuration declares that security should be " +
                           "present, but it was not found at " +
                           securityConfigPath);
                throw new IllegalStateException(
                    "Unable to continue without security.");
            }
        } else {
            securityDir = null;
            sp = SecurityParams.makeDefault();
        }
        sp.initRMISocketPolicies();

        /*
         * Read the version form the boot params and check for an upgrade
         * (or downgrade) situation.
         */
        final KVVersion previousVersion = bp.getSoftwareVersion();
        assert previousVersion != null;

        boolean updateConfigFile = false;

        if (!previousVersion.equals(KVVersion.CURRENT_VERSION)) {

            /* Throws ISE if upgrade cannot be done */
            VersionUtil.checkUpgrade(previousVersion);

            logger.log(Level.INFO,
                       "Upgrade software version from version {0} to {1}",
                       new Object[] {
                             previousVersion.getNumericVersionString(),
                             KVVersion.CURRENT_VERSION.getNumericVersionString()
                        });

            bp.setSoftwareVersion(KVVersion.CURRENT_VERSION);
            updateConfigFile = true;
        }

        if (bp.getRootdir() == null) {
            bp.setRootdir(bootstrapDir);
            updateConfigFile = true;
        }

        if (updateConfigFile) {
            ConfigUtils.createBootstrapConfig(bp, configPath);
        }

        kvRoot = new File(bp.getRootdir());
        try {
            startup();
        } catch (RemoteException e) {
            cleanupRegistry();
            throw e;
        }

        /*
         * At this point, both the SNASecurity and TrustedLoginImpl should have
         * been initialized, we add them as parameter listeners.
         */
        if (sp.isSecure()) {
            final LoginUpdater loginUpdater = new LoginUpdater();
            loginUpdater.addGlobalParamsUpdaters(snaSecurity);
            loginUpdater.addServiceParamsUpdaters(snaSecurity);
            if (trustedLogin != null) {
                loginUpdater.addGlobalParamsUpdaters(trustedLogin);
                loginUpdater.addServiceParamsUpdaters(trustedLogin);
            }

            snParameterTracker.addListener(
                loginUpdater.new ServiceParamsListener());
            globalParameterTracker.addListener(
                loginUpdater.new GlobalParamsListener());
        }
    }

    /**
     * Available for testing.  During testing, we allow the SNA to cohabitate
     * with the client, and the client may alter the socket policies.  This
     * resets them to standard configuration.
     */
    public void resetRMISocketPolicies() {
        sp.initRMISocketPolicies();
        if (isRegistered()) {
            StorageNodeParams snp =
                ConfigUtils.getStorageNodeParams(kvConfigPath, logger);
            snp.setRegistryCSF(sp);
        } else {
            BootstrapParams.initRegistryCSF(sp);
        }
    }

    /**
     * Start an instance of the Storage Node Agent
     */
    private void startup()
        throws RemoteException {

        if (kvRoot.exists() && isRegistered()) {
            /* SNA is registered, do registered startup */
            startupRegistered();
        } else {
            /* SNA is not registered, do un-registered startup */
            startupUnregistered();
        }
    }

    private void logwarning(String msg, Exception e) {
        logger.log(Level.WARNING, msg, e);
    }

    private void logsevere(String msg, Exception e) {
        logger.log(Level.SEVERE, msg, e);
    }

    private void revertToBootstrap() {

        try {
            File configPath = new File(bootstrapDir, bootstrapFile);
            bp.setStoreName(null);
            bp.setHostingAdmin(false);
            bp.setId(1);
            ConfigUtils.createBootstrapConfig(bp, configPath);
            snaName = GlobalParams.SNA_SERVICE_NAME;
            snid = new StorageNodeId(0);
        } catch (Exception e) {
            logsevere("Cannot revert to bootstrap configuration", e);
            throw new IllegalStateException(e);
        }
    }

    /**
     * Unregistered startup:
     * 1. Create a default registry
     * 2. Kill any running processes (bootstrap admin).
     * 3. Start up a bootstrap admin instance
     */
    private void startupUnregistered()
        throws RemoteException {

        registry = createRegistry(null);
        BootstrapParams.initRegistryCSF(sp);

        snaSecurity = new SNASecurity(this, bp, sp, null /* gp */,
                                      null /* snp */, logger);

        bindUnregisteredSNA();
        bindUnregisteredTrustedLogin();

        /*
         * Start the mgmt agent AFTER the RMI registry exists.  The JMX
         * implementation uses this registry.
         */
        mgmtAgent = MgmtAgentFactory.getAgent(this, null, statusTracker);

        capacity = bp.getCapacity();
        numCPUs = bp.getNumCPUs();
        memoryMB = bp.getMemoryMB();
        mountPointsString = joinStringList(bp.getMountPoints(), ",");

        /**
         * The fault handler needs a logger in the event an exception occurs
         * prior to, or during registration.
         */
        snai.getFaultHandler().setLogger(logger);

        /**
         * If restarted without being registered the bootstrap admin needs to
         * be killed if present, otherwise it will fail to start up because its
         * http port will be in use.  In the event that there is a new config
         * file with a different port also match based on kvhome and configfile
         * name.
         */
        ManagedService.killManagedProcesses
            (getStoreName(), makeBootstrapAdminName(), getLogger());

        ManagedService.killManagedProcesses
            (bootstrapDir, bootstrapFile, getLogger());

        startBootstrapAdmin();
        statusTracker.update(ServiceStatus.WAITING_FOR_DEPLOY);
    }

    private synchronized void startupRegistered()
        throws RemoteException {

        initStorePaths();
        logger.info("Registered startup, config file: " + kvConfigPath);
        StorageNodeParams snp = null;
        GlobalParams gp = null;
        try {
            snp = ConfigUtils.getStorageNodeParams(kvConfigPath, logger);
            gp = ConfigUtils.getGlobalParams(kvConfigPath, logger);
        } catch (IllegalStateException e) {
            logger.info("Exception reading config file: " + e.getMessage());
        }
        if (snp == null || gp == null) {
            logger.info("Could not get required parameters, reverting to " +
                        "unregistered state");
            revertToBootstrap();
            cleanupRegistry();
            start();
            return;
        }

        /**
         * Set up monitoring and reset logger to use the real snid.  Monitor
         * must be set up before the logger is created because the
         * AgentRepository registers itself in LoggerUtils.
         */
        setupMonitoring(gp, snp);
        logger.info("Changing log files to directory: " +
                    FileNames.getLoggingDir(kvRoot, getStoreName()));
        logger = LoggerUtils.getLogger(StorageNodeAgentImpl.class, gp, snp);
        snai.getFaultHandler().setLogger(logger);

        snaSecurity = new SNASecurity(this, bp, sp, gp, snp, logger);

        /*
         * Any socket timeouts observed by the ClientSocketFactory in this
         * process will be logged to this logger.
         */
        ClientSocketFactory.setTimeoutLogger(logger);

        logger.info("Starting StorageNodeAgent for " + getStoreName());
        statusTracker.setLogger(logger);

        /**
         * The Registry is required for services to start.
         */
        if (registry == null) {
            registry = createRegistry(snp);
        }

        /**
         * Initialize state from parameters.
         */
        snid = snp.getStorageNodeId();
        JmxAgent.setRMISocketPolicy(sp.getRMISocketPolicy());
        initSNParams(snp);

        /* Set up the CSF that will be used to access services. */
        snp.setRegistryCSF(sp);

        snai.startTestInterface();

        /**
         * Kill leftover managed processes and start new ones.
         */
        cleanupRunningComponents();

        /*
         * Bind the Trusted Login interface so that components can access it
         * when they start up.
         */
        RegistryUtils.unbind(getHostname(), getRegistryPort(),
                             "SNA:" + InterfaceType.TRUSTED_LOGIN,
                             trustedLogin);
        bindRegisteredTrustedLogin(gp, snp);

        /*
         * Must precede the startup of the RN components, so their state can be
         * tracked.
         */
        startMasterBalanceManager(snp.getMasterBalance());
        startComponents();

        monitorAgent.startup();

        /**
         * Rebind using new name.  Unbind, change name, rebind.
         */
        RegistryUtils.unbind(getHostname(), getRegistryPort(), snaName,
                             exportableSnaif);
        snaName = snid.getFullName();

        bindRegisteredSNA(snp);

        statusTracker.update(ServiceStatus.RUNNING);
        if (adminService != null) {
            adminService.registered(this);
        }

        logger.info("Started StorageNodeAgent for " + getStoreName());
    }

    private void bindUnregisteredSNA()
        throws RemoteException {

        final RMISocketPolicy policy = sp.getRMISocketPolicy();
        final SocketFactoryPair sfp = bp.getStorageNodeAgentSFP(policy);

        initExportableSnaif();

        RegistryUtils.rebind(getHostname(), getRegistryPort(), snaName,
                             exportableSnaif,
                             sfp.getClientFactory(),
                             sfp.getServerFactory());
        logger.info("Bound to registry port " + getRegistryPort() +
                    " using name " + snaName +
                    " with SSF:" + sfp.getServerFactory());
    }

    private void bindUnregisteredTrustedLogin()
        throws RemoteException {

        final RMISocketPolicy trustedPolicy = sp.getTrustedRMISocketPolicy();

        if (trustedPolicy != null) {
            final SocketFactoryPair tsfp =
                bp.getSNATrustedLoginSFP(trustedPolicy);
            final String snaTLName = GlobalParams.SNA_LOGIN_SERVICE_NAME;
            final SNAFaultHandler faultHandler = new SNAFaultHandler(logger);
            final TrustedLoginHandler loginHandler =
                new TrustedLoginHandler(snid, true /* localId */);
            trustedLogin = new TrustedLoginImpl(faultHandler, loginHandler,
                                                logger);
            RegistryUtils.rebind(getHostname(), getRegistryPort(),
                                 snaTLName,
                                 trustedLogin, tsfp.getClientFactory(),
                                 tsfp.getServerFactory());
            logger.info("Bound trusted login to registry port " +
                        getRegistryPort() + " using name " + snaTLName +
                        " with SSF:" + tsfp.getServerFactory());
        }
    }

    private void bindRegisteredSNA(StorageNodeParams snp)
        throws RemoteException {

        final String csfName =
            ClientSocketFactory.factoryName(getStoreName(),
                                            StorageNodeId.getPrefix(),
                                            RegistryUtils.InterfaceType.
                                            MAIN.interfaceName());
        final RMISocketPolicy rmiPolicy = sp.getRMISocketPolicy();
        final SocketFactoryPair sfp =
            snp.getStorageNodeAdminSFP(rmiPolicy, csfName);

        initExportableSnaif();

        RegistryUtils.rebind(getHostname(), getRegistryPort(), getStoreName(),
                             snaName, RegistryUtils.InterfaceType.MAIN,
                             exportableSnaif,
                             sfp.getClientFactory(), sfp.getServerFactory());
        logger.info("Rebound to registry port " + getRegistryPort() +
                    " using name " + snaName + " with SSF:" +
                    sfp.getServerFactory());
    }

    private void bindRegisteredTrustedLogin(GlobalParams gp,
                                            StorageNodeParams snp)
        throws RemoteException {

        final RMISocketPolicy trustedPolicy =
            sp.getTrustedRMISocketPolicy();
        if (trustedPolicy != null) {
            final SocketFactoryPair tsfp =
                bp.getSNATrustedLoginSFP(trustedPolicy);
            final SNAFaultHandler faultHandler = new SNAFaultHandler(logger);
            final long sessionTimeout =
                gp.getSessionTimeoutUnit().toMillis(gp.getSessionTimeout());
            final int sessionLimit = snp.getSessionLimit();
            final TrustedLoginHandler loginHandler =
                new TrustedLoginHandler(snid, false /* localId */,
                                        sessionTimeout, sessionLimit);
            final String snaTLName = GlobalParams.SNA_LOGIN_SERVICE_NAME;
            trustedLogin = new TrustedLoginImpl(
                faultHandler, loginHandler, logger);
            RegistryUtils.rebind(getHostname(), getRegistryPort(),
                                 snaTLName,
                                 trustedLogin, tsfp.getClientFactory(),
                                 tsfp.getServerFactory());
            logger.info("Bound trusted login to registry port " +
                        getRegistryPort() + " using name " + "SNA" +
                        " with SSF:" + tsfp.getServerFactory());
        }
    }

    void checkRegistered(String method)
        throws IllegalStateException {

        if (getStoreName() == null) {
            throw new IllegalStateException
                (method + ": Storage Node Agent is not registered");
        }
    }

    private void initExportableSnaif() {
        try {
            exportableSnaif =
                SecureProxy.create(snai, snaSecurity.getAccessChecker(),
                                   snai.getFaultHandler());
            logger.info(
                "Successfully created secure proxy for the storage node agent");
        } catch (ConfigurationException ce) {
            throw new IllegalStateException("Unabled to create proxy", ce);
        }
    }

    @SuppressWarnings("unused")
    private StorageNodeAgentImpl getImpl() {
        return snai;
    }

    StorageNodeStatus getStatus() {
        return new StorageNodeStatus(statusTracker.getServiceStatus());
    }

    MonitorAgentImpl getMonitorAgent() {
        return monitorAgent;
    }

    /**
     * Initialize store variables.
     */
    private File initStorePaths() {

        final File kvDir =
            FileNames.getKvDir(kvRoot.toString(), getStoreName());
        final StorageNodeId id = new StorageNodeId(bp.getId());
        snRoot = FileNames.getStorageNodeDir(kvDir, id);
        kvConfigPath =
            FileNames.getSNAConfigFile(kvRoot.toString(), getStoreName(), id);
        return kvDir;
    }

    /**
     * Ensure that the store directory exists and has a security policy file.
     */
    private void ensureStoreDirectory() {

        final File kvDir = initStorePaths();
        if (!snRoot.isDirectory()) {
            if (FileNames.makeDir(snRoot)) {
                logger.info("Created a new store directory: " + snRoot);
            }
        }

        /**
         * Make sure there's a Java security policy file and if not, copy it
         * from the bootstrap directory.  This file goes into the store
         * directory.
         */
        final File javaSecPolicy = FileNames.getSecurityPolicyFile(kvDir);
        if (!javaSecPolicy.exists()) {
            logger.fine("Creating security policy file: " + javaSecPolicy);
            final File fromFile =
                new File(bootstrapDir, FileNames.JAVA_SECURITY_POLICY_FILE);
            if (!fromFile.exists()) {
                throw new IllegalStateException
                    ("Cannot find bootstrap security file " + fromFile);
            }
            try {
                FileUtils.copyFile(fromFile, javaSecPolicy);
            } catch (IOException ie) {
                throw new IllegalStateException
                    ("Could not create policy file", ie);
            }
        }
    }

    protected Registry getRegistry() {
        return registry;
    }

    public int getRegistryPort() {
        return bp.getRegistryPort();
    }

    public String getServiceName() {
        return snaName;
    }

    public StorageNodeId getStorageNodeId() {
        return snid;
    }

    /**
     * These next few are for testing purposes.
     */
    protected int getServiceWaitMillis() {
        return serviceWaitMillis;
    }
    protected int getRepnodeWaitSecs() {
        return repnodeWaitSecs;
    }

    protected int getMaxLink() {
        return maxLink;
    }

    protected int getLinkExecWaitSecs() {
        return linkExecWaitSecs;
    }

    /**
     * Create a Registry if not already done.  Because the SNA does not change
     * the registry port when it is "registered" the bootstrap registry can
     * remain unmodified.
     * @param snp
     */
    @SuppressWarnings("null")
    private Registry createRegistry(StorageNodeParams snp)
        throws RemoteException {

        /* Set the hostname for the registry. */
        System.setProperty("java.rmi.server.hostname", getHostname());

        final RMISocketPolicy rmiPolicy = sp.getRMISocketPolicy();
        final SocketFactoryPair sfp = (snp == null) ?
            StorageNodeParams.getDefaultRegistrySFP(rmiPolicy) :
            snp.getRegistrySFP(rmiPolicy);

        final ServerSocketFactory ssf = sfp.getServerFactory();

        logger.info("Creating a Registry on port " +
                    getHostname() + ":" + getRegistryPort() +
                    " server socket factory:" + ssf);

        /* Note that no CSF is supplied. */

        ExportException throwEE = null ;
        /* A little over 2 min (the CLOSE_WAIT timeout.) */
        final int limitMs = 128000;
        final int retryPeriodMs = 1000;
        for (int totalWaitMs = 0; totalWaitMs <= limitMs;
             totalWaitMs += retryPeriodMs) {
            try {
                throwEE = null;
                return LocateRegistry.createRegistry(getRegistryPort(),
                                                     null, ssf);
            } catch (ExportException ee) {
                throwEE = ee;
                if (TestStatus.isActive() &&
                    (ee.getCause() instanceof BindException)) {

                    logger.info("Registry bind exception:" +
                                ee.getCause().getMessage() +
                                " Registry port:" + getRegistryPort());
                    try {
                        Thread.sleep(retryPeriodMs);
                    } catch (InterruptedException e) {
                        throw throwEE;
                    }
                    continue;
                }
                throw throwEE;
            }
        }

        /* Timed out after retries in test. */
        throw throwEE;
    }

    private void cleanupRegistry() {
        /**
         * Unbind this object, trusted login if it exists,  and clean up
         * registry.  Don't throw.
         */
        try {
            if (isRegistered()) {
                RegistryUtils.unbind(getHostname(), getRegistryPort(),
                                     GlobalParams.SNA_LOGIN_SERVICE_NAME,
                                     trustedLogin);
                RegistryUtils.unbind(getHostname(), getRegistryPort(),
                                     getStoreName(), snaName,
                                     RegistryUtils.InterfaceType.MAIN,
                                     exportableSnaif);
                if (monitorAgent != null) {
                    monitorAgent.stop();
                }
                snai.stopTestInterface();
            } else {
                try {
                    RegistryUtils.unbind(getHostname(), getRegistryPort(),
                                         GlobalParams.SNA_LOGIN_SERVICE_NAME,
                                         trustedLogin);
                    RegistryUtils.unbind(getHostname(), getRegistryPort(),
                                         snaName, exportableSnaif);
                } catch (RemoteException re) {
                    /* ignore */
                }
            }
            if (registry != null) {
                UnicastRemoteObject.unexportObject(registry, true);
            }
        } catch (Exception ignored) {
        } finally {
            registry = null;
        }
    }

    /*
     * Set system information:
     * 1. The number of CPUs based on reality
     * 2. If available, the amount of real memory in the system
     * NOTE: if using threads for managing services setting memoryMB can
     * result in over-allocation of RepNode parameters (e.g. je.maxMemory)
     * without direct control over the corresponding Java heap, so in threads
     * mode do not set memoryMB.
     */
    private void setSystemInfo() {
        OperatingSystemMXBean bean =
            ManagementFactory.getOperatingSystemMXBean();
        logger.info("System architecture is " + bean.getArch());
        int ncpu = bp.getNumCPUs();
        if (ncpu == 0) {
            ncpu = bean.getAvailableProcessors();
            logger.info("Setting number of CPUs to " + ncpu);
            bp.setNumCPUs(ncpu);
        }
        /*
         * Don't set memoryMB for threads, or in unit tests where we use many
         * RNs on a single machine.
         */
        if ((!useThreads) && !TestStatus.manyRNs()) {
            int mb = bp.getMemoryMB();
            if (mb == 0) {
                long bytes = getTotalPhysicalMemorySize(bean);
                if (bytes != 0) {
                    mb = (int) (bytes >> 20);
                    logger.info("Setting memory MB to " + mb);
                    bp.setMemoryMB(mb);
                } else {
                    logger.info("Cannot get memory size");
                }
            }
        }
    }

   /**
    * Get the get the total physical memory available on the machine if the
    * mbean implements the getTotalPhysicalMemorySize method (the Sun mbean
    * does). If the mbean does not implement the method, it returns 0.
    * It is a little more complicated than above... if running a 32-bit JVM
    * and capacity 1 the memoryMB should not be > 2G or the JVM will fail to
    * start.  Logic is added for this situation.
    */
    private long getTotalPhysicalMemorySize(OperatingSystemMXBean bean) {
        final Class<? extends OperatingSystemMXBean> beanClass =
            bean.getClass();
        try {
            final int maxInt = Integer.MAX_VALUE;
            final Method m = beanClass.getMethod("getTotalPhysicalMemorySize");
            m.setAccessible(true); /* Since it's a native method. */
            long mem = (Long)m.invoke(bean);

            /*
             * This call will work because if the above worked we are likely
             * using a Sun JVM.
             */
            String bits = System.getProperty("sun.arch.data.model");
            if (bits == null) {
                return mem;
            }
            int intBits = Integer.parseInt(bits);
            if (intBits == 32) {
                int cap = bp.getCapacity();
                if (mem / cap > maxInt) {
                    logger.info("Reducing total memory from " +
                                mem + " to " + (maxInt * cap) + " bytes");
                    mem = maxInt * cap;
                }
            }
            return mem;
        } catch (Exception e) {
            return 0;
        }
    }

    /**
     * Provide a shutdown hook so that if the SNA is killed externally it can
     * attempt to cleanly shut down managed services.  There is no reason to
     * unbind from the RMI Registry since the process is exiting.  This hook
     * will only be installed for registered SNA instances.
     *
     * NOTE: if/when we allow RepNodes and Admin instances to stay up and
     * re-register themselves if the SNA dies, this code must change.
     *
     * NOTE: the Logger instance used here may already be shut down and the
     * information not logged.
     */
    private class ShutdownThread extends Thread {

        @Override
        public void run() {
            /* if statusTracker is null there is nothing to shut down */
            if ((statusTracker != null) &&
                (statusTracker.getServiceStatus() == ServiceStatus.RUNNING) ||
                (statusTracker.getServiceStatus() ==
                 ServiceStatus.WAITING_FOR_DEPLOY)) {
                logger.info("Shutdown thread running, stopping services");
                try {
                    shutdown(true, false);
                } finally {
                    logger.info("Shutdown thread exiting");
                }
            }
        }
    }

    void addShutdownHook() {
        if (logger != null) {
            logger.fine("Adding shutdown hook");
        }
        Runtime.getRuntime().addShutdownHook(new ShutdownThread());
    }

    public BootstrapParams getBootstrapParams() {
        return bp;
    }

    public String getStoreName() {
        return bp.getStoreName();
    }

    public String getHostname() {
        return bp.getHostname();
    }

    public String getHAHostname() {
        return bp.getHAHostname();
    }

    boolean isLoopbackAddress() {
        if (isLoopback == null) {
            isLoopback = checkLoopback(getHAHostname());
        }
        return isLoopback;
    }

    public String getHAPortRange() {
        return bp.getHAPortRange();
    }

    public String getServicePortRange() {
        return bp.getServicePortRange();
    }

    public Logger getLogger() {
        return logger;
    }

    public String getBootstrapDir() {
        return bootstrapDir;
    }

    public String getBootstrapFile() {
        return bootstrapFile;
    }

    public File getSecurityDir() {
        return securityDir;
    }

    public String getSecurityConfigFile() {
        return securityConfigFile;
    }

    public File getKvConfigFile() {
        return kvConfigPath;
    }

    /**
     * Return the value of the processStartPrefix property.
     */
    String getCustomProcessStartupPrefix() {
        return customProcessStartupPrefix;
    }

    boolean verbose() {
        return isVerbose;
    }

    /**
     * Advisory interface indicating whether the RepNode is running.  This is
     * good for testing but should not be trusted 100%.
     */
    boolean isRunning(RepNodeId rnid) {
        ServiceManager mgr = repNodeServices.get(rnid.getFullName());
        if (mgr != null) {
            return mgr.isRunning();
        }
                return false;
    }

    /*
     * Useful for testing
     */
    ServiceManager getServiceManager(RepNodeId rnid) {
        return repNodeServices.get(rnid.getFullName());
    }

    public static boolean checkLoopback(String host) {
        InetSocketAddress isa = new InetSocketAddress(host, 0);
        return isa.getAddress().isLoopbackAddress();
    }

    /**
     * Start all components that are configured.
     */
    private void startComponents() {

        /**
         * Start RepNodes first.
         */
        List<ParameterMap> repNodes =
            ConfigUtils.getRepNodes(kvConfigPath, logger);
        for (ParameterMap map : repNodes) {
            RepNodeParams rn = new RepNodeParams(map);
            if (!rn.isDisabled()) {
                startRepNodeInternal(rn);
            } else {
                logger.info(rn.getRepNodeId().getFullName() +
                            ": Skipping automatic start of stopped RepNode ");
            }
        }

        /**
         * Now the Admin if this SN is hosting it.  It may already be running.
         * This will be the case during registration of the SNA that is hosting
         * the Admin.
         */
        if (adminService == null) {
            AdminParams ap = ConfigUtils.getAdminParams(kvConfigPath, logger);
            if (ap != null && !ap.isDisabled()) {
                startAdminInternal(ap, bp.isHostingAdmin());
            }
        }
    }

    private boolean stopAdminService(boolean stopService, boolean force) {
        if (adminService == null) {
            return false;
        }
        boolean stopped = false;
        String serviceName = adminService.getService().getServiceName();
        try {

            logger.info(serviceName + ": Stopping AdminService");
            /**
             * Make sure the service won't automatically restart.
             */
            adminService.dontRestart();

            /**
             * Try clean shutdown first.  If that fails for any reason use a
             * bigger hammer to be sure the service is gone.  Give the admin
             * service a few seconds to come up if it's not already.  Stopping
             * the service while it's not yet up can be problematic.
             */
            if (stopService && !adminService.forceOK(force)) {
                CommandServiceAPI admin = ServiceUtils.waitForAdmin
                    (getHostname(), getRegistryPort(), getLoginManager(),
                     5, ServiceStatus.RUNNING);
                admin.stop(force);
                adminService.waitFor(serviceWaitMillis);
                stopped = true;
            }
        } catch (Exception e) {

            /**
             * Eat the exception but log the problem and make sure that the
             * service is really stopped.
             */
            logwarning("Exception stopping Admin service", e);
        }
        if (!stopped) {
            adminService.stop();
        }
        logger.info(serviceName + ": Stopped AdminService");
        unbindService(GlobalParams.COMMAND_SERVICE_NAME);
        unbindService(GlobalParams.ADMIN_LOGIN_SERVICE_NAME);
        mgmtAgent.removeAdmin();
        adminService = null;
        return true;
    }

    private void stopRepNodeServices(boolean stopService, boolean force) {

        for (ServiceManager mgr : repNodeServices.values()) {
            try {
                /**
                 * Make sure the service won't automatically restart.
                 */
                mgr.dontRestart();

                if (mgr.forceOK(force)) {
                    mgr.stop();
                } else {
                    ManagedRepNode mrn = (ManagedRepNode) mgr.getService();

                    /**
                     * Don't try to shut down if it's known to be down already.
                     */
                    if (stopService) {
                        if (mgr.isRunning()) {

                            /**
                             * Try clean shutdown first.  If that fails for any
                             * reason use a bigger hammer to be sure the
                             * service is gone.  Give the RN some time in case
                             * it's not yet running.  Stopping it at a random
                             * time can cause problems.
                             *
                             * NOTE: this timeout is helpful but not critical
                             * so it need not be tuneable.
                             */
                            RepNodeAdminAPI rna =
                                mrn.waitForRepNodeAdmin(this, 5);
                            if (rna != null) {
                                rna.shutdown(force);
                            }
                        }
                        mgr.waitFor(serviceWaitMillis);
                    }
                }
            } catch (Exception e) {

                /**
                 * Eat the exception but log it and make sure that the service
                 * is really stopped.
                 */
                logwarning((mgr.getService().getServiceName() +
                            ": Exception stopping RepNode"), e);
                mgr.stop();
            }
            unbindService(makeRepNodeBindingName
                          (mgr.getService().getServiceName()));
        }
        repNodeServices.clear();
    }

    private String makeRepNodeBindingName(String fullName) {
        return RegistryUtils.bindingName(getStoreName(),
                                         fullName,
                                         RegistryUtils.InterfaceType.ADMIN);
    }

    /**
     * Unbind a managed service from this SNA's registry.
     *
     * This is done to ensure that even on forcible stop the service is
     * cleaned out of the registry.  Errors are expected (already unbound)
     * and ignored.
     */
    private void unbindService(String serviceName) {
        try {
            registry.unbind(serviceName);
        } catch (NotBoundException nbe) {
            /* ignore */
        } catch (RemoteException re) {
            /* ignore */
        }
    }

    /**
     * Starts the master balance manager component
     *
     * @param enabled if true master balancing is enabled. If false it's
     * disabled; the SNA will not initiate any rebalancing at this node and
     * will decline to participate in rebalancing requests from other SNAs.
     */
    private void startMasterBalanceManager(boolean enabled) {
        /* Cleanup any existing MBM */
        stopMasterBalanceManager();

        assert masterBalanceManager == null;

        /* Now start up the master balance manager. */
        final SNInfo snInfo =
                new MasterBalanceManager.SNInfo(getStoreName(), snid,
                                                getHostname(),
                                                getRegistryPort());

        masterBalanceManager =
            MasterBalanceManager.create(enabled, snInfo, logger,
            		                    getLoginManager());
    }

    /**
     * Stops the master balance manager component
     */
    private void stopMasterBalanceManager() {
        if (masterBalanceManager == null) {
            return;
        }
        masterBalanceManager.shutdown();
        masterBalanceManager = null;
    }

    /**
     * Detect and kill running processes for this store.
     */
    private void cleanupRunningComponents() {

        /**
         * Kill RepNodes
         */
        List<ParameterMap> repNodes =
            ConfigUtils.getRepNodes(kvConfigPath, logger);
        for (ParameterMap map : repNodes) {
            RepNodeParams rn = new RepNodeParams(map);
            ManagedService.killManagedProcesses
                (getStoreName(), rn.getRepNodeId().getFullName(), getLogger());
        }

        /**
         * Admin may be named as the bootstrap admin or via the config
         * file. Try both.  Don't kill a currently-managed bootstrap admin, as
         * indicated by a non-null adminService.
         */
        if (adminService == null) {
            ManagedService.killManagedProcesses
                (getStoreName(), makeBootstrapAdminName(), getLogger());
        }
        AdminParams ap = ConfigUtils.getAdminParams(kvConfigPath, logger);
        if (ap != null) {
            ManagedService.killManagedProcesses
                (getStoreName(), ap.getAdminId().getFullName(), getLogger());
        }
    }

    /**
     * Make sure that the mount point, if specified, exists and is a directory.
     * TODO: should symlinks be created in the SNs directory to point to the
     * RepNode directories?
     */
    private File validateRepNodeDirectory(RepNodeParams rnp) {
        File mp = rnp.getMountPoint();
        if (mp != null) {
            if (!mp.isDirectory()) {
                String msg = "Directory specified for RepNode is either" +
                    " not present or is not a directory: " +
                    mp.getAbsoluteFile();
                logger.info(msg);
                throw new IllegalArgumentException(msg);
            }
            return mp;
        }
        return null;
    }

    /**
     * Internal method to start a RepNode, shared by external and internal
     * callers.
     */
    private boolean startRepNodeInternal(RepNodeParams rnp) {

        RepNodeId rnid = rnp.getRepNodeId();
        String serviceName = rnid.getFullName();
        logger.info(serviceName + ": Starting RepNode");
        try {
            File repNodeDir = validateRepNodeDirectory(rnp);

            /**
             * Create a ManagedService object used by the ServiceManager to
             * start the service.
             */
            ManagedRepNode ms = new ManagedRepNode
                (sp, rnp, kvRoot, snRoot, getStoreName());

            ServiceManager mgr = repNodeServices.get(serviceName);
            if (mgr != null) {
                /* The service may be running */
                boolean isRunning = mgr.isRunning();
                if (isRunning) {
                    logger.info(serviceName +
                                ": Attempt to start a running RepNode.");
                    return true;
                }
                logger.info(serviceName + " exists but is not runnable." +
                            "  Attempt to stop it and restart.");
                stopRepNode(rnid, true);
                /*
                 * recurse back to startRepNode to make sure state gets set
                 * correctly.
                 */
                return startRepNode(rnid);
            }
            if (useThreads) {
                /* start in thread */
                mgr = new ThreadServiceManager(this, ms);
            } else {
                /* start in process */
                mgr = new ProcessServiceManager(this, ms);
            }
            checkForRecovery(rnid, repNodeDir);
            mgmtAgent.addRepNode(rnp, mgr);

            mgr.start();

            /**
             * Add service to map of running services.
             */
            repNodeServices.put(serviceName, mgr);
            logger.info(serviceName + ": Started RepNode");
        } catch (Exception e) {
            logsevere((serviceName + ": Exception starting RepNode"), e);
            return false;
        }
        return true;
    }

    /**
     * Utility method for waiting until a RepNode reaches one of the given
     * states.  Primarily here for the exception handling.
     */
    public RepNodeAdminAPI waitForRepNodeAdmin(RepNodeId rnid,
                                               ServiceStatus[] targets) {
        return waitForRepNodeAdmin(rnid, targets, repnodeWaitSecs);
    }

    private RepNodeAdminAPI waitForRepNodeAdmin(RepNodeId rnid,
                                                ServiceStatus[] targets,
                                                int waitSecs) {
        RepNodeAdminAPI rnai = null;
        try {
            rnai = ServiceUtils.waitForRepNodeAdmin
                (getStoreName(), getHostname(), getRegistryPort(), rnid, snid,
                 getLoginManager(), waitSecs, targets);
        } catch (Exception e) {
            File logDir = FileNames.getLoggingDir(kvRoot, getStoreName());
            String logName =
                logDir + File.separator + rnid.toString() + "*.log";
            String msg = "Failed to attach to RepNodeService for " +
                rnid + " after waiting " + waitSecs +
                " seconds; see log, " + logName + ", on host " +
                getHostname() + " for more information.";
            logsevere(msg, e);

            /*
             * Check if the process didn't actually start up, and throw an
             * exception if so. That's different from a timeout exception, and
             * it would be better to propagate that information.
             */
            RegistryUtils.checkForStartupProblem(getStoreName(), getHostname(),
                                                 getRegistryPort(), rnid, snid,
                                                 getLoginManager());
            return null;
        }
        return rnai;
    }

    /**
     * Utility method for waiting until the Admin reaches the given state.
     */
    public CommandServiceAPI waitForAdmin(ServiceStatus target,
                                          int timeoutSecs) {

        CommandServiceAPI cs = null;
        try {
            cs = ServiceUtils.waitForAdmin
                (getHostname(), getRegistryPort(), getLoginManager(),
                 timeoutSecs, target);
        } catch (Exception e) {

            String msg = "Failed to attach to AdminService for after waiting " +
                repnodeWaitSecs + " seconds.";
            logger.severe(msg);
            throw new IllegalStateException(msg, e);
        }
        return cs;
    }

    /**
     * Remove the data directory for the resource.  This method does not deal
     * with mount points and should not be called for any service that is using
     * one.
     */
    void removeDataDir(ResourceId rid) {
        File dataDir = FileNames.getServiceDir(kvRoot.toString(),
                                               getStoreName(),
                                               null,
                                               snid,
                                               rid);
        logger.info("Removing data directory for resource " +
                    rid + ": " + dataDir);
        if (dataDir.exists()) {
            removeFiles(dataDir);
        }
    }

    /**
     * Stop a running RepNode
     */
    public boolean stopRepNode(RepNodeId rnid, boolean force) {
        return stopRepNode(rnid, force, serviceWaitMillis);
    }

    /*
     * This function should never fail to stop a RepNode if the RepNode exists
     * and is running.  The last resort is to forcibly kill the RN, even if the
     * force boolean is false.
     */
    boolean stopRepNode(RepNodeId rnid, boolean force, int waitMillis) {

        boolean stopped = false;
        String serviceName = rnid.getFullName();
        logger.info(serviceName + ": stopRepNode called");
        ServiceManager mgr = repNodeServices.get(serviceName);
        boolean isRunning = true;
        if (mgr != null) {
            isRunning = mgr.isRunning();
        }
        if (mgr == null || !isRunning) {
            logger.info(serviceName + ": RepNode is not running");
        }
        if (mgr == null) {
            return false;
        }

        /*
         * Set the service state in the config file to disabled.  Once this is
         * done the RN will not be automatically restarted.
         */
        setServiceStoppedState
            (serviceName, ParameterState.COMMON_DISABLED, true);
        try {

            /*
             * Make sure the service won't automatically restart.
             */
            mgr.dontRestart();

            /*
             * If force is true, skip directly to killing the service.
             */
            if (isRunning && !mgr.forceOK(force)) {
                ManagedRepNode mrn = (ManagedRepNode) mgr.getService();
                RepNodeAdminAPI rna = mrn.getRepNodeAdmin(this);
                rna.shutdown(force);

                /*
                 * Wait for the execution context (process or thread).
                 */
                mgr.waitFor(waitMillis);
                stopped = true;
                logger.info(serviceName + ": Stopped RepNode");
            }
        } catch (RuntimeException e) {
            logwarning((serviceName + ": Exception stopping RepNode"), e);
        } catch (RemoteException re) {
            logwarning((serviceName + ": Exception stopping RepNode"), re);
        } finally {

            /*
             * Ask the ServiceManager to stop it if shutdown failed.
             */
            if (!stopped) {
                mgr.stop();
            }

            /*
             * Remove service and set active state in RNP to false.
             */
            unbindService(makeRepNodeBindingName(serviceName));
            repNodeServices.remove(serviceName);
            try {
                mgmtAgent.removeRepNode(rnid);
            } catch (RuntimeException ce) {
                logwarning
                    ((serviceName + ": Exception removing RepNode from mgmt" +
                      " agent"), ce);
            }
        }
        return isRunning;
    }

    private void startBootstrapAdmin() {
        if (bp.getAdminHttpPort() == 0) {
            createBootstrapAdmin = false;
        }
        if (bp.getForceBootstrapAdmin()) {
            createBootstrapAdmin = true;
        }
        if (createBootstrapAdmin) {
            startAdminInternal(null, true);
        } else {
            logger.info("No admin port, not starting Bootstrap Admin");
        }
    }

    /**
     * Used by SNAImpl.  It will never pass a null AdminParams.
     */
    public boolean startAdmin(AdminParams ap) {

        /**
         * Set active state in AP.  This must be done before the service is
         * started to avoid conflict on the file.
         */
        setServiceStoppedState
            (ap.getAdminId().getFullName(),
             ParameterState.COMMON_DISABLED, false);
        return startAdminInternal(ap, getBootstrapParams().isHostingAdmin());
    }

    /**
     * Internal method to start an AdminService, shared by external and
     * internal callers.
     */
    private boolean startAdminInternal(AdminParams ap, boolean isBootstrap) {

        if (ap == null && !isBootstrap) {
            throw new IllegalStateException
                ("Params for admin do not exist, should have been created.");
        }

        String serviceName = (ap != null ?
                              ap.getAdminId().getFullName() :
                              ManagedService.BOOTSTRAP_ADMIN_NAME);

        try {

            /**
             * Create a ManagedService object used by the ServiceManager to
             * start the service.
             */
            if (adminService != null) {
                /* The service may be running */
                boolean isRunning = adminService.isRunning();
                if (isRunning) {
                    logger.info(serviceName +
                                ": Attempt to start a running AdminService");
                    return true;
                }
                logger.info(serviceName + " exists but is not runnable." +
                            "  Attempt to stop it and restart.");
                stopAdminService(true, true);
                /* fall through to start */
            }
            ManagedAdmin ms;
            logger.info(serviceName + ": Starting AdminService");
            if (ap != null) {
                ms = new ManagedAdmin
                    (sp, ap, kvRoot, snRoot, getStoreName());
            } else {
                ms = new ManagedBootstrapAdmin(this);
            }
            ServiceManager mgr = null;
            if (useThreads) {
                /* start in thread */
                mgr = new ThreadServiceManager(this, ms);
            } else {
                /* start in process */
                mgr = new ProcessServiceManager(this, ms);
            }
            if (ap != null) {
                checkForRecovery(ap.getAdminId(), null);
            }

            mgmtAgent.addAdmin(ap, mgr);

            mgr.start();
            adminService = mgr;

            logger.info(serviceName + ": Started AdminService");
        } catch (Exception e) {
            String msg = "Exception starting AdminService: " + e;
            logger.severe(msg);
            return false;
        }
        return true;
    }

    synchronized boolean isRegistered() {
        return getStoreName() != null;
    }

    /**
     * Make a service name for the BootstrapAdmin.  It must include the
     * registry port to uniquely identify the bootstrap admin process in the
     * event there is more than one SN on the host.
     */
    public String makeBootstrapAdminName() {
        return ManagedService.BOOTSTRAP_ADMIN_NAME + "." + getRegistryPort();
    }

    /**
     * Setup all that is needed for logging and monitoring, for registered
     * SNAs.
     */
    private void setupMonitoring(GlobalParams globalParams,
                                 StorageNodeParams snp) {

        if (monitorAgent != null) {
            return;
        }

        /*
         * The AgentRepository is the buffered monitor data and belongs
         * to the monitorAgent, but is instantiated outside to take care of
         * initialization dependencies.
         */
        AgentRepository monitorBuffer =
            new AgentRepository(globalParams.getKVStoreName(),
                                snp.getStorageNodeId());
        statusTracker.addListener(monitorBuffer);
        monitorAgent = new MonitorAgentImpl(this,
                                            globalParams,
                                            snp,
                                            sp,
                                            monitorBuffer,
                                            statusTracker);
    }

    /**
     * Send Storage Node and bootstrap information back as a reply to the
     * register() call
     */
    public static class RegisterReturnInfo {
        final private List<ParameterMap> maps;
        private ParameterMap bootMap;
        private ParameterMap mountMap;

        public RegisterReturnInfo(StorageNodeAgent sna) {
            BootstrapParams bp = sna.getBootstrapParams();
            maps = new ArrayList<ParameterMap>();
            bootMap = bp.getMap().copy();
            bootMap.setParameter(ParameterState.GP_ISLOOPBACK,
                                 Boolean.toString(sna.isLoopbackAddress()));
            mountMap = bp.getMountMap().copy();
            maps.add(bootMap);
            maps.add(mountMap);
        }

        public RegisterReturnInfo(List<ParameterMap> maps) {
            this.maps = maps;
            bootMap = null;
            mountMap = null;
            for (ParameterMap pmap : maps) {
                if (pmap.getName().equals(ParameterState.BOOTSTRAP_PARAMS)) {
                    bootMap = pmap;
                }
                if (pmap.getName().equals
                    (ParameterState.BOOTSTRAP_MOUNT_POINTS)) {
                    mountMap = pmap;
                }
            }
        }

        public List<ParameterMap> getMaps() {
            return maps;
        }

        public ParameterMap getBootMap() {
            return bootMap;
        }

        public ParameterMap getMountMap() {
            return mountMap;
        }

        public boolean getIsLoopback() {
            if (bootMap == null) {
                throw new IllegalStateException
                    ("BootMap cannot be null when asking for loopback info");
            }
            Parameter p = bootMap.get(ParameterState.GP_ISLOOPBACK);
            if (p == null) {
                throw new IllegalStateException
                    ("GP_ISLOOPBACK parameter is not set in boot map");
            }
            return p.asBoolean();
        }
    }

    /**
     * Implementation methods for StorageNodeAgentInterface.  These are
     * called by the public interface methods that wrap the calls with a
     * ProcessFaultHandler object for consistent exception handling.
     */
    List<ParameterMap> register(GlobalParams gp,
                                StorageNodeParams snp,
                                boolean hostingAdmin) {

        logger.info("Register: root: " + kvRoot + ", store: " +
                    gp.getKVStoreName() + ", hostingAdmin: " + hostingAdmin);

        /**
         * Allow retries to succeed.
         */
        if (isRegistered()) {
            logger.info("Register: Storage Node Agent is already registered " +
                        "to " + getStoreName());
            return new RegisterReturnInfo(this).getMaps();
        }

        if (gp.isLoopbackSet()) {
            if (gp.isLoopback() != isLoopbackAddress()) {
                String msg="Register: Cannot mix loopback and non-loopback " +
                    "addresses in the same store.  The store value " +
                    (gp.isLoopback() ? "is" : "is not") +
                     " configured to use loopback addresses but storage node " +
                    snp.getHostname() + ":" + snp.getRegistryPort() + " " +
                    (isLoopbackAddress() ? "is" : "is not") +
                    " a loopback address.";

                logger.info(msg);
                throw new IllegalStateException(msg);
            }
        } else {
            gp.setIsLoopback(isLoopbackAddress());
        }

        /*
         * Make sure that system information, such as number of CPUs, is set if
         * it is available.  It is returned in RegisterReturnInfo and set in
         * snp.setInstallationInfo().
         */
        setSystemInfo();

        /**
         * Create the ultimate return object and initialize fields in the
         * StorageNodeParams that the SNA owns.
         */
        RegisterReturnInfo rri = new RegisterReturnInfo(this);

        snp.setInstallationInfo
            (rri.getBootMap(), rri.getMountMap(), hostingAdmin);

        /**
         * Initialize state from parameters.
         */
        initSNParams(snp);

        try {

            /**
             * There is a test-only race where the bootstrap admin may still be
             * coming up so make sure it is running before continuing.
             */
            if (adminService != null) {
                ServiceUtils.waitForAdmin(getHostname(), getRegistryPort(),
                                          getLoginManager(), 40,
                                          ServiceStatus.RUNNING);
            }

            /**
             * If not hosting the admin, shut down the bootstrap admin.
             */
            if (!hostingAdmin) {
                stopAdminService(true, true);
            }

            /**
             * Rewrite the bootstrap config file with the KV Store name and Id
             * for this SN.  Do this after testing kvConfigPath above to make
             * reverting to bootstrap state automatic.
             */
            File configPath = new File(bootstrapDir, bootstrapFile);
            bp.setStoreName(gp.getKVStoreName());
            bp.setId(snp.getStorageNodeId().getStorageNodeId());
            bp.setHostingAdmin(hostingAdmin);
            ConfigUtils.createBootstrapConfig(bp, configPath);

            /**
             * Make sure the kvstore directory has been created.  This call
             * uses state set in the BootstrapParams above.
             */
            ensureStoreDirectory();

            /**
             * If there is a configuration file already this is bad.  It means
             * the SNA was not registered but there is state that appears
             * otherwise.  kvConfigPath is set in ensureStoreDirectory().
             */
            if (kvConfigPath.exists()) {
                String msg = "Configuration file was not expected in store " +
                    "directory: " + kvConfigPath;
                throw new IllegalStateException(msg);
            }

            /**
             * Change Logger instances now that this class has an identity.
             */
            String newName = snp.getStorageNodeId().getFullName();
            logger = LoggerUtils.getLogger(StorageNodeAgentImpl.class, gp, snp);
            logger.fine("Storage Node named " + newName +
                        " Registering to store " + getStoreName());
            snai.getFaultHandler().setLogger(logger);
            if (adminService != null) {
                adminService.resetLogger(logger);
            }

            /**
             * Create a new KVstore config file for the store.
             */
            LoadParameters lp = new LoadParameters();
            lp.addMap(snp.getMap());
            if (snp.getMountMap() != null) {
                lp.addMap(snp.getMountMap());
            }
            lp.addMap(gp.getMap());
            lp.saveParameters(kvConfigPath);

            /**
             * Restart as if coming up for the first time but registered this
             * time.
             */
            start();
        } catch (Exception e) {

            /**
             * Any exceptions from register() will revert the SNA to bootstrap
             * mode.
             */
            String msg = "Register failed: " + e.getMessage() + "\n" +
                LoggerUtils.getStackTrace(e);
            revertToBootstrap();
            if (adminService == null) {
                startBootstrapAdmin();
            }
            logger.severe(msg);
            throw new IllegalStateException(msg, e);
        }
        return rri.getMaps();
    }

    public void shutdown(boolean stopServices, boolean force) {

        logger.info(snaName + ": Shutdown starting, " +
                    (stopServices ? "stopping services" :
                     "not stopping services") +
                    (force ? " (forced)" : ""));
        statusTracker.update(ServiceStatus.STOPPING);

        /**
         * Stop running services.  Minimally cause the SNA to not attempt to
         * restart.
         */
        stopRepNodeServices(stopServices, force);
        stopMasterBalanceManager();
        stopAdminService(stopServices, force);

        cleanupRegistry();
        logger.info(snaName + ": Shutdown complete");
        statusTracker.update(ServiceStatus.STOPPED);
        mgmtAgent.shutdown();
    }

    boolean createAdmin(AdminParams adminParams) {

        checkRegistered("createAdmin");
        String adminName = adminParams.getAdminId().getFullName();
        logger.info(adminName + ": Creating AdminService");

        /**
         * Creation of RepNodes, admins and parameter changes are synchronized
         * in order to coordinate changes to the config file(s).
         */
        synchronized(this) {

            /**
             * Add the new Admin to configuration.
             */
            LoadParameters lp =
                LoadParameters.getParameters(kvConfigPath, logger);

            /**
             * Does the Admin already exist?
             */
            if (lp.getMap(adminName) != null) {
                String msg = adminName +
                    ": AdminService exists in config file";
                logger.info(msg);
            } else {
                lp.addMap(adminParams.getMap());
                lp.saveParameters(kvConfigPath);
            }

            assert TestHookExecute.doHookIfSet(restartAdminHook, this);

            /**
             * If this SN is hosting the admin, don't start it; it is already
             * running.  It is possible that "isHostingAdmin()" will be true
             * and there is no bootstrap admin running.  This may be a
             * test-only case, but handle it anyway.
             */
            if (bp.isHostingAdmin() && adminService != null) {
                ManagedBootstrapAdmin mba =
                    (ManagedBootstrapAdmin) adminService.getService();
                mba.resetAsManagedAdmin
                    (adminParams, kvRoot, securityDir, snRoot, getStoreName(),
                     logger);

                /*
                 * Give the admin a chance to read potentially modified
                 * parameters
                 */
                try {
                    mba.getAdmin(this).newParameters();
                } catch (RemoteException e) {
                    String msg = adminName +
                        ": Failed to contact bootstrap AdminService";
                    logger.info(msg);
                    throw new IllegalStateException(msg, e);
                }
                logger.info(adminName + ": Created AdminService");
                return true;
            }
            return startAdminInternal(adminParams, false);
        }
    }

    /**
     * TODO: eventually use adminId if multiple admins are supported.
     */
    boolean stopAdmin(@SuppressWarnings("unused") AdminId adminId,
                                                 boolean force) {

        if (adminService == null) {
            String msg = "Stopping AdminService: service is not running";

            /**
             * Throw an exception if the service has not been created.
             */
            if (ConfigUtils.getAdminParams(kvConfigPath, logger) == null) {
                msg += "; service does not exist";
                throw new IllegalStateException(msg);
            }
            logger.warning(msg);
            return false;
        }
        String adminName = adminService.getService().getServiceName();
        boolean retval = stopAdminService(true, force);

        /**
         * Set disabled to true in config file
         */
        setServiceStoppedState
            (adminName, ParameterState.COMMON_DISABLED, true);
        return retval;
    }

    /**
     * Idempotent -- if the service does not exist, it is fine.
     * NOTE: for now the adminId is ignored as the SNA can only support a
     * single admin instance.  In order to fully support multiple admins
     * the name of the ParameterMap stored in the SNA's config file must
     * change to use the admin ID vs "adminParams" in order to uniquely
     * identify the correct instance to remove.  This is an upgrade issue.
     */
    boolean destroyAdmin(AdminId adminId, boolean deleteData) {

        boolean retval = false;

        if (adminService == null) {
            logger.warning("Destroying AdminService: service is not running");
        } else {
            String serviceName = adminService.getService().getServiceName();
            logger.info(serviceName + ": Destroying AdminService");
            retval = stopAdminService(true, true);
        }

        /**
         * Set bootstrap configuration to indicate that the Admin is no longer
         * hosted, if it was previously.
         */
        if (bp.isHostingAdmin()) {
            bp.setHostingAdmin(false);
            File configPath = new File(bootstrapDir, bootstrapFile);
            ConfigUtils.createBootstrapConfig(bp, configPath);
        }

        /**
         * Remove admin from the config file.  This happens even if the
         * stopAdminService() call above failed.
         */
        removeConfigurable(adminId, ParameterState.ADMIN_TYPE, deleteData);

        logger.info("Destroyed AdminService");
        return retval;
    }

    /**
     * Remove a RepNode or Admin from the config file.
     */
    boolean removeConfigurable(ResourceId rid, String type,
                               boolean deleteData) {
        ParameterMap map =
            ConfigUtils.removeComponent(kvConfigPath, rid, type, logger);
        if (deleteData && map != null) {
            /**
             * Determine data dir.  If this is not a RepNode it will not
             * have a mount point and the data dir will be in the default
             * location.
             */
            if (map.getType().equals(ParameterState.REPNODE_TYPE)) {
                RepNodeParams rnp = new RepNodeParams(map);
                File mountPoint = rnp.getMountPoint();
                if (mountPoint != null) {
                    File rnDir = new File(mountPoint, rid.getFullName());
                    if (rnDir.exists()) {
                        logger.info("Removing data directory for RepNode " +
                                    rid + ": " + rnDir);
                        removeFiles(rnDir);
                    }
                    return true;
                }
            }

            /**
             * If here, remove the data dir in the default location.  It may be
             * either a RepNode or Admin.
             */
            removeDataDir(rid);
        }
        return (map != null);
    }

    StringBuilder getStartupBuffer(ResourceId rid) {
        if (rid instanceof RepNodeId) {
            ServiceManager mgr = repNodeServices.get(rid.getFullName());
            if (mgr != null) {
                return mgr.getService().getStartupBuffer();
            }
        } else {
            ManagedAdmin ma = (ManagedAdmin) adminService.getService();
            if (rid.equals(ma.getResourceId())) {
                return ma.getStartupBuffer();
            }
        }
        throw new IllegalStateException
            ("Resource " + rid + " is not running on this storage node");
    }

    /**
     * Utility method used only by createRepNode to configure a newly-created
     * RepNode.   This method will never fail.  If the call to configure fails
     * it is logged (as SEVERE) but the RepNode will still exist as far as the
     * SNA is concerned.  If the RepNode does not manage to acquire a topology
     * that is a problem for the administrator.
     */
    private void
             configureRepNode(Set<Metadata<? extends MetadataInfo>> metadataSet,
                              RepNodeId rnid) {

        /**
         * When creating the RepNode it needs to be configured using the
         * Topology.  Don't wait all that long.  If the service is going
         * to start up and it's taking a while it will eventually get the
         * configuration information from other sources.
         */

        ServiceStatus[] targets =
            {ServiceStatus.WAITING_FOR_DEPLOY, ServiceStatus.RUNNING};

        /* This will log a failure to get the interface */
        RepNodeAdminAPI rnai = waitForRepNodeAdmin(rnid, targets,
                                                   repnodeWaitSecs);
        if (rnai != null) {
            try {
                rnai.configure(metadataSet);
            } catch (RemoteException re) {
                logsevere((rnid + ": Failed to configure RepNodeService"), re);
            }
        }
    }

    private void setServiceStoppedState(String serviceName,
                                        String paramName,
                                        boolean state) {

        LoadParameters lp =
            LoadParameters.getParameters(kvConfigPath, logger);
        ParameterMap map = lp.getMap(serviceName);
        if (map != null) {
            map.setParameter(paramName, Boolean.toString(state));
            lp.saveParameters(kvConfigPath);
        }
    }

    RepNodeParams lookupRepNode(RepNodeId rnid) {

        return ConfigUtils.getRepNodeParams(kvConfigPath, rnid, logger);
    }

    boolean createRepNode(RepNodeParams repNodeParams,
                          Set<Metadata<? extends MetadataInfo>> metadataSet) {

        /**
         * Creation of RepNodes, admins and parameter changes are synchronized
         * in order to coordinate changes to the config file(s).
         */
        synchronized(this) {
            RepNodeId rnid = repNodeParams.getRepNodeId();
            String serviceName = rnid.getFullName();
            logger.info(serviceName + ": Creating RepNode");

            /**
             * Add the new RepNode to configuration.
             */
            LoadParameters lp =
                LoadParameters.getParameters(kvConfigPath, logger);

            /**
             * The RepNode may be in the config file but not configured.  This
             * would happen if the SNA exited after writing the config file but
             * before the RepNode configure() method succeeded.
             *
             * So, allow the RepNode to exist and be running and if it's not
             * yet deployed, deploy (configure) it.  If it is deployed then
             * this call was in error so return false.
             */
            boolean startService = true;
            RepNodeParams rnp = null;
            ParameterMap map = lp.getMap(serviceName);
            if (map != null) {
                ServiceManager mgr = repNodeServices.get(serviceName);
                rnp = new RepNodeParams(map);
                if (mgr != null) {
                    /**
                       The service may have been created.  Don't start it.
                    */
                    String msg = serviceName +
                        ": RepNode exists, not starting process";
                    logger.info(msg);
                    startService = false;
                } else {
                    String msg = serviceName +
                        ": RepNode exists but is not running, will attempt " +
                        "to start it";
                    logger.info(msg);
                }
            } else {
                lp.addMap(repNodeParams.getMap());
                lp.saveParameters(kvConfigPath);
                rnp = ConfigUtils.getRepNodeParams(kvConfigPath, rnid, logger);
            }

            /*
             * Test: exit here between creation and startup.
             */
            assert TestHookExecute.doHookIfSet(restartRNHook, this);
            assert TestHookExecute.doHookIfSet(FAULT_HOOK, 0);

            /*
             * Start the service.
             */
            if (startService) {
                if (!startRepNodeInternal(rnp)) {
                    return false;
                }
            }

            /*
             * Test exit after start, before configure.
             */
            assert TestHookExecute.doHookIfSet(stopRNHook, this);
            logger.info(serviceName + ": Configuring RepNode");
            configureRepNode(metadataSet, rnid);
            logger.info(serviceName + ": Created RepNode");
            return true;
        }
    }

    /**
     * Start an already created RepNode.
     */
    boolean startRepNode(RepNodeId repNodeId) {

        String serviceName = repNodeId.getFullName();
        RepNodeParams rnp =
            ConfigUtils.getRepNodeParams(kvConfigPath, repNodeId, logger);
        if (rnp == null) {
            String msg = serviceName + ": RepNode has not been created";
            logger.info(msg);

            /* This rep node should have been created by this point. */
            throw new IllegalStateException(msg);
        }

        /**
         * Set active state in RNP.
         */
        setServiceStoppedState
            (serviceName, ParameterState.COMMON_DISABLED, false);
        return startRepNodeInternal(rnp);
    }

    /**
     * Test-oriented interface to wait for the RepNode to exit.
     */
    protected boolean waitForRepNodeExit(RepNodeId rnid, int timeoutSecs) {
        String serviceName = rnid.getFullName();
        ServiceManager mgr = repNodeServices.get(serviceName);
        if (mgr != null) {
            mgr.waitFor(timeoutSecs * 1000);
            return true;
        }
        return false;
    }

    void replaceRepNodeParams(RepNodeParams repNodeParams) {

        /**
         * Creation of RepNodes, admins and parameter changes are synchronized
         * in order to coordinate changes to the config file(s).
         */
        synchronized(this) {
            String serviceName = repNodeParams.getRepNodeId().getFullName();
            LoadParameters lp =
                LoadParameters.getParameters(kvConfigPath, logger);
            if (lp.removeMap(serviceName) == null) {
                throw new IllegalStateException
                    ("newProperties: RepNode service " + serviceName + " is not " +
                     "managed by this Storage Node: " + snaName);
            }
            lp.addMap(repNodeParams.getMap());
            lp.saveParameters(kvConfigPath);
        }
    }

    void replaceAdminParams(AdminId adminId, ParameterMap params) {

        /**
         * Creation of RepNodes, admins and parameter changes are synchronized
         * in order to coordinate changes to the config file(s).
         */
        synchronized(this) {
            String serviceName = adminId.getFullName();
            LoadParameters lp =
                LoadParameters.getParameters(kvConfigPath, logger);
            if (lp.removeMap(serviceName) == null) {
                if (lp.removeMapByType(ParameterState.ADMIN_TYPE) == null) {
                    throw new IllegalStateException
                        ("newProperties: Admin service " + serviceName +
                         " is not " + "managed by this Storage Node: " +
                         snaName);
                }
            }
            lp.addMap(params);
            lp.saveParameters(kvConfigPath);
        }
    }

    void replaceGlobalParams(GlobalParams globalParams) {

        /**
         * Creation of RepNodes, admins and global parameter changes are
         * synchronized in order to coordinate changes to the config file(s).
         */
        synchronized(this) {
            LoadParameters lp =
                LoadParameters.getParameters(kvConfigPath, logger);
            if (lp.removeMapByType(ParameterState.GLOBAL_TYPE) == null) {
                logger.warning("Missing GlobalParams on Storage Node: " +
                               snaName);
            }
            lp.addMap(globalParams.getMap());
            lp.saveParameters(kvConfigPath);
            globalParameterTracker.notifyListeners(
                null, globalParams.getMap());
        }
    }

    /**
     * Only a restricted set of parameters can be changed via this mechanism.
     * Because there are 2 files changed sequentially there's a slight chance
     * of inconsistency in the face of a failure.  There's nothing to be done
     * for that other than making the call again.
     *
     * The ParameterMap is one of two types -- normal parameters or a map of
     * mount points.  They are handled differently.  Normal parameters are
     * *merged* into the current map.  This mechanism does not allow actual
     * removal of parameters, which is the desired semantic.  The map
     * containing mount points is replaced, allowing removal.
     *
     * Because the map of mount points and some of the SNA parameters are also
     * part of the bootstrap state, those parameters are changed as well.
     */
    void newParams(ParameterMap params) {
        /**
         * Creation of RepNodes, admins and parameter changes are synchronized
         * in order to coordinate changes to the config file(s).
         */
        synchronized(this) {

            /**
             * Apply any changes to BootstrapParams first
             */
            changeBootstrapParams(params);

            boolean isModified = false;
            LoadParameters lp =
                LoadParameters.getParameters(kvConfigPath, logger);
            ParameterMap curMap = lp.getMap(ParameterState.SNA_TYPE);
            if (curMap == null) {
                throw new IllegalStateException
                    ("Could not get StorageNodeParams from file: " +
                     kvConfigPath);
            }
            StorageNodeParams snp = new StorageNodeParams(curMap);
            ParameterMap oldMap = curMap.copy();

            /**
             * Change params if there are new params
             */
            if (!isMountMap(params)) {
                ParameterMap diff = curMap.diff(params, false);
                logger.info("newParams, changing: " + diff);

                /**
                 * Merge the new/modified values into the current map.  This
                 * relies on the fact that the changeable bootstrap params are
                 * a subset of StorageNodeParams.
                 */
                if (curMap.merge(params, true) > 0) {
                    isModified = true;
                    if (lp.removeMap(curMap.getName()) == null) {
                        throw new IllegalStateException
                            ("Failed to remove StorageNodeParams from file");
                    }
                    lp.addMap(curMap);
                }
            } else {

                /**
                 * If the mount point map is new or modifies the existing one,
                 * apply the change.  The way to entirely clear a mount map is
                 * to pass an empty map rather than a null entry.  Null means
                 * that there is no mount point change.
                 */
                ParameterMap curMountMap =
                    lp.getMap(ParameterState.BOOTSTRAP_MOUNT_POINTS);
                if (curMountMap == null || !curMountMap.equals(params)) {
                    isModified = true;
                    if (curMountMap != null) {
                        lp.removeMap(curMountMap.getName());
                    }
                    logger.info("newParams, changing mount map:\n" +
                                "    from: " + curMountMap + "\n" +
                                "    to: " + params);
                    lp.addMap(params);
                }
            }

            /**
             * Apply changes to the config file
             */
            if (isModified) {
                lp.saveParameters(kvConfigPath);
                initSNParams(snp);
            }

            /* Update login policy according to new SNA params */
            if (isModified && !isMountMap(params)) {
                snParameterTracker.notifyListeners(oldMap, params);
            }
        }
    }

    /**
     * Return the SNA's notion of its current StorageNodeParams and
     * GlobalParams, for verification.
     */
    LoadParameters getParams() {
        return LoadParameters.getParameters(kvConfigPath, logger);
    }

    /**
     * Initialize locally cached values from the StorageNodeParams.  This is
     * called at registration time, during startupRegistered, and from
     * newParams, so that the new parameters can take effect.
     */
    private void initSNParams(StorageNodeParams snp) {
        serviceWaitMillis = snp.getServiceWaitMillis();
        repnodeWaitSecs = snp.getRepNodeStartSecs();
        maxLink = snp.getMaxLinkCount();
        linkExecWaitSecs = snp.getLinkExecWaitSecs();
        capacity = snp.getCapacity();
        logFileLimit = snp.getLogFileLimit();
        logFileCount = snp.getLogFileCount();
        numCPUs = snp.getNumCPUs();
        memoryMB = snp.getMemoryMB();
        mountPointsString = joinStringList(snp.getMountPoints(), ",");
        customProcessStartupPrefix = snp.getProcessStartupPrefix();

        /*
         * Start the management agent here.  This covers the cases of
         * startupRegistered and newParams.  If nothing regarding the
         * configuration of the MgmtAgent has changed, then the factory will
         * return the currently established MgmtAgent.
         */
        final MgmtAgent newMgmtAgent =
            MgmtAgentFactory.getAgent(this, snp, statusTracker);

        if (newMgmtAgent != mgmtAgent) {
            mgmtAgent = newMgmtAgent;

            /*
             * If any RepNodes are running at this time, let the new MgmtAgent
             * know about them.
             */
            for (ServiceManager mgr : repNodeServices.values()) {
                ManagedRepNode mrn = (ManagedRepNode) mgr.getService();
                try {
                    mgmtAgent.addRepNode(mrn.getRepNodeParams(), mgr);
                } catch (Exception e) {
                    String msg = mrn.getResourceId().getFullName() +
                        ": Exception adding RepNode to MgmtAgent: " +
                        e.getMessage();
                    throw new IllegalStateException(msg, e);
                }
            }

            /* If there is an admin, announce its existence.*/
            if (adminService != null) {
                ManagedAdmin ma = (ManagedAdmin) adminService.getService();
                try {
                    mgmtAgent.addAdmin(ma.getAdminParams(), adminService);
                } catch (Exception e) {
                    String msg = "Exception adding Admin to MgmtAgent: " +
                        e.getMessage();
                    throw new IllegalStateException(msg, e);
                }
            }
        }
    }

    /* Merge List of String into one String, with delimiter. */
    private static String joinStringList(List<String> a, String delimiter) {
        String r = "";
        if (a != null) {
            int n = 0;
            for (String s : a) {
                if (n++ > 0) {
                    r += delimiter;
                }
                r += s;
            }
        }
        return r;
    }

    /**
     * Change bootstrap config file.  Filter out non-bootstrap parameters and
     * possibly replace the mount map.  The map is *either* a normal parameter
     * map *or* a map of mount points.
     */
    private void changeBootstrapParams(ParameterMap params) {
        boolean isModified = false;
        File configPath = new File(bootstrapDir, bootstrapFile);
        bp = ConfigUtils.getBootstrapParams(configPath, logger);
        if (!isMountMap(params)) {
            ParameterMap curMap = bp.getMap();
            ParameterMap bmap =
                params.filter(EnumSet.of(ParameterState.Info.BOOT));
            if (bmap.size() > 0) {
                if (curMap.merge(bmap, true) > 0) {
                    isModified = true;
                }
            }
        } else {
            ParameterMap curMountMap = bp.getMountMap();
            if (curMountMap == null || !curMountMap.equals(params)) {
                isModified = true;
                bp.setMountMap(params);
            }
        }
        if (isModified) {
            ConfigUtils.createBootstrapConfig(bp, configPath);
        }
    }

    private boolean isMountMap(ParameterMap pmap) {
        String name = pmap.getName();
        return (name != null &&
                name.equals(ParameterState.BOOTSTRAP_MOUNT_POINTS));
    }

    /**
     * Functions for snapshot implementation
     */

    /**
     * List the snapshots on the SN.  Assume that if there is more than one
     * managed service they all have the same list, so pick the first one
     * found.
     */
    String [] listSnapshots() {

        ResourceId rid = null;
        File repNodeDir = null;

        AdminParams ap = ConfigUtils.getAdminParams(kvConfigPath, logger);
        if (ap != null) {
            rid = ap.getAdminId();
        } else {
            List<ParameterMap> repNodes =
                ConfigUtils.getRepNodes(kvConfigPath, logger);
            for (ParameterMap map : repNodes) {
                RepNodeParams rn = new RepNodeParams(map);
                rid = rn.getRepNodeId();
                repNodeDir = rn.getMountPoint();
                break;
            }
        }
        if (rid == null) {
            logger.warning("listSnapshots: Unable to find managed services");
            return new String[0];

        }
        File snapDir = FileNames.getSnapshotDir(kvRoot.toString(),
                                                getStoreName(),
                                                repNodeDir,
                                                snid,
                                                rid);
        if (snapDir.isDirectory()) {
            File [] snapFiles = snapDir.listFiles();
            String [] snaps = new String[snapFiles.length];
            for (int i = 0; i < snaps.length; i++) {
                snaps[i] = snapFiles[i].getName();
            }
            return snaps;
        }
                return new String[0];
    }

    String snapshotAdmin(AdminId aid, String name)
        throws RemoteException {

        if (adminService == null) {
            String msg = "AdminService " + aid + " is not running";
            logger.warning(msg);
            throw new IllegalStateException(msg);
        }
        try {
            ManagedAdmin ma = (ManagedAdmin) adminService.getService();
            CommandServiceAPI cs = ma.getAdmin(this);
            String[] files = cs.startBackup();
            String path = null;
            try {
                path = snapshot(aid, null, name, files);
            } finally {
                cs.stopBackup();
            }
            return path;
        } catch (RemoteException re) {
            logwarning(("Exception attempting to snapshot Admin " + aid), re);
            throw re;
        }
    }

    String snapshotRepNode(RepNodeId rnid, String name)
        throws RemoteException {

        String serviceName = rnid.getFullName();
        ServiceManager mgr = repNodeServices.get(serviceName);
        if (mgr == null) {
            String msg = rnid + ": RepNode is not running";
            logger.warning(msg);
            throw new IllegalStateException(msg);
        }
        try {
            ManagedRepNode mrn = (ManagedRepNode) mgr.getService();
            RepNodeAdminAPI rna = mrn.getRepNodeAdmin(this);
            File repNodeDir = mrn.getRepNodeParams().getMountPoint();
            String[] files = rna.startBackup();
            String path = null;
            try {
                path = snapshot(rnid, repNodeDir, name, files);
            } finally {
                rna.stopBackup();
            }
            return path;
        } catch (RemoteException re) {
            logwarning
                (("Exception attempting to snapshot RepNode " + rnid), re);
            throw re;
        }
    }

    /**
     * Shared code for RN and Admin snapshot removal.
     *
     * @name the name of the snapshot to remove, if null, remove all snapshots
     * @rid the ResourceId used to find the snapshot directory
     */
    void removeSnapshot(ResourceId rid, String name) {

        File serviceDir = null;
        if (rid instanceof RepNodeId) {
            RepNodeParams rnp =
                ConfigUtils.getRepNodeParams
                (kvConfigPath, (RepNodeId) rid, logger);
            if (rnp == null) {
                String msg = rid.getFullName() + ": RepNode has not been created";
                logger.info(msg);
                /* This rep node should have been created by this point. */
                throw new IllegalStateException(msg);
            }
            serviceDir = rnp.getMountPoint();
        }

        File snapshotDir =
            FileNames.getSnapshotDir(kvRoot.toString(),
                                     getStoreName(),
                                     serviceDir,
                                     snid,
                                     rid);
        if (name != null) {
            removeFiles(new File(snapshotDir, name));
        } else {
            if (snapshotDir.isDirectory()) {
                for (File file : snapshotDir.listFiles()) {
                    logger.info(rid + ": Removing snapshot " + file.getName());
                    removeFiles(file);
                }
            }
        }
    }

    /**
     * Make hard links from the array of file names in files in the source
     * directory to the destination directory.  On *nix and Solaris systems the
     * ln command can take the form:
     *   ln file1 file2 file3 ... fileN destinationDir
     * This allows a single exec to create links for a number of files,
     * amortizing the cost of the exec.  Issues to be aware of:
     * -- max number of args in a single command line
     * -- max length of command line
     */
    private void makeLinks(File srcBase, File destDir, String[] files) {
        if (isWindows) {
            for (String file : files) {
                File src = new File(srcBase, file);
                File dest = new File(destDir, file);
                windowsMakeLink(src, dest);
            }
            return;
        }
        int nfiles = 0;
        List<String> command = new ArrayList<String>();
        command.add(LINK_COMMAND);
        command.add("-f"); /* overwrites any existing target */
        for (String file : files) {
            command.add(new File(srcBase, file).toString());
            if (++nfiles >= maxLink) {
                /*
                 * Perform the operation and reset.
                 */
                command.add(destDir.toString());
                execute(command);
                command = new ArrayList<String>();
                command.add(LINK_COMMAND);
                command.add("-f"); /* overwrites any existing target */
                nfiles = 0;
            }
        }
        /*
         * Execute the command for the remaining files
         */
        if (command.size() > 2) {
            command.add(destDir.toString());
            execute(command);
        }
    }

    /**
     * Make a hard link from src (existing) to dest (the new link) This
     * executes "ln" in a new process to perform the link.  Ideally it'd be
     * built into Java but that's not the case (yet).
     */
    private void windowsMakeLink(File src, File dest) {
        if (!isWindows) {
            throw new IllegalStateException
                ("Function should only be called on Windows");
        }
        List<String> command = new ArrayList<String>();
        command.add("fsutil");
        command.add("hardlink");
        command.add("create");
        command.add(dest.toString());
        command.add(src.toString());
        execute(command);
    }

    private void execute(List<String> command) {
        /**
         * Leave off Logger argument.  It makes the log output too verbose
         */
        ProcessMonitor pm =
            new ProcessMonitor(command, 0, "snapshot", null);
        try {
            pm.startProcess();
            if (!pm.waitProcess(linkExecWaitSecs * 1000)) {
                throw new IllegalStateException
                    ("Timeout waiting for ln process to complete");
            }
        } catch (Exception e) {
            logger.info("Snapshot failed to make links with command: " + command + ": " + e);
            throw new SNAFaultException(e);
        }
    }

    /**
     * The guts of snapshot shared by RepNode and Admin backup.  Assumes that
     * DbBackup has been called on the service to get the file array.
     * 1.  make the target directory
     * 2.  for each log file in the list, make a hard link from it
     *     to the target directory
     *
     * @return the full path to the new snapshot directory
     */
    private String snapshot(ResourceId rid,
                            File serviceDir,
                            String name,
                            String [] files) {

        File srcBase = FileNames.getEnvDir(kvRoot.toString(),
                                           getStoreName(),
                                           serviceDir,
                                           snid,
                                           rid);
        File destBase = FileNames.getSnapshotDir(kvRoot.toString(),
                                                 getStoreName(),
                                                 serviceDir,
                                                 snid,
                                                 rid);
        File destDir = new File(destBase, name);
        logger.info("Creating snapshot of " + rid + ": " + name + " (" +
                    files.length + " files)");

        /**
         * Create the snapshot directory, make sure it does not exist first.
         */
        if (destDir.exists()) {
            String msg =
                "Snapshot directory exists, cannot overwrite: " + destDir;
            logger.warning(msg);
            throw new IllegalStateException(msg);
        }
        FileNames.makeDir(destDir);

        makeLinks(srcBase, destDir, files);
        logger.info("Completed snapshot of " + rid + ": " + name);
        return destDir.toString();
    }

    /**
     * Recursive delete.  Danger!
     */
    private void removeFiles(File target) {
        if (target.isDirectory()) {
            for (File f : target.listFiles()) {
                removeFiles(f);
            }
        }
        if (target.exists() && !target.delete()) {
            String msg = "Unable to remove file or directory " + target;
            logger.warning(msg);
            throw new IllegalStateException(msg);
        }
    }

    /**
     * This method must not be called for a running service.  See if there is
     * an environment ready to be used for recovery of the service.  If so,
     * replace the existing environment, if any, with the new one.
     */
    private void checkForRecovery(ResourceId rid, File dir) {
        File recoveryDir = FileNames.getRecoveryDir(kvRoot.toString(),
                                                    getStoreName(),
                                                    dir, snid, rid);
        if (recoveryDir.isDirectory()) {
            File[] files = recoveryDir.listFiles();
            if (files.length != 1) {
                logger.info(rid + ": only one file is allowed in recovery " +
                            " directory " + recoveryDir + ", not recovering");
                return;
            }
            if (!files[0].isDirectory()) {
                logger.info("Recovery file " + files[0] + " is not a directory"
                            + ", cannot use it for recovery");
                return;
            }

            File envDir = FileNames.getEnvDir(kvRoot.toString(),
                                              getStoreName(),
                                              dir, snid, rid);
            logger.info(rid + ": recovering from " + files[0] +
                        " to environment directory " + envDir);

            /**
             * First rename the old env dir if present.  TODO: maybe remove it.
             */
            if (envDir.isDirectory()) {
                File target = new File(envDir.toString() + ".old");
                if (target.exists()) {
                    removeFiles(target);
                }
                if (!envDir.renameTo(target)) {
                    logger.warning(rid + ": failed to rename old env dir");
                    return;
                }
            }
            if (!files[0].renameTo(envDir)) {
                logger.warning(rid + ": failed to rename recovery directory");
            }
        }
    }

    public MgmtAgent getMgmtAgent() {
        return mgmtAgent;
    }

    public Integer getCapacity() {
        return capacity;
    }

    public int getLogFileLimit() {
        return logFileLimit;
    }

    public int getLogFileCount() {
        return logFileCount;
    }

    public int getNumCpus() {
        return numCPUs;
    }

    public int getMemoryMB() {
        return memoryMB;
    }

    public String getMountPointsString() {
        return mountPointsString;
    }

    public SNASecurity getSNASecurity() {
        return snaSecurity;
    }

    public LoginManager getLoginManager() {
        return snaSecurity.getLoginManager();
    }

    ProcessFaultHandler getFaultHandler() {
        return snai.getFaultHandler();
    }
}
