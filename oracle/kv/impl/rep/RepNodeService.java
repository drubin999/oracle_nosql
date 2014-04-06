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

package oracle.kv.impl.rep;

import java.io.File;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetSocketAddress;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.api.RequestDispatcher;
import oracle.kv.impl.api.RequestDispatcherImpl;
import oracle.kv.impl.api.RequestHandlerImpl;
import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.fault.SystemFaultException;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.monitor.AgentRepository;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterListener;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterTracker;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.rep.admin.RepNodeAdmin;
import oracle.kv.impl.rep.admin.RepNodeAdminImpl;
import oracle.kv.impl.rep.login.RepNodeLoginService;
import oracle.kv.impl.rep.monitor.MonitorAgentImpl;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.login.LoginUpdater;
import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.ConfigurableService;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.ServiceStatusTracker;
import oracle.kv.impl.util.UserDataControl;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.registry.ServerSocketFactory;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationGroup;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;
import com.sleepycat.je.rep.utilint.HostPortPair;

/**
 * This is the "main" that represents the RepNode. It handles startup and
 * houses all the pieces of software that share a JVM such as a request
 * handler, the administration support and the monitor agent.
 */
public class RepNodeService implements ConfigurableService {

    private RepNodeId repNodeId;
    private Params params;

    /**
     * The components that make up the service.
     */
    private RequestHandlerImpl reqHandler = null;
    private RepNodeSecurity rnSecurity = null;
    private RepNodeAdminImpl admin = null;
    private MonitorAgentImpl monitorAgent = null;
    private RepNodeLoginService loginService = null;
    private RepNode repNode = null;

    /**
     * The thread that monitors other RNs looking for updates to metadata.
     */
    private MetadataUpdateThread metadataUpdateThread = null;
    
    /**
     *  The status of the service
     */
    private ServiceStatusTracker statusTracker;

    /**
     * Statistics tracker for user operations
     */
    private OperationsStatsTracker opStatsTracker;

    /**
     * Parameter change tracker
     */
    private final ParameterTracker parameterTracker;

    /**
     * Global parameter change tracker
     */
    private final ParameterTracker globalParameterTracker;

    /**
     * The object used to coordinate concurrent request to stop the rep node
     * service.
     */
    private final Object stopLock = new Object();

    /**
     * The fault handler associated with the service.
     */
    private final RepNodeServiceFaultHandler faultHandler;

    /**
     * True if running in a thread context.
     */
    private final boolean usingThreads;

    /**
     * These are the default CMS GC parameters that are always applied. They
     * can be overridden and/or enhanced by user-supplied parameters.
     *
     * A note about the rationale behind the CMSInitiatingOccupancyFraction
     * setting: We currently size the JE cache size (by default) at 70% of the
     * total heap and use a NewRatio (the ratio of old to new space) of 18. So
     * if NewRatio + 1 represents the total heap space, old space is
     *
     * (1 - ( 1 / (NewRatio + 1)) * 100 =
     *
     * (1 - (1 / (18 + 1)) * 100 = 94.7% of total heap space.
     *
     * 70% of total heap used for the JE cache represents (70/94.7)*100 =
     * 73.92% of old space. The CMS setting of 77% allows us a 77% - 73.92 =
     * 3.08% margin for error when the JE cache is full, before unproductive
     * CMS cycles kick in.
     *
     * SR 22779 has a more detailed discussion of this topic.
     */
    public static final String DEFAULT_CMS_GC_ARGS=
        "-XX:+UseConcMarkSweepGC " +
        "-XX:+UseParNewGC " +
        "-XX:+DisableExplicitGC " +
        "-XX:NewRatio=18 " +
        "-XX:SurvivorRatio=4 " +
        "-XX:MaxTenuringThreshold=10 " +
        "-XX:CMSInitiatingOccupancyFraction=77 " ;

    /**
     * These are the default arguments that are used if the G1 GC is
     * explicitly enabled.
     */
    public static final String DEFAULT_G1_GC_ARGS=
        "-XX:+UseG1GC " +
        "-XX:NewRatio=15 " +
        "-XX:MaxTenuringThreshold=10 " +
        "-XX:InitiatingHeapOccupancyPercent=80";

    public static final String DEFAULT_MISC_JAVA_ARGS =
        /*
         * Don't use large pages on the Mac, where they are not supported and,
         * in Java 1.7.0_13, doing so with these other flags causes a crash.
         * [#22165]
         */
        ((!"Mac OS X".equals(System.getProperty("os.name"))) ?
            "-XX:+UseLargePages" :
            "");

    protected Logger logger;

    /**
     * The constructor. Nothing interesting happens until the call to
     * newProperties.
     */
    public RepNodeService() {
        this(false);
    }

    public RepNodeService(boolean usingThreads) {
        super();
        faultHandler = new RepNodeServiceFaultHandler
            (this, logger, ProcessExitCode.RESTART);
        this.usingThreads = usingThreads;
        parameterTracker = new ParameterTracker();
        globalParameterTracker = new ParameterTracker();
    }

    /**
     * Initialize a RepNodeService.  This must be invoked before start() is
     * called.
     */
    public void initialize(SecurityParams securityParams,
                           RepNodeParams repNodeParams,
                           LoadParameters lp) {

        securityParams.initRMISocketPolicies();

        GlobalParams globalParams =
            new GlobalParams(lp.getMap(ParameterState.GLOBAL_TYPE));

        StorageNodeParams storageNodeParams =
            new StorageNodeParams(lp.getMap(ParameterState.SNA_TYPE));

        /* construct the Params from its components */
        params = new Params(securityParams, globalParams,
                            storageNodeParams, repNodeParams);

        repNodeId = repNodeParams.getRepNodeId();

        /*
         * The AgentRepository is the buffered monitor data and belongs to the
         * monitorAgent, but is instantiated outside to take care of
         * initialization dependencies. Don't instantiate any loggers before
         * this is in place, because constructing the AgentRepository registers
         * it in LoggerUtils, and ensures that loggers will tie into the
         * monitoring system.
         */
        AgentRepository monitorBuffer =
            new AgentRepository(globalParams.getKVStoreName(), repNodeId);

        logger = LoggerUtils.getLogger(this.getClass(), params);
        faultHandler.setLogger(logger);

        /*
         * Any socket timeouts observed by the ClientSocketFactory in this
         * process will be logged to this logger.
         */
        ClientSocketFactory.setTimeoutLogger(logger);

        rnSecurity = new RepNodeSecurity(this, logger);

        RequestDispatcher requestDispatcher = new RequestDispatcherImpl
            (globalParams.getKVStoreName(),
             params.getRepNodeParams(),
             rnSecurity.getLoginManager(),
             new ThreadExceptionHandler(),
             logger);

        rnSecurity.setDispatcher(requestDispatcher);

        statusTracker = new ServiceStatusTracker(logger, monitorBuffer);

        repNode =  new RepNode(params, requestDispatcher, this);
        rnSecurity.setTopologyManager(requestDispatcher.getTopologyManager());
        reqHandler = new RequestHandlerImpl(requestDispatcher, faultHandler,
                                            rnSecurity.getAccessChecker()) ;

        repNode.initialize(params, reqHandler.getListenerFactory());
        addParameterListener(repNode.getRepEnvManager());

        opStatsTracker =  new OperationsStatsTracker
            (this, params.getRepNodeParams().getMap(), monitorBuffer);
        reqHandler.initialize(params, repNode, opStatsTracker);
        addParameterListener(opStatsTracker);

        admin = new RepNodeAdminImpl(this, repNode);
        loginService = new RepNodeLoginService(this);
        monitorAgent = new MonitorAgentImpl(this, monitorBuffer);

        metadataUpdateThread = new MetadataUpdateThread(requestDispatcher,
                                                        repNode,
                                                        logger);
        metadataUpdateThread.start();

        addParameterListener(UserDataControl.getParamListener());

        final LoginUpdater loginUpdater = new LoginUpdater();

        loginUpdater.addServiceParamsUpdaters(rnSecurity);
        loginUpdater.addGlobalParamsUpdaters(rnSecurity);

        loginUpdater.addServiceParamsUpdaters(loginService);
        loginUpdater.addGlobalParamsUpdaters(loginService);

        addParameterListener(loginUpdater.new ServiceParamsListener());
        addGlobalParameterListener(
            loginUpdater.new GlobalParamsListener());

        /* Initialize system properties. */
        File kvDir = FileNames.getKvDir(storageNodeParams.getRootDirPath(),
                                        globalParams.getKVStoreName());

        String policyFile;
        try {
            policyFile =
                FileNames.getSecurityPolicyFile(kvDir).getCanonicalPath();
        } catch (IOException e) {
            throw new SystemFaultException
                ("IO Exception trying to access security policy file: " +
                 FileNames.getSecurityPolicyFile(kvDir), e);
        }

        if (policyFile != null && new File(policyFile).exists()) {
            System.setProperty("java.security.policy", policyFile);
        }

        if (System.getSecurityManager() == null) {
            System.setSecurityManager(new SecurityManager());
        }

        /* Sets the hostname to be associated with rmi stubs. */
        System.setProperty("java.rmi.server.hostname",
                           storageNodeParams.getHostname());
        /* Disable to allow for faster timeouts on failed connections. */
        System.setProperty("java.rmi.server.disableHttp", "true");

        if (!usingThreads) {
            storageNodeParams.setRegistryCSF(securityParams);
        }
    }

    /**
     * Notification that there are modified service parameters.
     */
    synchronized public void newParameters() {
        ParameterMap oldMap = params.getRepNodeParams().getMap();
        ParameterMap newMap =
            ConfigUtils.getRepNodeMap(params.getStorageNodeParams(),
                                      params.getGlobalParams(),
                                      repNodeId, logger);

        /* Do nothing if maps are the same */
        if (oldMap.equals(newMap)) {
            logger.info("newParameters are identical to old parameters");
            return;
        }
        logger.info("newParameters: refreshing parameters");
        params.setRepNodeParams(new RepNodeParams(newMap));
        parameterTracker.notifyListeners(oldMap, newMap);
    }

    /**
     * Notification that there are modified global parameters
     */
    synchronized public void newGlobalParameters() {
        ParameterMap oldMap = params.getGlobalParams().getMap();
        ParameterMap newMap =
            ConfigUtils.getGlobalMap(params.getStorageNodeParams(),
                                     params.getGlobalParams(),
                                     logger);

        /* Do nothing if maps are the same */
        if (oldMap.equals(newMap)) {
            logger.info(
                "newGlobalParameters are identical to old global parameters");
            return;
        }
        logger.info("newGlobalParameters: refreshing global parameters");
        params.setGlobalParams(new GlobalParams(newMap));
        globalParameterTracker.notifyListeners(oldMap, newMap);
    }

    public void addParameterListener(ParameterListener listener) {
        parameterTracker.addListener(listener);
    }

    private void addGlobalParameterListener(ParameterListener listener) {
        globalParameterTracker.addListener(listener);
    }
    /**
     * Starts the RepNodeService.
     *
     * @see #start(Topology)
     */
    @Override
    public void start() {

        start(null);
    }

    /**
     * Starts the RepNodeService.
     *
     * Invokes the startup method on each of the components constituting the
     * service. Proper sequencing of the startup methods is important and is
     * explained in the embedded method components.
     * <p>
     * The <code>topology</code> argument is non-null only in test situations
     * and is used to bypass the explicit initialization of the service by the
     * SNA. When a topology is passed in, the service does not need to wait for
     * the SNA to supply the topology since it's already available.
     *
     * @param topology the topology to use if non null.
     */
    public void start(Topology topology) {

        statusTracker.update(ServiceStatus.STARTING);

        try {

            logger.info("Starting RepNodeService");
            /*
             * Start the monitor agent first, so the monitor can report state
             * and events.
             */
            monitorAgent.startup();

            /*
             * Start up admin.  Most requests will fail until the repNode is
             * started but this is done early to allow ping() to function.
             */
            admin.startup();

            /*
             * Start up the repNode so that it's available for admin and user
             * requests. The startup results in a replicated environment handle
             * being established. This may take some time if there is a long
             * recovery involved.
             */
            final ReplicatedEnvironment repEnv = repNode.startup();

            topologyStartup(repEnv, topology);

            /*
             * Starts the RepNodeSecurity, which relies on topology startup
             */
            rnSecurity.startup();

            /*
             * Start servicing requests last after the node has been
             * initialized. It's at this point that the reqHandler appears in
             * the registry and requests can be sent to it.
             */
            reqHandler.startup();

            /*
             * Start the login service.
             */
            loginService.startup();

            statusTracker.update(ServiceStatus.RUNNING);
            logger.info("Started RepNodeService");
        } catch (RemoteException re) {
            statusTracker.update(ServiceStatus.ERROR_NO_RESTART);
            throw new IllegalStateException
                ("RepNodeService startup failed", re);
        }
    }

    /**
     * Establishes topology during service startup.
     * <p>
     * For an established node, the Topology is stored in a non-replicated
     * database in the environment. If the topology is available there, no
     * further action is needed.
     * <p>
     * If the Topology is not available in the environment, and this is the
     * first node in the replication group, it waits for the SNA to supply the
     * Topology via a {@link RepNodeAdminImpl#configure call}.
     * <p>
     * For second and subsequent nodes in the replication group, it simply goes
     * ahead expecting the normal Topology propagation mechanism to discover it
     * has an empty Topology and therefore push the current Topology to it. Any
     * requests directed to the node while it's waiting for the Topology to be
     * pushed are rejected, and the request dispatcher redirects them.
     * <p>
     * Note that a newly created replica may get two configure calls, one from
     * the SNA and another redundant call from the Topology push mechanism.
     * This is routine and it simply uses the most current topology as usual.
     *
     * @param repEnv the startup environment handle
     *
     * @param testTopology non null if the topology is explicitly supplied as
     * part of a unit test
     */
    private void topologyStartup(ReplicatedEnvironment repEnv,
                                 Topology testTopology)
        throws RemoteException {

        if (testTopology != null) {
            /*
             * Topology supplied (a test scenario) use it directly,
             * effectively simulating the call the SNA would have made.
             */
            logger.info("Test environment topology self-supplied.");
            final Set<Metadata<? extends MetadataInfo>> metadataSet =
                new HashSet<Metadata<? extends MetadataInfo>>(1);
            metadataSet.add(testTopology);
            admin.configure(metadataSet,
                            (AuthContext) null, SerialVersion.CURRENT);
            return;
        }

        if (repNode.getTopology() != null) {
            /* Have been initialized with topology, keep going. */
            return;
        }

        /*
         * Proceed to establish the request handler interface, so NOP requests
         * can be serviced, the empty topology detected and new Topology pushed
         * to this node.
         */
        logger.info("Topology needs to be pushed to this node.");
    }

    /**
     * Invokes the stop method on each of the components constituting the
     * service. Proper sequencing of the stop methods is important and is
     * explained in the embedded method comments.
     *
     * If the service has already been stopped the method simply returns.
     */
    @Override
    public void stop(boolean force) {
        synchronized (stopLock) {
            if (statusTracker.getServiceStatus().isTerminalState()) {
                /* If the service has already been stopped. */
                return;
            }
            statusTracker.update(ServiceStatus.STOPPING);

            try {

                /*
                 * Stop the login handler first, so we are no longer
                 * accepting requests.
                 */
                loginService.stop();

                /*
                 * Stop the request handler next, so we are no longer
                 * accepting requests.
                 */
                reqHandler.stop();

                /*
                 * Push all stats out of the operation stats collector, to
                 * attempt to get them to the admin monitor or to the local log.
                 */
                opStatsTracker.pushStats();

                /*
                 * Stop the admin. Note that an admin shutdown request may be
                 * in progress, but the request should not be impacted by
                 * stopping the admin service. Admin services may be using the
                 * environment, so stop them first.
                 */
                admin.stop();

                /*
                 * Close any internal kvstore handle that may be in use
                 */
                rnSecurity.stop();

                /*
                 * Stop the metadata updater
                 */
                if (metadataUpdateThread != null) {
                    metadataUpdateThread.shutdown();
                }

                /*
                 * Stop the rep node next.
                 */
                repNode.stop(force);

                /* Set the status to STOPPED */
                statusTracker.update(ServiceStatus.STOPPED);

                /*
                 * Shutdown the monitor last.
                 */
                monitorAgent.stop();
            } catch (RemoteException re) {
                statusTracker.update(ServiceStatus.ERROR_NO_RESTART);
                throw new IllegalStateException
                    ("RepNodeService stop failed", re);
            } finally {
                if (!usingThreads) {
                    /* Flush any log output. */
                    LoggerUtils.closeHandlers
                        ((params != null) ?
                         params.getGlobalParams().getKVStoreName() :
                         "UNKNOWN");
                }
            }
        }
    }

    public ServiceStatusTracker getStatusTracker() {
        return statusTracker;
    }

    public OperationsStatsTracker getOpStatsTracker() {
        return opStatsTracker;
    }

    public RepNodeService.Params getParams() {
        return params;
    }

    /**
     * Returns the RepNodeId associated with the service
     */
    public RepNodeId getRepNodeId() {
        return repNode.getRepNodeId();
    }

    public StorageNodeId getStorageNodeId() {
        return params.getStorageNodeParams().getStorageNodeId();
    }

    public RepNodeParams getRepNodeParams() {
        return params.getRepNodeParams();
    }

    /**
     * Returns the fault handler associated with the service
     */
    public ProcessFaultHandler getFaultHandler() {
       return faultHandler;
    }

    /**
     * Returns the current SecurityMetadata
     */
    public SecurityMetadata getSecurityMetadata() {
        return repNode.getSecurityMDManager().getSecurityMetadata();
    }

    public boolean getUsingThreads() {
        return usingThreads;
    }

    /**
     * Rebinds the remote component in the registry associated with this SN
     */
    public void rebind(Remote remoteComponent,
                       RegistryUtils.InterfaceType type,
                       ClientSocketFactory clientSocketFactory,
                       ServerSocketFactory serverSocketFactory)
        throws RemoteException {

    	StorageNodeParams snp = params.getStorageNodeParams();
        RegistryUtils.rebind(snp.getHostname(), snp.getRegistryPort(),
                             params.getGlobalParams().getKVStoreName(),
                             getRepNodeId().getFullName(), type,
                             remoteComponent,
                             clientSocketFactory,
                             serverSocketFactory);
    }

    /**
     * Unbinds the remote component in the registry associated with this SN
     */
    public boolean unbind(Remote remoteComponent,
                          RegistryUtils.InterfaceType type)
        throws RemoteException {

    	StorageNodeParams snp = params.getStorageNodeParams();

        return RegistryUtils.unbind
            (snp.getHostname(), snp.getRegistryPort(),
             params.getGlobalParams().getKVStoreName(),
             getRepNodeId().getFullName(), type, remoteComponent);
    }

    RepNode getRepNode() {
        return repNode;
    }

    public RequestHandlerImpl getReqHandler() {
        return reqHandler;
    }

    public RepNodeAdmin getRepNodeAdmin() {
        return admin;
    }

    public RepNodeSecurity getRepNodeSecurity() {
        return rnSecurity;
    }

    public LoginManager getLoginManager() {
        return rnSecurity.getLoginManager();
    }

    /**
     * Issue a BDBJE update of the target node's HA address.
     */
    public void updateMemberHAAddress(String groupName,
                                      String targetNodeName,
                                      String targetHelperHosts,
                                      String newNodeHostPort) {

        /*
         * Setup the helper hosts to use for finding the master to execute this
         * update.
         */
        Set<InetSocketAddress> helperSockets = new HashSet<InetSocketAddress>();
        StringTokenizer tokenizer = 
                new StringTokenizer(targetHelperHosts,
                                    ParameterUtils.HELPER_HOST_SEPARATOR);
        while (tokenizer.hasMoreTokens()) {
            String helper = tokenizer.nextToken();
            helperSockets.add(HostPortPair.getSocket(helper));
        }

        /*
         * Check the target node's HA address. If it is already changed
         * and if that node is alive, don't make any further changes. If the
         * target still has its old HA address, attempt an update.
         */
        ReplicationGroupAdmin rga =
            new ReplicationGroupAdmin(groupName, helperSockets,
                                      ((repNode != null) ?
                                       repNode.getRepNetConfig() : null));
        ReplicationGroup rg = rga.getGroup();
        com.sleepycat.je.rep.ReplicationNode jeRN =
            rg.getMember(targetNodeName);
        if (jeRN == null) {
            throw new IllegalStateException
                (targetNodeName + " does not exist in replication group " +
                 groupName);
        }

        String newHostName = HostPortPair.getHostname(newNodeHostPort);
        int newPort = HostPortPair.getPort(newNodeHostPort);

        if ((jeRN.getHostName().equals(newHostName)) &&
            (jeRN.getPort() == newPort)) {

            /*
             * This node is already changed, nothing more to do. Do this
             * check in case the change has been made previously, and this
             * node is alive, as the updateAddress() call will incur an
             * exception if the node is alive.
             */
            return;
        }

        rga.updateAddress(targetNodeName, newHostName, newPort);
    }

    /**
     * A convenience class to package all the parameter components used by
     * the Rep Node service
     */
    public static class Params {
        private final SecurityParams securityParams;
        private volatile GlobalParams globalParams;
        private final StorageNodeParams storageNodeParams;
        private volatile RepNodeParams repNodeParams;

        public Params(SecurityParams securityParams,
                      GlobalParams globalParams,
                      StorageNodeParams storageNodeParams,
                      RepNodeParams repNodeParams) {
            super();
            this.securityParams = securityParams;
            this.globalParams = globalParams;
            this.storageNodeParams = storageNodeParams;
            this.repNodeParams = repNodeParams;
        }

        public SecurityParams getSecurityParams() {
            return securityParams;
        }

        public GlobalParams getGlobalParams() {
            return globalParams;
        }

        public StorageNodeParams getStorageNodeParams() {
            return storageNodeParams;
        }

        public RepNodeParams getRepNodeParams() {
            return repNodeParams;
        }

        public void setRepNodeParams(RepNodeParams params) {
            repNodeParams = params;
        }

        public void setGlobalParams(GlobalParams params) {
            globalParams = params;
        }
    }

    /**
     * The uncaught exception handler associated with top level threads run by
     * the request dispatcher. In the RN service an unhandled exception results
     * in the process being restarted by the SNA.
     */
    private class ThreadExceptionHandler implements UncaughtExceptionHandler {

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            if (logger != null) {
                logger.log(Level.SEVERE, "uncaught exception in thread", e);
            } else {
                System.err.println("Uncaught exception:");
                e.printStackTrace(System.err);
            }

            /* Have the SNA restart the service. */
            faultHandler.queueShutdown(e, ProcessExitCode.RESTART);
        }
    }
}
