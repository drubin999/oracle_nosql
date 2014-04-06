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
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import oracle.kv.impl.admin.CommandService;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.api.RequestHandler;
import oracle.kv.impl.api.RequestHandlerAPI;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.monitor.MonitorAgent;
import oracle.kv.impl.monitor.MonitorAgentAPI;
import oracle.kv.impl.rep.admin.RepNodeAdmin;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.security.login.LoginHandle;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.login.TrustedLogin;
import oracle.kv.impl.security.login.TrustedLoginAPI;
import oracle.kv.impl.security.login.UserLogin;
import oracle.kv.impl.security.login.UserLoginAPI;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.sna.StorageNodeAgentInterface;
import oracle.kv.impl.test.RemoteTestAPI;
import oracle.kv.impl.test.RemoteTestInterface;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.ResourceId.ResourceType;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.HostPort;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryArgs;
import oracle.kv.impl.util.registry.ssl.SSLClientSocketFactory;

/**
 * RegistryUtils encapsulates the operations around registry lookups and
 * registry bindings. It's responsible for producing RMI binding names for the
 * various remote interfaces used by the store, and wrapping them in an
 * appropriate API class.
 * <p>
 * All KV store registry binding names are of the form:
 * <p>
 * { kvstore name } : { component full name } :
 *                         [ main | monitor | admin | login | trusted_login ]
 * <p>
 * This class is responsible for the construction of these names. Examples:
 * FooStore:sn1:monitor names the monitor interface of the SNA.
 * Foostore:rg5-rn12:admin names the admin interface of the RN rg5-rn2.
 *
 * In future we may make provisions for making a version be a part of the name,
 * e.g. FooStore:sn1:monitor:5, for version 5 of the SNA monitor interface.
 * <p>
 * Note that all exception handling is done by the invokers of the get/rebind
 * methods.
 *
 * @see VersionedRemote
 */
public class RegistryUtils {

    /* The overriding registry CSF used on the client side. */
    private static ClientSocketFactory registryCSF = null;

    /* The map of kvstore name to registry ClientSocketFactory */
    private static final Map<String, ClientSocketFactory>
        storeToRegistryCSFMap =
        new ConcurrentHashMap<String, ClientSocketFactory>();

    /* The topology that is the basis for generation of binding names. */
    private final Topology topology;

    /* The login manager to be used for initializing APIs */
    private final LoginManager loginMgr;

    /* Used to separate the different name components */
    private static final String BINDING_NAME_SEPARATOR = ":";

    /*
     * The different interface types associated with a remotely accessed
     * topology component. The component, depending upon its function, may only
     * implement a subset of the interfaces listed below.
     */
    public enum InterfaceType {

        MAIN,     /* The main interface. */
        MONITOR,  /* The monitoring interface. */
        ADMIN,    /* The administration interface. */
        TEST,     /* The test interface. */
        LOGIN,    /* The standard login interface */
        TRUSTED_LOGIN; /* The trusted login interface */

        public String interfaceName() {
            return name().toLowerCase();
        }
    }

    /**
     * The constructor
     *
     * @param topology the topology used as the basis for operations
     * @param loginMgr a null-allowable LoginManager to be used for
     *  authenticating RMI access.  When accessing a non-secure instance
     *  or when executing only operations that do not require authentication,
     *  this may be null.
     */
    public RegistryUtils(Topology topology, LoginManager loginMgr) {
        super();
        this.topology = topology;
        this.loginMgr = loginMgr;
    }

    /**
     * Returns the API wrapper for the RMI stub associated with request handler
     * for the RN identified by <code>repNodeId</code>. If the RN is not
     * present in the topology <code>null</code> is returned.
     */
    public RequestHandlerAPI getRequestHandler(RepNodeId repNodeId)
        throws RemoteException, NotBoundException {

        final RepNode repNode = topology.get(repNodeId);

        return (repNode == null) ?
                null :
                RequestHandlerAPI.wrap
                       ((RequestHandler) lookup(repNodeId.getFullName(),
                                                InterfaceType.MAIN,
                                                repNode.getStorageNodeId()));
    }

    /**
     * Returns the API wrapper for the RMI stub associated with the
     * administration of the RN identified by <code>repNodeId</code>.
     */
    public RepNodeAdminAPI getRepNodeAdmin(RepNodeId repNodeId)
        throws RemoteException, NotBoundException {

        final RepNode repNode = topology.get(repNodeId);

        return RepNodeAdminAPI.wrap
            ((RepNodeAdmin) lookup(repNodeId.getFullName(),
                                   InterfaceType.ADMIN,
                                   repNode.getStorageNodeId()),
             getLogin(repNodeId));
    }

    /**
     * A RepNodeAdmin interface will look like this:
     * storename:rg1-rn1:ADMIN
     */
    public static boolean isRepNodeAdmin(String serviceName) {
        if (serviceName.toLowerCase().indexOf("rg") > 0) {
            return (serviceName.endsWith
                    (InterfaceType.ADMIN.toString()));
        }
        return false;
    }

    /**
     * Returns the API wrapper for the RMI stub associated with the
     * login of the RN identified by <code>repNodeId</code>.
     */
    public UserLoginAPI getRepNodeLogin(RepNodeId repNodeId)
        throws RemoteException, NotBoundException {

        final RepNode repNode = topology.get(repNodeId);

        final LoginHandle loginHdl = getLogin(repNodeId);

        return UserLoginAPI.wrap
            ((UserLogin) lookup(repNodeId.getFullName(),
                                InterfaceType.LOGIN,
                                repNode.getStorageNodeId()),
             loginHdl);
    }

    /**
     * Check whether the service name appears to be a valid RepNode UserLogin
     * interface for some kvstore.
     * @returns the store name from the RN login entry if this appears
     * to be a valid rep node login and null otherwise.
     */
    public static String isRepNodeLogin(String serviceName) {
        /*
         * A RepNode UserLogin interface will look like this:
         * storename:rg1-rn1:LOGIN
         */
        final int firstSep = serviceName.indexOf(BINDING_NAME_SEPARATOR);
        if (firstSep < 0) {
            return null;
        }
        if (serviceName.toLowerCase().indexOf(
                RepGroupId.getPrefix(), firstSep) > 0 &&
                serviceName.endsWith(
                    BINDING_NAME_SEPARATOR + InterfaceType.LOGIN.toString())) {
            return serviceName.substring(0, firstSep);
        }
        return null;
    }

    /**
     * Check whether the service name appears to be a valid RepNode UserLogin
     * interface for the named kvstore.
     */
    public static boolean isRepNodeLogin(String serviceName, String storeName) {
        return storeName.equals(isRepNodeLogin(serviceName));
    }

    /**
     * Returns the API wrapper for the RMI stub associated with the test
     * interface of the RN identified by <code>repNodeId</code>.
     */
    public RemoteTestAPI getRepNodeTest(RepNodeId repNodeId)
        throws RemoteException, NotBoundException {

        final RepNode repNode = topology.get(repNodeId);

        return RemoteTestAPI.wrap
            ((RemoteTestInterface) lookup(repNodeId.getFullName(),
                                          InterfaceType.TEST,
                                          repNode.getStorageNodeId()));
    }

    /**
     * Returns the API wrapper for the RMI stub associated with the monitor for
     * the service identified by <code>resourceId</code>. This should only be
     * called for components that support monitoring. An attempt to obtain an
     * agent for a non-monitorable component will result in an {@link
     * IllegalStateException}.
     */
    public static MonitorAgentAPI getMonitor(String storeName,
                                             String snHostname,
                                             int snRegistryPort,
                                             ResourceId resourceId,
                                             LoginManager loginMgr)
        throws RemoteException, NotBoundException {

        return MonitorAgentAPI.wrap
            ((MonitorAgent) getInterface(storeName, snHostname, snRegistryPort,
                                         resourceId.getFullName(),
                                         InterfaceType.MONITOR),
             getLogin(loginMgr, snHostname, snRegistryPort,
                      resourceId.getType()));
    }

    /**
     * Looks up the Admin (CommandService) in the registry without benefit of
     * the Topology. Used to bootstrap storage node registration.
     * @throws RemoteException
     * @throws NotBoundException
     */
    public static CommandServiceAPI getAdmin(String hostname,
                                             int registryPort,
                                             LoginManager loginMgr)
        throws RemoteException, NotBoundException {

        final Registry registry = getRegistry(hostname, registryPort);

        return CommandServiceAPI.wrap
            ((CommandService)
             registry.lookup(GlobalParams.COMMAND_SERVICE_NAME),
             getLogin(loginMgr, hostname, registryPort, ResourceType.ADMIN));
    }

    /**
     * Look up an Admin (CommandService) when a Topology is available.
     * @throws RemoteException
     * @throws NotBoundException
     */
    public CommandServiceAPI getAdmin(StorageNodeId snid)
        throws RemoteException, NotBoundException {

        final Registry registry = getRegistry(snid);
        return CommandServiceAPI.wrap
            ((CommandService)
             registry.lookup(GlobalParams.COMMAND_SERVICE_NAME),
             getLogin(snid));
    }

    public RemoteTestAPI getAdminTest(StorageNodeId snid)
        throws RemoteException, NotBoundException {

        final Registry registry = getRegistry(snid);
        return RemoteTestAPI.wrap
            ((RemoteTestInterface)
             registry.lookup(GlobalParams.COMMAND_SERVICE_TEST_NAME));
    }

    public static RemoteTestAPI getAdminTest(String hostname,
                                             int registryPort)
        throws RemoteException, NotBoundException {

        final Registry registry = getRegistry(hostname, registryPort);
        return RemoteTestAPI.wrap
            ((RemoteTestInterface)
             registry.lookup(GlobalParams.COMMAND_SERVICE_TEST_NAME));
    }

    public UserLoginAPI getAdminLogin(StorageNodeId snid)
        throws RemoteException, NotBoundException {

        final Registry registry = getRegistry(snid);
        return UserLoginAPI.wrap
            ((UserLogin)
             registry.lookup(GlobalParams.ADMIN_LOGIN_SERVICE_NAME),
             getLogin(snid));
    }

    public static UserLoginAPI getAdminLogin(String hostname,
                                             int registryPort,
                                             LoginManager loginMgr)
        throws RemoteException, NotBoundException {

        final Registry registry = getRegistry(hostname, registryPort);
        return UserLoginAPI.wrap
            ((UserLogin)
             registry.lookup(GlobalParams.ADMIN_LOGIN_SERVICE_NAME),
             getLogin(loginMgr, hostname, registryPort, ResourceType.ADMIN));
    }

    /**
     * Returns an API wrapper for the RMI handle to the RepNodeAdmin.
     */
    public static RepNodeAdminAPI getRepNodeAdmin(String storeName,
                                                  String snHostname,
                                                  int snRegistryPort,
                                                  RepNodeId rnId,
                                                  LoginManager loginMgr)
        throws RemoteException, NotBoundException {

        return RepNodeAdminAPI.wrap
                ((RepNodeAdmin) getInterface(storeName,
                                             snHostname,
                                             snRegistryPort,
                                             rnId.getFullName(),
                                             InterfaceType.ADMIN),
                 getLogin(loginMgr, snHostname, snRegistryPort,
                          ResourceType.REP_NODE));
    }

    /**
     * Returns an API wrapper for the RMI handle to the RepNode UserLogin.
     */
    public static UserLoginAPI getRepNodeLogin(String storeName,
                                               String snHostname,
                                               int snRegistryPort,
                                               RepNodeId rnId,
                                               LoginManager loginMgr)
        throws RemoteException, NotBoundException {

        return getRepNodeLogin(storeName, snHostname, snRegistryPort,
                               rnId.getFullName(), loginMgr);
    }

    /**
     * Returns an API wrapper for the RMI handle to the RepNode UserLogin.
     */
    public static UserLoginAPI getRepNodeLogin(String storeName,
                                               String snHostname,
                                               int snRegistryPort,
                                               String rnFullName,
                                               LoginManager loginMgr)
        throws RemoteException, NotBoundException {

        return UserLoginAPI.wrap
                ((UserLogin) getInterface(storeName,
                                          snHostname,
                                          snRegistryPort,
                                          rnFullName,
                                          InterfaceType.LOGIN),
                 getLogin(loginMgr, snHostname, snRegistryPort,
                          ResourceType.REP_NODE));
    }

    /**
     * If a startup problem is found for this resource, throw an exception with
     * the information, because that's the primary problem.
     */
    public static void checkForStartupProblem(String storeName,
                                              String snHostname,
                                              int snRegistryPort,
                                              ResourceId rId,
                                              StorageNodeId snId,
                                              LoginManager loginMgr) {

        StringBuilder startupProblem = null;

        try {
            final StorageNodeAgentAPI sna = getStorageNodeAgent(storeName,
                                                                snHostname,
                                                                snRegistryPort,
                                                                snId,
                                                                loginMgr);
            startupProblem =  sna.getStartupBuffer(rId);
        } catch (Exception secondaryProblem) {

            /*
             * We couldn't get to the SNA to check. Eat this secondary problem
             * so the first problem isn't hidden.
             */
        }

        if (startupProblem != null) {
            throw new OperationFaultException("Problem starting process for " +
                                              rId + ":" +
                                              startupProblem.toString());
        }
    }

    private static Remote getInterface(String storeName,
                                       String snHostname,
                                       int snRegistryPort,
                                       String rnName,
                                       InterfaceType interfaceType)
        throws RemoteException, NotBoundException {

        final Registry registry =
            getRegistry(snHostname, snRegistryPort, storeName);
        return registry.lookup(bindingName(storeName, rnName, interfaceType));
    }

    /**
     * Looks up a StorageNode in the registry without benefit of the
     * Topology. Used to bootstrap storage node registration.
     * @throws RemoteException
     * @throws NotBoundException
     */
    public static StorageNodeAgentAPI getStorageNodeAgent(String hostname,
                                                          int registryPort,
                                                          String serviceName,
                                                          LoginManager loginMgr)
        throws RemoteException, NotBoundException {

        final Registry registry = getRegistry(hostname, registryPort);

        final StorageNodeAgentInterface snai =
            (StorageNodeAgentInterface) registry.lookup(serviceName);

        final LoginHandle loginHdl = getLogin(loginMgr, hostname, registryPort,
                                              ResourceType.STORAGE_NODE);

        return StorageNodeAgentAPI.wrap(snai, loginHdl);
    }

    public StorageNodeAgentAPI getStorageNodeAgent(StorageNodeId storageNodeId)
        throws RemoteException, NotBoundException {

        final StorageNodeAgentInterface snai =
            (StorageNodeAgentInterface) lookup(storageNodeId.getFullName(),
                                               InterfaceType.MAIN,
                                               storageNodeId);

        final LoginHandle loginHdl = getLogin(storageNodeId);

        return StorageNodeAgentAPI.wrap(snai, loginHdl);
    }

    public static RemoteTestAPI getStorageNodeAgentTest
        (String storeName, String hostname,
         int registryPort, StorageNodeId snid)
        throws RemoteException, NotBoundException {

        final Registry registry = getRegistry(hostname, registryPort);
        final String serviceName = bindingName(storeName,
                                               snid.getFullName(),
                                               InterfaceType.TEST);
        return RemoteTestAPI.wrap
            ((RemoteTestInterface) registry.lookup(serviceName));
    }

    public RemoteTestAPI getStorageNodeAgentTest(StorageNodeId storageNodeId)
        throws RemoteException, NotBoundException {

        return RemoteTestAPI.wrap
            ((RemoteTestInterface) lookup(storageNodeId.getFullName(),
                                          InterfaceType.TEST,
                                          storageNodeId));
    }

    /**
     * Get the SNA trusted login
     */
    public static TrustedLoginAPI getStorageNodeAgentLogin(String hostname,
                                                           int registryPort)
        throws RemoteException, NotBoundException {

        final Registry registry = getRegistry(hostname, registryPort);

        return TrustedLoginAPI.wrap
            ((TrustedLogin) registry.lookup(
                GlobalParams.SNA_LOGIN_SERVICE_NAME));
    }

    /**
     * Get the non-bootstrap SNA interface without using Topology.
     */
    public static StorageNodeAgentAPI getStorageNodeAgent(String storeName,
                                                          String snHostname,
                                                          int snRegistryPort,
                                                          StorageNodeId snId,
                                                          LoginManager loginMgr)
        throws RemoteException, NotBoundException {

        final StorageNodeAgentInterface snai =
            (StorageNodeAgentInterface) getInterface(storeName,
                                                     snHostname,
                                                     snRegistryPort,
                                                     snId.getFullName(),
                                                     InterfaceType.MAIN);
        final LoginHandle loginHdl =
            getLogin(loginMgr, snHostname, snRegistryPort,
                     ResourceType.STORAGE_NODE);

        return StorageNodeAgentAPI.wrap(snai, loginHdl);
    }

    /**
     * Get the non-bootstrap SNA interface without using Topology.
     */
    public static StorageNodeAgentAPI getStorageNodeAgent(String storename,
                                                          StorageNode sn,
                                                          LoginManager loginMgr)
        throws RemoteException,
        NotBoundException {

        return getStorageNodeAgent(storename,
                                   sn.getHostname(),
                                   sn.getRegistryPort(),
                                   sn.getResourceId(),
                                   loginMgr);
    }

    /**
     * Get the non-bootstrap SNA interface using the given Parameters object.
     */
    public static StorageNodeAgentAPI getStorageNodeAgent(Parameters parameters,
                                                          StorageNodeId snId,
                                                          LoginManager loginMgr)
        throws RemoteException, NotBoundException {
        final String storename = parameters.getGlobalParams().getKVStoreName();
        return getStorageNodeAgent(storename, parameters.get(snId),
                                   snId, loginMgr);
    }

    public static StorageNodeAgentAPI getStorageNodeAgent(String storename,
                                                          StorageNodeParams snp,
                                                          StorageNodeId snId,
                                                          LoginManager loginMgr)
        throws RemoteException, NotBoundException {

        return getStorageNodeAgent(storename, snp.getHostname(),
                                   snp.getRegistryPort(), snId, loginMgr);
    }

    /**
     * Rebinds the RMI stub associated with request handler for the RN
     * identified by <code>repNodeId</code>.
     */
    public void rebind(RepNodeId repNodeId, RequestHandler requestHandler)
        throws RemoteException {

        final RepNode repNode = topology.get(repNodeId);
        rebind(repNodeId.getFullName(), InterfaceType.MAIN,
               repNode.getStorageNodeId(), requestHandler);
    }

    /**
     * Rebinds the RMI stub associated with monitor agent for the RN identified
     * by <code>repNodeId</code>.
     */
    public void rebind(RepNodeId repNodeId, MonitorAgent monitorAgent)
        throws RemoteException {

        final RepNode repNode = topology.get(repNodeId);
        rebind(repNodeId.getFullName(), InterfaceType.MONITOR,
               repNode.getStorageNodeId(), monitorAgent);
    }

    /**
     * Rebinds the RMI stub associated with administration of the RN
     * identified by <code>repNodeId</code>.
     */
    public void rebind(RepNodeId repNodeId, RepNodeAdmin repNodeAdmin)
        throws RemoteException {

        final RepNode repNode = topology.get(repNodeId);
        rebind(repNodeId.getFullName(), InterfaceType.ADMIN,
               repNode.getStorageNodeId(), repNodeAdmin);
    }

    /**
     * Rebinds the object in the registry after assembling the binding name
     * from its base name and suffix if any.
     *
     * @param baseName the base name
     * @param interfaceType the suffix if any to be appended to the baseName
     * @param storageNodeId the storage node used to locate the registry
     * @param object the remote object to be bound
     *
     * @throws RemoteException
     */
    private void rebind(String baseName,
                        InterfaceType interfaceType,
                        StorageNodeId storageNodeId,
                        Remote object)
        throws RemoteException {

        final Registry registry = getRegistry(storageNodeId);
        final Remote stub = export(object, null, null);
        try {
            registry.rebind(bindingName(baseName, interfaceType), stub);
        } catch (RemoteException re) {
            UnicastRemoteObject.unexportObject(object, true);
            throw re;
        }
    }

    /**
     * Rebinds the object in the registry after assembling the binding name.
     * This form of rebind is used when a Topology object is not yet available.
     *
     * @param hostname the hostname associated with the registry
     * @param registryPort the registry port
     * @param storeName the name of the KV store
     * @param baseName the base name
     * @param interfaceType the suffix if any to be appended to the baseName
     * @param object the remote object to be bound
     * @param clientSocketFactory the client socket factory; a null value
     *                             results in use of RMI default timeout values
     * @param serverSocketFactory the server socket factory; a null value
     *                             results in use of RMI default bind values
     *
     * @throws RemoteException
     */
    public static void rebind(String hostname,
                              int registryPort,
                              String storeName,
                              String baseName,
                              InterfaceType interfaceType,
                              Remote object,
                              ClientSocketFactory clientSocketFactory,
                              ServerSocketFactory serverSocketFactory)
        throws RemoteException {

        rebind(hostname, registryPort,
               bindingName(storeName, baseName, interfaceType), object,
               clientSocketFactory,
               serverSocketFactory);
    }

    /**
     * Overloading only for rebinds where the default socket factories are
     * adequate. This may be in unit tests, or with KVS services that don't
     * need fine control over server connection backlogs or client timeouts.
     */
    public static void rebind(String hostname,
                              int registryPort,
                              String storeName,
                              String baseName,
                              InterfaceType interfaceType,
                              Remote object)
        throws RemoteException {

        rebind(hostname, registryPort,
               storeName, baseName, interfaceType, object,
               null, null); /* Use default socket factories. */
    }

    /**
     * Rebinds the object in the registry using the specified service name.
     * This form of rebind is used by the StorageNodeAgent before it is
     * registered with a Topology.
     *
     * @param hostname the hostname associated with the registry
     * @param registryPort the registry port
     * @param serviceName
     * @param object the remote object to be bound
     *
     * @throws RemoteException
     */
    public static void rebind(String hostname,
                              int registryPort,
                              String serviceName,
                              Remote object,
                              ClientSocketFactory clientSocketFactory,
                              ServerSocketFactory serverSocketFactory)
        throws RemoteException {

        final Registry registry = getRegistry(hostname, registryPort);
        final Remote stub =
                export(object, clientSocketFactory, serverSocketFactory);

        try {
            registry.rebind(serviceName, stub);
        } catch (RemoteException re) {
            UnicastRemoteObject.unexportObject(object, true);
            throw new RemoteException("Can't rebind " + serviceName + " at " +
                                      hostname + ":" + registryPort +
                                      " csf: " + clientSocketFactory +
                                      " ssf: " + serverSocketFactory, re);
        }
    }

    /**
     * Unbinds the object in the registry after assembling the binding name. It
     * also unexports the object after letting all active operations finish.
     *
     * @param hostname the hostname associated with the registry
     * @param registryPort the registry port
     * @param storeName the name of the KV store
     * @param baseName the base name
     * @param interfaceType the suffix if any to be appended to the baseName
     * @param object the object being unbound
     *
     * @return true if the registry had a binding and it was unbound,false if
     * the binding was not present in the registry
     *
     * @throws RemoteException
     */
    public static boolean unbind(String hostname,
                                 int registryPort,
                                 String storeName,
                                 String baseName,
                                 InterfaceType interfaceType,
                                 Remote object)
        throws RemoteException {

        return unbind(hostname,
                      registryPort,
                      bindingName(storeName, baseName, interfaceType),
                      object);
    }

    /**
     * Static unbind method for non-topology object.
     */
    public static boolean unbind(String hostname,
                                 int registryPort,
                                 String serviceName,
                                 Remote object)
        throws RemoteException {

        final Registry registry = getRegistry(hostname, registryPort);

        try {
            registry.unbind(serviceName);
            /* Stop accepting new calls for this object. */
            UnicastRemoteObject.unexportObject(object, true);
            return true;
        } catch (NotBoundException e) {
            return false;
        }
    }

    /**
     * Returns the binding name used in the registry.
     */
    public static String bindingName(String storeName,
                                     String baseName,
                                     InterfaceType interfaceType) {
        return storeName + BINDING_NAME_SEPARATOR +
            baseName  + BINDING_NAME_SEPARATOR + interfaceType;
    }

    /**
     * Initialize socket policies for client utility use (e.g. admin client).
     * Whether to use SSL is determined from system properties.
     * TBD: the reference to system property usage could be invalidated by
     * pending propsed change to the security configuration API.
     */
    public static void initRegistryCSF() {
        final RMISocketPolicy rmiPolicy =
            ClientSocketFactory.ensureRMISocketPolicy();

        final String registryCsfName =
            ClientSocketFactory.registryFactoryName();
        final SocketFactoryArgs args = new SocketFactoryArgs().
            setCsfName(registryCsfName);

        setRegistryCSF(rmiPolicy.getRegistryCSF(args), null);
    }


    /**
     * Set socket timeouts for ClientSocketFactory.
     * Only for KVStore client usage.
     * Whether to use SSL is determined from system properties, if not yet
     * configured.
     */
    public static synchronized void setRegistrySocketTimeouts(
        int connectMs, int readMs, String storeName) {

        final RMISocketPolicy rmiPolicy =
            ClientSocketFactory.ensureRMISocketPolicy();
        final String registryCsfName =
            ClientSocketFactory.registryFactoryName();
        final SocketFactoryArgs args = new SocketFactoryArgs().
            setKvStoreName(storeName).
            setCsfName(registryCsfName).
            setCsfConnectTimeout(connectMs).
            setCsfReadTimeout(readMs);

        setRegistryCSF(rmiPolicy.getRegistryCSF(args), storeName);
    }

    /**
     * Set the supplied CSF for use during registry lookups within the
     * KVStore server.
     */
    public static synchronized void setServerRegistryCSF
        (ClientSocketFactory clientSocketFactory) {
        setRegistryCSF(clientSocketFactory, null);
    }

    /**
     * Set the supplied CSF for use during registry lookups, but except when
     * testing and not using SSL. If a storeName is specified, the CSF is
     * set as the default CSF AND for store-specific use.  If storeName is null
     * it is only used as the default registry CSF.
     */
    public static synchronized void setRegistryCSF
        (ClientSocketFactory clientSocketFactory, String storeName) {

        /*
         * TBD: The disabling of CSF setting when TestStatus.isActive() is true
         * might no longer be needed.  If so, the SSL test can also be removed.
         */
        if (!TestStatus.isActive() ||
            clientSocketFactory == null ||
            clientSocketFactory.getClass() == SSLClientSocketFactory.class) {
            registryCSF = clientSocketFactory;
            if (storeName != null) {
                storeToRegistryCSFMap.put(storeName, clientSocketFactory);
            }
        }
    }

    /**
     * Common function to get a Registry reference using a pre-configured CSF
     * with an optional storeName as a CSF lookup key, for cases where multiple
     * stores are accessed concurrently.
     */
    public static synchronized Registry getRegistry(
        String hostname, int port, String storeName)
        throws RemoteException {

        ClientSocketFactory csf =
            (storeName == null) ? null : storeToRegistryCSFMap.get(storeName);
        if (csf == null) {
            csf = registryCSF;
        }
        return LocateRegistry.getRegistry(hostname, port, csf);
    }

    /**
     * Convenience version of the method above, where the storeName is not
     * specified.  This is suitable for all calls within a KVStore
     * component and for calls by applications that have no ability to access
     * multiple stores concurrently.
     */
    public static synchronized Registry getRegistry(String hostname, int port)
        throws RemoteException {

        return getRegistry(hostname, port, null);
    }

    public static int findFreePort(int start, int end, String hostname) {
        ServerSocket serverSocket = null;
        for (int current = start; current <= end; current++) {
            try {
                /*
                 * Try a couple different methods to be sure that the port is
                 * truly available.
                 */
                serverSocket = new ServerSocket(current);
                serverSocket.close();

                /**
                 * Now using the hostname.
                 */
                serverSocket = new ServerSocket();
                final InetSocketAddress sa =
                    new InetSocketAddress(hostname, current);
                serverSocket.bind(sa);
                serverSocket.close();
                return current;
            } catch (IOException e) /* CHECKSTYLE:OFF */ {
                /* Try the next port number. */
            } /* CHECKSTYLE:ON */
        }
        return 0;
    }

    /* Internal Utility methods. */

    private static Remote export(Remote object,
                                 ClientSocketFactory csf,
                                 ServerSocketFactory ssf)
        throws RemoteException {

        int port = 0;
        ServerSocket ss = null;
        if (ssf !=  null) {
            try {
                /*
                 * Allow the ssf to determine what server socket should be
                 * used.  This allows us to specify the port number when
                 * exporting the object, which allows the server socket to
                 * be deterministically closed when this object is unexported.
                 */
                ss = ssf.prepareServerSocket();
                if (ss != null) {
                    port = ss.getLocalPort();
                }
            } catch (IOException ioe) {
                throw new ExportException(
                    "Unable to create ServerSocket for export", ioe);
            }
        }

        try {
            final Remote remote =
                UnicastRemoteObject.exportObject(object, port, csf, ssf);

            /*
             * The prepared server socket, if any should have been consumed,
             * so null out ss to suppress the call to discardServerSocket
             * below.
             */
            ss = null;
            return remote;
        } finally {
            /* If the export did not succeed, we may need to clean up */
            if (ss != null && ssf != null) {
                ssf.discardServerSocket(ss);
            }
        }
    }

    /**
     * Returns the binding name used in the registry.
     */
    private String bindingName(String baseName, InterfaceType interfaceType) {
        return bindingName(topology.getKVStoreName(), baseName, interfaceType);
    }

    /**
     * Returns the registry associated with the storage node.
     */
    private Registry getRegistry(StorageNodeId storageNodeId)
        throws RemoteException {

        final StorageNode storageNode = topology.get(storageNodeId);

        return getRegistry(storageNode.getHostname(),
                           storageNode.getRegistryPort(),
                           topology.getKVStoreName());
    }

    /**
     * Looks up an object in the registry after assembling the binding name
     * from its base name and suffix if any.
     *
     * @param baseName the base name
     * @param interfaceType the suffix if any to be appended to the baseName
     * @param topology the topology used to resolve all resource ids
     * @param storageNodeId the storage node used to locate the registry
     *
     * @return the stub associated with the remote object
     *
     * @throws RemoteException
     * @throws NotBoundException
     */
    private Remote lookup(String baseName,
                          InterfaceType interfaceType,
                          StorageNodeId storageNodeId)
        throws RemoteException, NotBoundException {

        final Registry registry = getRegistry(storageNodeId);

        return registry.lookup(bindingName(baseName, interfaceType));
    }

    private LoginHandle getLogin(ResourceId resourceId) {

        if (loginMgr == null) {
            return null;
        }

        /*
         * Because not all LoginManagers can resolve topology, attempt to
         * resolve it here if possible.
         */
        if (topology != null) {
            final Topology.Component<?> comp = topology.get(resourceId);
            if (comp != null) {
                final StorageNodeId snId = comp.getStorageNodeId();
                final StorageNode sn = topology.get(snId);
                if (sn != null) {
                    return loginMgr.getHandle(
                        new HostPort(sn.getHostname(),
                                     sn.getRegistryPort()),
                        resourceId.getType());
                }
            }
        }

        return loginMgr.getHandle(resourceId);
    }

    private static LoginHandle getLogin(LoginManager loginMgr,
                                        String hostname,
                                        int registryPort,
                                        ResourceType rtype) {

        if (loginMgr == null) {
            return null;
        }

        return loginMgr.getHandle(new HostPort(hostname, registryPort),
                                  rtype);
    }

}
