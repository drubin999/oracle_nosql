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

package oracle.kv.util;

import java.io.IOException;
import java.io.PrintStream;
import java.rmi.AccessException;
import java.rmi.ConnectException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.PasswordCredentials;
import oracle.kv.impl.admin.CommandService;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.rep.RepNodeStatus;
import oracle.kv.impl.rep.admin.RepNodeAdmin;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.security.login.LoginHandle;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.sna.StorageNodeStatus;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.ResourceId.ResourceType;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.FormatUtils;
import oracle.kv.impl.util.HostPort;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * Pings all the RNs and SNs associated with a KVS. It's provided a registry
 * on some SN in the KVS. It uses this registry to locate some one RN whose
 * Topology is then used to obtain knowledge about all the other nodes in the
 * KVS.
 *
 * Ping also provides utility methods to other functionality that needs to
 * ping remote services.
 */
public class Ping {

    /* External commands, for "java -jar" usage. */
    public static final String COMMAND_NAME = "ping";
    public static final String COMMAND_DESC =
        "attempts to contact a store to get status of running services";
    public static final String COMMAND_ARGS =
       CommandParser.getHostUsage() + " " +
       CommandParser.getPortUsage() + " " +
       CommandParser.getUserUsage() + "\n\t" +
       CommandParser.getSecurityUsage();

    private static boolean verbose = false;
    private static boolean isSecured = false;
    private static PasswordCredentials loginCreds = null;

    public static void main(String[] args)
        throws RemoteException {

        class PingParser extends CommandParser {

            PingParser(String[] args1) {
                super(args1);
            }

            @Override
            public void usage(String errorMsg) {
                if (errorMsg != null) {
                    System.err.println(errorMsg);
                }
                System.err.println(KVSTORE_USAGE_PREFIX + COMMAND_NAME +
                                   "\n\t" + COMMAND_ARGS);
                System.exit(-1);
            }

            @Override
            protected boolean checkArg(String arg) {
                return false;
            }

            @Override
            protected void verifyArgs() {
                if (getHostname() == null) {
                    missingArg(HOST_FLAG);
                }
                if (getRegistryPort() == 0) {
                    missingArg(PORT_FLAG);
                }
            }
        }

        PingParser pp = new PingParser(args);
        pp.parseArgs();
        Ping.verbose = pp.getVerbose();

        final KVStoreLogin storeLogin =
            new KVStoreLogin(pp.getUserName(), pp.getSecurityFile());
        prepareAuthentication(storeLogin);

        if (Ping.isSecured) {
            try {
                Ping.loginCreds = storeLogin.makeShellLoginCredentials();
            } catch (IOException ioe) {
                System.err.println("Failed to get login credentials: " +
                                   ioe.getMessage());
                return;
            }
        }

        Topology topology =
            getTopology(pp.getHostname(), pp.getRegistryPort());
        if (topology == null) {
            return;
        }
        pingTopology(topology, System.err);
    }

    private static void prepareAuthentication(final KVStoreLogin storeLogin) {
        try {
            storeLogin.loadSecurityProperties();
        } catch (IllegalArgumentException iae) {
            System.out.println(iae.getMessage());
        }

        /* Needs authentication */
        Ping.isSecured = storeLogin.foundSSLTransport();
        storeLogin.prepareRegistryCSF();
    }

    private interface StorageNodeCallback {
        void nodeCallback(StorageNode sn, StorageNodeStatus status);
    }

    private interface RepNodeCallback {
        void nodeCallback(RepNode rn, RepNodeStatus status);
    }

    public static String displayStorageNode(Topology topology,
                                            StorageNode sn,
                                            StorageNodeStatus status) {
        final Datacenter dc = topology.get(sn.getDatacenterId());
        final String dcName = (dc != null) ? dc.getName() : "?";
        final String dcType =
            (dc != null) ? String.valueOf(dc.getDatacenterType()) : "?";
        final String snStatus = status != null ?
            ("   Status: " + status.getServiceStatus() +
             "   Ver: " + status.getKVVersion()) : "UNREACHABLE";
        return "Storage Node [" +
            sn.getResourceId() + "] on " +
            sn.getHostname() + ":" + sn.getRegistryPort() +
            "    Zone: [name=" + dcName +
            " id=" + sn.getDatacenterId() +
            " type=" + dcType + "] " +
            snStatus;
    }

    private static void outputStorageNode(Topology topology,
                                          StorageNode sn,
                                          StorageNodeStatus status,
                                          PrintStream out) {
        out.println(displayStorageNode(topology, sn, status));
    }

    public static String displayRepNode(RepNode rn,
                                        RepNodeStatus status) {
        return "\tRep Node [" +
            rn.getResourceId() + "]\tStatus: " +
            (status != null ?
             status +
             String.format(" at sequence number: %,d",
                           status.getVlsn()) +
             " haPort: " + status.getHAPort() :
             "UNREACHABLE");

    }

    private static void outputRepNode(RepNode rn,
                                      RepNodeStatus status,
                                      PrintStream out) {
        out.println(displayRepNode(rn, status));
    }

    /**
     * Pings all the SNs and RNs that make up the topology, print results
     * to the specified PrintStream.
     */
    public static void pingTopology(Topology topology, PrintStream out) {

        final Map<StorageNode, StorageNodeStatus> snmap =
            new HashMap<StorageNode, StorageNodeStatus>();

        final Map<RepNode, RepNodeStatus> rnmap =
            new HashMap<RepNode, RepNodeStatus>();

        out.println("Pinging components of store " +
                    topology.getKVStoreName() +
                    " based upon topology sequence #" +
                    topology.getSequenceNumber());
        out.println("Time: " +
                    FormatUtils.formatDateAndTime (System.currentTimeMillis()));
        out.println(topology.getKVStoreName() + " comprises " +
                    topology.getPartitionMap().getNPartitions() +
                    " partitions and " +
                    topology.getStorageNodeMap().size() +
                    " Storage Nodes");

        /**
         * Collect status of SNs and RNs
         */
        forEachStorageNode(topology, new StorageNodeCallback() {
                @Override
                public void nodeCallback(StorageNode sn,
                                         StorageNodeStatus status) {
                    snmap.put(sn, status);
                }
            });

        forEachRepNode(topology, new RepNodeCallback() {
                @Override
                public void nodeCallback(RepNode rn, RepNodeStatus status) {
                    rnmap.put(rn, status);
                }
            });

        /**
         * Group output by Storage Node.
         */
        List<StorageNode> sns = new ArrayList<StorageNode>(snmap.keySet());

        Collections.sort(sns, new Comparator<StorageNode>() {

                @Override
                public int compare(StorageNode o1, StorageNode o2) {
                    return o1.getStorageNodeId().getStorageNodeId() -
                        o2.getStorageNodeId().getStorageNodeId();
                }});

        /* Print in sn order. */
        for (StorageNode sn : sns) {
            StorageNodeStatus status = snmap.get(sn);
            outputStorageNode(topology, sn, status, out);
            for (Entry<RepNode, RepNodeStatus> rentry : rnmap.entrySet()) {
                final RepNode rn = rentry.getKey();
                if (sn.getStorageNodeId().equals(rn.getStorageNodeId())) {
                    outputRepNode(rn, rentry.getValue(), out);
                }
            }
        }
    }

    /**
     * Returns a service status map of all the SNs and RNs that make up the
     * topology.
     *
     * @return a map of all node resource ids to their statuses.
     */
    public static Map<ResourceId, ServiceStatus>
        getTopologyStatus(Topology topology) {

        final Map<ResourceId, ServiceStatus> ret =
            new HashMap<ResourceId, ServiceStatus>();

        forEachStorageNode(topology, new StorageNodeCallback() {
                @Override
                public void nodeCallback(StorageNode sn,
                                         StorageNodeStatus status) {
                    ret.put(sn.getResourceId(),
                            status != null ? status.getServiceStatus() :
                            ServiceStatus.UNREACHABLE);
                }
            });

        forEachRepNode(topology, new RepNodeCallback() {
                @Override
                public void nodeCallback(RepNode rn, RepNodeStatus status) {
                    ret.put(rn.getResourceId(),
                            (status != null ? status.getServiceStatus() :
                             ServiceStatus.UNREACHABLE));
                }
            });
        return ret;
    }

    /**
     * Returns the replication node which is the master for the specified
     * replication group. If the master is not found @code null is returned.
     *
     * @return a replication node or @code null
     */
    public static RepNode getMaster(Topology topology, RepGroupId rgId) {

        final List<RepNode> master = new ArrayList<RepNode>();

        forEachRepNode(topology, rgId, new RepNodeCallback() {
                @Override
                public void nodeCallback(RepNode rn, RepNodeStatus status) {
                    if ((status != null) &&
                        status.getReplicationState().isMaster()) {
                        master.add(rn);
                    }
                }
            });
        return (master.size() == 1) ? master.get(0) : null;
    }

    public static RepNodeStatus getMasterStatus(Topology topology,
                                                RepGroupId rgId) {

        final List<RepNodeStatus> master = new ArrayList<RepNodeStatus>();

        forEachRepNode(topology, rgId, new RepNodeCallback() {
                @Override
                public void nodeCallback(RepNode rn, RepNodeStatus status) {
                    if ((status != null) &&
                        status.getReplicationState().isMaster()) {
                        master.add(status);
                    }
                }
            });
        return (master.size() == 1) ? master.get(0) : null;
    }

    /**
     * Get replication node status for each node in the shard.
     */
    public static Map<RepNodeId, RepNodeStatus>
        getRepNodeStatus(Topology topology, RepGroupId rgId) {

        /* Get status for each node in the shard */
        final Map<RepNodeId, RepNodeStatus> statusMap =
            new HashMap<RepNodeId, RepNodeStatus>();

        forEachRepNode(topology, rgId, new RepNodeCallback() {
                @Override
                public void nodeCallback(RepNode rn, RepNodeStatus status) {
                    statusMap.put(rn.getResourceId(), status);
                }
            });
        return statusMap;
    }


    private static void forEachStorageNode(final Topology topology,
                                           StorageNodeCallback callback) {

        /* LoginManager not needed for ping */
        RegistryUtils regUtils = new RegistryUtils(topology,
                                                   (LoginManager) null);

        for (StorageNode sn : topology.getStorageNodeMap().getAll()) {
            StorageNodeStatus status = null;
            try {
                StorageNodeAgentAPI sna =
                    regUtils.getStorageNodeAgent(sn.getResourceId());
                status = sna.ping();
            } catch (RemoteException re) {
                if (verbose) {
                    System.err.println
                        ("Ping failed for " + sn.getResourceId() + ": " +
                         re.getMessage());
                    re.printStackTrace();
                }
            } catch (NotBoundException e) {
                if (verbose) {
                    System.err.println("No RMI service for SN: " +
                                       sn.getResourceId() +
                                       " message: " + e.getMessage());
                }
            }
            callback.nodeCallback(sn, status);
        }
    }

    private static void forEachRepNode(final Topology topology,
                                       RepNodeCallback callback) {

        /* LoginManager not needed for ping */
        RegistryUtils regUtils = new RegistryUtils(topology,
                                                   (LoginManager) null);

        for (RepGroup rg : topology.getRepGroupMap().getAll()) {
            for (RepNode rn : rg.getRepNodes()) {
                RepNodeStatus status = null;
                try {
                    RepNodeAdminAPI rna =
                        regUtils.getRepNodeAdmin(rn.getResourceId());
                    status = rna.ping();
                } catch (RemoteException re) {
                    if (verbose) {
                        System.err.println("Ping failed for " +
                                           rn.getResourceId() + ": " +
                                           re.getMessage());
                        re.printStackTrace();
                    }
                } catch (NotBoundException e) {
                    if (verbose) {
                        System.err.println("No RMI service for RN: " +
                                           rn.getResourceId() +
                                           " message: " + e.getMessage());
                    }
                }
                callback.nodeCallback(rn, status);
            }
        }
    }

    private static void forEachRepNode(final Topology topology,
                                       RepGroupId rgId,
                                       RepNodeCallback callback) {

        final RepGroup group = topology.get(rgId);

        if (group == null) {
            return;
        }
        /* LoginManager not needed for ping */
        final RegistryUtils regUtils = new RegistryUtils(topology,
                                                         (LoginManager) null);

        for (RepNode rn : group.getRepNodes()) {
            RepNodeStatus status = null;
            try {
                RepNodeAdminAPI rna =
                    regUtils.getRepNodeAdmin(rn.getResourceId());
                status = rna.ping();
            } catch (RemoteException re) {
                if (verbose) {
                    System.err.println("Ping failed for " +
                                       rn.getResourceId() + ": " +
                                       re.getMessage());
                    re.printStackTrace();
                }
            } catch (NotBoundException e) {
                if (verbose) {
                    System.err.println("No RMI service for RN: " +
                                       rn.getResourceId() +
                                       " message: " + e.getMessage());
                }
            }
            callback.nodeCallback(rn, status);
        }
    }

    /**
     * Give a storage node in the kvstore, find a copy of the topology from
     * one of its resident repNodes.
     */
    public static Topology getTopology(String snHostname, int snRegistryPort)

        throws RemoteException, AccessException {

        try {
            final Registry snRegistry =
                RegistryUtils.getRegistry(snHostname, snRegistryPort,
                                          null /* storeName */);

            for (String serviceName : snRegistry.list()) {
                if (GlobalParams.SNA_SERVICE_NAME.equals(serviceName)) {
                    /* not yet registered. */
                    System.err.println
                        ("SNA at hostname: " + snHostname +
                         ", registry port: " + snRegistryPort +
                         " is not registered." +
                         "\n\tNo further information is available");
                    return null;
                }

                Remote stub = null;
                try {
                    stub = snRegistry.lookup(serviceName);

                    if (stub instanceof CommandService) {
                        LoginManager loginMgr = null;
                        if (Ping.isSecured) {
                            loginMgr = KVStoreLogin.getAdminLoginMgr(
                                snHostname, snRegistryPort, Ping.loginCreds);
                        }
                        HostPort target =
                            new HostPort(snHostname, snRegistryPort);
                        CommandServiceAPI admin =
                            CommandServiceAPI.wrap(
                                (CommandService)stub,
                                getLogin(loginMgr, target, ResourceType.ADMIN));
                        return admin.getTopology();
                    }

                    if (stub instanceof RepNodeAdmin) {
                        LoginManager loginMgr = null;
                        if (Ping.isSecured) {
                            loginMgr = KVStoreLogin.getRepNodeLoginMgr(
                                snHostname, snRegistryPort, Ping.loginCreds,
                                null /* storeName */);
                        }
                        final HostPort target =
                            new HostPort(snHostname, snRegistryPort);
                        final RepNodeAdminAPI admin =
                            RepNodeAdminAPI.wrap(
                                (RepNodeAdmin)stub,
                                getLogin(loginMgr, target,
                                         ResourceType.REP_NODE));
                        final Topology topology = admin.getTopology();
                        if (topology == null) {
                            continue;
                        }
                        return topology;
                    }
                } catch (AuthenticationFailureException afe) {
                    System.err.println("Login failed.");
                    return null;
                } catch (Exception e) {

                    /**
                     * Ignore the failure and continue.  This is a user-facing
                     * utility and this message can add confusion in a case
                     * where there is no problem.
                     */
                    if (verbose) {
                        System.err.println("Failed to " +
                                           ((stub == null) ? "lookup" :
                                            "connect to") + " service " +
                                           serviceName +
                                           " Exception message:" +
                                           e.getMessage());
                        e.printStackTrace();
                    }
                }
            }

            System.err.println("SNA at hostname: " + snHostname +
                               " registry port: " + snRegistryPort +
                               " has no available Admins or RNs registered.");
        } catch (ConnectException ce) {
            System.err.println("Could not connect to registry at " +
                               snHostname + ":" + snRegistryPort + ": " +
                               ce.getMessage());
            return null;
        }
        return null;
    }

    private static LoginHandle getLogin(LoginManager loginMgr,
                                        HostPort target,
                                        ResourceType rtype) {
        if (loginMgr == null) {
            return null;
        }
        return loginMgr.getHandle(target, rtype);
    }

}
