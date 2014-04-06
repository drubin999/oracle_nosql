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

package oracle.kv.impl.util;

import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.Arrays;

import oracle.kv.FaultException;
import oracle.kv.KVStoreException;
import oracle.kv.impl.fault.InternalFaultException;
import oracle.kv.impl.rep.admin.RepNodeAdmin;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.security.login.LoginHandle;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.ResourceId.ResourceType;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * Locates a current Topology given a list of SN registry locations
 */
public class TopologyLocator {

    public static final String HOST_PORT_SEPARATOR = ":";

    /**
     * Obtains a current topology from the SNs identified via
     * <code>registryHostPorts</code>.
     * <p>
     * The location of the Topology is done as a two step process.
     * <ol>
     * <li>It first identifies an initial Topology that is the most up to date
     * one know to the RNs hosted by the supplied SNs.</li>
     * <li>
     * It then uses the initial Topology to further search some bounded number
     * of RNs, for an even more up to date Topology and returns it.</li>
     * </ol>
     *
     * @param registryHostPorts one or more strings containing the registry
     * host and port associated with the SN. Each string has the format:
     * hostname:port.
     *
     * @param maxRNs the maximum number of RNs to examine for an up to date
     * Topology
     *
     * @param loginMgr a login manager that will be used to supply login tokens
     * to api calls.
     */
    public static Topology get(String[] registryHostPorts,
                               int maxRNs,
                               LoginManager loginMgr,
                               String expectedStoreName)
        throws KVStoreException {

        Topology initialTopology =
            getInitialTopology(registryHostPorts, loginMgr, expectedStoreName);

        /*
         * Now use the initial Topology to find an even more current version
         * if it exists.
         */
        int maxTopoSeqNum = 0;
        RepNodeAdminAPI currentAdmin = null;
        Exception exception = null;
        final RegistryUtils registryUtils =
            new RegistryUtils(initialTopology, loginMgr);

        l1:
        for (RepGroup rg : initialTopology.getRepGroupMap().getAll()) {
            for (RepNode rn : rg.getRepNodes()) {
                try {
                    RepNodeAdminAPI admin =
                        registryUtils.getRepNodeAdmin(rn.getResourceId());
                    int seqNum = admin.getTopoSeqNum();
                    if (seqNum > maxTopoSeqNum) {
                        maxTopoSeqNum = seqNum;
                        currentAdmin = admin;
                    }
                    if (--maxRNs < 0) {
                        break l1;
                    }
                } catch (SessionAccessException e) {
                    exception = e;
                } catch (RemoteException e) {
                    exception = e;
                } catch (NotBoundException e) {
                    exception = e;
                }
            }
        }

        if (currentAdmin != null) {
            try {
                return currentAdmin.getTopology();
            } catch (RemoteException e) {
                exception = e;
            }
        }

        throw new KVStoreException("Could not establish an initial Topology" +
                                   " from: " +
                                   Arrays.toString(registryHostPorts),
                                   exception);
    }

    /**
     * Locates an initial Topology based upon the supplied SNs. This method
     * is intended for use in the KVS client and meets the client side
     * exception interfaces.
     */
    private static Topology getInitialTopology(String[] registryHostPorts,
                                               LoginManager loginMgr,
                                               String expectedStoreName)
        throws KVStoreException {

        Exception cause = null;
        int maxTopoSeqNum = 0;
        RepNodeAdminAPI currentAdmin = null;

        for (final String registryHostPort : registryHostPorts) {
            final HostPort hostPort = HostPort.parse(registryHostPort);
            final String registryHostname = hostPort.hostname();
            final int registryPort = hostPort.port();

            try {
                final Registry snRegistry =
                    RegistryUtils.getRegistry(registryHostname, registryPort,
                                              expectedStoreName);

                for (String serviceName : snRegistry.list()) {

                    try {

                        /**
                         * Skip things that don't look like RepNodes (this is
                         * for the client.
                         */
                        if (!RegistryUtils.isRepNodeAdmin(serviceName)) {
                            continue;
                        }

                        final Remote stub = snRegistry.lookup(serviceName);
                        if (!(stub instanceof RepNodeAdmin)) {
                            continue;
                        }
                        final LoginHandle loginHdl =
                            (loginMgr == null) ?
                            null :
                            loginMgr.getHandle(
                                new HostPort(registryHostname, registryPort),
                                ResourceType.REP_NODE);
                        final RepNodeAdminAPI admin =
                            RepNodeAdminAPI.wrap((RepNodeAdmin)stub, loginHdl);
                        final int seqNum = admin.getTopoSeqNum();
                        if (seqNum > maxTopoSeqNum) {
                            maxTopoSeqNum = seqNum;
                            currentAdmin = admin;
                        }
                    } catch (SessionAccessException e) {
                        cause = e;
                    } catch (AccessException e) {
                        cause = e;
                    } catch (NotBoundException e) {
                        /*
                         * Should not happen since we are iterating over a
                         * bound list.
                         */
                        cause = e;
                    } catch (InternalFaultException e) {
                        /*
                         * Be robust even in the presence of internal faults.
                         * Keep trying with other functioning nodes.
                         */
                        if (cause == null) {
                            /*
                             * Preserve non-fault exception as the reason
                             * if one's already present.
                             */
                            cause = e;
                        }
                    }
                }
            } catch (RemoteException e) {
                cause = e;
            }
        }

        if (currentAdmin == null) {
            throw new KVStoreException
                ("Could not contact any RepNode at: " +
                 Arrays.toString(registryHostPorts), cause);
        }

        try {
            return currentAdmin.getTopology();
        } catch (RemoteException e) {
            throw new KVStoreException
                ("Could not establish an initial Topology from: " +
                 Arrays.toString(registryHostPorts), cause);
        } catch (InternalFaultException e) {
            /* Clients expect FaultException */
            throw new FaultException(e, false);
        }
    }
}
