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

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Arrays;

import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * Utilities that provide helper methods for service access.
 */
public class ServiceUtils {

    /**
     * Get and wait for a RepNodeAdmin handle to reach one of the states in
     * the ServiceStatus array parameter.
     */
    @SuppressWarnings("null")
    public static RepNodeAdminAPI waitForRepNodeAdmin
    (String storeName,
     String hostName,
     int port,
     RepNodeId rnid,
     StorageNodeId snid,
     LoginManager loginMgr,
     long timeoutSec,
     ServiceStatus[] targetStatus)
    throws Exception {

        Exception exception = null;
        RepNodeAdminAPI rnai = null;
        ServiceStatus status = null;

        long limitMs = System.currentTimeMillis() + 1000 * timeoutSec;

        while (System.currentTimeMillis() <= limitMs) {

            /**
             * The stub may be stale, get it again on exception.
             */
            if (exception != null) {
                rnai = null;
            }
            try {
                if (rnai == null) {
                    rnai = RegistryUtils.getRepNodeAdmin
                        (storeName, hostName, port, rnid, loginMgr);
                }
                status = rnai.ping().getServiceStatus();
                for (ServiceStatus tstatus : targetStatus) {
                    /**
                     * Treat UNREACHABLE as "any".
                     */
                    if (tstatus == ServiceStatus.UNREACHABLE) {
                        return rnai;
                    }
                    if (status == tstatus) {
                        return rnai;
                    }
                }
                exception = null;
            } catch (RemoteException e) {
                exception = e;
            } catch (NotBoundException e) {
                exception = e;
            }

            /* 
             * Check now for any process startup problems before
             * sleeping.
             */
            if (snid != null) {
                RegistryUtils.checkForStartupProblem(storeName,
                                                     hostName,
                                                     port,
                                                     rnid,
                                                     snid,
                                                     loginMgr);
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
                throw new IllegalStateException("unexpected interrupt");
            }
        }

        if (status != null) {
            throw new IllegalStateException
                ("RN current status: " + status + " target status: " +
                 Arrays.toString(targetStatus));
        }
        throw exception;
    }

    /**
     * A version of waitForRepNodeAdmin where the Topology and RepNodeId are
     * known.  Derive the rest.
     */
    public static RepNodeAdminAPI waitForRepNodeAdmin(
        Topology topology,
        RepNodeId rnid,
        LoginManager loginMgr,
        long timeoutSec,
        ServiceStatus[] targetStatus)
        throws Exception {

        RepNode rn = topology.get(rnid);
        StorageNode sn = topology.get(rn.getStorageNodeId());
        return waitForRepNodeAdmin(topology.getKVStoreName(),
                                   sn.getHostname(),
                                   sn.getRegistryPort(),
                                   rnid,
                                   sn.getStorageNodeId(),
                                   loginMgr,
                                   timeoutSec,
                                   targetStatus);

    }

    /**
     * Get and wait for a CommandService handle to reach the requested status.
     * Treat UNREACHABLE as "any" and return once the handle is acquired.
     */
    @SuppressWarnings("null")
    public static CommandServiceAPI waitForAdmin(String hostname,
                                                 int registryPort,
                                                 LoginManager loginMgr,
                                                 long timeoutSec,
                                                 ServiceStatus targetStatus)
        throws Exception {

        Exception exception = null;
        CommandServiceAPI admin = null;
        ServiceStatus status = null;

        long limitMs = System.currentTimeMillis() + 1000 * timeoutSec;

        while (System.currentTimeMillis() <= limitMs) {

            /**
             * The stub may be stale, get it again on exception.
             */
            if (exception != null) {
                admin = null;
            }
            try {
                if (admin == null) {
                    admin = RegistryUtils.getAdmin(hostname, registryPort,
                                                   loginMgr);
                }

                status = admin.ping() ;

                /**
                 * Treat UNREACHABLE as "any".
                 */
                if (targetStatus == ServiceStatus.UNREACHABLE) {
                    return admin;
                }
                if (status == targetStatus) {
                    return admin;
                }
                exception = null;
            } catch (RemoteException e) {
                exception = e;
            } catch (NotBoundException e) {
                exception = e;
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
                throw new IllegalStateException("unexpected interrupt");
            }
        }

        if (status != null) {
            throw new IllegalStateException("Admin status: " + status +
                                            "Target status: " + targetStatus);
        }
        throw exception;
    }
}
