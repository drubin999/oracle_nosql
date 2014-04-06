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

package oracle.kv.impl.mgmt;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.measurement.ProxiedServiceStatusChange;
import oracle.kv.impl.measurement.ServiceStatusChange;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.rep.monitor.StatsPacket;
import oracle.kv.impl.sna.ServiceManager;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.PortRange;
import oracle.kv.impl.util.ServiceStatusTracker;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * The Agent represents external values to the mgmt Agent implementations.
 */
public abstract class AgentInternal implements MgmtAgent {

    protected final StorageNodeAgent sna;
    private final Map<RepNodeId, RepNodeStatusReceiver> rnStatusReceivers =
        new HashMap<RepNodeId, RepNodeStatusReceiver>();
    private final List<ServiceManager.Listener> smListeners =
        new ArrayList<ServiceManager.Listener>();
    private ServiceStatusTracker snaStatusTracker;
    private AdminStatusReceiver adminStatusReceiver = null;

    protected AgentInternal(StorageNodeAgent sna) {
        this.sna = sna;
    }

    @Override
    public void setSnaStatusTracker(ServiceStatusTracker tracker) {
        if (snaStatusTracker == tracker) {
            return;
        }
        snaStatusTracker = tracker;
        snaStatusTracker.addListener(new StatusListener());

        /* Set the starting status from the tracker's current status. */
        ServiceStatusChange s =
            new ServiceStatusChange(snaStatusTracker.getServiceStatus());
        updateSNStatus(s, s);
    }

    @Override
    public void shutdown() {
        /*
         * If a notification was sent about the StorageNode being STOPPED, give
         * it a couple of seconds to escape before killing the mechanism that
         * sends it.
         */
        try {
            Thread.sleep(2000);
        } catch (Exception e) {
        }
        unexportAll();
        clearSmListeners();
    }

    protected abstract void updateSNStatus
        (ServiceStatusChange prev, ServiceStatusChange current);

    class StatusListener implements ServiceStatusTracker.Listener {
        @Override
        public void update(ServiceStatusChange prevStatus,
                           ServiceStatusChange newStatus) {

            AgentInternal.this.updateSNStatus(prevStatus, newStatus);
        }
    }

    protected void addServiceManagerListener
        (final RepNodeId rnId, final ServiceManager mgr) {

        /*
         * If the RepNode is already running, then we need to install its
         * status receiver right away.  Otherwise, the ServiceManager.Listener
         * will do it when the RepNode starts, and of course will also do it
         * any time the RepNode restarts for any reason.
         */
        if (mgr.isRunning()) {
            installRepNodeStatusReceiver(rnId);
        }

        ServiceManager.Listener listener = mgr.new Listener() {
            @Override
            public void startupCallback() {
                installRepNodeStatusReceiver(rnId);
            }
        };

        smListeners.add(listener);
    }

    /**
     * register an Admin service instance.
     */
    protected void addAdminServiceManagerListener(final ServiceManager mgr) {

        /*
         * If the Admin is already running, then we need to install its
         * status receiver right away.  Otherwise, the ServiceManager.Listener
         * will do it when the Admin starts, and of course will also do it
         * any time the Admin restarts for any reason.
         */
        if (mgr.isRunning()) {
            installAdminStatusReceiver();
        }

        ServiceManager.Listener listener = mgr.new Listener() {
            @Override
            public void startupCallback() {
                installAdminStatusReceiver();
            }
        };

        smListeners.add(listener);
    }

    public void installRepNodeStatusReceiver(final RepNodeId rnId) {

        /*
         * Wait for the repnode to come up.  UNREACHABLE is interpreted
         * to mean any state is acceptable.  We want a handle on the
         * RepNode so that we can enable mgmt on it, regardless of its
         * service state.
         */
        ServiceStatus[] targets = {ServiceStatus.UNREACHABLE};
        RepNodeAdminAPI rnai = sna.waitForRepNodeAdmin(rnId, targets);
        if (rnai == null) {
            /*
             * There is nothing we can do here. The failure has been
             * logged already.
             */
            return;
        }
        try {
            RepNodeStatusReceiver rnsr = getRepNodeStatusReceiver(rnId);
            rnai.installStatusReceiver(rnsr);
        } catch (RemoteException e) {
            sna.getLogger().log
                (Level.WARNING,
                 "Error installing RepNodeStatusReceiver for " +
                 rnId.getFullName() + ".", e);
        }
    }

    private RepNodeStatusReceiver getRepNodeStatusReceiver(RepNodeId rnid)
        throws RemoteException {

        RepNodeStatusReceiver rnsr = rnStatusReceivers.get(rnid);
        if (rnsr != null) {
            /*
             * We'll avoid trying to unregister the receiver from the
             * RepNodeAdmin.  If we are here, the RepNode has been restarted
             * and no longer has a reference to the receiver anyway.
             */
            unexportStatusReceiver(rnsr, true);
        }

        rnsr = new RepNodeStatusReceiverImpl(rnid);
        rnStatusReceivers.put(rnid, rnsr);
        return rnsr;
    }

    private class RepNodeStatusReceiverImpl
        extends UnicastRemoteObject implements RepNodeStatusReceiver {

        private static final long serialVersionUID = 1L;

        RepNodeId rnid;

        RepNodeStatusReceiverImpl(RepNodeId rnid)
            throws RemoteException {
            this.rnid = rnid;
        }

        @Override
        public void updateRnStatus(ServiceStatusChange newStatus) {
            AgentInternal.this.updateRepNodeStatus(rnid, newStatus);
        }

        @Override
        public void receiveStats(StatsPacket packet)
            throws RemoteException {

            AgentInternal.this.updateRepNodePerfStats(rnid, packet);
        }

        @Override
        public void receiveNewParams(ParameterMap newMap)
            throws RemoteException {

            AgentInternal.this.updateRepNodeParameters(rnid, newMap);
        }

        @Override
        public String toString() {
            return rnid.getFullName();
        }
    }

    protected void unexportStatusReceiver(RepNodeId rnid) {
        RepNodeStatusReceiver rnsr = rnStatusReceivers.get(rnid);
        if (rnsr != null) {
            unexportStatusReceiver(rnsr, true);
        }
    }

    private void unexportStatusReceiver
        (RepNodeStatusReceiver rnsr, boolean remove) {

        try {
            UnicastRemoteObject.unexportObject(rnsr, true);
        } catch (RemoteException ignored) {
        }
        if (remove) {
            rnStatusReceivers.remove(rnsr);
        }
    }

    public void installAdminStatusReceiver() {

        /*
         * Wait for the Admin to come up.
         */
        CommandServiceAPI cs =
            sna.waitForAdmin(ServiceStatus.UNREACHABLE, 120);
        if (cs == null) {
            /*
             * There is nothing we can do here. The failure has been
             * logged already.
             */
            return;
        }
        try {
            AdminStatusReceiver asr = getAdminStatusReceiver();
            cs.installStatusReceiver(asr);
        } catch (RemoteException e) {
            sna.getLogger().log
                (Level.WARNING,
                 "Error installing AdminStatusReceiver.", e);
        }
    }

    private AdminStatusReceiver getAdminStatusReceiver()
        throws RemoteException {

        if (adminStatusReceiver != null) {
            unexportAdminStatusReceiver();
        }

        adminStatusReceiver = new AdminStatusReceiverImpl();
        return adminStatusReceiver;
    }

    private class AdminStatusReceiverImpl
        extends UnicastRemoteObject implements AdminStatusReceiver {

        protected AdminStatusReceiverImpl()
            throws RemoteException {

            super();
        }

        private static final long serialVersionUID = 1L;

        @Override
        public void updateAdminStatus(ServiceStatusChange newStatus,
                                      boolean isMaster) {
            AgentInternal.this.updateAdminStatus(newStatus, isMaster);
        }

        @Override
        public void receiveNewParams(ParameterMap newMap)
            throws RemoteException {

            AgentInternal.this.updateAdminParameters(newMap);
        }
    }

    protected void unexportAdminStatusReceiver() {
        try {
            UnicastRemoteObject.unexportObject(adminStatusReceiver, true);
        } catch (RemoteException ignored) {
        }
        adminStatusReceiver = null;
    }

    public abstract void updateAdminParameters(ParameterMap newMap);

    public abstract void updateAdminStatus(ServiceStatusChange newStatus,
                                  boolean isMaster);

    protected void unexportAll() {
        for (RepNodeId rnid : rnStatusReceivers.keySet()) {
            /*
             * Don't remove the entry from the map while iterating.
             */
            unexportStatusReceiver(rnStatusReceivers.get(rnid), false);
        }
        /* Empty the map after unexporting all of its entries. */
        rnStatusReceivers.clear();
    }

    abstract protected void updateRepNodeStatus(RepNodeId which,
                                                ServiceStatusChange newStatus);

    abstract protected void updateRepNodePerfStats(RepNodeId which,
                                                   StatsPacket packet);

    abstract protected void updateRepNodeParameters(RepNodeId which,
                                                    ParameterMap map);

    @Override
    public void proxiedStatusChange(ProxiedServiceStatusChange sc) {
        /* First figure out what kind of a thing is being reported on. */
        ResourceId rid = sc.getTarget(sna.getStorageNodeId());
        if (rid instanceof RepNodeId) {
            RepNodeId rnid = (RepNodeId) rid;
            updateRepNodeStatus(rnid, sc);
        }
        if (rid instanceof AdminId) {
             /*
              * Proxied statuses are never "RUNNING" so it's safe to say
              * that "isMaster" is false.
              */
            updateAdminStatus(sc, false);
        }
    }

    private void clearSmListeners() {
        for (ServiceManager.Listener listener : smListeners) {
            listener.removeSelf();
        }
        smListeners.clear();
    }

    /* Accessors for SNA parameters. */

    public int getSnId() {
        return sna.getStorageNodeId().getStorageNodeId();
    }

    public int getRegistryPort() {
        return sna.getRegistryPort();
    }

    public int getFreePort() {
        String rangeString = sna.getServicePortRange();
        final List<Integer> range = PortRange.getRange(rangeString);
        return RegistryUtils.findFreePort
            (range.get(0), range.get(1), getHostname());
    }

    public boolean restrictPortRange() {
        return (sna.getServicePortRange() != null);
    }

    public String getStoreName() {
        return nullToEmptyString(sna.getStoreName());
    }

    public String getHostname() {
        return nullToEmptyString(sna.getHostname());
    }

    public String getHAHostname() {
        return nullToEmptyString(sna.getHAHostname());
    }

    public String getBootstrapDir() {
        return nullToEmptyString(sna.getBootstrapDir());
    }

    public String getBootstrapFile() {
        return nullToEmptyString(sna.getBootstrapFile());
    }

    public String getKvConfigFile() {
        return nullToEmptyString(sna.getKvConfigFile().toString());
    }

    public int getAdminHttpPort() {
        return sna.getBootstrapParams().getAdminHttpPort();
    }

    public boolean isHostingAdmin() {
        return sna.getBootstrapParams().isHostingAdmin();
    }

    public String getRootDir() {
        return nullToEmptyString(sna.getBootstrapParams().getRootdir());
    }

    public Integer getCapacity() {
        return sna.getCapacity();
    }

    public Integer getLogFileLimit() {
        return sna.getLogFileLimit();
    }

    public Integer getLogFileCount() {
        return sna.getLogFileCount();
    }

    public String getSnHaPortRange() {
        return nullToEmptyString(sna.getHAPortRange());
    }

    public int getNumCpus() {
        return sna.getNumCpus();
    }

    public int getMemoryMB() {
        return sna.getMemoryMB();
    }

    public String getMountPointsString() {
        return nullToEmptyString(sna.getMountPointsString());
    }

    /**
     * JDMK doesn't like null strings.
     */
    private static String nullToEmptyString(String s) {
        return s == null ? "" : s;
    }

}
