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

import java.rmi.RemoteException;
import java.util.List;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.measurement.Measurement;
import oracle.kv.impl.measurement.ServiceStatusChange;
import oracle.kv.impl.monitor.AgentRepository;
import oracle.kv.impl.monitor.AgentRepository.Snapshot;
import oracle.kv.impl.monitor.MonitorAgent;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.ConfigurationException;
import oracle.kv.impl.security.KVStoreRole;
import oracle.kv.impl.security.SecureProxy;
import oracle.kv.impl.security.annotations.SecureAPI;
import oracle.kv.impl.security.annotations.SecureAutoMethod;
import oracle.kv.impl.security.annotations.SecureR2Method;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.ServiceStatusTracker;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RMISocketPolicy;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryPair;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.registry.VersionedRemoteImpl;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * The implementation of the SNA monitor agent. The MonitorAgent is typically
 * the first component started by the RepNodeService.
 */
@SecureAPI
public class MonitorAgentImpl
    extends VersionedRemoteImpl implements MonitorAgent {

    /* The service hosting this component. */
    private final StorageNodeAgent sna;
    private final Logger logger;
    private final AgentRepository measurementBuffer;
    private final GlobalParams globalParams;
    private final StorageNodeParams snp;
    private final SecurityParams securityParams;
    private final ServiceStatusTracker statusTracker;
    private MonitorAgent exportableMonitorAgent;

    public MonitorAgentImpl(StorageNodeAgent sna,
                            GlobalParams globalParams,
                            StorageNodeParams snp,
                            SecurityParams securityParams,
                            AgentRepository agentRepository,
                            ServiceStatusTracker statusTracker) {

        this.sna = sna;
        this.globalParams = globalParams;
        this.snp = snp;
        this.securityParams = securityParams;
        logger = LoggerUtils.getLogger(this.getClass(), globalParams, snp);
        measurementBuffer = agentRepository;
        this.statusTracker = statusTracker;
    }

    @Override
    @SecureR2Method
    public List<Measurement> getMeasurements(short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles={ KVStoreRole.AUTHENTICATED })
    public List<Measurement> getMeasurements(AuthContext authCtx,
                                             short serialVersion) {

        /* Empty out the measurement repository. */
        Snapshot snapshot = measurementBuffer.getAndReset();
        List<Measurement> monitorData = snapshot.measurements;

        /*
         * Add a current service status to each measurement pull.
         */
        if (snapshot.serviceStatusChanges == 0) {
            monitorData.add(new ServiceStatusChange
                            (statusTracker.getServiceStatus()));
        }

        logger.fine("MonitorAgent: Getting " + monitorData.size() +
                    " measurements");
        return monitorData;
    }

    /**
     * Starts up monitoring. The Monitor agent is bound in the registry.
     */
    public void startup()
        throws RemoteException {

        final String kvStoreName = globalParams.getKVStoreName();
        final StorageNodeId snId = snp.getStorageNodeId();
        final String csfName =
                ClientSocketFactory.factoryName(kvStoreName,
                                                StorageNodeId.getPrefix(),
                                                RegistryUtils.InterfaceType.
                                                MONITOR.interfaceName());
        final RMISocketPolicy policy = securityParams.getRMISocketPolicy();
        final SocketFactoryPair sfp = snp.getMonitorSFP(policy, csfName);

        initExportableMonitorAgent();

        logger.info("Starting MonitorAgent. " +
                    " Server socket factory:" + sfp.getServerFactory() +
                    " Client socket factory:" + sfp.getClientFactory());

        RegistryUtils.rebind(snp.getHostname(),
                             snp.getRegistryPort(),
                             kvStoreName,
                             snId.getFullName(),
                             RegistryUtils.InterfaceType.MONITOR,
                             exportableMonitorAgent,
                             sfp.getClientFactory(),
                             sfp.getServerFactory());
    }

    /**
     * Unbind the monitor agent in the registry.
     *
     * <p>
     * In future, it may be worth waiting for the monitor poll period, so that
     * the last state can be communicated to the MonitorController.
     */
    public void stop()
        throws RemoteException {

        RegistryUtils.unbind(snp.getHostname(),
                             snp.getRegistryPort(),
                             globalParams.getKVStoreName(),
                             snp.getStorageNodeId().getFullName(),
                             RegistryUtils.InterfaceType.MONITOR,
                             exportableMonitorAgent);
        logger.info("Stopping MonitorAgent");
    }

    public AgentRepository getAgentRepository() {
        return measurementBuffer;
    }

    private void initExportableMonitorAgent() {
        try {
            exportableMonitorAgent =
                SecureProxy.create(this,
                                   sna.getSNASecurity().getAccessChecker(),
                                   sna.getFaultHandler());
        } catch (ConfigurationException ce) {
            throw new IllegalStateException("Unabled to create proxy", ce);
        }
    }
}
