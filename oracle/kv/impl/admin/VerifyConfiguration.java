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

package oracle.kv.impl.admin;

import java.io.Serializable;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.TopologyCheck.CONFIG_STATUS;
import oracle.kv.impl.admin.TopologyCheck.RNLocationInput;
import oracle.kv.impl.admin.TopologyCheck.TOPO_STATUS;
import oracle.kv.impl.admin.TopologyCheckUtils.SNServices;
import oracle.kv.impl.admin.TopologyCheck.REMEDY_TYPE;
import oracle.kv.impl.admin.TopologyCheck.Remedy;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.topo.Rules;
import oracle.kv.impl.admin.topo.Rules.Results;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.rep.RepNodeStatus;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.sna.StorageNodeStatus;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.FormatUtils;
import oracle.kv.impl.util.VersionUtil;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.util.Ping;

/**
 * Object encapsulating various verification methods.
 */
public class VerifyConfiguration {

    private static final String eol = System.getProperty("line.separator");
    private final Admin admin;
    private final boolean listAll;
    private final boolean showProgress;
    private final Logger logger;

    private final TopologyCheck topoChecker;

    /* buffer the progress report to display in the command line utility. */
    private final StringBuilder progressReport;

    /*
     * Found violations are stored here. The collection of violations is cleared
     * each time verify is run.
     */
    private final List<Problem> violations;

    /*
     * Issues that are only advisory, and are not true violations.
     */
    private final List<Problem> warnings;

    /*
     * Violations that have suggested repairs that can be carried out by
     * the topology checker.
     */
    private final Repairs repairs;

    /**
     * Construct a verification object.
     *
     * @param showProgress if true, log a line before checking each storage
     * node and its resident services.
     * @param listAll if true, list information for all services checked.
     * @param logger output is issued via this logger.
     */
    public VerifyConfiguration(Admin admin,
                               boolean showProgress,
                               boolean listAll,
                               Logger logger) {

        this.admin = admin;
        this.showProgress = showProgress;
        this.listAll = listAll;
        this.logger = logger;
        violations = new ArrayList<Problem>();
        warnings = new ArrayList<Problem>();
        progressReport = new StringBuilder();
        repairs = new Repairs();

        topoChecker = new TopologyCheck(logger,
                                        admin.getCurrentTopology(),
                                        admin.getCurrentParameters());
    }

    /**
     * Check whether the current topology obeys layout rules, and that the
     * store matches the layout, software version, and parameters described by
     * the most current topology and parameters. The verifyTopology() method
     * may be called multiple times in the life of a VerifyConfiguration
     * instance, but should be called serially.
     *
     * @return true if no violations were found.
     */
    public boolean verifyTopology() {
        return verifyTopology(admin.getCurrentTopology(),
                              admin.getLoginManager(),
                              admin.getCurrentParameters(),
                              true);
    }

    /**
     * Checks that the nodes in the store have been upgraded to the target
     * version.
     *
     * @param targetVersion
     * @return true if all nodes are up to date
     */
    public boolean verifyUpgrade(KVVersion targetVersion,
                                 List<StorageNodeId> snIds) {

        admin.getLogger().log(Level.INFO,
                              "Verifying upgrade to target version: {0}",
                              targetVersion.getNumericVersionString());

        violations.clear();
        warnings.clear();
        repairs.clear();

        final Topology topology = admin.getCurrentTopology();
        final RegistryUtils registryUtils =
            new RegistryUtils(topology, admin.getLoginManager());

        if (snIds == null) {
            snIds = topology.getStorageNodeIds();
        }
        for (StorageNodeId snId : snIds) {
            verifySNUpgrade(snId, targetVersion, registryUtils, topology);
        }
        VerifyResults vResults = new VerifyResults(progressReport.toString(),
                                                   violations, warnings);
        logger.log(Level.INFO, "{0}", vResults.display());
        return vResults.okay();
    }

    private void verifySNUpgrade(StorageNodeId snId,
                                 KVVersion targetVersion,
                                 RegistryUtils registryUtils,
                                 Topology topology) {
        StorageNodeStatus snStatus = null;

        try {
            snStatus = registryUtils.getStorageNodeAgent(snId).ping();
        } catch (RemoteException re) {
            violations.add(new RMIFailed(snId, re, "ping()", showProgress,
                                         logger, progressReport));
        } catch (NotBoundException nbe) {
            violations.add(new RMIFailed(snId, nbe, showProgress, logger,
                                         progressReport));
        }

        if (snStatus != null) {
            final KVVersion snVersion = snStatus.getKVVersion();

            if (snVersion.compareTo(targetVersion) < 0) {
                warnings.add(new UpgradeNeeded(snId,
                                               snVersion,
                                               targetVersion,
                                               showProgress, logger,
                                               progressReport));
            } else {
                /* SN is at or above target. Make sure RNs are up-to-date */
                verifyRNUpgrade(snVersion,
                                topology.getHostedRepNodeIds(snId),
                                registryUtils);
            }
        }

        if (listAll) {
            String details = "Verify upgrade: " +
                Ping.displayStorageNode(topology,
                                        topology.get(snId),
                                        snStatus);
            logger.info(details);
            progressReport.append(details).append(eol);
        }
    }

    private void verifyRNUpgrade(KVVersion snVersion,
                                 Set<RepNodeId> hostedRepNodeIds,
                                 RegistryUtils registryUtils) {

        for (RepNodeId rnId : hostedRepNodeIds) {
            try {
                final RepNodeAdminAPI rn = registryUtils.getRepNodeAdmin(rnId);
                final KVVersion rnVersion = rn.getInfo().getSoftwareVersion();

                if (rnVersion.compareTo(snVersion) != 0) {
                    warnings.add(new UpgradeNeeded(rnId,
                                                   rnVersion,
                                                   snVersion,
                                                   showProgress, logger,
                                                   progressReport));
                }
            } catch (RemoteException re) {
                violations.add(new RMIFailed(rnId, re, "ping()", showProgress,
                                             logger, progressReport));
            } catch (NotBoundException nbe) {
                violations.add(new RMIFailed(rnId, nbe, showProgress, logger,
                                             progressReport));
            }
        }
    }

    /**
     * Checks that the nodes in the store meet the specified prerequisite
     * version in order to be upgraded to the target version.
     *
     * @param targetVersion
     * @param prerequisiteVersion
     * @return true if no violations were found
     */
    public boolean verifyPrerequisite(KVVersion targetVersion,
                                      KVVersion prerequisiteVersion,
                                      List<StorageNodeId> snIds) {

        admin.getLogger().log(Level.INFO,
                              "Checking upgrade to target version: {0}, " +
                              "prerequisite: {1}",
                   new Object[]{targetVersion.getNumericVersionString(),
                                prerequisiteVersion.getNumericVersionString()});

        final Topology topology = admin.getCurrentTopology();
        final RegistryUtils registryUtils =
            new RegistryUtils(topology, admin.getLoginManager());

        if (snIds == null) {
            snIds = topology.getStorageNodeIds();
        }
        for (StorageNodeId snId : snIds) {
            verifySNPrerequisite(snId,
                                 targetVersion,
                                 prerequisiteVersion,
                                 registryUtils,
                                 topology);
        }
        VerifyResults vResults = new VerifyResults(progressReport.toString(),
                                                   violations, warnings);
        logger.log(Level.INFO, "{0}", vResults.display());
        return vResults.okay();
    }

    private void verifySNPrerequisite(StorageNodeId snId,
                                      KVVersion targetVersion,
                                      KVVersion prerequisiteVersion,
                                      RegistryUtils registryUtils,
                                      Topology topology) {
        StorageNodeStatus snStatus = null;

        try {
            final StorageNodeAgentAPI sna =
                        registryUtils.getStorageNodeAgent(snId);
            snStatus = sna.ping();
        } catch (RemoteException re) {
            violations.add(new RMIFailed(snId, re, "ping()", showProgress,
                                         logger, progressReport));
        } catch (NotBoundException nbe) {
            violations.add(new RMIFailed(snId, nbe, showProgress, logger,
                                         progressReport));
        }

        if (snStatus != null) {
            final KVVersion snVersion = snStatus.getKVVersion();

            /* Check if the SN is too old (dosen't meet prereq) */
            if (snVersion.compareTo(prerequisiteVersion) < 0) {
                violations.add(new UpgradeNeeded(snId,
                                                 snVersion,
                                                 prerequisiteVersion,
                                                 showProgress, logger,
                                                 progressReport));
                /*
                 * Meets prereq, so check if the SN is too new (downgrade across
                 * minor version)
                 */
            } else if (VersionUtil.compareMinorVersion(snVersion,
                                                       targetVersion) > 0) {
                violations.add(new BadDowngrade(snId,
                                                snVersion,
                                                prerequisiteVersion,
                                                showProgress, logger,
                                                progressReport));
            }
        }

        if (listAll) {
            String details = "Verify prerequisite: " +
                Ping.displayStorageNode(topology,
                                        topology.get(snId),
                                        snStatus);
            logger.info(details);
            progressReport.append(details).append(eol);
        }
    }

    /**
     * Include the date and time in the display. It's redundant when verify
     * information is printed in the log files, but it's useful for context
     * when the verify output goes to the CLI, and is included in a bug report.
     */
    public static String displayTopology(Topology topology) {
        return ("Verify: starting verification of " +
                topology.getKVStoreName() +
                " based upon topology sequence #" +
                topology.getSequenceNumber() + eol +
                topology.getPartitionMap().getNPartitions() +
                " partitions and " +
                topology.getStorageNodeMap().size() +
                " storage nodes. Version: " +
                KVVersion.CURRENT_VERSION.getNumericVersionString() +
                " Time: " +
                FormatUtils.formatDateAndTime(System.currentTimeMillis()));
    }

    /**
     * Non-private entry point for unit tests. Provides a way to supply an
     * intentionally corrupt topology or parameters, to test for error cases.
     */
    synchronized boolean verifyTopology(Topology topology,
                                        LoginManager loginMgr,
                                        Parameters currentParams,
                                        boolean topoIsDeployed) {

        RegistryUtils registryUtils = new RegistryUtils(topology, loginMgr);
        Results results = Rules.validate(topology, currentParams,
                                         topoIsDeployed);
        violations.clear();
        violations.addAll(results.getViolations());

        warnings.clear();
        warnings.addAll(results.getWarnings());

        repairs.clear();

        logger.info(displayTopology(topology));
        checkServices(topology, currentParams, registryUtils);

        VerifyResults vResults = new VerifyResults(progressReport.toString(),
                                                   violations, warnings);
        logger.log(Level.INFO, "{0}", vResults.display());
        return vResults.okay();
    }

    /**
     * For each SN in the store, contact each service, conduct check.
     */
    private void checkServices(Topology topology,
                               Parameters currentParams,
                               RegistryUtils registryUtils) {

        Map<StorageNodeId, SNServices> sortedResources =
            TopologyCheckUtils.groupServicesBySN(topology, currentParams);

        /* The check is done in SN order */
        for (SNServices nodeInfo : sortedResources.values()) {

            StorageNodeId snId = nodeInfo.getStorageNodeId();

            /* If show progress is set, issue a line per Storage Node. */
            if (showProgress) {
                String msg = "Verify: == checking storage node " + snId +
                    " ==";
                logger.info(msg);
                progressReport.append(msg).append(eol);
            }

            /* Check the StorageNodeAgent on this node. */
            checkStorageNode(registryUtils, topology, currentParams, snId,
                             nodeInfo);

            /* If the Admin is there, check it. */
            AdminId adminId = nodeInfo.getAdminId();
            if (adminId != null) {
                checkAdmin(currentParams, adminId);
            }

            /* Check all RepNodes on this storage node. */
            for (RepNodeId rnId : nodeInfo.getAllRepNodeIds()) {
                checkRepNode(registryUtils, topology, snId, rnId,
                             currentParams);
            }
        }
    }

    /**
     * Ping this admin and check its params.
     */
    private void checkAdmin(Parameters params, AdminId aId) {
        StorageNodeId hostSN = params.get(aId).getStorageNodeId();
        StorageNodeParams snp = params.get(hostSN);
        boolean pingProblem = false;
        CommandServiceAPI cs = null;
        ServiceStatus status = ServiceStatus.UNREACHABLE;
        try {
            cs = RegistryUtils.getAdmin(snp.getHostname(),
                                        snp.getRegistryPort(),
                                        admin.getLoginManager());
            status = cs.ping();
        } catch (RemoteException re) {
            violations.add(new RMIFailed(aId, re, "ping()",showProgress,
                                         logger, progressReport));
            pingProblem = true;
        } catch (NotBoundException e) {
            violations.add(new RMIFailed(aId, e, showProgress, logger,
                                       progressReport));
            pingProblem = true;
        }

        /*
         * Check the JE HA metadata and SN remote config against the AdminDB
         * for this admin.
         */
        Remedy remedy = topoChecker.checkAdminLocation(admin, aId);
        if (remedy.canFix()) {
            repairs.add(remedy);
        }

        if (!pingProblem) {
            if (status.equals(ServiceStatus.RUNNING)) {
                checkAdminParams(cs, params, aId, hostSN);
            } else {
                violations.add(new StatusNotRight(aId, ServiceStatus.RUNNING,
                                                  status, showProgress,
                                                  logger, progressReport));
            }
        }

        if (listAll) {
            String details= ("Verify: \tAdmin [" + aId + "]\t\tStatus: " +
                             status);
            logger.info(details);
            progressReport.append(details).append(eol);
        }
    }

    /**
     * Ping the storage node. If it does not respond, add it to the problem
     * list. If the version doesn't match that of the admin, also add that to
     * the list.
     */
    @SuppressWarnings("null")
    private void checkStorageNode(RegistryUtils regUtils,
                                  Topology topology,
                                  Parameters currentParams,
                                  StorageNodeId snId,
                                  SNServices nodeInfo) {

        boolean pingProblem = false;
        StorageNodeStatus snStatus = null;
        StorageNodeAgentAPI sna = null;
        ServiceStatus status = ServiceStatus.UNREACHABLE;
        try {
            sna = regUtils.getStorageNodeAgent(snId);
            snStatus = sna.ping();
            status = snStatus.getServiceStatus();
        } catch (RemoteException re) {
            violations.add(new RMIFailed(snId, re, "ping()", showProgress,
                                       logger, progressReport));
            pingProblem = true;
        } catch (NotBoundException e) {
            violations.add(new RMIFailed(snId, e, showProgress, logger,
                                       progressReport));
            pingProblem = true;
        }

        if (listAll) {
            String details = "Verify: " +
                Ping.displayStorageNode(topology,
                                        topology.get(snId),
                                        snStatus);
            logger.info(details);
            progressReport.append(details).append(eol);
        }

        if (!pingProblem) {
            if (status.equals(ServiceStatus.RUNNING)) {
                checkSNParams(sna, snId, currentParams, nodeInfo);
            } else {
                violations.add(new StatusNotRight(snId, ServiceStatus.RUNNING,
                                                  status, showProgress, logger,
                                                  progressReport));
            }

            if (!KVVersion.CURRENT_VERSION.equals(snStatus.getKVVersion())) {
                violations.add(new VersionDifference(snId,
                                                   snStatus.getKVVersion(),
                                                   showProgress, logger,
                                                   progressReport));
            }
        }
    }

    /**
     * If this repNode is not disabled, ping it.
     */
    private void checkRepNode(RegistryUtils regUtils,
                              Topology topology,
                              StorageNodeId snId,
                              RepNodeId rnId,
                              Parameters currentParams) {

        boolean pingProblem = false;
        ServiceStatus status = ServiceStatus.UNREACHABLE;
        RepNodeStatus rnStatus = null;
        RepNodeAdminAPI rna = null;

        boolean isDisabled = currentParams.get(rnId).isDisabled();
        ServiceStatus expected = isDisabled ? ServiceStatus.UNREACHABLE :
            ServiceStatus.RUNNING;

        try {
            rna = regUtils.getRepNodeAdmin(rnId);
            rnStatus = rna.ping();
            status = rnStatus.getServiceStatus();
        } catch (RemoteException re) {
            if (!expected.equals(ServiceStatus.UNREACHABLE)) {
                violations.add(new RMIFailed(rnId, re, "ping()", showProgress,
                                             logger, progressReport));
                pingProblem = true;
            } else {
                /* The RN is configured as being disabled, issue a warning */
                reportStoppedRN(rnId, snId);
            }
        } catch (NotBoundException e) {
            if (!expected.equals(ServiceStatus.UNREACHABLE)) {
                violations.add(new RMIFailed(rnId, e, showProgress, logger,
                                             progressReport));
                pingProblem = true;
            } else {
                /* The RN is configured as being disabled, issue a warning */
                reportStoppedRN(rnId, snId);
            }
        }

        if (!pingProblem) {
            if (status.equals(expected)) {
                if (status.equals(ServiceStatus.RUNNING)) {
                    checkRNParams(rna, rnId, topology, currentParams);
                }
            } else {
                violations.add(new StatusNotRight(rnId, expected, status,
                                                  showProgress, logger,
                                                  progressReport));
            }
        }

        if (listAll) {
            StringBuilder details = new StringBuilder();
            details.append("Verify: ").
                append(Ping.displayRepNode(topology.get(rnId), rnStatus));

            if (expected.equals(ServiceStatus.UNREACHABLE)) {
                /*
                 * Add this note so that the unreachable state won't seem like
                 * a bug.
                 */
                details.append("(Stopped)");
            }

            logger.info(details.toString());
            progressReport.append(details).append(eol);
        }
    }

    /**
     * Report a RN that is not up.
     */
    private void reportStoppedRN(RepNodeId rnId, StorageNodeId snId) {
        warnings.add(new ServiceStopped(rnId, snId, showProgress,
                                        logger, progressReport, true));

        /*
         * If there are no previously detected errors with this RN, and its
         * location is correct, suggest that it should be restarted.
         */
        boolean previousRepair = repairs.repairExists(rnId);

        if (!previousRepair) {
            repairs.add(new Remedy(new RNLocationInput(TOPO_STATUS.HERE,
                                                       CONFIG_STATUS.HERE),
                                   snId, rnId, REMEDY_TYPE.CREATE_RN, null));
        }
    }

    private void checkSNParams(StorageNodeAgentAPI sna,
                               StorageNodeId snId,
                               Parameters currentParams,
                               SNServices nodeInfo) {

        LoadParameters remoteParams;
        try {
            remoteParams = sna.getParams();
        } catch (RemoteException re) {
            violations.add(new RMIFailed(snId, re, "getParams", showProgress,
                                       logger, progressReport));
            return;
        }

        if (!compareParams(snId, remoteParams,
                           currentParams.get(snId).getMap())) {
            return;
        }

        /* Check the SNAs mount maps if present */
        ParameterMap mountMap = currentParams.get(snId).getMountMap();
        if (mountMap != null) {
            if (!compareParams(snId, remoteParams, mountMap)) {
                return;
            }
        } else {
            if (remoteParams.getMap(ParameterState.BOOTSTRAP_MOUNT_POINTS) !=
                null) {
                violations.add(new ParamMismatch
                             (snId, "Parameter collection " +
                              ParameterState.BOOTSTRAP_MOUNT_POINTS +
                              "missing", showProgress, logger,
                              progressReport));
                return;
            }

        }

        if (!compareParams(snId, remoteParams,
                           currentParams.getGlobalParams().getMap())) {
            return;
        }

        /*
         * Make sure all services that are supposed to be on this SN, according
         * to the topo and params, the SN's config.xml, and the JE HA group
         * have location info that are all correct.
         * The RNs to check are the union of those that are in the SN's
         * config.xml and those in the topology.
         */
        topoChecker.saveSNRemoteParams(snId, remoteParams);
        Set<RepNodeId> rnsToCheck = topoChecker.getPossibleRNs(snId);
        for (RepNodeId rnId: rnsToCheck) {
            Remedy remedy = null;
            try {
                remedy = topoChecker.checkRNLocation
                        (admin, snId, rnId, false /* called by deployNewRN */,
                         false /* makeRNEnabled */);

                if (remedy.getType() != REMEDY_TYPE.OKAY) {
                    logger.log(Level.INFO, "{0}", new Object[] {remedy});
                    Problem p;
                    if (remedy.getType() == REMEDY_TYPE.CREATE_RN) {
                        p = new ServiceStopped(remedy.getRNId(),
                                               remedy.getSNId(),
                                               showProgress,
                                               logger,
                                               progressReport,
                                               false); /* isDisabled */
                    } else {
                        p = new ParamMismatch(remedy.getRNId(),
                                              remedy.problemDescription(),
                                              showProgress, logger,
                                              progressReport);
                    }

                    violations.add(p);
                    if (remedy.canFix()) {
                        repairs.add(remedy);
                    }
                }
            } catch (RemoteException e) {
                violations.add
                (new RMIFailed(snId, e, "checkRNLocation", showProgress,
                               logger, progressReport));
            } catch (NotBoundException e) {
                violations.add
                (new RMIFailed(snId, e, showProgress, logger, progressReport));
            }
        }

        /*
         * The Admins to check are the union of those that are in the SN's
         * config.xml and those in AdminDB params.
         */
        ParameterMap adminMap =
            remoteParams.getMapByType(ParameterState.ADMIN_TYPE);
        if (adminMap != null) {
            AdminId aid =
                new AdminId(adminMap.getOrZeroInt(ParameterState.AP_ID));
            if (nodeInfo.getAdminId() == null ||
                !aid.equals(nodeInfo.getAdminId())) {
                violations.add(new ParamMismatch
                             (snId, "Storage Node is managing admin " + aid +
                              "but the admin does not know this",
                              showProgress, logger, progressReport));
            }
        } else if (nodeInfo.getAdminId() != null) {
            violations.add(new ParamMismatch
                         (snId, "Storage Node is not managing an Admin but " +
                          "the admin believes it is",
                          showProgress, logger, progressReport));
        }
    }

    /**
     * See if this repNode's params match those held in the admin db.
     */
    private void checkRNParams(RepNodeAdminAPI rna,
                               RepNodeId rnId,
                               Topology topology,
                               Parameters currentParams) {

        /* Ask the RN for its params */
        LoadParameters remoteParams;
        try {
            remoteParams = rna.getParams();
        } catch (RemoteException re) {
            violations.add(new RMIFailed(rnId, re, "getParams", showProgress,
                                       logger, progressReport));
            return;
        }

        /*
         * Check RN's in-memory notion of its params, excluding helper hosts
         * and the disable bit.
         * Changes in the helper param do not restart the RN even though the
         * underlying JE param is immutable. Although changes in immutable
         * params usually require bouncing the RNs, kvstore refrains from doing
         * so in this case because helpers are only used at RN startup, and
         * it's unnecessary to bounce the RN, so it may be quite
         * valid that the RN has different helpers. Comparing the helpers
         * in the SN config.xml against the AdminDB is a sufficient and
         * meaningful check.
         * The disable bit is excluded because it only has significance to the
         * SN, and has no meaning to the RN.
         */
        if (!compareParams(rnId, remoteParams,
                           currentParams.get(rnId).getMap(),
                           new ExcludeDisableFilter())) {
            return;
        }


        /* Check SN related params */
        StorageNodeId snId = topology.get(rnId).getStorageNodeId();
        if (!compareParams(rnId, remoteParams,
                           currentParams.get(snId).getMap(),
                           new SNPFilter())) {
            return;
        }

        /* Check Global related params */
        if (!compareParams(rnId, remoteParams,
                           currentParams.getGlobalParams().getMap())) {
            return;
        }
    }

    /**
     * See if this Admin replica's params match those held in the admin db
     * of the master Admin.
     */
    private void checkAdminParams(CommandServiceAPI cs,
                                  Parameters currentParams,
                                  AdminId targetAdminId,
                                  StorageNodeId hostSN) {

        if (admin != null && targetAdminId.equals
            (admin.getParams().getAdminParams().getAdminId())) {

            /*
             * No need to check, this is the same Admin instance. Bypassing
             * the check also make unit tests that use the Admin at
             * a lower level than the RMI layer run correctly.
             */
            return;
        }

        LoadParameters remoteParams;
        try {
            remoteParams = cs.getParams();
        } catch (RemoteException re) {
            violations.add(new RMIFailed(targetAdminId, re, "getParams",
                                       showProgress,logger, progressReport));
            return;
        }

        if (!compareParams(targetAdminId, remoteParams,
                           currentParams.get(targetAdminId).getMap())) {
            return;
        }

        if (!compareParams(targetAdminId, remoteParams,
                           currentParams.get(hostSN).getMap(),
                           new SNPFilter())) {
            return;
        }

        if (!compareParams(targetAdminId, remoteParams,
                           currentParams.getGlobalParams().getMap())) {
            return;
        }
    }

    /**
     * Compare the adminCopy of the parameter map against what's in the
     * remote params.
     */
    private boolean compareParams(ResourceId rId,
                                  LoadParameters remoteParams,
                                  ParameterMap adminCopy) {
        return compareParams(rId, remoteParams, adminCopy,
                             new MapFilter());
    }

    private boolean compareParams(ResourceId rId,
                                  LoadParameters remoteParams,
                                  ParameterMap adminCopy,
                                  MapFilter filter) {

        ParameterMap remoteCopy =
            remoteParams.getMapByType(adminCopy.getType());

        if (remoteCopy == null) {
            violations.add(new ParamMismatch(rId, "Parameter collection " +
                                             adminCopy.getType() + " missing",
                                             showProgress, logger,
                                             progressReport));
            return false;
        }

        /**
         * Filter.  Most times this will be a no-op
         */
        remoteCopy = filter.filter(remoteCopy);
        adminCopy = filter.filter(adminCopy);
        if (!remoteCopy.equals(adminCopy)) {
            violations.add(new ParamMismatch(rId, adminCopy, remoteCopy,
                                             showProgress, logger,
                                             progressReport));
            return false;
        }

        return true;
    }

    /**
     * Filter maps for comparison.  Default will filter *out* those parameters
     * in skipParams.
     */
    private class MapFilter {
        ParameterMap filter(ParameterMap map) {
            return map.filter(ParameterState.skipParams, false);
        }
    }

    /**
     * Filter out unused StorageNodeParams from service comparisons because
     * changing StorageNodeParams won't update running services.
     */
    private class SNPFilter extends MapFilter {
        @Override
        ParameterMap filter(ParameterMap map) {
            return map.filter(ParameterState.serviceParams, true);
        }
    }

    /**
     * Filter out the disable param.
     */
    private class ExcludeDisableFilter extends MapFilter {
        @Override
        ParameterMap filter(ParameterMap map) {
            Set<String> disableName = new HashSet<String>();
            disableName.add(ParameterState.COMMON_DISABLED);
            return map.filter(disableName, false);
        }
    }

    /**
     * Classes to record violations.
     */
    public interface Problem {
        public ResourceId getResourceId();
    }

    /** Report a disabled service. */
    public static class ServiceStopped implements Problem, Serializable {
        private static final long serialVersionUID = 1L;
        private final ResourceId rId;
        private final StorageNodeId snId;
        private final boolean isDisabled;

        ServiceStopped(ResourceId rId,
                       StorageNodeId snId,
                       boolean showProgress,
                       Logger logger,
                       StringBuilder progressReport,
                       boolean isDisabled) {
            this.rId = rId;
            this.snId = snId;
            this.isDisabled = isDisabled;
            recordProgress(showProgress, logger, progressReport, this);
        }

        @Override
        public ResourceId getResourceId() {
            return rId;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(rId + " on " + snId);
            if (isDisabled) {
                sb.append(" was previously stopped and");
            }
            sb.append(" is not running. Consider restarting it with ");
            sb.append("'plan start-service'.");
            return sb.toString();
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((rId == null) ? 0 : rId.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (!(obj instanceof ServiceStopped))
                return false;
            ServiceStopped other = (ServiceStopped) obj;
            if (rId == null) {
                if (other.rId != null)
                    return false;
            } else if (!rId.equals(other.rId))
                return false;
            return true;
        }
    }

    public static class StatusNotRight implements Problem, Serializable {
        private static final long serialVersionUID = 1L;
        private final ResourceId rId;
        private final ServiceStatus expected;
        private final ServiceStatus current;

        StatusNotRight(ResourceId rId,
                       ServiceStatus expected,
                       ServiceStatus current,
                       boolean showProgress,
                       Logger logger,
                       StringBuilder progressReport) {
            this.rId = rId;
            this.expected = expected;
            this.current = current;
            recordProgress(showProgress, logger, progressReport, this);
        }

        @Override
        public ResourceId getResourceId() {
            return rId;
        }

        @Override
        public String toString() {
            return "Expected status " + expected + " but was " + current;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result
                    + ((current == null) ? 0 : current.hashCode());
            result = prime * result
                    + ((expected == null) ? 0 : expected.hashCode());
            result = prime * result + ((rId == null) ? 0 : rId.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof StatusNotRight)) {
                return false;
            }
            StatusNotRight other = (StatusNotRight) obj;
            if (current != other.current) {
                return false;
            }
            if (expected != other.expected) {
                return false;
            }
            if (rId == null) {
                if (other.rId != null) {
                    return false;
                }
            } else if (!rId.equals(other.rId)) {
                return false;
            }
            return true;
        }
    }

    public static class RMIFailed implements Problem, Serializable {
        private static final long serialVersionUID = 1L;
        private final ResourceId rId;
        private final String desc;

        RMIFailed(ResourceId rId, RemoteException e, String methodName,
                  boolean showProgress, Logger logger,
                  StringBuilder progressReport) {
            this.rId = rId;
            desc = methodName + " failed for " + rId + " : " + e.getMessage();
            recordProgress(showProgress, logger, progressReport, this);
        }

        RMIFailed(ResourceId rId, NotBoundException e, boolean showProgress,
                  Logger logger, StringBuilder progressReport) {
            this.rId = rId;
            desc = "No RMI service for " + rId + ": service name=" +
                e.getMessage();
            recordProgress(showProgress, logger, progressReport, this);
        }

        @Override
        public ResourceId getResourceId() {
            return rId;
        }

        @Override
        public String toString() {
            return desc;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((desc == null) ? 0 : desc.hashCode());
            result = prime * result + ((rId == null) ? 0 : rId.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof RMIFailed)) {
                return false;
            }
            RMIFailed other = (RMIFailed) obj;
            if (desc == null) {
                if (other.desc != null) {
                    return false;
                }
            } else if (!desc.equals(other.desc)) {
                return false;
            }
            if (rId == null) {
                if (other.rId != null) {
                    return false;
                }
            } else if (!rId.equals(other.rId)) {
                return false;
            }
            return true;
        }
    }

    public static class ParamMismatch implements Problem, Serializable {
        private static final long serialVersionUID = 1L;
        private final ResourceId rId;
        private final String mismatch;

        private static String getMismatchMessage(ParameterMap adminCopy,
                                                 ParameterMap remoteCopy,
                                                 ResourceId resourceId) {
            ParameterMap onRemoteButNotAdmin =
                    adminCopy.diff(remoteCopy, false);
            ParameterMap onAdminButNotRemote =
                    remoteCopy.diff(adminCopy, false);
            return "  Param on Admin but not on " + resourceId + " = " +
                    onAdminButNotRemote.showContents(true) + eol +
                       "  Param on " + resourceId + " but not on Admin = " +
                    onRemoteButNotAdmin.showContents(true);

        }

        ParamMismatch(ResourceId rId, ParameterMap adminCopy,
                      ParameterMap remoteCopy,
                      boolean showProgress, Logger logger,
                      StringBuilder progressReport) {
            this(rId, getMismatchMessage(adminCopy, remoteCopy, rId),
                 showProgress, logger, progressReport);
        }

        ParamMismatch(ResourceId rId, String msg, boolean showProgress,
                      Logger logger, StringBuilder progressReport) {

            this.rId = rId;
            mismatch = msg;
            recordProgress(showProgress, logger, progressReport, this);
        }

        @Override
        public ResourceId getResourceId() {
            return rId;
        }

        @Override
        public String toString() {
            return "Mismatch between metadata in admin service and " + rId +
                ": " + eol + mismatch;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result
                    + ((mismatch == null) ? 0 : mismatch.hashCode());
            result = prime * result + ((rId == null) ? 0 : rId.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof ParamMismatch)) {
                return false;
            }
            ParamMismatch other = (ParamMismatch) obj;
            if (mismatch == null) {
                if (other.mismatch != null) {
                    return false;
                }
            } else if (!mismatch.equals(other.mismatch)) {
                return false;
            }
            if (rId == null) {
                if (other.rId != null) {
                    return false;
                }
            } else if (!rId.equals(other.rId)) {
                return false;
            }
            return true;
        }
    }

    public static class VersionDifference implements Problem, Serializable {
        private static final long serialVersionUID = 1L;
        private final StorageNodeId snId;
        private final String desc;

        VersionDifference(StorageNodeId snId,
                          KVVersion snVersion,
                          boolean showProgress,
                          Logger logger,
                          StringBuilder progressReport) {
            this.snId = snId;
            desc = "Admin service version is " + KVVersion.CURRENT_VERSION +
                " but storage node version is " + snVersion;
            recordProgress(showProgress, logger, progressReport, this);
        }

        @Override
        public ResourceId getResourceId() {
            return snId;
        }

        @Override
        public String toString() {
            return desc;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((desc == null) ? 0 : desc.hashCode());
            result = prime * result + ((snId == null) ? 0 : snId.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof VersionDifference)) {
                return false;
            }
            VersionDifference other = (VersionDifference) obj;
            if (desc == null) {
                if (other.desc != null) {
                    return false;
                }
            } else if (!desc.equals(other.desc)) {
                return false;
            }
            if (snId == null) {
                if (other.snId != null) {
                    return false;
                }
            } else if (!snId.equals(other.snId)) {
                return false;
            }
            return true;
        }
    }

    public static class UpgradeNeeded implements Problem, Serializable {
        private static final long serialVersionUID = 1L;
        private final ResourceId rId;
        private final String desc;

        UpgradeNeeded(ResourceId rId,
                      KVVersion rVersion,
                      KVVersion targetVersion,
                      boolean showProgress,
                      Logger logger,
                      StringBuilder progressReport) {
            this.rId = rId;
            desc = "Node needs to be upgraded from " +
                   rVersion.getNumericVersionString() +
                   " to version " +
                   targetVersion.getNumericVersionString() +
                   " or newer";

            recordProgress(showProgress, logger, progressReport, this);
        }

        @Override
        public ResourceId getResourceId() {
            return rId;
        }

        @Override
        public String toString() {
            return desc;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((desc == null) ? 0 : desc.hashCode());
            result = prime * result + ((rId == null) ? 0 : rId.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof UpgradeNeeded)) {
                return false;
            }
            UpgradeNeeded other = (UpgradeNeeded) obj;
            if (desc == null) {
                if (other.desc != null) {
                    return false;
                }
            } else if (!desc.equals(other.desc)) {
                return false;
            }
            if (rId == null) {
                if (other.rId != null) {
                    return false;
                }
            } else if (!rId.equals(other.rId)) {
                return false;
            }
            return true;
        }
    }

    public static class BadDowngrade implements Problem, Serializable {
        private static final long serialVersionUID = 1L;
        private final ResourceId rId;
        private final String desc;

        BadDowngrade(ResourceId rId,
                    KVVersion rVersion,
                    KVVersion targetVersion,
                    boolean showProgress,
                    Logger logger,
                    StringBuilder progressReport) {
            this.rId = rId;
            desc = "Node cannot be downgraded to " +
                   targetVersion.getNumericVersionString() +
                   " because it is already at a newer minor version " +
                   rVersion.getNumericVersionString();

            recordProgress(showProgress, logger, progressReport, this);
        }

        @Override
        public ResourceId getResourceId() {
            return rId;
        }

        @Override
        public String toString() {
            return desc;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((desc == null) ? 0 : desc.hashCode());
            result = prime * result + ((rId == null) ? 0 : rId.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof BadDowngrade)) {
                return false;
            }
            BadDowngrade other = (BadDowngrade) obj;
            if (desc == null) {
                if (other.desc != null) {
                    return false;
                }
            } else if (!desc.equals(other.desc)) {
                return false;
            }
            if (rId == null) {
                if (other.rId != null) {
                    return false;
                }
            } else if (!rId.equals(other.rId)) {
                return false;
            }
            return true;
        }
    }


    public String getProgressReport() {
        return progressReport.toString();
    }

    private static void recordProgress(boolean showProgress,
                                       Logger logger,
                                       StringBuilder progressReport,
                                       Problem problem) {
        if (showProgress) {
            String msg = "Verify:         " + problem.getResourceId() + ": " +
                          problem;
            logger.info(msg);
            progressReport.append(msg).append(eol);
        }
    }

    /**
     * @showProgress if true, also print progress audit trail. Of interest
     * only in verbose mode.
     */
    public VerifyResults getResults() {
        if (showProgress) {
            return new VerifyResults(getProgressReport(), violations,
                                     warnings);
        }
        return new VerifyResults(violations, warnings);
    }

    public TopologyCheck getTopoChecker() {
        return topoChecker;
    }

    public List<Remedy> getRepairs() {
        return repairs.getRepairs();
    }

    /**
     * Organize repairs by type, because REMOVE_RN repairs have to be done last.
     */
    private class Repairs {
        private final List<Remedy> creates;
        private final List<Remedy> removes;
        private final List<Remedy> other;

        public Repairs() {
            this.creates = new ArrayList<Remedy>();
            this.removes = new ArrayList<Remedy>();
            this.other = new ArrayList<Remedy>();
        }

        void clear() {
            creates.clear();
            removes.clear();
            other.clear();
        }

        void add(Remedy remedy) {
            switch (remedy.getType()) {
                case CREATE_RN:
                    creates.add(remedy);
                    break;
                case REMOVE_RN:
                    removes.add(remedy);
                    break;
                default:
                    other.add(remedy);
                    break;
            }
        }

        List<Remedy> getRepairs() {
            List<Remedy> r = new ArrayList<Remedy>();
            r.addAll(creates);
            r.addAll(other);
            r.addAll(removes);
            return r;
        }

        boolean repairExists(RepNodeId rnId) {
            for (Remedy r : creates) {
                if (r.getRNId().equals(rnId)) {
                    return true;
                }
            }

            for (Remedy r : other) {
                if (r.getRNId().equals(rnId)) {
                    return true;
                }
            }

            for (Remedy r : removes) {
                if (r.getRNId().equals(rnId)) {
                    return true;
                }
            }
            return false;
        }
    }
}
