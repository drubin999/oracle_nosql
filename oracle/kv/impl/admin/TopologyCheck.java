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

import java.net.InetSocketAddress;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;


import oracle.kv.impl.admin.TopologyCheckUtils.SNServices;
import oracle.kv.impl.admin.VerifyConfiguration.Problem;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.DeploymentInfo;
import oracle.kv.impl.admin.plan.PortTracker;
import oracle.kv.impl.admin.plan.task.ChangeServiceAddresses;
import oracle.kv.impl.admin.plan.task.RelocateRN;
import oracle.kv.impl.admin.plan.task.Utils;
import oracle.kv.impl.admin.topo.Validations.InsufficientRNs;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.je.rep.ReplicationNode;
import com.sleepycat.je.rep.utilint.HostPortPair;
import com.sleepycat.je.rep.ReplicationNetworkConfig;

/**
 * TopologyCheck is used by VerifyConfiguration, by RepairPlan and by other
 * plan tasks that modify topology. It checks that the three representations of
 * layout metadata (the topology, the remote SN config files, and the JEHA
 * group db) are consistent with each other.
 *
 * Both RNs and Admins are checked.
 *
 * The class provides repair methods that can fix some inconsistencies.
 */
public class TopologyCheck {
    private final static String CHECK = "TopoCheck:";

    /* TODO: use kvstore param */
    private static final int JE_HA_TIMEOUT_MS = 5000;

    /*
     * The remedies for topology mismatches are embodied in this static map.
     * Setup the map of remedies for detected problems.
     */
    private static Map<RNLocationInput, REMEDY_TYPE> RECOMMENDATIONS;
    static {
        initRecommendations();
    }

    /*
     * For efficiency, TopoChecker saves the config.xml and information
     * derived from the topology for use across checking various services.
     */

    /* Information about hosted services, as derived from the SN's config.xml*/
    private final Map<StorageNodeId, SNServices> snRemoteParams;

    /*
     * A collection of topology resource ids, grouped by hostingSN, generated
     * from the AdminDb's topology and params
     */
    private final Map<StorageNodeId, SNServices> topoGroupedBySN;

    /*
     * RN to SN mappings, and Admin to SN mappings as determined by JEHA rep
     * group dbs.
     */
    private final Map<ResourceId, JEHAInfo> jeHAGroupDBInfo;

    private final Logger logger;

    /**
     * The topology and params are used at construction time to reorganize
     * topology information for mismatch detection.
     */
    public TopologyCheck(Logger logger, Topology topo, Parameters params) {
        this.logger = logger;
        snRemoteParams = new HashMap<StorageNodeId, SNServices>();
        jeHAGroupDBInfo = new HashMap<ResourceId, JEHAInfo>();
        topoGroupedBySN = TopologyCheckUtils.groupServicesBySN(topo, params);
    }

    /**
     * A remote config.xml was obtained from a SN. Save for future use.
     */
    public void saveSNRemoteParams(StorageNodeId snId, LoadParameters lp) {
        snRemoteParams.put(snId, processRemoteInfo(snId, lp));
    }

    /**
     * Return the union of RNs that are referenced by the SN's config.xml and
     * those referenced by the AdminDB/topology.
     */
    public Set<RepNodeId> getPossibleRNs(StorageNodeId snId) {
        Set<RepNodeId> rnsToCheck = new HashSet<RepNodeId>();
        rnsToCheck.addAll(snRemoteParams.get(snId).getAllRNs());
        rnsToCheck.addAll(topoGroupedBySN.get(snId).getAllRepNodeIds());
        return rnsToCheck;
    }

    /**
     * Check whether this RN should be on this SN. Recommend a fix if
     * - the RN's location information is inconsistent
     * - the RN's location information is consistent, but the RN needs to be
     * re-enabled.
     *
     * Assumes that the checker has been constructed with the most up to date
     * topology and params. If SN remote config params are loaded with
     * saveSNRemoteParams, assume that's also up to date.
     * @throws NotBoundException
     * @throws RemoteException
     */
    public Remedy checkRNLocation(Admin admin,
                                  StorageNodeId snId,
                                  RepNodeId rnId,
                                  boolean calledByDeployNewRN,
                                  boolean makeRNEnabled)
        throws RemoteException, NotBoundException {

        /*
         * Check that the topo, JE HA, and SN are consistent about the RN's
         * location. Assemble our three inputs:
         *  a. topo
         *  b. remote SN config
         *  c. JEHA groupDB
         */

        /* a. Get the topo's viewpoint */
        TOPO_STATUS topoStatus = TOPO_STATUS.GONE;
        SNServices servicesOnThisSN = topoGroupedBySN.get(snId);
        if (servicesOnThisSN != null) {
            if (servicesOnThisSN.getAllRepNodeIds().contains(rnId)) {
                topoStatus = TOPO_STATUS.HERE;
            }
        }

        /*
         * b. Get the remote SN config. If we can't reach the SN, we can't do
         * any kind of check. If there are problems reaching the SN, this will
         * throw an exception.
         */
        if (snRemoteParams.get(snId) == null) {
            Topology current = admin.getCurrentTopology();
            RegistryUtils regUtils =
                new RegistryUtils(current, admin.getLoginManager());
            StorageNodeAgentAPI sna = regUtils.getStorageNodeAgent(snId);
            LoadParameters remoteParams = sna.getParams();
            snRemoteParams.put(snId, processRemoteInfo(snId, remoteParams));
        }

        /* Does the SN say that the RN is present? */
        CONFIG_STATUS configStatus;
        if (snRemoteParams.get(snId).contains(rnId)) {
            configStatus = CONFIG_STATUS.HERE;
        } else {
            configStatus = CONFIG_STATUS.GONE;
        }

        /* c. Get JEHA group metadata */
        JEHAInfo jeHAInfo = jeHAGroupDBInfo.get(rnId);
        if (jeHAInfo == null) {
            populateRNJEHAGroupDB(admin, new RepGroupId(rnId.getGroupId()));
        }
        jeHAInfo = jeHAGroupDBInfo.get(rnId);

        /*
         * Now that all inputs are assembled, generate a RNLocationInput that
         * serves to lookup the required fix.
         */
        RNLocationInput remedyKey = null;
        if (jeHAInfo == null) {
            /* We don't have JE HA info */
            if (calledByDeployNewRN) {
                /* We know that this RN can't exist anywhere else. */
                remedyKey = new RNLocationInput(topoStatus, configStatus,
                                                OTHERSN_STATUS.GONE);
            } else {
                /*
                 * No JEHA info, so check the information gathered from the
                 * config.xmls for other SN. We may be able to conjecture
                 * whether this RN exists anywhere else. If it does show
                 * up on other SNs, we have some info.
                 */
                if (readAllSNRemoteParams(admin.getCurrentTopology(),
                                          admin.getLoginManager())) {
                    boolean foundRN = searchOtherSNs(snId, rnId);
                    OTHERSN_STATUS otherSNStatus;
                    if (foundRN) {
                        otherSNStatus = OTHERSN_STATUS.HERE;
                    } else {
                        otherSNStatus = OTHERSN_STATUS.GONE;
                    }

                    remedyKey = new RNLocationInput(topoStatus, configStatus,
                                                  otherSNStatus);
                } else {
                    /*
                     * Couldn't reach all SNs, so there's no proxy for JEHA
                     * info. Any repairs would have to be made solely on the
                     * topology and config status.
                     */
                    remedyKey = new RNLocationInput(topoStatus, configStatus);
                }
            }
        } else {
            /*
             * Great, we have definitive JE HA status. Use that to drive the
             * repairs to make topo and config match JE HA.
             */
            JEHA_STATUS jeHAStatus;
            if (jeHAInfo.getSNId().equals(snId)) {
                jeHAStatus = JEHA_STATUS.HERE;
            } else {
                jeHAStatus = JEHA_STATUS.GONE;
            }

            remedyKey = new RNLocationInput(topoStatus, configStatus,
                                            jeHAStatus);
        }

        /* Look up a remedy. If all seems okay, we'll get a REMEDY_TYPE.OKAY */
        REMEDY_TYPE remedyType = RECOMMENDATIONS.get(remedyKey);

        /* We have a problem but no remedy -- there's nothing we can do */
        if (remedyType == null) {
            remedyType = REMEDY_TYPE.NO_FIX;
        }

        /*
         * Even if the RN's information is consistent, we may still need to
         * generate a recommendation for repair. If the RN is disabled,
         * re-enable and restart the RN.
         */
        if (remedyType.equals(REMEDY_TYPE.OKAY) && makeRNEnabled) {
            if (topoStatus.equals(TOPO_STATUS.HERE) &&
                admin.getCurrentParameters().get(rnId).isDisabled()) {
                remedyType = REMEDY_TYPE.CREATE_RN;
            }
        }

        return new Remedy(remedyKey, snId, rnId, remedyType, jeHAInfo);
    }

    /**
     * Check whether it is possible to move this Admin from oldSN to newSN.
     * Base this decision on the Admin's JE HA group information.
     */
    public Remedy checkAdminMove(Admin admin,
                                 AdminId adminId,
                                 StorageNodeId oldSN,
                                 StorageNodeId newSN) {

        /*
         * Find all the Admins and ask them for the JEHA Group info.
         */
        populateAdminJEHAGroupDB(admin);
        JEHAInfo jeHAInfo = jeHAGroupDBInfo.get(adminId);

        if (jeHAInfo == null) {
            /*
             * We were not able to get JE HA info. This doesn't make sense,
             * because this code runs on the Admin, which means that there
             * must be an Admin master, and the JEHAGroup info should be
             * available.
             */
            throw new NonfatalAssertionException
                ("Attempting to check location for admin " + adminId +
                 " but could not obtain JE HA group db info");

        }

        Parameters params = admin.getCurrentParameters();
        Topology topo = admin.getCurrentTopology();

        /*
         * Compare the
         *  1. jeHA snId for this admin
         *  2. the Admin params snid
         *  3. the remote config.xml for the new SN
         *
         * These conditions should be true:
         *  a. jeHASNId is either oldSN or newSN
         *  b. adminParamsSNID is either oldSN or newSN
         *  c. the newSN's config.xml has either no admin or this admin.
         */
        StorageNodeId jeHASNId = jeHAInfo.getSNId();
        StorageNodeId adminParamsSNId = params.get(adminId).getStorageNodeId();
        SNServices remoteInfo = readOneSNRemoteParams(topo, newSN,
                                                      admin.getLoginManager());
        boolean remoteNewSNCorrect = false;
        if (remoteInfo != null) {
            AdminId remoteAdminId = remoteInfo.getAdminId();
            remoteNewSNCorrect = ((remoteAdminId == null) ||
                                  (adminId.equals(remoteAdminId)));
        }

        AdminLocationInput adminLoc =
            new AdminLocationInput(jeHASNId, adminParamsSNId,
                                   remoteNewSNCorrect);

        if (!((jeHASNId.equals(oldSN) || jeHASNId.equals(newSN)) &&
              (adminParamsSNId.equals(oldSN) || adminParamsSNId.equals(newSN)) &&
              remoteNewSNCorrect)) {

            /* Unexpectedly, conditions a, b and c are not fulfilled. */
            return new Remedy(adminLoc, adminId, REMEDY_TYPE.RUN_REPAIR,
                              jeHAInfo);
        }

        return new Remedy(adminLoc, adminId, REMEDY_TYPE.OKAY, jeHAInfo);
    }

    /**
     * Check that the Admin's JE HA group matches the AdminDB.
     *
     * Since Admins are
     *  1. only moved as a result of migrate-sn
     *  2. by definition, quorum exists
     *  3. the source SN is shut down
     * the AdminDB is always seen as source of truth. Unlike RNs, we never
     * need to revert an Admin location back to the old SN. If the Admin's
     * JE HA group does not match the AdminDB, update its HA address.
     */
    public Remedy checkAdminLocation(Admin admin,
                                     AdminId adminId) {

        /*
         * Find all the Admins and ask them for the JEHA Group info.
         */
        populateAdminJEHAGroupDB(admin);
        JEHAInfo jeHAInfo = jeHAGroupDBInfo.get(adminId);

        if (jeHAInfo == null) {
            /*
             * We were not able to get JE HA info. This doesn't make sense,
             * because this code runs on the Admin, which means that there
             * must be an Admin master, and the JEHAGroup info should be
             * available.
             */
            throw new NonfatalAssertionException
                ("Attempting to check location for admin " + adminId +
                 " but could not obtain JE HA group db info");

        }

        Parameters params = admin.getCurrentParameters();
        Topology topo = admin.getCurrentTopology();

        /*
         * Compare the
         *  1. jeHA snId for this admin
         *  2. the Admin params snid
         *  3. the remote config.xml for the new SN
         *
         * These conditions should be true:
         *  a. jeHASNId == adminParamsSNID
         *  b. the remote SN has this Admin in its config file.
         */
        StorageNodeId jeHASNId = jeHAInfo.getSNId();
        StorageNodeId adminParamsSNId = params.get(adminId).getStorageNodeId();
        SNServices remoteInfo = readOneSNRemoteParams(topo, adminParamsSNId,
                                                      admin.getLoginManager());
        boolean remoteSNCorrect = false;
        if ((remoteInfo != null) &&
            (adminId.equals(remoteInfo.getAdminId()))) {
            remoteSNCorrect = true;
        }

        AdminLocationInput adminLoc =
            new AdminLocationInput(jeHASNId, adminParamsSNId, remoteSNCorrect);

        if (!(jeHASNId.equals(adminParamsSNId) && remoteSNCorrect)) {

            /* Conditions a and b are not fulfilled. */
            return new Remedy(adminLoc, adminId, REMEDY_TYPE.FIX_ADMIN,
                              jeHAInfo);
        }

        return new Remedy(adminLoc, adminId, REMEDY_TYPE.OKAY, jeHAInfo);
    }

    /**
     * @return true if this RN is in the config file of an SN other than this
     * one.
     */
    private boolean searchOtherSNs(StorageNodeId snId, RepNodeId rnId) {
        for (Map.Entry<StorageNodeId, SNServices> e :
                 snRemoteParams.entrySet()) {

            /* This is this SN, skip it */
            if (e.getKey().equals(snId)) {
                continue;
            }

            if (e.getValue().contains(rnId)) {
                /* Found a different SN that has this RN */
                return true;
            }
        }

        /* Didn't find this RN on any other SN */
        return false;
    }

    /**
     * Try to get the JE HA repgroup db information for this shard.
     */
    private void populateRNJEHAGroupDB(Admin admin, RepGroupId rgId) {

        /*
         * Assemble a set of sockets to query by adding all the helper
         * hosts and nodehostports  from the rep node params for each
         * member of the shard.
         */
        Topology topo = admin.getCurrentTopology();
        Parameters params = admin.getCurrentParameters();
        RepGroup rg = topo.get(rgId);
        if (rg == null) {

            /*
             * Something is quite inconsistent; there's a RN in the SN that is
             * not in the topology. Give up on trying to get JE HA info.
             */
            return;
        }

        final Set<InetSocketAddress> helperSockets =
            new HashSet<InetSocketAddress>();

        /*
         * Find the set of SNs that the topo thinks owns the RN, in order
         * to optimize the translation. The translation will look at those
         * first.
         */
        Set<StorageNodeId> snCheckSet = new HashSet<StorageNodeId>();
        for (RepNode member : rg.getRepNodes()) {
            RepNodeParams rnp = params.get(member.getResourceId());
            snCheckSet.add(rnp.getStorageNodeId());
            helperSockets.addAll
                (HostPortPair.getSockets(rnp.getJEHelperHosts()));
            helperSockets.add
                (HostPortPair.getSocket(rnp.getJENodeHostPort()));
        }

        ReplicationNetworkConfig repNetConfig = admin.getRepNetConfig();
        /* Armed with the helper sockets, see if we can find a JE Master */
        Set<ReplicationNode> groupDB =
            TopologyCheckUtils.getJEHAGroupDB(rgId.getGroupName(),
                                              JE_HA_TIMEOUT_MS,
                                              logger,
                                              helperSockets,
                                              repNetConfig);

        StringBuilder helperHosts = new StringBuilder();
        for (ReplicationNode rNode : groupDB) {
            if (helperHosts.length() > 0) {
                helperHosts.append(",");
            }
            helperHosts.append(HostPortPair.getString(rNode.getHostName(),
                                                      rNode.getPort()));
        }

        for (ReplicationNode rNode : groupDB) {
            StorageNodeId foundSNId = TopologyCheckUtils.translateToSNId
                (topo, params, snCheckSet, rNode.getHostName(),
                 rNode.getPort());

            if (foundSNId == null) {
                /*
                 * Not expected -- why is the SN referred to by the RN not
                 * in the topology?
                 */
                logger.severe(CHECK + " couldn't find SN for " +
                              rNode.getHostName() + ":" + rNode.getPort());
            } else {
                jeHAGroupDBInfo.put(RepNodeId.parse(rNode.getName()),
                                    new JEHAInfo(foundSNId,
                                                 rNode,
                                                 helperHosts.toString()));
            }
        }
    }

    /**
     * Try to get the JE HA repgroup db information for the Admin group.
     */
    private void populateAdminJEHAGroupDB(Admin admin) {

        /*
         * Assemble a set of sockets to query by adding all the helper
         * hosts and nodehostports from the admin params.
         */
        Parameters params = admin.getCurrentParameters();

        final Set<InetSocketAddress> helperSockets =
            new HashSet<InetSocketAddress>();

        /*
         * Find the set of SNs that the topo thinks owns admins, in order to
         * optimize the translation of nodeHostPort to SN. The translation will
         * look at those first.
         */
        Set<StorageNodeId> snCheckSet = new HashSet<StorageNodeId>();
        for (AdminParams ap: params.getAdminParams()) {
            snCheckSet.add(ap.getStorageNodeId());
            helperSockets.addAll
                (HostPortPair.getSockets(ap.getHelperHosts()));
            helperSockets.add
                (HostPortPair.getSocket(ap.getNodeHostPort()));
        }

        String kvstoreName =
                admin.getCurrentParameters().getGlobalParams().getKVStoreName();
        String groupName = Admin.getAdminRepGroupName(kvstoreName);
        ReplicationNetworkConfig repNetConfig = admin.getRepNetConfig();
        /* Armed with the helper sockets, see if we can find a JE Master */
        Set<ReplicationNode> groupDB =
            TopologyCheckUtils.getJEHAGroupDB(groupName,
                                              JE_HA_TIMEOUT_MS,
                                              logger,
                                              helperSockets,
                                              repNetConfig);

        StringBuilder helperHosts = new StringBuilder();
        for (ReplicationNode rNode : groupDB) {
            if (helperHosts.length() > 0) {
                helperHosts.append(",");
            }
            helperHosts.append(HostPortPair.getString(rNode.getHostName(),
                                                      rNode.getPort()));
        }

        Topology topo = admin.getCurrentTopology();
        for (ReplicationNode rNode : groupDB) {
            StorageNodeId foundSNId = TopologyCheckUtils.translateToSNId
                (topo, params, snCheckSet, rNode.getHostName(),
                 rNode.getPort());

            if (foundSNId == null) {
                /*
                 * Not expected -- why is the SN referred to by the
                 * ReplicationNode not in the topology?
                 */
                logger.severe(CHECK + " couldn't find SN for " +
                              rNode.getHostName() + ":" + rNode.getPort());
            } else {
                jeHAGroupDBInfo.put(AdminId.parse(rNode.getName()),
                                    new JEHAInfo(foundSNId,
                                                 rNode,
                                                 helperHosts.toString()));
            }
        }
    }

    /**
     * Try to get remote params for all SNs in the topology. Used when we are
     * are trying to deduce what has happened without JE HA rep group db info.
     * @param loginManager
     * @return true if all SNs are found.
     */
    private boolean readAllSNRemoteParams(Topology topo,
                                          LoginManager loginManager) {

        /* Make sure each SN has a copy of its remote params fetched. */
        List<StorageNodeId> allSNs = topo.getStorageNodeIds();
        for (StorageNodeId snId : allSNs) {
            if (readOneSNRemoteParams(topo, snId, loginManager) == null) {
                /* Give up, we can't guarantee that we will find all SN info */
                return false;
            }
        }
        return true;
    }

    /**
     * Read one SN's remote config file and save it in the snRemoteParams map.
     * @return the newly generated information.
     */
    private SNServices readOneSNRemoteParams(Topology topo,
                                             StorageNodeId snId,
                                             LoginManager loginManager) {
        RegistryUtils regUtils = new RegistryUtils(topo, loginManager);

        SNServices remoteInfo = snRemoteParams.get(snId);
        if (remoteInfo != null) {
            return remoteInfo;
        }

        /* Try to get params from the remote SN */
        LoadParameters remoteParams;
        try {
            StorageNodeAgentAPI sna = regUtils.getStorageNodeAgent(snId);
            remoteParams = sna.getParams();
            remoteInfo = processRemoteInfo(snId, remoteParams);
            snRemoteParams.put(snId, remoteInfo);
            logger.info(CHECK + "loaded remote params for " + snId);
            return remoteInfo;
        } catch (NotBoundException re) {
            logger.info(CHECK + " failed to reach " + snId +
                        " to load SN params: "+ re);
        } catch (RemoteException re) {
            logger.info(CHECK + " failed to reach " + snId +
                        " to load SN params: "+ re);
        }
        return null;
    }

    /**
     * Make a table of recommendations for how to repair inconsistencies.  For
     * context, here are the steps taken when a RN is first created by
     * DeployNewRN:
     * 1. update AdminDB
     * 2. create on new SN w/sna.createRepNode
     * 3. that ends up updating the JE HA rep group
     *
     * RNRelocate steps
     * 1. disable RN on old SN
     *    a. update AdminDB w/disable bit
     *    b. remote call to update SN config w/disable bit
     * 2. update AdminDB to move RN to new SN
     * 3. update JE HA rep group
     * 4. create RN on new SN
     * 5. delete RN on old SN
     */
    static private void initRecommendations() {

        /*
         * When JEHA groupdb is available, give that status highest priority.
         * If it is available, change other locations to match.
         *
         *   RN is on SN      should be
         * topo|config|JEHA|  on thisSN failed task    |  remedy
         * --------------------------------------------------------------
         *  T     T     T      no problems    none
         *  T     T     F      DeployNewRN    clear from AdminDb and config
         *  T     F     T      RelocateRN     call sna.createRepNode
         *  T     F     F      DeployNewRN/   clear from AdminDB or
         *                     RelocateRN     (change AdminDB back to SN
         *                                  indicated by JE HA
         *  F     T     T      RelocateRN     re-add to AdminDB, reenable on SN
         *  F     T     F      RelocateRN     remove from this SN, call delete
         *  F     F     T     --- can't happen ---- check migrateSN
         *  F     F     F     no problems    none
         *
         * When JE HA info is not available, we can still reason what might
         * have happened by looking in the config files of other SNs.
         *
         * topo|cnfg|in other
         *     |    | SN    |
         *     |    |configs|  failed task    |  remedy
         * ----------------------------------------------------------------
         *  T     T     T        RelocateRN     eventually need to disable on
         *                                        other SN
         *  T     T     F        none           the user should re-start the
         *                                        RN
         *  T     F     T        RelocateRN     unknown, we know that
         *                                        task failed, not sure if
         *                                        after 2 or 3
         *  T     F     F    DeployNewRN or     remove from topo
         *  F     T     T        RelocateRN     eventually need to disable,
         *                                        but not safe to decide now
         *  F     T     F        RelocateRN     can't happen
         *  F     F     n/a
         */

        RECOMMENDATIONS = new HashMap<RNLocationInput, REMEDY_TYPE>();

        /* All three inputs (AdminDB, SN config, JE HA) are available */
        addAction(new RNLocationInput(TOPO_STATUS.HERE,
                                      CONFIG_STATUS.HERE,
                                      JEHA_STATUS.HERE),
                  REMEDY_TYPE.OKAY);
        addAction(new RNLocationInput(TOPO_STATUS.HERE,
                                      CONFIG_STATUS.HERE,
                                      JEHA_STATUS.GONE),
                  REMEDY_TYPE.CLEAR_ADMIN_CONFIG);
        addAction(new RNLocationInput(TOPO_STATUS.HERE,
                                      CONFIG_STATUS.GONE,
                                      JEHA_STATUS.HERE),
                  REMEDY_TYPE.CREATE_RN);
        addAction(new RNLocationInput(TOPO_STATUS.HERE,
                                      CONFIG_STATUS.GONE,
                                      JEHA_STATUS.GONE),
                  REMEDY_TYPE.REVERT_RN);
        addAction(new RNLocationInput(TOPO_STATUS.GONE,
                                      CONFIG_STATUS.HERE,
                                      JEHA_STATUS.HERE),
                  REMEDY_TYPE.REVERT_RN);
        addAction(new RNLocationInput(TOPO_STATUS.GONE,
                                      CONFIG_STATUS.HERE,
                                      JEHA_STATUS.GONE),
                  REMEDY_TYPE.REMOVE_RN);
        addAction(new RNLocationInput(TOPO_STATUS.GONE,
                                      CONFIG_STATUS.GONE,
                                      JEHA_STATUS.HERE),
                  REMEDY_TYPE.NO_FIX);
        addAction(new RNLocationInput(TOPO_STATUS.GONE,
                                      CONFIG_STATUS.GONE,
                                      JEHA_STATUS.GONE),
                  REMEDY_TYPE.OKAY);

        /*
         * JE HA GroupDB not available, but there is information about what
         * is in other remote SN config files. For these situations, try to
         * move forward, don't revert, because we don't know what the JE HA
         * situation is.
         */
        /* Relocate, needs to be disabled on the other SN */
        addAction(new RNLocationInput(TOPO_STATUS.HERE,
                                      CONFIG_STATUS.HERE,
                                      OTHERSN_STATUS.HERE),
                  REMEDY_TYPE.DISABLE);

        addAction(new RNLocationInput(TOPO_STATUS.HERE,
                                      CONFIG_STATUS.HERE,
                                      OTHERSN_STATUS.GONE),
                  REMEDY_TYPE.CREATE_RN);

        /* Don't know what to do */
        addAction(new RNLocationInput(TOPO_STATUS.HERE,
                                      CONFIG_STATUS.GONE,
                                      OTHERSN_STATUS.HERE),
                  REMEDY_TYPE.NO_FIX);

        /* Deploy didn't finish */
        addAction(new RNLocationInput(TOPO_STATUS.HERE,
                                      CONFIG_STATUS.GONE,
                                      OTHERSN_STATUS.GONE),
                  REMEDY_TYPE.CLEAR_ADMIN_CONFIG);

        /* Relocate failed, need remove this SN if disabled. */
        addAction(new RNLocationInput(TOPO_STATUS.GONE,
                                      CONFIG_STATUS.HERE,
                                      OTHERSN_STATUS.HERE),
                  REMEDY_TYPE.NO_FIX);
        addAction(new RNLocationInput(TOPO_STATUS.GONE,
                                      CONFIG_STATUS.HERE,
                                      OTHERSN_STATUS.GONE),
                  REMEDY_TYPE.NO_FIX); // TODO, figure out what to do
        addAction(new RNLocationInput(TOPO_STATUS.GONE,
                                      CONFIG_STATUS.GONE,
                                      OTHERSN_STATUS.GONE),
                  REMEDY_TYPE.OKAY);
        addAction(new RNLocationInput(TOPO_STATUS.GONE,
                                      CONFIG_STATUS.GONE,
                                      OTHERSN_STATUS.HERE),
                  REMEDY_TYPE.OKAY);
    }

    /**
     * Use this method to add recommendations to the map, guarding against
     * two remedies for the same set of inputs.
     */
    static private void addAction(RNLocationInput key,
                                  REMEDY_TYPE rType) {
        REMEDY_TYPE oldRemedy = RECOMMENDATIONS.put(key, rType);
        if (oldRemedy != null) {
            throw new IllegalStateException("Tried to overwrite remedy " +
                                            key + "/" + oldRemedy + " with " +
                                            rType);
        }
    }

   /**
     * Return all violations which are of a certain type, and are for a
     * specified kind of topology component.
     */
    private <T extends Problem> Set<T> filterViolations
                       (VerifyResults results, Class<T> problemClass) {

        Set<T> found = new HashSet<T>();
        for (Problem p : results.getViolations()) {
            if (p.getClass().equals(problemClass)) {
                found.add(problemClass.cast(p));
            }
        }
        return found;
    }

    /** Apply all remedies in the list */
    public void applyRemedies(List<Remedy> repairs, AbstractPlan plan) {
        for (Remedy r: repairs) {
            applyRemedy(r, plan, plan.getDeployedInfo(), null, null);
        }
    }

    /**
     * Given a remedy type, apply a fix.
     * @return true if fix was completed.
     */
    public boolean applyRemedy(Remedy remedy,
                               AbstractPlan plan,
                               DeploymentInfo deploymentInfo,
                               StorageNodeId oldSNId,
                               String newMountPoint) {

        plan.getLogger().info(CHECK + " applying " + remedy);

        boolean rnWorkDone = false;
        boolean adminWorkDone = false;
        switch(remedy.getType()) {
        case CLEAR_ADMIN_CONFIG:
            rnWorkDone = repairWithClearRN(remedy, plan, deploymentInfo);
            break;
        case CREATE_RN:
            rnWorkDone = repairStartRN(remedy, plan);
            break;
        case REVERT_RN:
            rnWorkDone = repairRevertRN(remedy, plan, deploymentInfo, oldSNId,
                                        newMountPoint);
            break;
        case REMOVE_RN:
            rnWorkDone = repairRemoveRN(remedy, plan);
            break;
        case FIX_ADMIN:
            adminWorkDone = repairAdmin(remedy, plan);
            break;
        case OKAY:
            /* Nothing to do */
            break;
        default:
            if (remedy.canFix()) {
                /* We should have had a way to fix this! */
                logger.info(CHECK + " did not act upon " + remedy);
                throw new UnsupportedOperationException();
            }

            /* Require manual intervention. */
            logger.info(CHECK + " required manual intervention for "+ remedy +
                        remedy.problemDescription());
        }
        if (rnWorkDone) {
            /*
             * Changes done to repair the location of a given RN may result
             * in changes to the admin db that need to be propagated to all
             * shard members.
             */
            ensureAdminDBAndRNParams
                (plan, new RepGroupId(remedy.getRNId().getGroupId()));
        }

        return rnWorkDone || adminWorkDone;
    }

    /**
     * Make sure that the AdminDB's params for a given admin are also correctly
     * reflected by the admin group's jeHAGroupDB, and also that the
     * Admin is started up.
     */
    private boolean repairAdmin(Remedy remedy, AbstractPlan plan) {
        PlannerAdmin admin = plan.getAdmin();
        Parameters params = admin.getCurrentParameters();
        Topology topo = admin.getCurrentTopology();
        AdminParams ap = params.get(remedy.getAdminId());
        try {
            ChangeServiceAddresses.changeAdminHAAddress
                (plan,
                 "repair Admin location for " + remedy.getAdminId(),
                 params,
                 remedy.getAdminId());

            LoginManager loginMgr = admin.getLoginManager();
            RegistryUtils regUtils = new RegistryUtils(topo,
                                                            loginMgr);
            StorageNodeAgentAPI sna =
                regUtils.getStorageNodeAgent(ap.getStorageNodeId());
            sna.createAdmin(ap.getMap());

            return true;
        } catch (OperationFaultException e) {
            plan.getLogger().info("Repair of Admin saw " + e);
            return false;
        } catch (RemoteException e) {
            plan.getLogger().info("Repair of Admin saw " + e);
            return false;
        } catch (NotBoundException e) {
            plan.getLogger().info("Repair of Admin saw " + e);
            return false;
        }
    }

    /**
     * Remove the RN from the admin db and config.xml of this SN.
     */
    private boolean repairWithClearRN(Remedy remedy,
                                      AbstractPlan plan,
                                      DeploymentInfo deploymentInfo) {
        PlannerAdmin admin = plan.getAdmin();
        StorageNodeId snId = remedy.getSNId();
        RepNodeId rnId = remedy.getRNId();
        Topology topo = admin.getCurrentTopology();
        if (remedy.getRNLocationInput().presentInSNConfig) {
            logger.info(CHECK + " trying to remove " + rnId + " from " +
                        snId + " config");
            RegistryUtils regUtils = new RegistryUtils(topo,
                                                       admin.getLoginManager());
            try {
                StorageNodeAgentAPI sna = regUtils.getStorageNodeAgent(snId);
                sna.destroyRepNode(rnId, true /*deleteData*/);
            } catch (NotBoundException re) {
                logger.info(CHECK + " couldn't reach " + snId + " to remove "
                            + rnId + " " + re);
                return false;
            } catch (RemoteException re) {
                logger.info(CHECK + " couldn't reach " + snId + " to remove "
                            + rnId + " " + re);
                return false;
            }
        }

        if (remedy.getRNLocationInput().presentInTopo) {
            topo.remove(rnId);
            admin.saveTopoAndRemoveRN(topo, deploymentInfo,
                                      rnId, plan);
            logger.info(CHECK + " trying to remove " + rnId +
                        " from topo and params");
        }

        return true;
    }

    /**
     * Change the admin db to make this RN refer to the SN which JE HA thinks
     * is correct.  Note that oldSNId may be null if we don't know what to
     * revert it to.
     */
    private boolean repairRevertRN(Remedy remedy,
                                   AbstractPlan plan,
                                   DeploymentInfo deploymentInfo,
                                   StorageNodeId oldSNId,
                                   String newMountPoint) {

        PlannerAdmin admin = plan.getAdmin();

        /* Change the topology back to the "old" SN */
        RepNodeId rnId = remedy.getRNId();
        StorageNodeId correctSNId = null;
        String correctJEHAHostPort = null;
        String correctHelpers = null;
        JEHAInfo jeHAInfo = remedy.getJEHAInfo();
        Topology topo = admin.getCurrentTopology();
        Parameters params = admin.getCurrentParameters();

        if (jeHAInfo != null) {
            /*
             * We got a definitive statement from the JEHA repgroup db to
             * determine where the RN should live.
             */
            correctSNId = jeHAInfo.getSNId();
            correctHelpers = jeHAInfo.getHelpers();
            correctJEHAHostPort = jeHAInfo.getHostPort();
        } else if (oldSNId != null) {

            /* We had some other means of determining the proper SN */
            correctSNId = oldSNId;
            PortTracker portTracker = new PortTracker(topo, params, oldSNId);
            int haPort = portTracker.getNextPort(oldSNId);
            String haHostname = params.get(oldSNId).getHAHostname();
            correctJEHAHostPort = HostPortPair.getString(haHostname, haPort);
            correctHelpers = TopologyCheckUtils.findPeerRNHelpers(rnId,
                                                                  params,
                                                                  topo);
            correctHelpers += "," + correctJEHAHostPort;
        }

        /* No known correct location - bail */
        if (correctSNId == null) {
            logger.info(CHECK + " could not find correct owning SN " + remedy);
            return false;
        }

        RepNode rn = topo.get(rnId);
        boolean topoUpdated = false;
        if (!rn.getStorageNodeId().equals(correctSNId)) {
            logger.info(CHECK + " updating topology so " + correctSNId +
                        " owns " + rnId);
            RepNode updatedRN = new RepNode(correctSNId);
            RepGroup rg = topo.get(rn.getRepGroupId());
            rg.update(rn.getResourceId(),updatedRN);
            topoUpdated = true;
        }

        /* Revert the RN's params, and any peer params */
        Set<RepNodeParams> needsUpdate =
            correctRNParams(topo,
                            params,
                            rnId,
                            correctSNId,
                            correctJEHAHostPort,
                            correctHelpers,
                            newMountPoint,
                            correctSNId.equals(remedy.getSNId()),
                            admin.getLoginManager());

        /* See which RNs might need to be prodded to refresh their params */
        boolean peersNeedUpdate = false;
        boolean rnNeedsUpdate = false;
        for (RepNodeParams updatedRNP : needsUpdate) {
            if (updatedRNP.getRepNodeId().equals(rnId)) {
                rnNeedsUpdate = true;
            } else {
                peersNeedUpdate = true;
            }
        }

        /* Write the changes to the AdminDB */
        if (topoUpdated) {
            admin.saveTopoAndParams(topo, deploymentInfo, needsUpdate,
                                    Collections.<AdminParams>emptySet(), plan);
            try {
                Utils.broadcastTopoChangesToRNs
                    (logger, admin.getCurrentTopology(),
                     CHECK + " updating topo",
                     admin.getParams().getAdminParams(), plan);
            } catch (InterruptedException e) {
                logger.info(CHECK +  " couldn't update topo: " + e);
                return false;
            }
        } else {
            if (!needsUpdate.isEmpty()) {
                plan.getAdmin().saveParams
                    (needsUpdate, Collections.<AdminParams>emptySet());
            }
        }

        /*
         * Restart the RN on the correct SN, make sure it houses the RN with
         * the correct params.
         */
        if (rnNeedsUpdate) {
            try {
                RelocateRN.startRN(plan, correctSNId, rnId);
            } catch (RemoteException e) {
                logger.info(CHECK + " couldn't start " + rnId);
            } catch (NotBoundException e) {
                logger.info(CHECK + " couldn't start " + rnId);
            }
        }

        /* Update params at peers, if needed */
        if (peersNeedUpdate) {
            try {
                Utils.refreshParamsOnPeers(plan, rnId);
            } catch (RemoteException e) {
                logger.info(this + " couldn't update helper hosts at peers");
                return false;
            } catch (NotBoundException e) {
                logger.info(this + " couldn't update helper hosts at peers");
                return false;
            }
        }

        return true;
    }

    /**
     * Start the RN on this SN.
     */
    private boolean repairStartRN(Remedy remedy, AbstractPlan plan) {

        try {
            RelocateRN.startRN(plan, remedy.getSNId(), remedy.getRNId());
        } catch (RemoteException e) {
            return false;
        } catch (NotBoundException e) {
            return false;
        }
        return true;
    }

    /**
     * Remove the RN from this SN.
     */
    private boolean repairRemoveRN(Remedy remedy, AbstractPlan plan) {

        try {
            return RelocateRN.destroyRepNode(plan,
                                             System.currentTimeMillis(),
                                             remedy.getSNId(),
                                             remedy.getRNId());
        } catch (InterruptedException e) {
            logger.info(CHECK + " couldn't remove " + remedy.getRNId() +
                        " from " + remedy.getSNId() + " because of " + e);
            return false;
        }
    }

    /**
     * Generate a set of correct RN params for all nodes of this shard. Set the
     * heap/cache, mountpoints, and helper hosts correctly.
     * @param loginManager
     */
    private Set<RepNodeParams> correctRNParams(Topology topo,
                                               Parameters params,
                                               RepNodeId rnId,
                                               StorageNodeId correctSNId,
                                               String correctJEHAHostPort,
                                               String correctHelpers,
                                               String newMountPoint,
                                               boolean correctSNIsNewSN,
                                               LoginManager loginManager) {
        /*
         * Do the params point at the right SN? If not, make a copy of the
         * RepNodeParams and fix its snId, and other attributes.
         */
        RepNodeParams rnp = params.get(rnId);
        RepNodeParams fixedRNP = new RepNodeParams(rnp);
        boolean paramsChanged = false;
        Set<RepNodeParams> needUpdate = new HashSet<RepNodeParams>();

        if (!rnp.getStorageNodeId().equals(correctSNId)) {
            fixedRNP.setStorageNodeId(correctSNId);
            Utils.setRNPHeapCacheGC(params.copyPolicies(),
                                    params.get(correctSNId),
                                    fixedRNP,
                                    topo);

            if (correctSNIsNewSN) {
                fixedRNP.setMountPoint(newMountPoint);
            } else {
                /* Look in the remote SN config file for the mount point info*/
                SNServices remoteInfo =
                        readOneSNRemoteParams(topo, correctSNId, loginManager);
                LoadParameters lp = remoteInfo.remoteParams;
                ParameterMap rMap = lp.getMap(rnId.getFullName(),
                                              ParameterState.REPNODE_TYPE);
                RepNodeParams remoteRNP = new RepNodeParams(rMap);
                fixedRNP.setMountPoint(remoteRNP.getMountPointString());
            }
            paramsChanged = true;

            logger.info(CHECK + " repair of repNodeParams for " +
                        correctSNId + "/" + rnId + " set storagedir " +
                        fixedRNP.getMountPointString());
        }

        /* Is its HA address correct? */
        if (!rnp.getJENodeHostPort().equals(correctJEHAHostPort)) {
            fixedRNP.setJENodeHostPort(correctJEHAHostPort);
            paramsChanged = true;
        }

        /* Are the helpers correct? */
        if (helperMismatch(rnp.getJEHelperHosts(), correctHelpers)) {
            fixedRNP.setJEHelperHosts(correctHelpers);
            paramsChanged = true;
        }

        /* Note that we always assume that this RN should be enabled */
        if (rnp.isDisabled()) {
            fixedRNP.setDisabled(false);
            paramsChanged = true;
        }

        /* See if any peer RNs need their helper hosts updated */
        RepNode rn = topo.get(rnId);
        for (RepNode peer : topo.get(rn.getRepGroupId()).getRepNodes()) {
            if (peer.getResourceId().equals(rnId)) {
                continue;
            }
            RepNodeParams peerRNP = params.get(peer.getResourceId());
            if (helperMismatch(peerRNP.getJEHelperHosts(),
                               correctHelpers)) {
                RepNodeParams newRNP = new RepNodeParams(peerRNP);
                newRNP.setJEHelperHosts(correctHelpers);
                needUpdate.add(newRNP);
            }
        }

        if (paramsChanged) {
            needUpdate.add(fixedRNP);
        }
        return needUpdate;
    }

    /**
     * return true if the two helper host lists don't match
     */
    private boolean helperMismatch(String helperListA, String helperListB) {

        List<String> helpersA = ParameterUtils.helpersAsList(helperListA);
        List<String> helpersB = ParameterUtils.helpersAsList(helperListB);

        if (!helpersA.containsAll(helpersB)) {
            /* mismatch */
            return true;
        }

        if (!helpersB.containsAll(helpersA)) {
            /* mismatch */
            return true;
        }

        return false;
    }

    /**
     * Check whether the Admin DB's copy of RepNodeParams and the RN's version
     * match for the given shard, and update the RN if needed. Ignore any
     * connectivity issues; this method should succeed if possible, but should
     * not cause an error if not possible.
     *
     * Since this considers the Admin DB's copy to be authoritative, this
     * should only be used when we are sure that the Admin DB has been
     * previously validated and repaired if required.
     */
    private void ensureAdminDBAndRNParams(AbstractPlan plan,
                                          RepGroupId rgId) {
        PlannerAdmin admin = plan.getAdmin();
        Topology topo = admin.getCurrentTopology();
        RegistryUtils regUtils = new RegistryUtils(topo,
                                                   admin.getLoginManager());
        Parameters currentParams = admin.getCurrentParameters();

        /* Check all the RNs of the shard */
        for (RepNode rn : topo.get(rgId).getRepNodes()) {
            RepNodeId rnId = rn.getResourceId();
            StorageNodeId snId = rn.getStorageNodeId();
            RepNodeParams rnp = currentParams.get(rnId);
            try {
                RepNodeAdminAPI rna = regUtils.getRepNodeAdmin(rnId);
                LoadParameters remoteParams = rna.getParams();
                if (remoteParams == null) {
                    logger.info(CHECK + " admin/rn param check for " + plan
                                + " did not find remote params for " + rnId);
                    continue;
                }
                ParameterMap remoteCopy =
                    remoteParams.getMapByType(ParameterState.REPNODE_TYPE);
                if (remoteCopy.equals(rnp.getMap())) {
                    /* Nothing to do, they match */
                    continue;
                }
                /* Write new params to the SN */
                StorageNodeAgentAPI sna = regUtils.getStorageNodeAgent(snId);
                sna.newRepNodeParameters(rnp.getMap());

                /* Notify the RN that there are new params. */
                RepNodeAdminAPI rnAdmin = regUtils.getRepNodeAdmin(rnId);
                rnAdmin.newParameters();
            } catch (RemoteException ignore) {
                logger.info(CHECK + " failed to reach " + snId + "/" + rnId +
                            " to ensure admin/rn params");
            } catch (NotBoundException ignore) {
                logger.info(CHECK + " failed to reach " + snId + "/" + rnId +
                            " to ensure admin/rn params");
            }
        }
    }

   /**
     * Remove empty shards if this topology has no RNs whatsoever. Used
     * when the initial deploy topology has failed before any RN or
     * partitions have been made.
     * TODO: what if the initial deploy fails because not all RNS could be
     * made? Then verify needs the number of partitions/the target topo
     * to do some fixing. Or just use topo rebalance?
     */
    public void repairInitialEmptyShards(VerifyResults results,
                                         AbstractPlan plan,
                                         DeploymentInfo deploymentInfo) {
        Set<InsufficientRNs> insufficientRNs =
            filterViolations(results, InsufficientRNs.class);
        logger.log(Level.FINE,
                   "{0} : RemoveInitialEmptyShards: insufficientRNs = {1}",
                   new Object[] {CHECK, insufficientRNs});
        if (insufficientRNs.isEmpty()) {
            return;
        }

        /*
         * This is not an initial deployment; some RNs exist. Use topo
         * rebalance to fix the problem.
         */
        Topology currentTopo = plan.getAdmin().getCurrentTopology();
        if (!currentTopo.getRepNodeIds().isEmpty()) {
            logger.log(Level.FINE,
                       "{0} : RemoveInitialEmptyShards: {1} RNs exist, " +
                       "try another repair approach",
                       new Object[] {CHECK, insufficientRNs});
            return;
        }

        /*
         * In general, an insufficient number of RNs means that the rebalance
         * command should be rerun. If there are no RNS at all, then the
         * initial deployment failed, and we can safely assume that there
         * are no underlying JE HA groups anywhere.
         */
        Set<RepGroupId> shardIds = currentTopo.getRepGroupIds();
        boolean shardsRemoved = false;
        for (RepGroupId rgId : shardIds) {
            currentTopo.remove(rgId);
            shardsRemoved = true;
        }

        if (shardsRemoved) {
            logger.info(CHECK + " for " + plan + " removed empty shards " +
                        shardIds);
            plan.getAdmin().saveTopo(currentTopo,  deploymentInfo, plan);
        }
    }

    /**
     * Encapsulate the information used to choose an Admin fix.
     */
    public static class AdminLocationInput {

        /* The SN which houses this Admin, based on JEHA */
        private final StorageNodeId jeHASNId;

        /* The SN which houses this Admin, based on its AdminParams */
        private final StorageNodeId adminParamsSNId;

        /* true if the adminParamsSNId also has the Admin in its config.xm */
        private final Boolean remoteNewSNCorrect;

        public AdminLocationInput(StorageNodeId jEHASNId,
                                  StorageNodeId adminParamsSNId,
                                  Boolean remoteNewSNCorrect) {
            this.jeHASNId = jEHASNId;
            this.adminParamsSNId = adminParamsSNId;
            this.remoteNewSNCorrect = remoteNewSNCorrect;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime
                    * result
                    + ((adminParamsSNId == null) ? 0
                            : adminParamsSNId.hashCode());
            result = prime * result
                    + ((jeHASNId == null) ? 0 : jeHASNId.hashCode());
            result = prime
                    * result
                    + ((remoteNewSNCorrect == null) ? 0
                            : remoteNewSNCorrect.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (!(obj instanceof AdminLocationInput))
                return false;
            AdminLocationInput other = (AdminLocationInput) obj;
            if (adminParamsSNId == null) {
                if (other.adminParamsSNId != null)
                    return false;
            } else if (!adminParamsSNId.equals(other.adminParamsSNId))
                return false;
            if (jeHASNId == null) {
                if (other.jeHASNId != null)
                    return false;
            } else if (!jeHASNId.equals(other.jeHASNId))
                return false;
            if (remoteNewSNCorrect == null) {
                if (other.remoteNewSNCorrect != null)
                    return false;
            } else if (!remoteNewSNCorrect.equals(other.remoteNewSNCorrect))
                return false;

            return true;
        }

        @Override
        public String toString() {
            return "AdminLocationInput [jeHASNId=" + jeHASNId
                    + ", adminParamsSNId=" + adminParamsSNId
                    + ", remoteJEHASNCorrect=" + remoteNewSNCorrect + "]";
        }
    }

    /*
     * Use these enums for the input to the RNLocationInput, to avoid confusion
     * from mixing up booleans.
     */
    enum TOPO_STATUS {HERE, GONE};
    enum CONFIG_STATUS {HERE, GONE};
    enum JEHA_STATUS {HERE, GONE};
    enum OTHERSN_STATUS{HERE, GONE};

    /**
     * Encapsulate the information used to choose a RN fix.
     */
    public static class RNLocationInput {

        /* if true, this service is on this SN, according to the topo. */
        private final boolean presentInTopo;
        /* if true, this service is on this SN, according to the config.xml. */
        private final boolean presentInSNConfig;

        /*
         * if we are able to get groupDB info, then jeHAKnown is true. If it's
         * false, then presentInJE HA has no meaning.
         */
        private final boolean jeHAKnown;
        /* if true, this service is on this SN, according to the JEHAGroupDB */
        private final boolean presentInJEHA;

        /* if true, this service is present in another SN config file */
        private final boolean otherSNKnown;
        private final boolean presentInOtherSNConfig;

        /*
         * Use when you know neither the JE HA group info nor what
         * other SNs hold.
         */
        RNLocationInput(TOPO_STATUS topoStatus,
                        CONFIG_STATUS configStatus) {
            this(topoStatus, configStatus, false, JEHA_STATUS.GONE, false,
                 OTHERSN_STATUS.GONE);
        }

        /* Use when you know the JE HA group info. */
        RNLocationInput(TOPO_STATUS topoStatus,
                        CONFIG_STATUS configStatus,
                        JEHA_STATUS jeHAStatus) {
            this(topoStatus, configStatus, true, jeHAStatus, false,
                 OTHERSN_STATUS.GONE);
        }

        /*
         * Use when you don't know the JE HA group info, but know what the
         * other SNs hold.
         */
        RNLocationInput(TOPO_STATUS topoStatus,
                        CONFIG_STATUS configStatus,
                        OTHERSN_STATUS otherSNStatus) {
            this(topoStatus, configStatus, false, JEHA_STATUS.GONE, true,
                 otherSNStatus);
        }

        RNLocationInput(TOPO_STATUS topoStatus,
                        CONFIG_STATUS configStatus,
                        boolean jeHAKnown,
                        JEHA_STATUS jeHAStatus,
                        boolean otherSNKnown,
                        OTHERSN_STATUS otherConfigStatus) {

            this.presentInTopo = topoStatus.equals(TOPO_STATUS.HERE);
            this.presentInSNConfig = configStatus.equals(CONFIG_STATUS.HERE);
            this.jeHAKnown = jeHAKnown;
            this.presentInJEHA = jeHAStatus.equals(JEHA_STATUS.HERE);
            this.otherSNKnown = otherSNKnown;
            this.presentInOtherSNConfig =
                otherConfigStatus.equals(OTHERSN_STATUS.HERE);

        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + (jeHAKnown ? 1231 : 1237);
            result = prime * result + (otherSNKnown ? 1231 : 1237);
            result = prime * result + (presentInJEHA ? 1231 : 1237);
            result = prime * result + (presentInOtherSNConfig ? 1231 : 1237);
            result = prime * result + (presentInSNConfig ? 1231 : 1237);
            result = prime * result + (presentInTopo ? 1231 : 1237);
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (!(obj instanceof RNLocationInput))
                return false;
            RNLocationInput other = (RNLocationInput) obj;
            if (jeHAKnown != other.jeHAKnown)
                return false;
            if (otherSNKnown != other.otherSNKnown)
                return false;
            if (presentInJEHA != other.presentInJEHA)
                return false;
            if (presentInOtherSNConfig != other.presentInOtherSNConfig)
                return false;
            if (presentInSNConfig != other.presentInSNConfig)
                return false;
            if (presentInTopo != other.presentInTopo)
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "LocationInput [presentInTopo=" + presentInTopo
                + ", presentInSNConfig=" + presentInSNConfig
                + ", jeHAKnown=" + jeHAKnown + ", presentInJEHA="
                + presentInJEHA + ", otherSNKnown=" + otherSNKnown
                + ", presentInOtherSNConfig=" + presentInOtherSNConfig
                + "]";
        }
    }

    public enum REMEDY_TYPE {
        CLEAR_ADMIN_CONFIG(true), /* Remove RN from topo/params for this SN */
        CREATE_RN(true),   /* Tell SN to create/start RN */
        DISABLE(false),    /* User should run stop-service */
        OKAY(false),       /* no work needed */
        NO_FIX(false),     /* manual work needed */
        REMOVE_RN(true),   /* Remove RN from this SN */
        REVERT_RN(true),   /* Remove RN from new SN to old SN */
        RUN_REPAIR(false), /* User must run plan repair-topology */
        FIX_ADMIN(true);   /* Make the Admin's JE HA location consistent */

        /*
         * If true, this issue can be fixed by the topology checker without
         * user intervention
         */
        private final boolean canFix;

        private REMEDY_TYPE(boolean canFix) {
            this.canFix = canFix;
        }

        /** Return information about the problem. */
        public String problemDescription(RepNodeId rnId,
                                         StorageNodeId snId,
                                         RNLocationInput rnLocationInput) {
            switch(this) {
            case CLEAR_ADMIN_CONFIG:
                return rnId + " is present in Admin metadata and on " +
                    snId + " configuration but has not been created. Must " +
                    "be removed from metadata";
            case CREATE_RN:
                return "Must create or start " + rnId + " on this SN.";
            case DISABLE:
                return rnId + " should be stopped and disabled on " + snId;
            case RUN_REPAIR:
                return "Please run plan repair-topology to fix " +
                    "inconsistent location metadata";
            case NO_FIX:
                return "No automatic fix available for problem with " + rnId;
            case OKAY:
                return "No problem with " + rnId;
            case REMOVE_RN:
                return rnId + " must be removed from " + snId;
            case REVERT_RN:
                return rnId + " must be moved back to its original hosting SN";
            case FIX_ADMIN:
                return "Ensure that the Admin's location metadata is consistent";
            default:
                return "Metadata issue with " + rnId + "/" +
                    snId + ": " + rnLocationInput;
            }
        }

        public boolean canFix() {
            return canFix;
        }
    }

    /**
     * Struct to contain the remedy type and other information needed by the
     * repair methods.
     */
    public static class Remedy {
        private final REMEDY_TYPE remedyType;
        private final JEHAInfo jeHAInfo;

        private final StorageNodeId snId;
        private final RepNodeId rnId;
        private final RNLocationInput rNLocationInput;

        private final AdminLocationInput adminLocationInput;
        private final AdminId adminId;

        public Remedy(RNLocationInput rNLocationInput,
                      StorageNodeId snId,
                      RepNodeId rnId,
                      REMEDY_TYPE remedyType,
                      JEHAInfo jeHAInfo) {
            this.rNLocationInput = rNLocationInput;
            this.snId = snId;
            this.rnId = rnId;
            this.remedyType = remedyType;
            this.jeHAInfo = jeHAInfo;
            this.adminLocationInput = null;
            this.adminId = null;
        }

        AdminId getAdminId() {
            return adminId;
        }

        public Remedy(AdminLocationInput adminLoc,
                      AdminId adminId,
                      REMEDY_TYPE remedyType,
                      JEHAInfo jeHAInfo) {
            this.rNLocationInput = null;
            this.snId = null;
            this.rnId = null;
            this.remedyType = remedyType;
            this.jeHAInfo = jeHAInfo;
            this.adminLocationInput = adminLoc;
            this.adminId = adminId;
        }

        public JEHAInfo getJEHAInfo() {
            return jeHAInfo;
        }

        public String problemDescription() {
            return remedyType.problemDescription(rnId, snId, rNLocationInput);
        }

        public REMEDY_TYPE getType() {
            return remedyType;
        }

        public boolean canFix() {
            return remedyType.canFix();
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("Remedy [");
            builder.append("remedyType=").append(remedyType).append(", ");
            if (jeHAInfo != null) {
                builder.append("jeHAInfo=").append(jeHAInfo).append(", ");
            }
            if (snId != null) {
                builder.append("snId=").append(snId).append(", ");
            }
            if (rnId != null) {
                builder.append("rnId=").append(rnId).append(", ");
            }
            if (rNLocationInput != null) {
                builder.append("rNLocationInput=").append(rNLocationInput)
                       .append(", ");
            }
            if (adminLocationInput != null) {
                builder.append("adminLocationInput=")
                       .append(adminLocationInput).append(", ");
            }
            if (adminId != null) {
                builder.append("adminId=").append(adminId);
            }
            builder.append("]");
            return builder.toString();
        }

        public StorageNodeId getSNId() {
            return snId;
        }

        public RepNodeId getRNId() {
            return rnId;
        }

        public RNLocationInput getRNLocationInput() {
            return rNLocationInput;
        }

        public AdminLocationInput getAdminLocationInput() {
            return adminLocationInput;
        }
    }

    /**
     * Process an SN's config.xml - to generate a list of the services it
     * thinks it hosts.
     */
    private SNServices processRemoteInfo(StorageNodeId snId,
                                         LoadParameters remoteParams) {

        /* Find all the RNs that are present in the SN's config file */
        List<ParameterMap> rnMaps =
            remoteParams.getAllMaps(ParameterState.REPNODE_TYPE);

        Set<RepNodeId> allRNs = new HashSet<RepNodeId>();
        for (ParameterMap map : rnMaps) {
            RepNodeId rnId = RepNodeId.parse(map.getName());
            allRNs.add(rnId);
        }

        /* Find all the Admins that are present in the SN's config file.*/
        ParameterMap adminMap =
            remoteParams.getMapByType(ParameterState.ADMIN_TYPE);
        AdminId aid = null;
        if (adminMap != null) {
            aid = new AdminId(adminMap.getOrZeroInt(ParameterState.AP_ID));
        }

        return new SNServices(snId, allRNs, aid, remoteParams);
    }

    /**
     * Info derived from the JEHA group db, about a node's hostname/port
     * and its peers.
     */
    public static class JEHAInfo {
        private final StorageNodeId translatedSNId;
        private final ReplicationNode jeReplicationNode;
        private final String groupWideHelperHosts;

        JEHAInfo(StorageNodeId translatedSNId,
                 ReplicationNode jeReplicationNode,
                 String groupWideHelperHosts) {
            this.translatedSNId = translatedSNId;
            this.jeReplicationNode = jeReplicationNode;
            this.groupWideHelperHosts = groupWideHelperHosts;
        }

        public StorageNodeId getSNId() {
            return translatedSNId;
        }

        String getHostPort() {
            return HostPortPair.getString(jeReplicationNode.getHostName(),
                                          jeReplicationNode.getPort());
        }

        String getHelpers() {
            return groupWideHelperHosts;
        }

        @Override
        public String toString() {
            return "JE derivedSN = " + translatedSNId +
                " RepNode=" + jeReplicationNode +
                " helpers=" + groupWideHelperHosts;
        }
    }
}
