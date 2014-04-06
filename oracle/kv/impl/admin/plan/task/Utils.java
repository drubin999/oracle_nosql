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

package oracle.kv.impl.admin.plan.task;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.rep.ReplicationMutableConfig;

import oracle.kv.impl.admin.AdminServiceParams;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.PlannerAdmin;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams.RNHeapAndCacheSize;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.api.TopologyInfo;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.rep.RepNodeStatus;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.rep.admin.RepNodeAdminFaultException;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.topo.change.TopologyChange;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.ServiceUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.util.Ping;

/**
 * Utility methods for tasks.
 */
public class Utils {

    /**
     * Returns the set of metadata required to configure a new RN.
     * @param topo the current topology
     * @param plan the plan
     * @return set of metadata
     */
    static Set<Metadata<? extends MetadataInfo>>
                    getMetadataSet(Topology topo, AbstractPlan plan) {
        
        if (topo == null) {
            throw new IllegalStateException("Requires non-null topology to " +
                                            "build metadata set");
        }
        final Set<Metadata<? extends MetadataInfo>> metadataSet =
                            new HashSet<Metadata<? extends MetadataInfo>>();
        metadataSet.add(topo);
        
        /*
         * In addition to the topology, a new node needs security and table
         * metadata if they have been defined.
         */
        Metadata<? extends MetadataInfo> md = plan.getAdmin().
                                    getMetadata(SecurityMetadata.class,
                                                MetadataType.SECURITY);
        if (md != null) {
            metadataSet.add(md);
        }
        md = plan.getAdmin().getMetadata(TableMetadata.class,
                                         MetadataType.TABLE);
        if (md != null) {
            metadataSet.add(md);
        }
        return metadataSet;
    }
    
    /**
     * Sends the list of topology changes or the entire new topology to all
     * repNodes.
     *
     * In general, we only send the changes, and not the actual topology.
     * However, if the instigating plan is rerun, we may not be able to create
     * the delta, because we do not know if the broadcast has already
     * executed. In that case, send the whole topology.
     *
     * If there are failures updating repNodes, this method will retry until
     * there is enough successful updates to meet the minimum threshold
     * specified by getBroadcastTopoThreshold(). The threshold is specified
     * as a percent of repNodes. This retry policy is necessary to seed the
     * repNodes with the new topology.
     * @return true if the a topology has been successfully sent to the desired
     * number of nodes, false if the broadcast was stopped due to an interrupt.
     *
     * TODO - It may be better to do this broadcast in a constrained parallel
     * way, so that the broadcast makes rapid progress even in the presence of
     * some one or a few bad network connections, which may stall on network
     * timeouts. (same for metadata broadcast below)
     *
     * TODO - This method can be removed once the prerequisite is bumped to
     * the metadata release.
     */
    public static boolean broadcastTopoChangesToRNs(Logger logger,
                                                    Topology topo,
                                                    String actionDescription,
                                                    AdminParams params,
                                                    AbstractPlan plan)
        throws InterruptedException {

        logger.log(Level.INFO,
                   "Broadcasting topology seq# {0}, changes for {1}",
                   new Object[]{topo.getSequenceNumber(), actionDescription});

        final List<RepNodeId> retryList = new ArrayList<RepNodeId>();
        final RegistryUtils registry =
            new RegistryUtils(topo, plan.getLoginManager());
        int nNodes = 0;

        for (RepGroup rg : topo.getRepGroupMap().getAll()) {
            for (RepNode rn : rg.getRepNodes()) {
                nNodes++;
                final RepNodeId rnId = rn.getResourceId();

                /* Send the topo. If error, record the RN for retry */
                int result = sendTopoChangesToRN(logger,
                                                 rnId,
                                                 topo,
                                                 actionDescription,
                                                 registry);
                if (result < 0) {
                    /* No need to broadcast, a newer topo was found */
                    return true;
                } else if (result == 0) {
                    retryList.add(rnId);
                }
            }
        }

        if (retryList.isEmpty()) {
            logger.log(Level.FINE,
                       "Successful broadcast to all nodes of topology " +
                       "seq# {0}, for {1}",
                       new Object[]{topo.getSequenceNumber(),
                                    actionDescription});

            return true;
        }

        /*
         * The threshold is a percent of existing nodes. The resulting
         * number of nodes must be > 0.
         */
        final int thresholdNodes =
               Math.max((nNodes * params.getBroadcastTopoThreshold()) / 100, 1);
        final int acceptableFailures = nNodes - thresholdNodes;
        final long delay = params.getBroadcastTopoRetryDelayMillis();

        int retries = 0;

        /* Continue to retry until the threshold is met, or interrupted */
        while (retryList.size() > acceptableFailures) {

            if (plan.isInterruptRequested()) {
                /* stop trying */
                logger.log(Level.INFO,
                           "{0} has been interrupted, stop attempts to " +
                           "broadcast topology changes for {1}",
                           new Object[] {plan.toString(), actionDescription});
                return false;
            }

            retries++;

            logger.log(Level.INFO,
                       "Failed to broadcast topology to {0} out of {1} " +
                       "nodes, will retry, acceptable failure threshold={2}, " +
                       "retries={3}",
                       new Object[]{retryList.size(), nNodes,
                                    acceptableFailures, retries});

            Thread.sleep(delay);

            /* Get a new registry in case the failures were network related */
            final RegistryUtils ru = new RegistryUtils(topo,
                                                       plan.getLoginManager());
            final Iterator<RepNodeId> itr = retryList.iterator();

            while (itr.hasNext()) {
                int result = sendTopoChangesToRN(logger,
                                                 itr.next(),
                                                 topo,
                                                 actionDescription,
                                                 ru);
                if (result < 0) {
                    /* No need to broadcast, a newer topo was found */
                    return true;
                } else if (result > 0) {
                    itr.remove();
                }
            }
        }

        logger.log(Level.INFO,
                   "Broadcast topology {0} for {1} successful to {2} out of " +
                   "{3} nodes",
                   new Object[]{topo.getSequenceNumber(),
                                actionDescription,
                                nNodes - retryList.size(), nNodes});
        return true;
    }

    /**
     * Sends the list of topology changes or the entire new topology to the
     * specified repNode.
     *
     * @return 1 if the update was successful, 0 if failed, -1 if a newer
     * topology was found
     */
    private static int sendTopoChangesToRN(Logger logger,
                                           RepNodeId rnId,
                                           Topology topo,
                                           String actionDescription,
                                           RegistryUtils registry) {

        StorageNodeId snId = topo.get(rnId).getStorageNodeId();

        try {
            final RepNodeAdminAPI rnAdmin = registry.getRepNodeAdmin(rnId);
            final int rnTopoSeqNum = rnAdmin.getTopoSeqNum();

            /*
             * Finding the same topology is possible as the RNs will be
             * busy propagating it throughout the store.
             */
            if (rnTopoSeqNum == topo.getSequenceNumber()) {
                return 1;
            }

            /*
             * Finding a newer topology in the wild means some other task
             * has an updated topology and has sent it out.
             */
            if (rnTopoSeqNum > topo.getSequenceNumber()) {
                logger.log(Level.FINE,
                           "{0} has a higher topology sequence number of " +
                           "{1} compared to this topology of {2} while {3}",
                           new Object[]{rnId, rnTopoSeqNum,
                                        topo.getSequenceNumber(),
                                        actionDescription});
                return -1;
            }

            /*
             * If the topology is empty or null, force updating the full topo.
             */
            final List<TopologyChange> changes =
                    (rnTopoSeqNum == Topology.EMPTY_SEQUENCE_NUMBER) ?
                                                null :
                                                topo.getChanges(rnTopoSeqNum);

            if ((changes != null) && (changes.size() > 0)) {
                final int actualTopoSeqNum =
                    rnAdmin.updateMetadata(new TopologyInfo(topo, changes));
                if ((rnTopoSeqNum - actualTopoSeqNum) > 1)  {
                    /*
                     * retry, the target has an older topology than acquired
                     * initially.
                     */
                    logger.log(Level.INFO,
                               "Older topology than expected for {0} on {1}." +
                               " Expected topo seq num: {2} actual: {3}",
                                new Object[]{rnId, snId, rnTopoSeqNum,
                                             actualTopoSeqNum});
                    return 0;
                }
            } else {
                rnAdmin.updateMetadata(topo);
            }
            return 1;
        } catch (RepNodeAdminFaultException rnfe) {
            /*
             * RN had problems with this request; often the problem is that
             * it is not in RUNNING state
             */
            logger.log(Level.INFO,
                      "Unable to update topology for {0} on {1} for {2}" +
                       " during broadcast: {3}",
                       new Object[]{rnId, snId, actionDescription, rnfe});
        } catch (NotBoundException notbound) {
            logger.log(Level.INFO,
                       "{0} on {1} cannot be contacted for topology update " +
                       "to {2} during broadcast: {3}",
                       new Object[]{rnId, snId, actionDescription, notbound});
        } catch (RemoteException e) {
            logger.log(Level.INFO,
                       "Could not update topology for {0} on {1} for " +
                       "{2}: {3}",
                       new Object[]{rnId, snId, actionDescription, e});
        }

        /*
         * Any other RuntimeExceptions indicate an unexpected problem, and will
         * fall through and throw out of this method.
         */
        return 0;
    }

    static void updateHelperHost(PlannerAdmin admin,
                                 Topology topo,
                                 RepGroupId rgId,
                                 RepNodeId rnId,
                                 Logger logger)
        throws RemoteException, NotBoundException {

        RepNodeParams oldRNP = admin.getRepNodeParams(rnId);
        RepNodeParams newRNP = new RepNodeParams(oldRNP);

        String updatedHelpers =
            Utils.findRNHelpers(admin, rnId, topo.get(rgId));

        /*
         * There are no other helpers available, probably because this is
         * a rep group of 1, so don't change the helper host.
         */
        if (updatedHelpers.length() == 0) {
            return;
        }

        newRNP.setJEHelperHosts(updatedHelpers);
        admin.updateParams(newRNP);
        StorageNodeId snId = newRNP.getStorageNodeId();
        logger.info("Changing helperHost for " + rnId + " on " + snId +
                    " to " + updatedHelpers);

        /* Ask the SNA to write a new configuration file. */
        RegistryUtils registryUtils =
            new RegistryUtils(topo, admin.getLoginManager());
        StorageNodeAgentAPI sna = registryUtils.getStorageNodeAgent(snId);
        sna.newRepNodeParameters(newRNP.getMap());
    }

    /**
     * Generate the most complete set of helper hosts possible by appending all
     * the nodeHostPort values for all other members of this HA repGroup.
     */
    private static String findRNHelpers(PlannerAdmin admin,
                                        RepNodeId targetRNId,
                                        RepGroup rg) {

        StringBuilder helperHosts = new StringBuilder();
        for (RepNode rn : rg.getRepNodes()) {
            RepNodeId rid = rn.getResourceId();
            if (rid.equals(targetRNId)) {
                continue;
            }

            if (helperHosts.length() != 0) {
                helperHosts.append(ParameterUtils.HELPER_HOST_SEPARATOR);
            }

            helperHosts.append
                (admin.getRepNodeParams(rid).getJENodeHostPort());
        }
        return helperHosts.toString();
    }

    static String findAdminHelpers(Parameters p,
                                   AdminId target) {
        StringBuilder helperHosts = new StringBuilder();
        if (p.getAdminCount() == 1) {
            /* If there is only one Admin, it is its own helper. */
            helperHosts.append(p.get(target).getNodeHostPort());
        } else {
            for (AdminParams ap : p.getAdminParams()) {
                AdminId aid = ap.getAdminId();
                if (aid.equals(target)) {
                    continue;
                }
                if (helperHosts.length() != 0) {
                    helperHosts.append(ParameterUtils.HELPER_HOST_SEPARATOR);
                }
                helperHosts.append(ap.getNodeHostPort());
            }
        }
        return helperHosts.toString();
    }

    /**
     * @throws OperationFaultException if the HA address could not be changed.
     */
    static void changeHAAddress(Topology topo,
                                Parameters parameters,
                                AdminParams adminParams,
                                RepNodeId rnId,
                                StorageNodeId oldNode,
                                StorageNodeId newNode,
                                AbstractPlan plan,
                                Logger logger)
        throws InterruptedException {

        RepNode targetRN = topo.get(rnId);
        RepGroup rg = topo.get(targetRN.getRepGroupId());

        /*
         * Only need to change the HA address if both the old and new SNs are
         * in primary data centers, since information about secondary nodes is
         * not recorded persistently in the replication group.
         */
        final Datacenter oldDC = topo.getDatacenter(oldNode);
        final Datacenter newDC = topo.getDatacenter(newNode);
        if (!(oldDC.getDatacenterType().isPrimary() &&
              newDC.getDatacenterType().isPrimary())) {
            return;
        }

        String targetNodeHostPort = parameters.get(rnId).getJENodeHostPort();

        /*
         * Find the first node that is not the target node, and ask it
         * to update addresses. If it can't, continue trying other
         * members of the group. If the master is not available,
         * continue trying for some time.
         */
        boolean done = false;
        String targetHelperHosts = parameters.get(rnId).getJEHelperHosts();
        final long delay = adminParams.getBroadcastTopoRetryDelayMillis();

        logger.log(Level.INFO,
                   "Change haPort for {0} to relocate from {1} to {2}",
                   new Object[]{rnId, oldNode, newNode});
        while (!done && !plan.isInterruptRequested()) {

            boolean groupHasNoMaster = false;

            /* Try each RN in turn. Only one has to get the update out. */
            for (RepNode rn : rg.getRepNodes()) {
                RepNodeId peerId = rn.getResourceId();
                if (peerId.equals(rnId)) {
                    continue;
                }

                /* Found a peer repNode */
                try {
                    RegistryUtils registry =
                        new RegistryUtils(topo, plan.getLoginManager());
                    RepNodeAdminAPI rnAdmin = registry.getRepNodeAdmin(peerId);
                    if (rnAdmin.updateMemberHAAddress(rnId.getGroupName(),
                                                      rnId.getFullName(),
                                                      targetHelperHosts,
                                                      targetNodeHostPort)) {
                        done = true;
                    } else {
                        logger.log
                            (Level.INFO,
                             "Attempting to update HA address for {0} while " +
                             "relocating from {1} to {2}  but shard has no " +
                             "master. Wait and retry",
                             new Object[]{rnId, oldNode, newNode});
                        groupHasNoMaster = true;
                    }
                } catch (RepNodeAdminFaultException e) {
                    logger.log(
                        Level.SEVERE,
                        "{0} experienced an exception when attempting to" +
                        " update HA address for {1} while relocating from" +
                        " {2} to {3}: {4}",
                         new Object[] { peerId, rnId, oldNode, newNode, e });
                } catch (NotBoundException e) {
                    logger.log(
                        Level.SEVERE,
                        "{0} could not be contacted, experienced an" +
                        " exception when attempting to update HA address" +
                        " for {1} while relocating from {2} to {3}: {4}",
                        new Object[] { peerId, rnId, oldNode, newNode, e });
                } catch (RemoteException e) {
                    logger.log(
                        Level.SEVERE,
                        "{0} could not be contacted, experienced an" +
                        " exception when attempting to update HA address" +
                        " for {1} while relocating from {2} to {3}: {4}",
                        new Object[] { peerId, rnId, oldNode, newNode, e });
                }
            }

            /* Someone was able to get the update out successfully */
            if (done) {
                break;
            }

            /*
             * No-one in the group was able to do the update. Retry only if the
             * group has no master, and we are waiting for a new master to be
             * elected. Wait before retrying. TODO: this uses the same pattern
             * as broadcastTopoChangesToRNs, in that it sleeps and is
             * susceptible to interrupt. Ideally, we make the plan interrupt
             * flag available to implement a softer interrupt. Should also
             * consider moving this retry loop to a higher level, so retries
             * are implemented by the task, as multiple phases. Not strictly
             * necessary if this is only called by serial tasks, because there
             * is no concern with tying up a planner thread.
             */
            if (groupHasNoMaster) {
                /*
                 * We only retry if we have positive info that there was no
                 * master and that a retry should work soon.
                 */
                logger.log(Level.INFO,
                           "No master for shard while updating HA address " +
                           "for {0} from {1} to {2}. Wait and retry",
                           new Object[]{rnId, oldNode, newNode});
                Thread.sleep(delay);
            } else {
                /*
                 * Unexpected problems, couldn't contact anyone in group,
                 * give up.
                 */
                logger.log(Level.INFO,
                           "Could not contact any member of the shard while " +
                           " updating HA address for {0} from {1} to {2}." +
                           " Give up.",
                           new Object[]{rnId, oldNode, newNode});
                break;
            }
        }

        if (!done) {
            throw new OperationFaultException
                ("Couldn't change HA address for " + rnId + " to " +
                 targetNodeHostPort + " while migrating " + oldNode + " to " +
                 newNode);
        }
    }

    static void stopRN(AbstractPlan plan,
                       StorageNodeId snId,
                       RepNodeId rnId)
        throws RemoteException, NotBoundException {

        plan.getLogger().log(Level.INFO, "Stopping {0} on {1}",
                             new Object[]{rnId, snId});

        /*
         * Update the rep node params to indicate that this node is now
         * disabled, and save the changes.
         */
        PlannerAdmin admin = plan.getAdmin();
        RepNodeParams rnp =
            new RepNodeParams(admin.getRepNodeParams(rnId));
        rnp.setDisabled(true);
        admin.updateParams(rnp);

        // TODO: ideally, put a call in to force a collection of monitored
        // data, to update monitoring before stopping this node.

        /* Tell the SNA to stop the node. */
        Topology topology = admin.getCurrentTopology();
        RegistryUtils registryUtils =
            new RegistryUtils(topology, admin.getLoginManager());
        StorageNodeAgentAPI sna = registryUtils.getStorageNodeAgent(snId);
        sna.stopRepNode(rnId, false);

        /* Stop monitoring this node. */
        admin.getMonitor().unregisterAgent(rnId);

        /*
         * Ask the monitor to collect status now for this rep node, so that it
         * will realize that it has been disabled, and this changed status
         * will display sooner.
         */
        admin.getMonitor().collectNow(snId);
    }

    static void startRN(AbstractPlan plan,
                        StorageNodeId snId,
                        RepNodeId rnId)
        throws RemoteException, NotBoundException {

        plan.getLogger().log(Level.INFO, "Starting {0} on {1}",
                             new Object[]{rnId, snId});

        /*
         * Check the topology to make sure that the RepNode exists, in
         * case the topology was changed after the plan was constructed, and
         * before it ran. TODO: this can't actually happen yet because we
         * don't support the removal of RepNodes. Test when store contraction
         * is supported in later releases.
         */
        PlannerAdmin admin = plan.getAdmin();
        Topology topology = admin.getCurrentTopology();
        RepNode rn = topology.get(rnId);
        if (rn == null) {
            throw new IllegalCommandException
                (rnId +
                 " was removed from the topology and can't be started");
        }

        /*
         * Update the rep node params to indicate that this node is now enabled,
         * and save the changes.
         */
        RepNodeParams rnp =
            new RepNodeParams(admin.getRepNodeParams(rnId));
        rnp.setDisabled(false);
        admin.updateParams(rnp);

        /* Tell the SNA to startup the node. */
        AdminServiceParams asp = admin.getParams();
        StorageNodeParams snp = admin.getStorageNodeParams(snId);
        String storeName = asp.getGlobalParams().getKVStoreName();
        StorageNodeAgentAPI sna =
            RegistryUtils.getStorageNodeAgent
            (storeName,
             snp.getHostname(),
             snp.getRegistryPort(),
             snId,
             admin.getLoginManager());
        sna.startRepNode(rnId);

        /*
         * Check if the RN experienced a problem directly at startup time.
         */
        RegistryUtils.checkForStartupProblem(storeName, snp.getHostname(),
                                             snp.getRegistryPort(), rnId, snId,
                                             admin.getLoginManager());

        /*
         * Tell the Monitor to start monitoring this node. Registering
         * an agent is idempotent
         */
        StorageNode sn = topology.get(snId);
        plan.getAdmin().getMonitor().registerAgent(sn.getHostname(),
                                                   sn.getRegistryPort(),
                                                   rnId);

    }

    public static Task.State waitForRepNodeState(AbstractPlan plan,
                                                 RepNodeId rnId,
                                                 ServiceStatus targetState)
        throws InterruptedException {

        AdminServiceParams asp = plan.getAdmin().getParams();
        AdminParams ap = asp.getAdminParams();
        long waitSeconds =
            ap.getWaitTimeoutUnit().toSeconds(ap.getWaitTimeout());

        String msg = "Waiting " + waitSeconds + " seconds for RepNode " +
            rnId + " to reach " + targetState;

        plan.getLogger().fine(msg);

        RepNodeParams rnp = plan.getAdmin().getRepNodeParams(rnId);

        /*
         * Since other, earlier tasks may have failed, it's possible that we
         * may be trying to wait for an nonexistent rep node.
         */
        if (rnp == null) {
            throw new OperationFaultException
                (msg + ", but that that RepNode doesn't exist in the store");
        }

        StorageNodeParams snp =
            plan.getAdmin().getStorageNodeParams(rnp.getStorageNodeId());

        String storename = asp.getGlobalParams().getKVStoreName();
        String hostname = snp.getHostname();
        int regPort = snp.getRegistryPort();
        StorageNodeId snId = snp.getStorageNodeId();
        LoginManager loginMgr = plan.getLoginManager();

        try {
            ServiceStatus[] target = {targetState};
            ServiceUtils.waitForRepNodeAdmin(storename,
                                             hostname,
                                             regPort,
                                             rnId,
                                             snId,
                                             loginMgr,
                                             waitSeconds,
                                             target);
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                throw (InterruptedException) e;
            }
            plan.getLogger().info("Timed out while " + msg);
            RegistryUtils.checkForStartupProblem(storename,
                                                 hostname,
                                                 regPort,
                                                 rnId,
                                                 snId,
                                                 loginMgr);

            return Task.State.ERROR;
        }

        /*
         * Ask the monitor to collect status now for this rep node,so a
         * new status will be available sooner
         */
        plan.getAdmin().getMonitor().collectNow(rnId);
        return Task.State.SUCCEEDED;

    }

    /**
     * Confirms the status of the specified SN. If shouldBeRunning is true
     * the SN must be up and have the status of RUNNING, otherwise
     * OperationFaultException is thrown. If shouldBeRunning is false, an
     * OperationFaultException is thrown only if the SN can be contacted and has
     * RUNNING status. Note that if shouldBeRunning is false this method is not
     * exact as it will pass a node which cannot be contacted.
     *
     * @param topology the current topology
     * @param snId the SN to check
     * @param shouldBeRunning true if the SN must have RUNNING status
     * @param infoMsg the message to include with the exception
     *
     * @throws OperationFaultException if the status of the SN does not meet
     *         the requirement
     */
    static void confirmSNStatus(Topology topology,
                                LoginManager loginMgr,
                                StorageNodeId snId,
                                boolean shouldBeRunning,
                                String infoMsg) {

        /* Check if this storage node is already running. */
        final RegistryUtils registry = new RegistryUtils(topology, loginMgr);

        try {
            StorageNodeAgentAPI sna = registry.getStorageNodeAgent(snId);
            ServiceStatus serviceStatus = sna.ping().getServiceStatus();

            if (shouldBeRunning) {
                if (serviceStatus == ServiceStatus.RUNNING) {
                    return;
                }
            } else {
                if (!serviceStatus.isAlive()) {
                    return;
                }
            }

            throw new OperationFaultException
                (snId + " has status " + serviceStatus + ". " + infoMsg);
        } catch (NotBoundException notbound) {
            if (shouldBeRunning) {
                throw new OperationFaultException
                    (snId + " cannot be contacted." + infoMsg);
            }
            /* Ok for this node to be unreachable */

        } catch (RemoteException remoteEx) {
            if (shouldBeRunning) {
                throw new OperationFaultException
                    (snId + " cannot be contacted." + infoMsg);
            }
            /* Ok for this node to be unreachable */
        }
    }

    /**
     * Sends the list of metadata changes or the entire metadata to all
     * repNodes.
     *     *
     * If there are failures updating repNodes, this method will retry until
     * there are enough successful updates to meet the minimum threshold
     * specified by getBroadcastTopoThreshold(). The threshold is specified
     * as a percent of repNodes. This retry policy is necessary to seed the
     * repNodes with the new metadata.
     *
     * TODO - It may be better to do this broadcast in a constrained parallel
     * way, so that the broadcast makes rapid progress even in the presence of
     * some one or a few bad network connections, which may stall on network
     * timeouts.
     *
     * TODO - The code implements "retry until interrupted or we have enough
     * coverage". It may be worthwhile changing that to "retry until
     * interrupted, or we have enough coverage and the last N retries made
     * no progress". Seems like ensuring at least a minimal level of retry
     * might be worthwhile.
     *
     * @param logger a logger
     * @param md a metadata object to broadcast to RNs
     * @param topo the current store topology
     * @param actionDescription description included in logging
     * @param params admin params
     * @param plan the current executing plan
     *
     * @return true if the a metadata has been successfully sent to the desired
     * number of nodes, false if the broadcast was stopped due to an interrupt.
     */
    static boolean broadcastMetadataChangesToRNs(Logger logger,
                                                 Metadata<?> md,
                                                 Topology topo,
                                                 String actionDescription,
                                                 AdminParams params,
                                                 AbstractPlan plan) {
        logger.log(Level.INFO,
                   "Broadcasting {0} for {1}",
                   new Object[]{md, actionDescription});

        final List<RepNodeId> retryList = new ArrayList<RepNodeId>();
        final RegistryUtils registry =
            new RegistryUtils(topo, plan.getLoginManager());
        int nNodes = 0;

        for (RepGroup rg : topo.getRepGroupMap().getAll()) {
            for (RepNode rn : rg.getRepNodes()) {
                nNodes++;
                final RepNodeId rnId = rn.getResourceId();

                /* Send the metadata. If error, record the RN for retry */
                int result = sendMetadataChangesToRN(logger,
                                                     rnId,
                                                     md,
                                                     topo,
                                                     actionDescription,
                                                     registry);
                if (result == STOP) {
                    /*
                     * No need to continue broadcasting, newer metadata was
                     * found
                     */
                    return true;
                }
                if (result == FAILED) {
                    retryList.add(rnId);
                }
            }
        }

        if (retryList.isEmpty()) {
            logger.log(Level.INFO,// TODO FINE
                       "Successful broadcast to all nodes of {0} metadata " +
                       "seq# {1}, for {2}",
                       new Object[]{md.getType(), md.getSequenceNumber(),
                                    actionDescription});
            return true;
        }

        /* TODO - change property or create a new one for the threshold value */

        /*
         * The threshold is a percent of existing nodes. The resulting
         * number of nodes must be > 0.
         */
        final int thresholdNodes =
               Math.max((nNodes * params.getBroadcastTopoThreshold()) / 100, 1);
        final int acceptableFailures = nNodes - thresholdNodes;
        final long delay = params.getBroadcastTopoRetryDelayMillis();

        int retries = 0;

        /* Continue to retry until the threshold is met, or interrupted */
        while (retryList.size() > acceptableFailures) {

            if (plan.isInterruptRequested()) {
                /* stop trying */
                logger.log(Level.INFO,
                           "{0} has been interrupted, stop attempts to " +
                           "broadcast {1} metadata changes for {2}",
                           new Object[] {plan.toString(), md.getType(),
                                         actionDescription});
                return false;
            }
            retries++;

            logger.log(Level.INFO,
                       "Failed to broadcast {0} metadata to {1} out of {2} " +
                       "nodes, will retry, acceptable failure threshold={3}, " +
                       "retries={4}",
                       new Object[]{md.getType(), retryList.size(), nNodes,
                                    acceptableFailures, retries});
            try {
                Thread.sleep(delay);
            } catch (InterruptedException ex) {
                logger.log(Level.INFO,
                           "{0} has been interrupted, stop attempts to " +
                           "broadcast {1} metadata changes for {2}",
                           new Object[] {plan.toString(), md.getType(),
                                         actionDescription});
                return false;
            }

            final Iterator<RepNodeId> itr = retryList.iterator();

            while (itr.hasNext()) {
                int result = sendMetadataChangesToRN(logger,
                                                     itr.next(),
                                                     md,
                                                     topo,
                                                     actionDescription,
                                                     registry);
                if (result == STOP) {
                    /*
                     * No need to continue broadcasting, newer metadata was
                     * found
                     */
                    return true;
                }
                if (result == SUCCEEDED) {
                    itr.remove();
                    if (retryList.size() <= acceptableFailures) {
                        break;
                    }
                }
            }
        }

        logger.log(Level.INFO,
                   "Broadcast {0} metadata {1} for {2} successful to {3} " +
                   "out of {4} nodes",
                   new Object[]{md.getType(), md.getSequenceNumber(),
                                actionDescription,
                                nNodes - retryList.size(), nNodes});
        return true;
    }

    /* Return values from sendMetadataChangesToRN() */
    private static int SUCCEEDED = 1;
    private static int FAILED = 0;
    private static int STOP = -1;

    /**
     * Sends the list of metadata changes or the entire new topology to the
     * specified repNode.
     *
     * @return 1 if the update was successful, 0 if failed, -1 if a newer
     * metadata was found
     */
    private static int sendMetadataChangesToRN(Logger logger,
                                               RepNodeId rnId,
                                               Metadata<?> md,
                                               Topology topo,
                                               String actionDescription,
                                               RegistryUtils registry) {
        final StorageNodeId snId = topo.get(rnId).getStorageNodeId();

        try {
            final RepNodeAdminAPI rnAdmin = registry.getRepNodeAdmin(rnId);
            int rnSeqNum = rnAdmin.getMetadataSeqNum(md.getType());

            /* If the RN is behind, attempt to update it */
            if (rnSeqNum < md.getSequenceNumber()) {
                final MetadataInfo info = md.getChangeInfo(rnSeqNum);

                /* If the info is empty, send the full metadata. */
                if (info.isEmpty()) {
                    logger.log(Level.INFO,  // TODO -FINE
                               "Unable to send {0} changes to {1} at {2}, " +
                               "sending full metadata",
                               new Object[]{md.getType(), rnId, rnSeqNum});

                    rnAdmin.updateMetadata(md);
                    return SUCCEEDED;
                }
                rnSeqNum = rnAdmin.updateMetadata(info);
            }

            /* Update was successful or the RN was already up-to-date */
            if (rnSeqNum == md.getSequenceNumber()) {
                return SUCCEEDED;
            }

            /*
             * Finding newer metadata in the wild means some other task
             * has an updated metadata and has sent it out. In this case we
             * can stop the broadcast.
             */
            if (rnSeqNum > md.getSequenceNumber()) {
                logger.log(Level.FINE,
                           "{0} has a higher {1} metadata sequence number of " +
                           "{2} compared to this metadata of {3} while {4}",
                           new Object[]{rnId, md.getType(), rnSeqNum,
                                        md.getSequenceNumber(),
                                        actionDescription});
                return STOP;
            }

            /*
             * If here, rnSeqNum < md.getSequenceNumber() meaning the update
             * failed.
             */
            logger.log(Level.INFO,
                       "Update of {0} metadata to {1} on {2} failed. " +
                       "Expected metadata seq num: {3} actual: {4} while {5}",
                        new Object[]{md.getType(), rnId, snId,
                                     md.getSequenceNumber(), rnSeqNum,
                                     actionDescription});

        } catch (RepNodeAdminFaultException rnfe) {
            /*
             * RN had problems with this request; often the problem is that
             * it is not in RUNNING state
             */
            logger.log(Level.INFO,
                      "Unable to update {0} metadata for {1} on {2} for {3}" +
                       " during broadcast: {4}",
                       new Object[]{md.getType(), rnId, snId,
                                    actionDescription, rnfe});
        } catch (NotBoundException notbound) {
            logger.log(Level.INFO,
                       "{0} on {1} cannot be contacted for {2} metadata " +
                       "update to {3} during broadcast: {4}",
                       new Object[]{rnId, snId, md.getType(),
                                    actionDescription, notbound});
        } catch (RemoteException e) {
            logger.log(Level.INFO,
                       "Could not update {0} metadata for {1} on {2} for " +
                       "{3}: {4}",
                       new Object[]{md.getType(), rnId, snId,
                                    actionDescription, e});
        }

        /*
         * Any other RuntimeExceptions indicate an unexpected problem, and will
         * fall through and throw out of this method.
         */
        return FAILED;
    }

    /**
     * For all members of this shard other than the skipRNId, write the new RN
     * params to the owning SNA, and tell the RN to refresh its params. Try to
     * be resilient; try all RNs, even in the face of a RMI failure from one.
     * @throws RemoteException
     * @throws NotBoundException
     */
    public static void refreshParamsOnPeers(AbstractPlan plan, 
                                            RepNodeId skipRNId)
        throws RemoteException, NotBoundException {

        PlannerAdmin admin = plan.getAdmin();
        Topology topo = admin.getCurrentTopology();
        RepGroupId rgId = topo.get(skipRNId).getRepGroupId();
        RegistryUtils registry = new RegistryUtils(topo,
                                                   plan.getLoginManager());
        plan.getLogger().log(Level.INFO,
                             "Writing new RN params to members of shard {0}",
                              rgId);

        RemoteException remoteExSeen = null;
        NotBoundException notBoundSeen = null;
        for (RepNode peer : topo.get(rgId).getRepNodes()) {
            RepNodeId peerId = peer.getResourceId();
            if (peerId.equals(skipRNId)) {
                /* Skip the relocated RN, it is not yet deployed */
                continue;
            }

            RepNodeParams peerRNP = admin.getRepNodeParams(peerId);

            /* Write a new config file on the SNA */
            try {
                StorageNodeAgentAPI sna =
                    registry.getStorageNodeAgent(peer.getStorageNodeId());
                sna.newRepNodeParameters(peerRNP.getMap());

                /* Have the RN notice its new params */
                RepNodeAdminAPI rnAdmin = registry.getRepNodeAdmin(peerId);
                rnAdmin.newParameters();
            } catch (RemoteException e) {
                /* Save the exception, carry on to all the others */
                remoteExSeen = e;
                plan.getLogger().info("Couldn't refresh params on " + peerId +
                                      e);
            } catch (NotBoundException e) {
                notBoundSeen = e;
                plan.getLogger().info("Couldn't refresh params on " + peerId +
                                      e);
            }
        }

        /* Now throw the exception we've seen */
        if (remoteExSeen != null) {
            throw remoteExSeen;
        } 

        if (notBoundSeen != null) {
            throw notBoundSeen;
        }
    }

    /** 
     * Calculate the heap, cache, and GC params, which are a function of
     * the number of RNs on this SN, and set the appropriate values in the
     * RepNodeParams.
     */
    public static void setRNPHeapCacheGC(ParameterMap policyMap,
                                         StorageNodeParams targetSNP,
                                         RepNodeParams targetRNP,
                                         Topology topo) {

        StorageNodeId targetSN = targetSNP.getStorageNodeId();
        RepNodeId targetRN = targetRNP.getRepNodeId();

        /* How many RNs will be hosted on this SN? */
        Set<RepNodeId> rnsOnSN = topo.getHostedRepNodeIds(targetSN);
        int numRNsOnSN = rnsOnSN.size();
        if (!rnsOnSN.contains(targetRN)) {
            numRNsOnSN += 1;
        }

        RNHeapAndCacheSize heapAndCache = 
            targetSNP.calculateRNHeapAndCache(policyMap,
                                              numRNsOnSN, 
                                              targetRNP.getRNCachePercent());
        targetRNP.setRNHeapAndJECache(heapAndCache);
        targetRNP.setParallelGCThreads(targetSNP.calcGCThreads());
    }

    /**
     * Throw an exception if the shard that this RN belongs to does not have a 
     * master, or will not have quorum once the target node is shutdown.
     * @param rnId target RN which will be shutdown for relocation.
     */
    static void verifyShardHealth(Parameters params,
                                  Topology topo, 
                                  RepNodeId rnId,
                                  StorageNodeId oldSN,
                                  StorageNodeId newSN,
                                  Logger logger) {

        /* 
         * TODO: In future releases, expose the ElectionQuorum class in JE
         * and use that to query for quorum, to prevent having to know so
         * much about what constitutes quorum in JE.
         */
        RepNodeId master = null;
        int numTries = 0;
        Set<RepNodeId> running = new HashSet<RepNodeId>();
        RepGroupId rgId = new RepGroupId(rnId.getGroupId());
        while (numTries < 3) {
            numTries ++;
            Map<RepNodeId, RepNodeStatus> status =
                Ping.getRepNodeStatus(topo, rgId);

            int electableGroupSize = 0;
            running.clear();
            boolean groupSizeOverrideUsed = false;
            for (Map.Entry<RepNodeId, RepNodeStatus> nodeStatus :
                     status.entrySet()) {

                RepNodeId thisRN = nodeStatus.getKey();

                /* Skip secondary nodes, and don't include in group size */
                final Datacenter thisDC = topo.getDatacenter(thisRN);
                if (thisDC.getDatacenterType().isSecondary()) {
                    continue;
                }

                electableGroupSize++;

                /* See it the emergency group size override has been used. */
                if (params.get(thisRN).getConfigProperties().contains(
                        ReplicationMutableConfig.
                        ELECTABLE_GROUP_SIZE_OVERRIDE)) {
                    groupSizeOverrideUsed = true;
                }

                if (nodeStatus.getValue() == null) {
                    continue;
                }

                /* Make sure there is a master even if it is the target */
                if (nodeStatus.getValue().getReplicationState().isMaster()) {
                    master = thisRN;
                }

                /* Don't count self, we're about to bring this node down. */
                if (thisRN.equals(rnId)) {
                    continue;
                }

                if (nodeStatus.getValue().getServiceStatus().equals
                    (ServiceStatus.RUNNING)) {
                    running.add(thisRN);
                }
            }

            /*
             * Check if there's quorum to ack the write. Even if there's a
             * master, lack of quorum will doom the write.
             */
            final int quorum = electableGroupSize/2 + 1;
            if ((running.size() < quorum) && !groupSizeOverrideUsed) {
                throw new OperationFaultException(
                    "Shard " + rgId + " will not have at least " + quorum +
                    " electable nodes up to execute writes, so can't move " +
                    rnId + " from " + oldSN + " to " + newSN +
                    ". Other running nodes are " + running);
            }

            /* We have quorum, and a master, all is well */
            if (master != null) {
                return;
            }

            /*
             * We have quorum, but no master. Wait 10 seconds (for a total of
             * 30 seconds) because it's possible that mastership is shifting.
             * Elections should take no more than 10 seconds, and
             * master rebalancing might add more time on top of that.
             */
            try {
                logger.info("Waiting for " + rgId +
                            " to establish a master. Running nodes are " +
                            running);
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                /*
                 * Just try again if we're interrupted, this wait can be
                 * approximate.
                 */
            }
        }

        throw new OperationFaultException
            ("Shard " + rgId + " can't execute writes. No master exists." +
             "Running nodes are " + running);
    }
}
