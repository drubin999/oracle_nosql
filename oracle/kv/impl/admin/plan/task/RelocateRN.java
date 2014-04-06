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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.NonfatalAssertionException;
import oracle.kv.impl.admin.PlannerAdmin;
import oracle.kv.impl.admin.TopologyCheck;
import oracle.kv.impl.admin.TopologyCheck.REMEDY_TYPE;
import oracle.kv.impl.admin.TopologyCheck.Remedy;
import oracle.kv.impl.admin.TopologyCheckUtils;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.admin.plan.PortTracker;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.VersionUtil;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.persist.model.Persistent;

/**
 * Move a single RN to a new storage node.
 * 1. stop/disable RN
 * 2. change params and topo
 * 3. update the other members of the rep group.
 * 4. broadcast the topo changes
 * 5. turn off the disable bit and tell the new SN to deploy the RN
 * 6. wait for the new RN to come up and become consistent with the shard
 *    master, then delete the log files of the old RN.
 */
@Persistent
public class RelocateRN extends SingleJobTask {
    private static final long serialVersionUID = 1L;

    /* Delay between calls to the awaitConsistency after an error */
    private static final int WAIT_FOR_CONSISTENCY_DELAY_MS = 1000 * 60;

    /* Wait a minute between consistency checks */
    private static final int WAIT_FOR_CONSISTENCY = 60;

    private RepNodeId rnId;
    private StorageNodeId oldSN;
    private StorageNodeId newSN;
    private String newMountPoint;

    private AbstractPlan plan;

    /* Hook to inject failures at different points in task execution */
    public static TestHook<Integer> FAULT_HOOK;

    public RelocateRN(AbstractPlan plan,
                      StorageNodeId oldSN,
                      StorageNodeId newSN,
                      RepNodeId rnId,
                      String newMountPoint) {

        super();
        this.oldSN = oldSN;
        this.newSN = newSN;
        this.plan = plan;
        this.rnId = rnId;
        this.newMountPoint = newMountPoint;

        /*
         * This task does not support moving an RN within the same SN.
         * Additional checks would be needed to make sure the directories
         * are different. Also more safeguards should be added when deleting
         * the old RN.
         */
        if (oldSN.equals(newSN)) {
            throw new NonfatalAssertionException("The RelocateRN task does " +
                                                 "not support relocating to " +
                                                 "the same Storage Node");
        }
    }

    /*
     * No-arg ctor for use by DPL.
     */
    @SuppressWarnings("unused")
    private RelocateRN() {
    }

    /**
     * Use the RNLocationCheck and the current state of the JE HA repGroupDB to
     * repair any inconsistencies between the AdminDB, the SNA config files,
     * and the JE HA repGroupDB.
     * @throws NotBoundException
     * @throws RemoteException
     */
    private boolean checkAndRepairLocation()
        throws RemoteException, NotBoundException {

        Admin admin = (Admin)plan.getAdmin();
        Logger logger = plan.getLogger();
        TopologyCheck checker =
            new TopologyCheck(logger,
                              admin.getCurrentTopology(),
                              admin.getCurrentParameters());

        /* ApplyRemedy will throw an exception if there is a problem */
        Remedy remedy = checker.checkRNLocation(admin, newSN, rnId,
                                                false /* calledByDeployRN */,
                                                true /* mustReenableRN */);
        if (remedy.getType() != REMEDY_TYPE.OKAY) {
            logger.info("RelocateRN check of newSN: " + remedy);
        }

        boolean newDone = checker.applyRemedy(remedy, plan,
                                              plan.getDeployedInfo(), oldSN,
                                              newMountPoint);

        remedy = checker.checkRNLocation(admin, oldSN, rnId,
                                         false /* calledByDeployRN */,
                                         true /* mustReenableRN */);
        if (remedy.getType() != REMEDY_TYPE.OKAY) {
            logger.info("RelocateRN check of oldSN: " + remedy);
        }

        boolean oldDone = checker.applyRemedy(remedy, plan,
                                              plan.getDeployedInfo(), oldSN,
                                              newMountPoint);
        return newDone && oldDone;
    }

    @Override
    public State doWork()
        throws Exception {

        final PlannerAdmin admin = plan.getAdmin();
        final Logger logger = plan.getLogger();
        long stopRNTime;

        /*
         * Prevent the inadvertent downgrade of a RN version by checking
         * that the destination SN is a version that is >= source SN.
         */
        checkVersions();

        /*
         * Before doing any work, make sure that the topology, params,
         * SN config files, and JE HA rep group are consistent. This is
         * most definitive if the JE HA repGroupDB can be read, which
         * is only possible if there is a master of the group. The correct
         * location can be deduced in some other limited cases too.
         */
        boolean done = checkAndRepairLocation();

        /* Check the topology after any fixes */
        final Topology current = admin.getCurrentTopology();
        RepNode rn = current.get(rnId);

        if (done && rn.getStorageNodeId().equals(newSN)) {
            /*
             * The check has been done, any small repairs needed were done, all
             * is consistent, and the RN is already living on the new
             * SN. Nothing more to be done with the topology and params.
             */
            logger.info(rnId + " is already on " + newSN +
                        ", no additional metadata changes needed." );
            stopRNTime = System.currentTimeMillis();
        } else {

            /*
             * There's work to do to update the topology, params, and JE HA
             * repGroupDB. Make sure both old and new SNs are up.
             */
	    final LoginManager loginMgr = admin.getLoginManager();
            Utils.confirmSNStatus(current,
				  loginMgr,
                                  oldSN,
                                  true,
                                  "Please ensure that " + oldSN +
                                  " is deployed and running before " +
                                  "attempting a relocate " + rnId + ".");
            Utils.confirmSNStatus(current,
				  loginMgr,
                                  newSN,
                                  true,
                                  "Please ensure that " + newSN +
                                  " is deployed and running before " +
                                  "attempting a relocate " + rnId + ".");

            /*
             * Relocation requires bringing down one RN in the shard. Before any
             * work is done, check if the shard is going to be resilient enough.
             * Does it currently have a master? If the target node is brought
             * down, will the shard have a quorum? If at all possible, avoid
             * changing admin metadata if it is very likely that the shard can't
             * update its own JE HA group membership.
             */
            final RepGroupId rgId = current.get(rnId).getRepGroupId();
            Utils.verifyShardHealth(admin.getCurrentParameters(),
                                    current, rnId, oldSN, newSN,
                                    plan.getLogger());

            assert TestHookExecute.doHookIfSet(FAULT_HOOK, 1);

            /* Step 1. Stop and disable the RN. */
            Utils.stopRN(plan, oldSN, rnId);

            /*
             * Assert that the RN's disable bit is set, because the task cleanup
             * implementation uses that as an indication that step 5 executed.
             */
            RepNodeParams rnp = admin.getRepNodeParams(rnId);
            if (!rnp.isDisabled()) {
                throw new IllegalStateException
                    ("Expected disabled bit to be set "+
                     "for " + rnId  +  ": " + rnp);
            }
            stopRNTime = System.currentTimeMillis();

            assert TestHookExecute.doHookIfSet(FAULT_HOOK, 2);

            /* Step 2. Change params and topo, as one transaction. */
            changeParamsAndTopo(oldSN, newSN, rgId);

            assert TestHookExecute.doHookIfSet(FAULT_HOOK, 3);

            /*
             * Step 3. Tell the HA group about the new location of this
             * node. This requires a quorum to update the HA group db, and may
             * take some retrying, as step 1 might have actually shut down the
             * master of the HA group.
             */
            Utils.changeHAAddress(admin.getCurrentTopology(),
                                  admin.getCurrentParameters(),
                                  admin.getParams().getAdminParams(),
                                  rnId, oldSN, newSN, plan, logger);

            assert TestHookExecute.doHookIfSet(FAULT_HOOK, 4);

            /*
             * Step 4. Send topology change to all nodes, send param changes
             * with updated helper hosts to RN peers
             */
            Topology topo = admin.getCurrentTopology();
            if (!Utils.broadcastTopoChangesToRNs
                (logger, topo,
                 "relocate " + rnId + " from " + oldSN + " to " + newSN,
                 admin.getParams().getAdminParams(), plan)) {

                /*
                 * The plan is interrupted before enough nodes saw the new
                 * topology.
                 */
                return State.INTERRUPTED;
            }

            /* Send the updated params to the RN's peers */
            Utils.refreshParamsOnPeers(plan, rnId);

            assert TestHookExecute.doHookIfSet(FAULT_HOOK, 5);

            /*
             * Step 5. Remove the disable flag for this RN, and deploy the RN on
             * the new SN.
             */
            startRN(plan, newSN, rnId);

            assert TestHookExecute.doHookIfSet(FAULT_HOOK, 6);
        }

        /*
         * Step 6: Destroy the old RN. Make sure the new RN is up and is current
         * with its master. The RNLocationCheck repair does not do this step,
         * so check if it's needed at this time.
         */
        return destroyRepNode(stopRNTime);
    }

    /**
     * Complain if the new SN is at an older version than the old SN.
     */
    private void checkVersions() {
        final PlannerAdmin admin = plan.getAdmin();
        final RegistryUtils regUtils =
            new RegistryUtils(admin.getCurrentTopology(),
                              admin.getLoginManager());

        String errorMsg =  " cannot be contacted. Please ensure that it " +
            "is deployed and running before attempting to deploy " +
            "this topology";

        KVVersion oldVersion = null;
        KVVersion newVersion = null;
        try {
            StorageNodeAgentAPI oldSNA = regUtils.getStorageNodeAgent(oldSN);
            oldVersion = oldSNA.ping().getKVVersion();
        } catch (RemoteException e) {
            throw new OperationFaultException(oldSN + errorMsg);
        } catch (NotBoundException e) {
            throw new OperationFaultException(oldSN + errorMsg);
        }

        try {
            StorageNodeAgentAPI newSNA = regUtils.getStorageNodeAgent(newSN);
            newVersion = newSNA.ping().getKVVersion();
        } catch (RemoteException e) {
            throw new OperationFaultException(newSN + errorMsg);
        } catch (NotBoundException e) {
            throw new OperationFaultException(newSN + errorMsg);
        }

        if (VersionUtil.compareMinorVersion(oldVersion, newVersion) > 0) {
            throw new OperationFaultException
                (rnId + " cannot be moved from " +  oldSN + " to " + newSN +
                 " because " + oldSN + " is at version " + oldVersion +
                 " and " + newSN + " is at older version " + newVersion +
                 ". Please upgrade " + newSN +
                 " to a version that is equal or greater than " + oldVersion);
        }
    }

    /**
     * Deletes the old RN on the original SN. Returns SUCCESS if the delete was
     * successful. This method calls awaitCOnsistency() on the new node
     * to make sure it is up and healthy before deleting the old node.
     *
     * @return SUCCESS if the old RN was deleted
     */
    private State destroyRepNode(long stopRNTime) {
        try {
            if (destroyRepNode(plan, stopRNTime, oldSN, rnId)) {
                return State.SUCCEEDED;
            }
        } catch (InterruptedException ie) {
            return State.INTERRUPTED;
        }

        throw new RuntimeException("Time out while waiting for " + rnId +
                                   " to come up on " + newSN +
                                   " and become consistent with" +
                                   " the master of the shard before deleting" +
                                   " the RepNode from its old home on " +
                                   oldSN);
    }

    /**
     * Deletes the old RN on the original SN. Returns SUCCESS if the delete was
     * successful. This method calls awaitConsistency() on the new node
     * to make sure it is up and healthy before deleting the old node.
     *
     * @return SUCCESS if the old RN was deleted
     * @throws InterruptedException
     */
    public static boolean destroyRepNode(AbstractPlan plan,
                                         long stopRNTime,
                                         StorageNodeId targetSNId,
                                         RepNodeId targetRNId)
        throws InterruptedException {

        final PlannerAdmin admin = plan.getAdmin();
        final Logger logger = plan.getLogger();

        long endCheckAtThisTime = System.currentTimeMillis() +
            admin.getParams().getAdminParams().getAwaitRNConsistencyPeriod();

        Topology useTopo = admin.getCurrentTopology();
        RegistryUtils registry = new RegistryUtils(useTopo,
                                                   admin.getLoginManager());
        do {
            logger.log(Level.INFO,
                       "Waiting for {0} to become " +
                       "consistent before removing it from {1}. Topology " +
                       "says it is on {2}",
                       new Object[]{targetRNId, targetSNId,
                                    useTopo.get(targetRNId).getStorageNodeId()});

            try {
                final RepNodeAdminAPI rnAdmin =
                    registry.getRepNodeAdmin(targetRNId);

                if (rnAdmin.awaitConsistency(stopRNTime, WAIT_FOR_CONSISTENCY,
                                             TimeUnit.SECONDS)) {

                    logger.log(Level.INFO,
                               "Attempting to delete {0} from {1}",
                               new Object[]{targetRNId, targetSNId});

                    StorageNodeAgentAPI oldSna =
                        registry.getStorageNodeAgent(targetSNId);
                    oldSna.destroyRepNode(targetRNId, true /* deleteData */);
                    return true;
                }
            } catch (RemoteException re) {

                /*
                 * Since we have gotten this far, we should do our best to
                 * finish. The call to awaitConsistency may fail due to various
                 * network issues or the RN not yet started or is very busy
                 * starting up. This last case can happen if
                 * WAIT_FOR_CONSISTENCY_MS > the socket timeout.
                 */
                logger.log(Level.INFO,
                           "Remote call to {0} failed with {1}",
                            new Object[]{targetRNId, re.getLocalizedMessage()});

                /*
                 * If we have not timed-out, sleep for a short bit to avoid
                 * spinning on a network error.
                 */
                if (endCheckAtThisTime > System.currentTimeMillis()) {
                    Thread.sleep(WAIT_FOR_CONSISTENCY_DELAY_MS);
                }
            } catch (NotBoundException nbe) {
                logger.log(Level.INFO,
                           "Registry call failed with {0}",
                           nbe.getLocalizedMessage());

                if (endCheckAtThisTime > System.currentTimeMillis()) {
                    Thread.sleep(WAIT_FOR_CONSISTENCY_DELAY_MS);
                }

                /* Reacquire the registry */
                registry = new RegistryUtils(admin.getCurrentTopology(),
                                             admin.getLoginManager());
            }
        } while (endCheckAtThisTime > System.currentTimeMillis());
        return false;
    }


    /**
     * Start the RN, update its params.
     * @throws RemoteException
     * @throws NotBoundException
     */
    static public void startRN(AbstractPlan plan,
                               StorageNodeId targetSNId,
                               RepNodeId targetRNId)
        throws RemoteException, NotBoundException {

        PlannerAdmin admin = plan.getAdmin();

        /*
         * Update the SN after any AdminDB param changes are done. Refetch
         * the params and topo because they might have been updated.
         */
        Topology topo = admin.getCurrentTopology();
        RepNodeParams rnp = new RepNodeParams(admin.getRepNodeParams(targetRNId));
        if (rnp.isDisabled()) {
            rnp.setDisabled(false);
            admin.updateParams(rnp);
        }
        plan.getLogger().log(Level.INFO,
                             "Starting up {0} on {1} with  {2}",
                             new Object[]{targetRNId, targetSNId, rnp});

        RegistryUtils regUtils = new RegistryUtils(topo,
                                                   admin.getLoginManager());
        StorageNodeAgentAPI sna =  regUtils.getStorageNodeAgent(targetSNId);

        /* Start or create the RN */
        sna.createRepNode(rnp.getMap(), Utils.getMetadataSet(topo, plan));

        /*
         * The start or create will be a no-op if the RN was already up, so
         * explicitly ask the RN to absorb new params.
         */
        sna.newRepNodeParameters(rnp.getMap());

        /* Register this repNode with the monitor. */
        StorageNode sn = topo.get(targetSNId);
        admin.getMonitor().registerAgent(sn.getHostname(),
                                         sn.getRegistryPort(),
                                         targetRNId);
    }

    /**
     * Update and persist the params and topo to make the RN refer to the new
     * SN. Check to see if this has already occurred, to make the work
     * idempotent.
     */
    private void changeParamsAndTopo(StorageNodeId before,
                                     StorageNodeId after,
                                     RepGroupId rgId) {

        Parameters parameters = plan.getAdmin().getCurrentParameters();
        Topology topo = plan.getAdmin().getCurrentTopology();
        PortTracker portTracker = new PortTracker(topo, parameters, after);

        /* Modify pertinent params and topo */
        StorageNodeId origParamsSN = parameters.get(rnId).getStorageNodeId();
        StorageNodeId origTopoSN = topo.get(rnId).getStorageNodeId();
        Set<RepNodeParams> changedRNParams = transferRNParams
            (parameters, portTracker, topo, before, after, rgId);
        boolean topoChanged = transferTopo(topo, before, after);

        /*
         * Sanity check that params and topo are in sync, both should be
         * either unchanged or changed
         */
        if ((changedRNParams.isEmpty() && topoChanged) ||
            (!changedRNParams.isEmpty() && !topoChanged)) {
            throw new IllegalStateException
                (rnId + " params and topo out of sync. Original params SN=" +
                 origParamsSN + ", orignal topo SN=" + origTopoSN +
                 " source SN=" + before + " destination SN=" + after);
        }

        /* Only do the update if there has been a change */
        Logger logger = plan.getLogger();
        if (!(topoChanged && !changedRNParams.isEmpty())) {
            logger.log(Level.INFO,
                       "No change to params or topology, no need to update " +
                       "in order to move {0} from {1} to {2}",
                       new Object[]{rnId, before, after});
            return;
        }

        plan.getAdmin().saveTopoAndParams(topo,
                                          plan.getDeployedInfo(),
                                          changedRNParams,
                                          Collections.<AdminParams>emptySet(),
                                          plan);
        logger.log(Level.INFO,
                   "Updating params and topo for move of {0} from " +
                   "{1} to {2}: {3}",
                   new Object[]{rnId, before, after, changedRNParams});
    }

    /**
     * The params fields that have to be updated are:
     * For the RN that is to be moved:
     *   a. new JE HA nodehostport value
     *   b. new mount point
     *   c. new storage node id
     *   d. calculate JE cache size, which may change due to the capacity
     *      and memory values of the destination storage node.
     * For the other RNs in this shard:
     *   a. new helper host values, that point to this new location for our
     *      relocated RN
     */
    private Set<RepNodeParams> transferRNParams(Parameters parameters,
                                                PortTracker portTracker,
                                                Topology topo,
                                                StorageNodeId before,
                                                StorageNodeId after,
                                                RepGroupId rgId) {

        Set<RepNodeParams> changed = new HashSet<RepNodeParams>();

        RepNodeParams rnp = parameters.get(rnId);
        ParameterMap policyMap = parameters.copyPolicies();

        if (rnp.getStorageNodeId().equals(after)) {

            /*
             * We're done, this task ran previously. Note that this does not
             * notice if a RN is on the same SN, but its mount point has
             * changed. In R2, we deliberately do not yet support automatic
             * movement of RNs across mount points on the same SN; it's left
             * to the user to do manually.
             */
            plan.getLogger().info(rnId + " already transferred to " + after);
            return changed;
        }

        /*
         * Sanity check -- this RNP should be pointing to the before SN, not
         * to some third party SN!
         */
        if (!rnp.getStorageNodeId().equals(before)) {
            throw new OperationFaultException
                ("Attempted to transfer " + rnId + " from " + before + " to " +
                 after + " but unexpectedly found it residing on " +
                 rnp.getStorageNodeId());
        }

        /*
         * Change the SN, helper hosts, nodeHostPort, and mount point for this
         * RN
         */
        int haPort = portTracker.getNextPort(after);

        String newSNHAHostname = parameters.get(after).getHAHostname();
        String oldNodeHostPort = rnp.getJENodeHostPort();
        String nodeHostPort = newSNHAHostname + ":" + haPort;
        plan.getLogger().log(Level.INFO,
                             "transferring HA port for {0} from {1} to {2}",
                             new Object[]{rnp.getRepNodeId(), oldNodeHostPort,
                                          nodeHostPort});

        rnp.setStorageNodeId(after);
        rnp.setJENodeHostPort(nodeHostPort);
        rnp.setMountPoint(newMountPoint);

        /*
         * Setting the helper hosts is not strictly necessary, as it should
         * not have changed, but take this opportunity to update the helper
         * list in case a previous param change had been interrupted.
         */
        rnp.setJEHelperHosts(
            TopologyCheckUtils.findPeerRNHelpers(rnId, parameters, topo));

        /*
         * Update the RN heap, JE cache size, and parallelGCThreads params,
         * which are a function of the characteristics  of the hosting storage
         * node
         */
        StorageNodeParams snp = parameters.get(after);
        Utils.setRNPHeapCacheGC(policyMap, snp, rnp, topo);
        changed.add(rnp);

        /* Change the helper hosts for other RNs in the group. */
        for (RepNode peer : topo.get(rgId).getRepNodes()) {
            RepNodeId peerId = peer.getResourceId();
            if (peerId.equals(rnId)) {
                continue;
            }

            RepNodeParams peerParam = parameters.get(peerId);
            String oldHelper = peerParam.getJEHelperHosts();
            String newHelpers = oldHelper.replace(oldNodeHostPort,
                                                  nodeHostPort);
            peerParam.setJEHelperHosts(newHelpers);
            changed.add(peerParam);
        }
        return changed;
    }

    /**
     * Find all RepNodes that refer to the old node, and update the topology to
     * refer to the new node.
     * @return true if a change has been made, return false if the RN is already
     * on the new SN.
     */
    private boolean transferTopo(Topology topo, StorageNodeId before,
                                 StorageNodeId after) {

        RepNode rn = topo.get(rnId);
        StorageNodeId inUseSNId = rn.getStorageNodeId();
        if (inUseSNId.equals(before)) {
            RepNode updatedRN = new RepNode(after);
            RepGroup rg = topo.get(rn.getRepGroupId());
            rg.update(rn.getResourceId(),updatedRN);
            return true;
        }

        if (inUseSNId.equals(after)) {
            return false;
        }

        throw new IllegalStateException(rn + " expected to be on old SN " +
                                        before + " or new SN " + after +
                                        " but instead is on " + inUseSNId);
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    public Runnable getCleanupJob() {
        return new Runnable() {
           @Override
               public void run() {
               try {
                   cleanupRelocation();
               } catch (Exception e) {
                   plan.getLogger().log
                       (Level.SEVERE,
                        "{0}: problem when cancelling relocation {1}",
                        new Object[] {this, LoggerUtils.getStackTrace(e)});

                   /*
                    * Don't try to continue with cleanup; a problem has
                    * occurred. Future, additional invocations of the plan
                    * will have to figure out the context and do cleanup.
                    */
                   throw new RuntimeException(e);
               }
           }
        };
    }

    /**
     * Do the minimum cleanup : when this task ends, check
     *  - the kvstore metadata as known by the admin (params, topo)
     *  - the configuration information, including helper hosts, as stored in
     *  the SN config file
     *  - the JE HA groupdb
     * and attempt to leave it all consistent. Do not necessarily try to revert
     * to the topology before the task.
     * @throws NotBoundException
     * @throws RemoteException
     */
    private void cleanupRelocation()
        throws RemoteException, NotBoundException {

        assert TestHookExecute.doHookIfSet(FAULT_HOOK, 7);

        boolean done = checkAndRepairLocation();
        final Topology current = plan.getAdmin().getCurrentTopology();
        RepNode rn = current.get(rnId);

        if (done) {
            if (rn.getStorageNodeId().equals(newSN)) {
                plan.getLogger().info("In RelocateRN cleanup, shard is " +
                                      " consistent, " + rnId +
                                      " is on the target " + newSN);

                /* attempt to delete the old RN */
                destroyRepNode(System.currentTimeMillis());
            }
            plan.getLogger().info("In RelocateRN cleanup, shard is " +
                                  "consistent, " + rnId + " is on " +
                                  rn.getStorageNodeId());
        } else {
            plan.getLogger().info("In RelocateRN cleanup, shard did not have " +
                                  "master, no cleanup attempted since " +
                                  "authoritative information is lacking.");
        }
    }

    /**
     * This is the older style cleanup, which attempts to reason about how far
     * the task proceeded, and then attempts to revert to the previous state.
     */
    @SuppressWarnings("unused")
    private boolean checkLocationConsistency()
        throws InterruptedException, RemoteException, NotBoundException {

        PlannerAdmin admin = plan.getAdmin();
        assert TestHookExecute.doHookIfSet(FAULT_HOOK, 7);

        /*
         * If step 5 occurred (enable bit on, RN pointing to new SN, then the HA
         * group and the params/topo are consistent, so attempt to delete the
         * old RN.
         */
        RepNodeParams rnp = admin.getRepNodeParams(rnId);
        if ((rnp.getStorageNodeId().equals(newSN)) &&
            !rnp.isDisabled()) {
            return
                destroyRepNode(System.currentTimeMillis()) == State.SUCCEEDED;
        }

        /*
         * If the RepNodeParams still point at the old SN, steps 2 and 3 did
         * not occur, nothing to clean up
         */
        if (rnp.getStorageNodeId().equals(oldSN)) {
            /*
             * If the original RN was disabled, attempt to re-enable it. Note
             * that this may enable a node which was disabled before the plan
             * run.
             */
            if (rnp.isDisabled()) {
                Utils.startRN(plan, oldSN, rnId);
            }
            return true;
        }

        /*
         * We are somewhere between steps 1 and 5. Revert both of the kvstore
         * params and topo, and the JE HA update, and the peer RNs helper
         * hosts.
         */
        Topology topo = admin.getCurrentTopology();
        changeParamsAndTopo(newSN, oldSN, topo.get(rnId).getRepGroupId());
        Utils.refreshParamsOnPeers(plan, rnId);

        Utils.changeHAAddress(topo,
                              admin.getCurrentParameters(),
                              admin.getParams().getAdminParams(),
                              rnId, newSN, oldSN, plan, plan.getLogger());

        /* refresh the topo, it's been updated */
        topo = admin.getCurrentTopology();
        if (!Utils.broadcastTopoChangesToRNs(plan.getLogger(),
                                             topo,
                                            "revert relocation of  " + rnId +
                                             " and move back from " +
                                             newSN + " to " + oldSN,
                                             admin.getParams().getAdminParams(),
                                             plan)) {
            /*
             * The plan is interrupted before enough nodes saw the new
             * topology.
             */
            return false;
        }

        return true;
    }

    @Override
    public String toString() {
        StorageNodeParams snpOld =
            (plan.getAdmin() != null ?
             plan.getAdmin().getStorageNodeParams(oldSN) : null);
        StorageNodeParams snpNew =
            (plan.getAdmin() != null ?
             plan.getAdmin().getStorageNodeParams(newSN) : null);
        return super.toString() + " move " + rnId + " from " +
            (snpOld != null ? snpOld.displaySNIdAndHost() : oldSN) +
            " to " +
            (snpNew != null ? snpNew.displaySNIdAndHost() : newSN);
    }

    @Override
    public void lockTopoComponents(Planner planner) {
        planner.lockShard(plan.getId(), plan.getName(),
                          new RepGroupId(rnId.getGroupId()));
    }
}
