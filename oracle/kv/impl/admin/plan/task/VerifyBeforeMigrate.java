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
import java.util.HashSet;
import java.util.Set;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.TopologyCheck;
import oracle.kv.impl.admin.TopologyCheck.REMEDY_TYPE;
import oracle.kv.impl.admin.TopologyCheck.Remedy;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.VersionUtil;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.persist.model.Persistent;

/**
 * Do topology verification before a MigrateSNPlan. We hope to detect any
 * topology inconsistencies and prevent cascading topology errors.
 *
 * Also check the version of the new SN, to prevent the inadvertent downgrade
 * of a RN version. 
 */
@Persistent
public class VerifyBeforeMigrate extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private AbstractPlan plan;
    private StorageNodeId oldSN;
    private StorageNodeId newSN;

    /* For DPL */
    VerifyBeforeMigrate() {
    }

    public VerifyBeforeMigrate(AbstractPlan plan,
                               StorageNodeId oldSN,
                               StorageNodeId newSN) {
        this.plan = plan;
        this.oldSN = oldSN;
        this.newSN = newSN;
    }

    @Override
    public String getName() {
        return "VerifyBeforeMigrate";
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    /**
     * Check that the rep groups for all the RNs that are involved in the move
     * have quorum. If they don't, the migrate will fail.
     */
    @Override
    public State doWork() throws Exception {

        // TODO: refactoring out PlannerAdmin in the future will remove this
        // cast.
        Admin admin = (Admin) plan.getAdmin();
        Parameters params = admin.getCurrentParameters();
        Topology topo = admin.getCurrentTopology();

        /*
         * Check that the new SN is at a version compatible with that of the
         * Admin. Ideally we'd check the destination SN is a version that is
         * greater than or equal to the source SN, but the source SN is down
         * and unavailable. Instead, check that the destination SN is a version
         * that is >= the Admin version. If the Admin is at an older version
         * that the source SN, this check will be too lenient. If the Admin has
         * is at a newer version than the source SN, this will be too
         * stringent. Nevertheless, it's worthwhile making the attempt. If the
         * check is insufficient, the migrated RNs and Admins will not come up
         * on the new SN, so the user will see the issue, though at a less
         * convenient time.
         */
        final RegistryUtils regUtils =
            new RegistryUtils(topo, admin.getLoginManager());

        String errorMsg =  newSN + " cannot be contacted. Please ensure " +
            "that it is deployed and running before attempting to migrate " +
            "to this storage node: ";

        KVVersion newVersion = null;
        try {
            StorageNodeAgentAPI newSNA = regUtils.getStorageNodeAgent(newSN);
            newVersion = newSNA.ping().getKVVersion();

        } catch (RemoteException e) {
            throw new OperationFaultException(errorMsg + e);
        } catch (NotBoundException e) {
            throw new OperationFaultException(errorMsg + e);
        }

        if (VersionUtil.compareMinorVersion(KVVersion.CURRENT_VERSION,
                                            newVersion) > 0) {
            throw new OperationFaultException
                ("Cannot migrate " + oldSN + " to " + newSN +
                 " because " + newSN + " is at older version " + newVersion +
                 ". Please upgrade " + newSN +
                 " to a version that is equal or greater than " +
                 KVVersion.CURRENT_VERSION);
        }

        /*
         * Find all the RNs that are purported to live on either the old
         * SN or the newSN. We need to check both old and new because the
         * migration might be re-executed, and the changes might already
         * have been partially carried out.
         */
        Set<RepNodeId> hostedRNs = new HashSet<RepNodeId>();
        for (RepNodeParams rnp : params.getRepNodeParams()) {
            if (rnp.getStorageNodeId().equals(oldSN) ||
                rnp.getStorageNodeId().equals(newSN)) {
                hostedRNs.add(rnp.getRepNodeId());
            }
        }

        for (RepNodeId rnId : hostedRNs) {
            Utils.verifyShardHealth(params, topo, rnId, oldSN, newSN,
                                    plan.getLogger());
        }

        /*
         * Make sure that the RNs involved are consistent in their topology,
         * config.xml, and JE HA addresses. However, we can only check the
         * new SN, because the old one may very well be down. If the migrate
         * has never executed, this RN will still be on the old SN and there
         * won't be anything to check on the new SN, but we will verify the
         * JEHA location.
         */
        TopologyCheck checker = new TopologyCheck(plan.getLogger(), topo,
                                                  params);

        for (RepNodeId rnId : hostedRNs) {
            Remedy remedy =
                checker.checkRNLocation(admin, newSN, rnId,
                                        false, /* calledByDeployRN */
                                        true /* mustReenableRN */);
            if (remedy.getType() != REMEDY_TYPE.OKAY) {
                throw new OperationFaultException
                    (rnId + " has inconsistent location metadata. Please " +
                     "run plan repair-topology : " + remedy);
            }

            if (remedy.getJEHAInfo() != null) {
                StorageNodeId currentHost = remedy.getJEHAInfo().getSNId();
                if (!currentHost.equals(oldSN) &&
                    !currentHost.equals(newSN)) {
                    throw new OperationFaultException
                        (rnId + " has inconsistent location metadata. "+
                         " and is living on " + currentHost  +
                         " rather than " + oldSN + " or " + newSN +
                         "Please run plan repair-topology");
                }
            }
        }

        AdminId adminToCheck = null;
        for (AdminParams ap : params.getAdminParams()) {
            if (ap.getStorageNodeId().equals(oldSN) ||
                ap.getStorageNodeId().equals(newSN)) {
                adminToCheck = ap.getAdminId();
            }
        }

        if (adminToCheck != null) {
            Remedy remedy =
                    checker.checkAdminMove(admin, adminToCheck, oldSN, newSN);
            if (remedy.getType() != REMEDY_TYPE.OKAY) {
                throw new OperationFaultException
                    (adminToCheck + " has inconsistent location metadata. Please " +
                     "run plan repair-topology : " + remedy);
            }

            if (remedy.getJEHAInfo() != null) {
                StorageNodeId currentHost = remedy.getJEHAInfo().getSNId();
                if (!currentHost.equals(oldSN) &&
                    !currentHost.equals(newSN)) {
                    throw new OperationFaultException
                        (adminToCheck + " has inconsistent location metadata. "+
                         " and is living on " + currentHost  +
                         " rather than " + oldSN + " or " + newSN +
                         "Please run plan repair-topology");
                }
            }
        }

        return Task.State.SUCCEEDED;
    }
}
