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

package oracle.kv.impl.rep.masterBalance;

import java.util.Collections;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.admin.RepNodeAdmin;
import oracle.kv.impl.topo.RepNodeId;

import com.sleepycat.je.rep.MasterReplicaTransitionException;
import com.sleepycat.je.rep.MasterTransferFailureException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.StateChangeEvent;


/**
 * MasterBalanceManager is RN side of master balancing support. It serves two
 * primary purposes:
 *
 * 1) It keeps the SN informed of RN HA state changes, so that the SNA has
 * an accurate picture of the master RNs that it is managing. This is done via
 * the MasterBalanceStateTracker.
 *
 * 2) Implements request for master transfers that result from the state
 * changes sent to the SNA in step 1.
 */
@SuppressWarnings("deprecation")
public class MasterBalanceManager implements MasterBalanceManagerInterface {

    /**
     *  The owning RepNode. The MBM is a component of the RepNode.
     */
    private final RepNode repNode;

    /**
     * A atomic reference to the thread currently performing a master transfer
     * if one is in progress.
     */
    private final AtomicReference<MasterTransferThread> activeTransfer =
        new AtomicReference<MasterTransferThread>(null);

    /**
     * Tracks state changes and communicates them asynchronously to the SNA
     */
    private final MasterBalanceStateTracker stateTracker;

    private final Logger logger;

    MasterBalanceManager(RepNode repNode, Logger logger) {
        this.repNode = repNode;
        this.logger = logger;

        stateTracker = new MasterBalanceStateTracker(repNode, logger);
    }


    /**
     * Factory method to create appropriate MBM based upon configuration
     */
    public static MasterBalanceManagerInterface create(RepNode repNode,
                                                       Logger logger) {
        return repNode.getRepNodeParams().getMasterBalance() ?
            new MasterBalanceManager(repNode, logger) :
            new MasterBalanceManagerDisabled(logger);
    }

    @Override
    public MasterBalanceStateTracker getStateTracker() {
        return stateTracker;
    }

    /**
     * Starts the state tracker thread
     */
    @Override
    public void startTracker() {
        stateTracker.start();
    }

    @Override
    public void initialize() {
       /* Nothing to do. Just a placeholder for now. */
    }

    @Override
    public void noteStateChange(StateChangeEvent stateChangeEvent) {
       stateTracker.noteStateChange(stateChangeEvent);
    }

    @Override
    public void shutdown() {
        stateTracker.shutdown();
    }

    /**
     * The method initiating a master transfer request.
     *
     * @see RepNodeAdmin#initiateMasterTransfer
     */
    @Override
    public boolean initiateMasterTransfer(RepNodeId replicaId,
                                          int timeout,
                                          TimeUnit timeUnit) {

        final ReplicatedEnvironment env = repNode.getEnv(0);

        if ((env == null) || !env.getState().isMaster()) {
            /* Not the current master, decline the request. */
            return false;
        }

        MasterTransferThread mtThread =
            new MasterTransferThread(env, replicaId, timeout, timeUnit);

        if (activeTransfer.compareAndSet(null, mtThread)) {
            /* Initiate the transfer. */
            mtThread.start();
            return true;
        }

        /* Transfer already in progress. */
        mtThread = activeTransfer.get();
        if (mtThread != null) {
            logger.info("Declined request: Master transfer initiated at: " +
                        new Date(mtThread.startTimeMs).toString() +
                        " with target replica:" + mtThread.replicaId +
                        " is already in progress. ");

            return false;
        }

        /*
         * Retry the call, since a MT just completed between the CAS() and
         * get() operations; should be extremely rare.
         */
        return initiateMasterTransfer(replicaId, timeout, timeUnit);
    }


    /**
     * A dedicated thread to complete a master transfer. The transfer is done
     * asynchronously via this thread to ensure that the RMI request for the
     * transfer is not blocked.
     * <p>
     * Upon completion (successful or otherwise) the thread exits and logs
     * the outcome.
     *
     * As successful transfer results in a state change to replica and this
     * RN MBM informs the local SNA MBM of the transition via
     * {@link #noteStateChange}. The remote SNA is similarly informed of the
     * related transition to Master by the remote RN.
     *
     * An unsuccessful transfer typically results in a master lease expiry at
     * the local SN MBM, which can retry a master transfer with a potentially
     * different RN pair until it's successful.
     */
    private class MasterTransferThread extends Thread {

        private final long startTimeMs = System.currentTimeMillis();
        private final ReplicatedEnvironment env;
        private final RepNodeId replicaId;
        private final int timeout;
        private final TimeUnit timeUnit;

        MasterTransferThread(ReplicatedEnvironment env,
                             RepNodeId replicaId,
                             int timeout,
                             TimeUnit timeUnit) {
            super("Master Transfer Thread. Target:" + replicaId.toString());
            this.env = env;
            this.replicaId = replicaId;
            this.timeout = timeout;
            this.timeUnit = timeUnit;
        }

        @Override
        public void run() {
            try {
                final Set<String> targetRN =
                    Collections.singleton(replicaId.getFullName());
                env.transferMaster(targetRN, timeout, timeUnit);
            } catch (MasterTransferFailureException mtfe) {
                logger.log(Level.INFO, "Requested transfer to " +
                           replicaId + " failed.", mtfe);
                return;
            } catch (IllegalStateException ise) {
                /* Node is no longer the master. */
                logger.log(Level.INFO, "Node is no longer the master", ise);
                return;
            } catch (@SuppressWarnings("deprecation") MasterReplicaTransitionException e) {
                /* 
                 * As of JE 5.0.92.KV3 (NoSQL R2.1.24), 
                 * MasterReplicatTransitionException is deprecated and no 
                 * longer thrown. However, keep this catch to maintain backward 
                 * compatibility with older JE versions, for a while.
                 */
                return;
            } catch (Exception e) {
                logger.log(Level.WARNING,
                           "Unexpected master transfer failure", e);
            } finally {
                activeTransfer.set(null);
                /* Deliberately a soft failure. The RN can survive it. */
                logger.log(Level.INFO, "Master transfer thread exited");
            }
        }
    }
}
