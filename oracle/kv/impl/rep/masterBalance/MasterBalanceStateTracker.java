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

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.StateTracker;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.sna.masterBalance.MasterBalanceManager;
import oracle.kv.impl.sna.masterBalance.MasterBalancingInterface;
import oracle.kv.impl.sna.masterBalance.MasterBalancingInterface.StateInfo;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.je.rep.StateChangeEvent;

/**
 * MasterBalanceStateTracker, is the MBM component, that keeps the SNA informed
 * of state changes so that the SNA can initiate master transfer operations
 * when necessary.
 *
 * @see MasterBalanceManager
 */
public class MasterBalanceStateTracker extends StateTracker {

    private static final String THREAD_NAME =
        MasterBalanceStateTracker.class.getSimpleName();

    /* RMI handle to sna. */
    private volatile StorageNodeAgentAPI sna;

    /**
     * The amount of time to wait between retries of failed RMI attempt to
     * contact the SNA.
     */
    static private final int RMI_RETRY_PERIOD_MS = 1000;

    /**
     * Creates the Manager. Note that the RN must be able to contact the SN so
     * that it can establish an RMI handle to the SNA. The RN is managed by the
     * SNA, and it's on the same machine as the SNA, so not being able to
     * contact it would imply there was some serious configuration issue
     *
     * @param rn the RN whose state is to be tracked
     * @param logger the logger to be used
     */
    public MasterBalanceStateTracker(RepNode rn,  Logger logger) {
        super(THREAD_NAME, rn, logger);
    }

    @Override
    protected void doNotify(StateChangeEvent sce) throws InterruptedException {
  
        if (!ensureTopology()) {
            /* Node has been shutdown. */
            return;
        }
        
        if (sna == null) {
            sna = getSNAHandle();
        }
        
        /* If still null, node has been shutdown. */
        if (sna == null) {
            return;
        }

        final int seqNum = getTopoSeqNum();
        final StateInfo stateInfo = new MasterBalancingInterface.
            StateInfo(rn.getRepNodeId(), sce.getState(), seqNum);

        /*
         * Note that we want to do our best to get the queue flushed on a
         * shutdown. This is why the while condition below does not test
         * for shutdown.
         */
        while (true) {
            /* Repeat until the SNA is informed */
            try {
                sna.noteState(stateInfo);
                /* Skip as above. */
                break;
            } catch (RemoteException e) {

                if (shutdown.get()) {
                    /*
                     * If the call is interrupted as part of the
                     * StoppableThread shutdown, it would result in a
                     * RemoteException.
                     */
                    return;
                }
                if (!isEmpty()) {
                    break;
                }

                Thread.sleep(RMI_RETRY_PERIOD_MS);

                if (!isEmpty()) {
                    /* Skip as above. */
                    break;
                }
            }
        }
    }

    /**
     * Ensures that a topology is available. Simply poll until the RN has one
     * available. A Topology may be missing during initialization before the
     * SNA or one of the RNs has had an opportunity to supply this RN with one.
     * This should be a transient state.
     *
     * This method has protected access to allow the unit test to override.
     * 
     * @return true if the topology was established or false if the node was
     * shutdown before it could be established.
     */
    protected boolean ensureTopology()
        throws InterruptedException {

        while (rn.getTopology() == null) {
            if (shutdown.get()) {
                return false;
            }

            Thread.sleep(1000);
        }
        return true;
    }
    
    /**
     * Returns an SNA handle, or null, if the state tracker was shut down.
     */
    private StorageNodeAgentAPI getSNAHandle() throws InterruptedException {
        final String storeName = rn.getGlobalParams().getKVStoreName();
        final StorageNodeParams snp = rn.getStorageNodeParams();

        /* Establish the SNA handle. */
        for (int retryCount = 0; retryCount < Integer.MAX_VALUE; retryCount++) {
            Exception retryException = null;

            if (shutdown.get()) {
                return null;
            }

            /*
             * The SNA must make an appearance eventually. Unless it's a unit
             * test, in which case it won't and it does not matter.
             */
            try {
                return RegistryUtils.getStorageNodeAgent(
                    storeName,
                    snp,
                    snp.getStorageNodeId(),
                    rn.getLoginManager());
            } catch (RemoteException e) {
                retryException = e;
            } catch (NotBoundException e) {
                retryException = e;
            }

            if ((retryCount % 10) == 0) {
                logger.info("Retrying to obtain SNA handle:" +
                            retryException.getMessage());
            }

            Thread.sleep(RMI_RETRY_PERIOD_MS);
        }
        throw new IllegalStateException("Unreachable code");
    }

    /**
     * It can be overridden when used in unit tests with mock RNs
     */
    protected int getTopoSeqNum() {
        return rn.getTopology().getSequenceNumber();
    }

    @Override
    public int initiateSoftShutdown() {
        assert shutdown.get();

        if (sna == null) {
            /*
             * The state can arise in when an RN is started and immediately
             * stopped and the run method has not had a chance to initialize
             * the sna iv. return 1 ms to effectively interrupt the thread,
             * causing it to exit.
             */
            return 1;
        }
        return super.initiateSoftShutdown();
    }
}
