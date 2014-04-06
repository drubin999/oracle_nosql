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

package oracle.kv.impl.sna.masterBalance;

import java.io.Serializable;
import java.rmi.RemoteException;

import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.sna.masterBalance.LeaseManager.Lease;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;

import com.sleepycat.je.rep.ReplicatedEnvironment.State;

/**
 *
 * Interfaces to support the master balancing when we have multiple RNs/SN. The
 * primary purpose of this interface is to track the state (Master or not
 * Master) of each of the RNs hosted on an SN. The SN uses this state
 * information to detect a master imbalance and coordinate the redistribution
 * of masters across SNs should it become necessary.
 * <p>
 * Note that while Topology components (e.g. RepNode, StorageNode, etc.) are
 * used in the interfaces, the components are free-standing, that is, they do
 * not have a Topology associated with them, so operations that require access
 * to the topology will fail.
 */
public interface MasterBalancingInterface {

    /**
     * Informs the SN of state changes to RNs hosted by it. This information is
     * used to track the state associated with the RNs hosted by the SN.
     * <p>
     * In addition to this interface, the SN also notices when an RN has failed
     * and will update its master node cache. This is accomplished by the
     * {@link MasterBalanceManager#noteExit(RepNodeId)} method which is not
     * part of this service interface.
     * <p>
     * The SN uses these updates to determine when an SN has become unbalanced
     * and will seek to initiate master transfers to correct the imbalance.
     *
     * @param stateInfo the information related to the state transition
     * @param serialVersion
     */
    public void noteState(StateInfo stateInfo,
                          AuthContext authContext,
                          short serialVersion) throws RemoteException;

    /**
     * Deprecated - to be removed
     */
    public void noteState(StateInfo stateInfo,
                          short serialVersion) throws RemoteException;

    /**
     * Returns the master density information associated with this SN. It may
     * return null, if the SNA does not have a copy of the topology, in which
     * case the SNA cannot yet participate in any master balancing activity.
     */
    public MDInfo getMDInfo(AuthContext authContext,
                            short serialVersion) throws RemoteException;

    /**
     * Deprecated - to be removed
     */
    public MDInfo getMDInfo(short serialVersion) throws RemoteException;

    /**
     * Leases a master slot for the RN on this lessor SN for the purposes of a
     * master transfer. The lease is held until it either expires, or a master
     * transfer operation initiated by the lessee SN is completed and the
     * lessor SN hears about it as a result of a {@link #noteState} request.
     * <p>
     * If the lessee already holds the lease it will be extended for the new
     * duration.
     * <p>
     * The lease request is rejected if:
     *
     * 1) The lease is already held by some other lessee
     *
     * 2) The RN is already a master
     *
     * 3) The RN is not currently a replica
     *
     * 4) If granting the lease would cause it to exceed the
     * <code>limitPTMD</code>.
     *
     * @param masterLease the lease details
     *
     * @return true if the lease was granted, false if the lease is already
     * held by some other lessee.
     */
    public boolean getMasterLease(MasterLeaseInfo masterLease,
                                  AuthContext authContext,
                                  short serialVersion) throws RemoteException;

    /**
     * Deprecated - to be removed
     */
    public boolean getMasterLease(MasterLeaseInfo masterLease,
                                  short serialVersion) throws RemoteException;

    /**
     * Cancels the lease granted via an earlier call to {@link #getMasterLease}.
     *
     * @param lesseeSN the SN that owns the lease
     * @param rn the RN associated with the lease
     *
     * @return true if the lease was still outstanding and was cancelled,
     * false otherwise
     */
    public boolean cancelMasterLease(StorageNode lesseeSN,
                                     RepNode rn,
                                     AuthContext authContext,
                                     short serialVersion)
       throws RemoteException;

    /**
     * Deprecated - to be removed
     */
    public boolean cancelMasterLease(StorageNode lesseeSN,
                                     RepNode rn,
                                     short serialVersion)
       throws RemoteException;

    /**
     * Informs this SN that it has an overloaded neighbor SN, that is, one with
     * a MD > BMD. This causes the SN to make a greater effort to rebalance
     * with its neighbors, that is, it will try migrating a master even if its
     * currently balanced.
     * <p>
     * For example, two SNs with a capacity 3, one with MD=33 and one with MD=0
     * will not engage in a master transfer by default, since it only
     * results in the two nodes swapping MDs and does not result in an overall
     * reduction in MD. This is a locally correct decision, but not a globally
     * optimal one. This request effectively say that if the MD at this node,
     * could be lowered below the BMD, then the overloaded SN making the
     * request could move an RN on to it.
     *
     * @param storageNodeId the unbalanced SN making the request
     */
    public void overloadedNeighbor(StorageNodeId storageNodeId,
                                   AuthContext authContext,
                                   short serialVersion)
        throws RemoteException;

    /**
     * Deprecated - to be removed
     */
    public void overloadedNeighbor(StorageNodeId storageNodeId,
                                   short serialVersion)
        throws RemoteException;

    /**
     * A struct that packages information used to request a lease
     */
    public class MasterLeaseInfo implements Serializable, Lease {

        private static final long serialVersionUID = 1L;

        /**
         * The SN requesting the lease
         */
        final StorageNode lesseeSN;

        /**
         * The RN at this node that will transition to the master state
         */
        final RepNode rn;

        /**
         * The limiting PTMD. The lease request is rejected if granting the
         * lease would cause the PTMD to be exceeded
         */
        final int limitPTMD;

       /**
        * The duration of the lease in ms.
        */
        final int durationMs;

        MasterLeaseInfo(StorageNode lesseeSN,
                        RepNode rn,
                        int limitPTMD,
                        int durationMs) {
            this.lesseeSN = lesseeSN;
            this.rn = rn;
            this.limitPTMD = limitPTMD;
            this.durationMs = durationMs;
        }

        @Override
        public String toString() {
            return String.format("Master lease:%s Lessee SN:%s " +
            		         "LimitPTMD:%d duration:%d ms,",
            		         rn, lesseeSN, limitPTMD, durationMs);
        }

        @Override
        public RepNodeId getRepNodeId() {
            return rn.getResourceId();
        }

        @Override
        public int getLeaseDuration() {
            return durationMs;
        }
    }

    /**
     * A struct used to package information associated with a state change.
     */
    public static class StateInfo implements Serializable {

        private static final long serialVersionUID = 1L;
        /**
         * the RN associated with the state change
         */
        final RepNodeId rnId;

        /**
         *  The new state.
         */
        final State state;

        /**
         * The topo sequence number associated with the topology in use at the
         * RN
         */
        final int topoSequenceNumber;

        public StateInfo(RepNodeId rnId, State state, int topoSequenceNumber) {
            super();
            this.rnId = rnId;
            this.state = state;
            this.topoSequenceNumber = topoSequenceNumber;
        }

        public RepNodeId getRnId() {
            return rnId;
        }

        public State getState() {
            return state;
        }

        public int getTopoSequenceNumber() {
            return topoSequenceNumber;
        }

        @Override
        public String toString() {
            return "State RN:" + rnId + " state: " + state +
                   " top seq num: " + topoSequenceNumber;
        }
    }

    /**
     * Struct used to package master density related info. It has accessors
     * that return the various forms of MD.
     */
    public static class MDInfo  implements Serializable {

        private static final long serialVersionUID = 1L;

        final int masterCount;
        final int masterLeaseCount;
        final int rnCount;
        final int balancedMD;

        /**
         * Attributes related to MD calculations
         *
         * @param masterCount the number of current masters at the SN
         * @param masterLeaseCount the number of master leases at the SN
         * @param rnCount the number of the RNs hosted by the SN
         * @param balancedMD the target balanced master density used to
         * determine whether a SN has an excessive number of masters
         */
        MDInfo(int masterCount,
               int masterLeaseCount,
               int rnCount,
               int balancedMD) {
            super();
            this.masterCount = masterCount;
            this.masterLeaseCount = masterLeaseCount;
            this.rnCount = rnCount;
            this.balancedMD = balancedMD;
        }

        /**
         * Returns the Post Transfer Master Density (PTMD) associated with this
         * SN. The PTMD is defined as:
         *
         * <pre>
         * (number of master RNs + number of master leases outstanding + 1) /
         *                 number of resident RNs
         * </pre>
         *
         * expressed as a percentage.
         *
         * Special provision is made in all MD calculations when only one
         * master is present at an SN to ensure that it does not exceed
         * the balanced MD.
         *
         * Note that all MDs are instantaneous values and can change as a
         * result of failovers, other master transfer operations, etc.
         */
        int getPTMD() {
            return adjustedMD(masterLeaseCount + masterCount + 1);

        }

        /**
         * Returns the current MD associated with this SN. Similar to the PTMD
         * computation as above, but with one less master.
         *
         * The MD is defined as:
         * <pre>
         * (number of master RNs + number of master leases outstanding) /
         *                 number of resident RNs
         * </pre>
         * expressed as a percentage and adjusted for a single master.
         */
        int getMD() {
            return adjustedMD(masterLeaseCount + masterCount);
        }

        /**
         * Returns the raw (unadjusted for any leases) MD.
         *
         * Used only during testing.
         */
        int getRawMD() {
            return adjustedMD(masterCount);
        }

        /**
         * Adjusts the MD so that the presence of a single master on a small
         * capacity system does not exceed the balanced MD.
         */
        private int adjustedMD(int masters) {
            final int md = (masters * 100) / rnCount;

            if ((masters == 1) && (md > balancedMD)) {
                return balancedMD;
            }
            return md;
        }

        /**
         * Returns the number of masters currently hosted by the SN
         */
        public int getMasterCount() {
            return masterCount;
        }

        /**
         * Returns the number of RNs hosted at the SN
         */
        public int getRNCount() {
            return rnCount;
        }
    }
}