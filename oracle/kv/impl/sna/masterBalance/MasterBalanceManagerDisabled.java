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

import java.rmi.RemoteException;
import java.util.logging.Logger;

import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;

/**
 * MasterBalanceManagerDisabled supplied the placebo method implementations
 * for use when the MasterBalanceManager has been disabled at the SNA
 */
class MasterBalanceManagerDisabled
    implements MasterBalanceManagerInterface {

    final Logger logger;

    MasterBalanceManagerDisabled(Logger logger) {
        super();
        this.logger = logger;
        logger.info("Master balance manager disabled at the SNA");
    }

    @Override
    public void noteState(StateInfo stateInfo,
                          short serialVersion)
        throws RemoteException {

        noteState(stateInfo, null, serialVersion);
    }

    @Override
    public void noteState(StateInfo stateInfo,
                          AuthContext authContext,
                          short serialVersion)
        throws RemoteException {
        /* NOP */
    }

    @Override
    public MDInfo getMDInfo(short serialVersion) throws RemoteException {
        return getMDInfo(null, serialVersion);
    }

    @Override
    public MDInfo getMDInfo(AuthContext authContext,
                            short serialVersion) throws RemoteException {
        /*
         * Returning null to the caller effectively tells it the SN
         * will not participate in master balancing.
         */
        return null;
    }

    @Override
    public boolean getMasterLease(MasterLeaseInfo masterLease,
                                  short serialVersion) throws RemoteException {
        return getMasterLease(masterLease, null, serialVersion);
    }

    @Override
    public boolean getMasterLease(MasterLeaseInfo masterLease,
                                  AuthContext authContext,
                                  short serialVersion) throws RemoteException {
        /* decline all requests for a master lease. */
        return false;
    }

    @Override
    public boolean cancelMasterLease(StorageNode lesseeSN,
                                     RepNode rn,
                                     short serialVersion)
        throws RemoteException {

        return cancelMasterLease(lesseeSN, rn, null, serialVersion);
    }

    @Override
    public boolean cancelMasterLease(StorageNode lesseeSN,
                                     RepNode rn,
                                     AuthContext authContext,
                                     short serialVersion)
        throws RemoteException {

        logger.info("Unexpected service request to cancel a master lease " +
                    "for RN: " + rn +
                    " Lessee SN: " + lesseeSN);
        return false;
    }

    @Override
    public void overloadedNeighbor(StorageNodeId storageNodeId,
                                   short serialVersion)
        throws RemoteException {

        overloadedNeighbor(storageNodeId, null, serialVersion);
    }

    @Override
    public void overloadedNeighbor(StorageNodeId storageNodeId,
                                   AuthContext authContext,
                                   short serialVersion)
        throws RemoteException {

        /* NOP */
    }

    @Override
    public void noteExit(RepNodeId rnId) {
        /* NOP */
    }

    @Override
    public void shutdown() {
        /* NOP */
    }
}
