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

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import oracle.kv.impl.topo.RepNodeId;

import com.sleepycat.je.rep.StateChangeEvent;

/**
 * MasterBalanceManagerDisabled supplied the placebo method implementations
 * for use when the MasterBalanceManager has been disabled at the RN
 */
class MasterBalanceManagerDisabled implements MasterBalanceManagerInterface {

    private final Logger logger;

    public MasterBalanceManagerDisabled(Logger logger) {
        super();
        this.logger = logger;
        logger.info("Master balance manager disabled at the RN");
    }

    @Override
    public boolean initiateMasterTransfer(RepNodeId replicaId,
                                          int timeout,
                                          TimeUnit timeUnit) {
        logger.info("Unexpected request for master transfer to " + replicaId +
                    "at disabled master balance manager");
        return false;
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void noteStateChange(StateChangeEvent stateChangeEvent) {
    }

    @Override
    public void initialize() {
    }

    @Override
    public void startTracker() {
    }

    @Override
    public MasterBalanceStateTracker getStateTracker() {
        /* Used only in test situations, should never be invoked. */
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "getStateTracker");
    }
}
