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

import oracle.kv.impl.topo.RepNodeId;

import com.sleepycat.je.rep.StateChangeEvent;

/**
 * The abstract base class. It has two subclasses:
 *
 * 1) MasterBalanceManager the real class that implements master balancing
 *
 * 2) MasterBalanceManagerDisabled that's used when MBM has been turned off.
 *
 * Please see MasterBalanceManager for the documentation associated
 * with the abstract methods below.
 */
public interface MasterBalanceManagerInterface {

    public abstract boolean initiateMasterTransfer(RepNodeId replicaId,
                                                   int timeout,
                                                   TimeUnit timeUnit);
    public abstract void shutdown();

    public abstract void noteStateChange(StateChangeEvent stateChangeEvent);

    public abstract void initialize();

    public abstract void startTracker();

    /* For testing only */
    public abstract MasterBalanceStateTracker getStateTracker();
}