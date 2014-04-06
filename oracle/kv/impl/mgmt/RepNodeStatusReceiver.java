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

package oracle.kv.impl.mgmt;

import java.rmi.Remote;
import java.rmi.RemoteException;

import oracle.kv.impl.measurement.ServiceStatusChange;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.rep.monitor.StatsPacket;

/**
 * These methods are called by the RepNode to inform the MgmtAgent of the
 * RepNode's status.  This remote interface does not need to be versioned,
 * because the two participants will always be local, and always be the from
 * same release.
 */
public interface RepNodeStatusReceiver extends Remote {

    public void updateRnStatus(ServiceStatusChange newStatus)
        throws RemoteException;

    public void receiveStats(StatsPacket packet)
        throws RemoteException;

    public void receiveNewParams(ParameterMap newMap)
        throws RemoteException;
}
