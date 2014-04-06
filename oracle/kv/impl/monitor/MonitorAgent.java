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

package oracle.kv.impl.monitor;

import java.rmi.RemoteException;
import java.util.List;

import oracle.kv.impl.measurement.Measurement;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.util.registry.VersionedRemote;

/**
 * A MonitorAgent has instrumentation to deliver to the 
 * {@link Collector}
 * 
 * RepNodes and StorageNodeAgents implement MonitorAgent. The contract
 */
public interface MonitorAgent extends VersionedRemote {
    
    /**
     * Return all new values of monitored data.
     *
     * @since 3.0
     */
    public List<Measurement> getMeasurements(AuthContext authCtx,
                                             short serialVersion)
        throws RemoteException;

    /**
     * To be removed after R2 compatibility period.
     * @deprecated
     */
    @Deprecated
    public List<Measurement> getMeasurements(short serialVersion)
        throws RemoteException;
}
