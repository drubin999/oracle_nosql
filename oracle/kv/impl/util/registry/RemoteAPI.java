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

package oracle.kv.impl.util.registry;

import java.rmi.RemoteException;

import oracle.kv.impl.util.SerialVersion;

/**
 * Base class for API classes that wrap remote interfaces to provide an API
 * called by clients of the remote service.
 *
 * @see VersionedRemote
 */
public abstract class RemoteAPI {

    private final short serialVersion;
    private final VersionedRemote remote;

    /**
     * Caches the effective version.  This constructor should be called only
     * by a private constructor in the API class, which is called only by the
     * API's wrap() method.
     */
    protected RemoteAPI(VersionedRemote remote)
        throws RemoteException {

        serialVersion =
            (short) Math.min(SerialVersion.CURRENT, remote.getSerialVersion());
        this.remote = remote;
    }

    /**
     * Returns the effective version, which is the minimum of the current
     * service and client version, and should be passed as the last argument of
     * each remote method.
     */
    public short getSerialVersion() {
        return serialVersion;
    }

    @Override
    public int hashCode() {
        return remote.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof RemoteAPI)) {
            return false;
        }
        final RemoteAPI o = (RemoteAPI) other;
        return remote.equals(o.remote);
    }
}
