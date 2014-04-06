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

import oracle.kv.impl.security.annotations.PublicAPI;
import oracle.kv.impl.util.SerialVersion;

/**
 * Base class for all service implementations.
 *
 * @see VersionedRemote
 */
@PublicAPI
public class VersionedRemoteImpl implements VersionedRemote {

    private short serialVersion = SerialVersion.CURRENT;

    @Override
    public short getSerialVersion() {
        return serialVersion;
    }

    /**
     * Overrides the value returned by getSerialVersion for testing.
     */
    public void setTestSerialVersion(short useSerialVersion) {
        serialVersion = useSerialVersion;
    }

    /**
     * Used by derived classes to signal an implementation error that allowed
     * a deprecated R2 RMI interface method to be directly accessed.
     */
    protected RuntimeException invalidR2MethodException() {
        return new UnsupportedOperationException(
            "Calls to this method must be made through the proxy interface.");
    }
}
