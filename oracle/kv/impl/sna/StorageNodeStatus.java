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

package oracle.kv.impl.sna;

import java.io.Serializable;

import oracle.kv.KVVersion;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;

/**
 * StorageNodeStatus represents the current status of a running StorageNode.
 * It includes ServiceStatus and Version.
 */
public class StorageNodeStatus implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Used during testing: A non-null value overrides the current version. */
    private static volatile KVVersion testCurrentKvVersion;

    private final ServiceStatus status;
    private final KVVersion kvVersion;

    public StorageNodeStatus(ServiceStatus status) {
        this.status = status;
        this.kvVersion = getCurrentKVVersion();
    }

    /** Gets the current version. */
    private static KVVersion getCurrentKVVersion() {
        return (testCurrentKvVersion != null) ?
            testCurrentKvVersion :
            KVVersion.CURRENT_VERSION;
    }

    /**
     * Set the current version to a different value, for testing.  Specifying
     * {@code null} reverts to the standard value.
     */
    public static void setTestKVVersion(final KVVersion testKvVersion) {
        testCurrentKvVersion = testKvVersion;
    }

    public ServiceStatus getServiceStatus() {
        return status;
    }

    public KVVersion getKVVersion() {
        return kvVersion;
    }

    @Override
    public String toString() {
        return status + "," + status;
    }
}
