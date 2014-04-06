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

import java.util.logging.Logger;

import oracle.kv.impl.topo.RepNodeId;

/**
 * Manages replica leases at a SN. A replica lease is associated with a RN that
 * is currently a Master and will transition to a replica to help balance
 * master load. It's established by the source SN.
 *
 * A replica lease is established at a source SN to ensure that an attempt is
 * not made to migrate a master RN multiple times while a master transfer
 * operation is in progress. That is, while the master RN is transitioning to
 * the replica state as a result of the master transfer.
 */
public class ReplicaLeaseManager extends LeaseManager {

    ReplicaLeaseManager(Logger logger) {
        super(logger);
    }

    synchronized void getReplicaLease(ReplicaLease lease) {

        final RepNodeId rnId = lease.getRepNodeId();
        LeaseTask leaseTask = leaseTasks.get(rnId);

        if (leaseTask != null) {
            /*
             * Establish a new lease after canceling the existing lease.
             */
            leaseTask.cancel();
            leaseTask = null;
        }

        leaseTask = new LeaseTask(lease);
        logger.info("Established replica lease:" + lease);
    }

    /* The replica lease. */
    static class ReplicaLease implements LeaseManager.Lease {

        /* The master RN that will transition to a replica. */
        private final RepNodeId rnId;

        private final int leaseDurationMs;

        ReplicaLease(RepNodeId rnId, int leaseDurationMs) {
            super();
            this.rnId = rnId;
            this.leaseDurationMs = leaseDurationMs;
        }

        @Override
        public String toString() {
            return String.format("Replica lease: %s, duration: %d ms",
                                 rnId.getFullName(), leaseDurationMs);
        }

        @Override
        public RepNodeId getRepNodeId() {
            return rnId;
        }

        @Override
        public int getLeaseDuration() {
            return leaseDurationMs;
        }
    }
}
