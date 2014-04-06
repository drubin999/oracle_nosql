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

package oracle.kv.impl.util;

import oracle.kv.Durability;
import oracle.kv.Durability.ReplicaAckPolicy;
import oracle.kv.Durability.SyncPolicy;

/**
 * Utility methods to translate between HA and KV durability.
 */
public class DurabilityTranslator {

    public static com.sleepycat.je.Durability
        translate(Durability durability) {

        final SyncPolicy masterSync = durability.getMasterSync();
        final SyncPolicy replicaSync = durability.getReplicaSync();
        final ReplicaAckPolicy replicaAck = durability.getReplicaAck();

        return new com.sleepycat.je.Durability(translate(masterSync),
                                               translate(replicaSync),
                                               translate(replicaAck));
    }

    public static Durability
        translate(com.sleepycat.je.Durability durability) {

        final com.sleepycat.je.Durability.SyncPolicy masterSync =
            durability.getLocalSync();
        final com.sleepycat.je.Durability.SyncPolicy replicaSync =
            durability.getReplicaSync();
        final com.sleepycat.je.Durability.ReplicaAckPolicy
        replicaAck = durability.getReplicaAck();

        return new Durability(translate(masterSync), translate(replicaSync),
                              translate(replicaAck));
    }

    /**
     * Translates syncPolicy
     */
    public static com.sleepycat.je.Durability.SyncPolicy
        translate(SyncPolicy sync) {

        if (sync == null) {
            return null;
        }

       if (SyncPolicy.NO_SYNC.equals(sync)) {
           return com.sleepycat.je.Durability.SyncPolicy.NO_SYNC;
       } else if (SyncPolicy.WRITE_NO_SYNC.equals(sync)) {
           return com.sleepycat.je.Durability.SyncPolicy.WRITE_NO_SYNC;
       } else if (SyncPolicy.SYNC.equals(sync)) {
           return com.sleepycat.je.Durability.SyncPolicy.SYNC;
       } else {
           throw new IllegalArgumentException("Unknown sync: " + sync);
       }
    }

    public static SyncPolicy
        translate(com.sleepycat.je.Durability.SyncPolicy sync) {

        if (sync == null) {
            return null;
        }

        if (com.sleepycat.je.Durability.SyncPolicy.NO_SYNC.equals(sync)) {
            return SyncPolicy.NO_SYNC;
        } else if (com.sleepycat.je.Durability.SyncPolicy.WRITE_NO_SYNC.
                equals(sync)) {
            return SyncPolicy.WRITE_NO_SYNC;
        } else if (com.sleepycat.je.Durability.SyncPolicy.SYNC.equals(sync)) {
            return SyncPolicy.SYNC;
        }
        throw new IllegalArgumentException("Unknown sync: " + sync);
    }

    public static ReplicaAckPolicy translate
        (com.sleepycat.je.Durability.ReplicaAckPolicy ackPolicy) {

        if (ackPolicy == null) {
            return null;
        }

        if (com.sleepycat.je.Durability.ReplicaAckPolicy.NONE.
                equals(ackPolicy)) {
            return ReplicaAckPolicy.NONE;
        } else if (com.sleepycat.je.Durability.ReplicaAckPolicy.
                SIMPLE_MAJORITY.equals(ackPolicy)) {
            return ReplicaAckPolicy.SIMPLE_MAJORITY;
        } if (com.sleepycat.je.Durability.ReplicaAckPolicy.ALL.
                equals(ackPolicy)) {
            return ReplicaAckPolicy.ALL;
        }
        throw new IllegalArgumentException("Unknown ack: " + ackPolicy);
    }

    public static com.sleepycat.je.Durability.ReplicaAckPolicy translate
        (ReplicaAckPolicy ackPolicy) {

        if (ackPolicy == null) {
            return null;
        }

        if (ReplicaAckPolicy.NONE.equals(ackPolicy)) {
            return com.sleepycat.je.Durability.ReplicaAckPolicy.NONE;
        } else if (ReplicaAckPolicy.SIMPLE_MAJORITY.equals(ackPolicy)) {
            return com.sleepycat.je.Durability.ReplicaAckPolicy.
                SIMPLE_MAJORITY;
        } if (ReplicaAckPolicy.ALL.equals(ackPolicy)) {
            return com.sleepycat.je.Durability.ReplicaAckPolicy.ALL;
        }
        throw new IllegalArgumentException("Unknown ack: " + ackPolicy);
    }
}
