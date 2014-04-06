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

package oracle.kv;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.EnumSet;

import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerialVersion;

/**
 * Defines the durability characteristics associated with a standalone write
 * (put or update) operation, or in the case of {@link KVStore#execute
 * KVStore.execute} with a set of operations performed in a single transaction.
 * <p>
 * The overall durability is a function of the {@link SyncPolicy} and {@link
 * ReplicaAckPolicy} in effect for the Master, and the {@link SyncPolicy} in
 * effect for each Replica.
 * </p>
 */
public class Durability implements FastExternalizable {

    /**
     * A convenience constant that defines a durability policy with COMMIT_SYNC
     * for Master commit synchronization.
     *
     * The policies default to COMMIT_NO_SYNC for commits of replicated
     * transactions that need acknowledgment and SIMPLE_MAJORITY for the
     * acknowledgment policy.
     */
    public static final Durability COMMIT_SYNC =
        new Durability(SyncPolicy.SYNC,
                       SyncPolicy.NO_SYNC,
                       ReplicaAckPolicy.SIMPLE_MAJORITY);

    /**
     * A convenience constant that defines a durability policy with
     * COMMIT_NO_SYNC for Master commit synchronization.
     *
     * The policies default to COMMIT_NO_SYNC for commits of replicated
     * transactions that need acknowledgment and SIMPLE_MAJORITY for the
     * acknowledgment policy.
     */
    public static final Durability COMMIT_NO_SYNC =
        new Durability(SyncPolicy.NO_SYNC,
                       SyncPolicy.NO_SYNC,
                       ReplicaAckPolicy.SIMPLE_MAJORITY);

    /**
     * A convenience constant that defines a durability policy with
     * COMMIT_WRITE_NO_SYNC for Master commit synchronization.
     *
     * The policies default to COMMIT_NO_SYNC for commits of replicated
     * transactions that need acknowledgment and SIMPLE_MAJORITY for the
     * acknowledgment policy.
     */
    public static final Durability COMMIT_WRITE_NO_SYNC =
        new Durability(SyncPolicy.WRITE_NO_SYNC,
                       SyncPolicy.NO_SYNC,
                       ReplicaAckPolicy.SIMPLE_MAJORITY);

    /**
     * Defines the synchronization policy to be used when committing a
     * transaction. High levels of synchronization offer a greater guarantee
     * that the transaction is persistent to disk, but trade that off for
     * lower performance.
     */
    public enum SyncPolicy {

        /**
         *  Write and synchronously flush the log on transaction commit.
         *  Transactions exhibit all the ACID (atomicity, consistency,
         *  isolation, and durability) properties.
         */
        SYNC,

        /**
         * Do not write or synchronously flush the log on transaction commit.
         * Transactions exhibit the ACI (atomicity, consistency, and isolation)
         * properties, but not D (durability); that is, database integrity will
         * be maintained, but if the application or system fails, it is
         * possible some number of the most recently committed transactions may
         * be undone during recovery. The number of transactions at risk is
         * governed by how many log updates can fit into the log buffer, how
         * often the operating system flushes dirty buffers to disk, and how
         * often log checkpoints occur.
         */
        NO_SYNC,

        /**
         * Write but do not synchronously flush the log on transaction commit.
         * Transactions exhibit the ACI (atomicity, consistency, and isolation)
         * properties, but not D (durability); that is, database integrity will
         * be maintained, but if the operating system fails, it is possible
         * some number of the most recently committed transactions may be
         * undone during recovery. The number of transactions at risk is
         * governed by how often the operating system flushes dirty buffers to
         * disk, and how often log checkpoints occur.
         */
        WRITE_NO_SYNC
    }

    private final static SyncPolicy[] SYNC_POLICIES_BY_ORDINAL;
    static {
        final EnumSet<SyncPolicy> set = EnumSet.allOf(SyncPolicy.class);
        SYNC_POLICIES_BY_ORDINAL = new SyncPolicy[set.size()];
        for (SyncPolicy op : set) {
            SYNC_POLICIES_BY_ORDINAL[op.ordinal()] = op;
        }
    }

    private static SyncPolicy getSyncPolicy(int ordinal) {
        if (ordinal < 0 || ordinal >= SYNC_POLICIES_BY_ORDINAL.length) {
            throw new RuntimeException("unknown SyncPolicy: " + ordinal);
        }
        return SYNC_POLICIES_BY_ORDINAL[ordinal];
    }

    /**
     * A replicated environment makes it possible to increase an application's
     * transaction commit guarantees by committing changes to its replicas on
     * the network. ReplicaAckPolicy defines the policy for how such network
     * commits are handled.
     */
    public enum ReplicaAckPolicy {

        /**
         * All replicas must acknowledge that they have committed the
         * transaction. This policy should be selected only if your replication
         * group has a small number of replicas, and those replicas are on
         * extremely reliable networks and servers.
         */
        ALL,

        /**
         * No transaction commit acknowledgments are required and the master
         * will never wait for replica acknowledgments. In this case,
         * transaction durability is determined entirely by the type of commit
         * that is being performed on the master.
         */
        NONE,

        /**
         * A simple majority of replicas must acknowledge that they have
         * committed the transaction. This acknowledgment policy, in
         * conjunction with an election policy which requires at least a simple
         * majority, ensures that the changes made by the transaction remains
         * durable if a new election is held.
         */
        SIMPLE_MAJORITY;
    }

    private final static ReplicaAckPolicy[] REPLICA_ACK_POLICIES_BY_ORDINAL;
    static {
        final EnumSet<ReplicaAckPolicy> set =
            EnumSet.allOf(ReplicaAckPolicy.class);
        REPLICA_ACK_POLICIES_BY_ORDINAL = new ReplicaAckPolicy[set.size()];
        for (ReplicaAckPolicy op : set) {
            REPLICA_ACK_POLICIES_BY_ORDINAL[op.ordinal()] = op;
        }
    }

    private static ReplicaAckPolicy getReplicaAckPolicy(int ordinal) {
        if (ordinal < 0 || ordinal >= REPLICA_ACK_POLICIES_BY_ORDINAL.length) {
            throw new RuntimeException("unknown ReplicaAckPolicy: " + ordinal);
        }
        return REPLICA_ACK_POLICIES_BY_ORDINAL[ordinal];
    }

    /* The sync policy in effect on the Master node. */
    private final SyncPolicy masterSync;

    /* The sync policy in effect on a replica. */
    final private SyncPolicy replicaSync;

    /* The replica acknowledgment policy to be used. */
    final private ReplicaAckPolicy replicaAck;

    /**
     * Creates an instance of a Durability specification.
     *
     * @param masterSync the SyncPolicy to be used when committing the
     * transaction on the Master.
     * @param replicaSync the SyncPolicy to be used remotely, as part of a
     * transaction acknowledgment, at a Replica node.
     * @param replicaAck the acknowledgment policy used when obtaining
     * transaction acknowledgments from Replicas.
     */
    public Durability(SyncPolicy masterSync,
                      SyncPolicy replicaSync,
                      ReplicaAckPolicy replicaAck) {
        this.masterSync = masterSync;
        this.replicaSync = replicaSync;
        this.replicaAck = replicaAck;
    }

    /**
     * For internal use only.
     * @hidden
     *
     * FastExternalizable constructor.
     */
    public Durability(ObjectInput in,
                      @SuppressWarnings("unused") short serialVersion)
        throws IOException {

        masterSync = getSyncPolicy(in.readUnsignedByte());
        replicaSync = getSyncPolicy(in.readUnsignedByte());
        replicaAck = getReplicaAckPolicy(in.readUnsignedByte());
    }

    /**
     * For internal use only.
     * @hidden
     *
     * FastExternalizable writer.
     */
    @Override
    public void writeFastExternal(ObjectOutput out, short serialVersion)
        throws IOException {

        out.writeByte(masterSync.ordinal());
        out.writeByte(replicaSync.ordinal());
        out.writeByte(replicaAck.ordinal());
    }

    /**
     * Returns this Durability as a serialized byte array, such that {@link
     * #fromByteArray} may be used to reconstitute the Durability.
     */
    public byte[] toByteArray() {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream(200);
        try {
            final ObjectOutputStream oos = new ObjectOutputStream(baos);

            oos.writeShort(SerialVersion.CURRENT);
            writeFastExternal(oos, SerialVersion.CURRENT);

            oos.flush();
            return baos.toByteArray();

        } catch (IOException e) {
            /* Should never happen. */
            throw new FaultException(e, false /*isRemote*/);
        }
    }

    /**
     * Deserializes the given bytes that were returned earlier by {@link
     * #toByteArray} and returns the resulting Durability.
     */
    public static Durability fromByteArray(byte[] keyBytes) {

        final ByteArrayInputStream bais = new ByteArrayInputStream(keyBytes);
        try {
            final ObjectInputStream ois = new ObjectInputStream(bais);

            final short serialVersion = ois.readShort();

            return new Durability(ois, serialVersion);

        } catch (IOException e) {
            /* Should never happen. */
            throw new FaultException(e, false /*isRemote*/);
        }
    }

    @Override
    public String toString() {
        return masterSync.toString() + "," +
               replicaSync.toString() + "," +
               replicaAck.toString();
    }

    /**
     * Returns the transaction synchronization policy to be used on the Master
     * when committing a transaction.
     */
    public SyncPolicy getMasterSync() {
        return masterSync;
    }

    /**
     * Returns the transaction synchronization policy to be used by the replica
     * as it replays a transaction that needs an acknowledgment.
     */
    public SyncPolicy getReplicaSync() {
        return replicaSync;
    }

    /**
     * Returns the replica acknowledgment policy used by the master when
     * committing changes to a replicated environment.
     */
    public ReplicaAckPolicy getReplicaAck() {
        return replicaAck;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((masterSync == null) ? 0 : masterSync.hashCode());
        result = prime * result
                + ((replicaAck == null) ? 0 : replicaAck.hashCode());
        result = prime * result
                + ((replicaSync == null) ? 0 : replicaSync.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof Durability)) {
            return false;
        }
        Durability other = (Durability) obj;
        if (masterSync == null) {
            if (other.masterSync != null) {
                return false;
            }
        } else if (!masterSync.equals(other.masterSync)) {
            return false;
        }
        if (replicaAck == null) {
            if (other.replicaAck != null) {
                return false;
            }
        } else if (!replicaAck.equals(other.replicaAck)) {
            return false;
        }
        if (replicaSync == null) {
            if (other.replicaSync != null) {
                return false;
            }
        } else if (!replicaSync.equals(other.replicaSync)) {
            return false;
        }
        return true;
    }
}
