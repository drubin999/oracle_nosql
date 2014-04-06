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
import java.util.UUID;

import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerialVersion;

import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.VLSN;

/**
 * A Version refers to a specific version of a key-value pair.
 * <p>
 * When a key-value pair is initially inserted in the KV Store, and each time
 * it is updated, it is assigned a unique version token.  The version is always
 * returned by the put method, for example, {@link KVStore#put put}, and is
 * also returned by get methods, for example, {@link KVStore#get get}.  The
 * version is important for two reasons:
 * <ol>
 *   <li>
 *   When an update or delete is to be performed, it may be important to only
 *   perform the update or delete if the last known value has not changed.  For
 *   example, if an integer field in a previously known value is to be
 *   incremented, it is important that the previous value has not changed in
 *   the KV Store since it was obtained by the client.  This can be guaranteed
 *   by passing the version of the previously known value to the {@link
 *   KVStore#putIfVersion putIfVersion} or {@link KVStore#deleteIfVersion
 *   deleteIfVersion} method.  If the version specified does not match the
 *   current version of the value in the KV Store, these methods will not
 *   perform the update or delete operation and will return an indication of
 *   failure.  Optionally, they will also return the current version and/or
 *   value so the client can retry the operation or take a different action.
 *   </li>
 *   <li>
 *   When a client reads a value that was previously written, it may be
 *   important to ensure that the KV Store node servicing the read operation
 *   has been updated with the information previously written.  This can be
 *   accomplished by passing the version of the previously written value as
 *   a {@link Consistency} parameter to the read operation, for example, {@link
 *   KVStore#get get}.  See {@link Consistency.Version} for more information.
 *   </li>
 * </ol>
 * </p>
 * <p>
 * It is important to be aware that the system may infrequently assign a new
 * Version to a key-value pair, for example, when migrating data for better
 * resource usage.  Therefore, when using the {@link KVStore#putIfVersion
 * putIfVersion} or {@link KVStore#deleteIfVersion deleteIfVersion} methods,
 * one cannot assume that the Version will remain constant until it is changed
 * by the application.
 * </p>
 */
public class Version implements FastExternalizable {

    /*
     * The UUID associated with the replicated environment.
     */
    private final UUID repGroupUuid;
    private final long repGroupVlsn;
    private final RepNodeId repNodeId;
    private final long repNodeLsn;

    /**
     * For internal use only.
     * @hidden
     *
     * Creates a Version with a logical VLSN but without a physical LSN.
     */
    public Version(UUID repGroupUuid, long repGroupVlsn) {
        assert repGroupUuid != null;
        assert repGroupVlsn > 0;

        this.repGroupUuid = repGroupUuid;
        this.repGroupVlsn = repGroupVlsn;
        this.repNodeId = null;
        this.repNodeLsn = 0;
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Creates a Version with a logical VLSN and physical LSN.
     */
    public Version(UUID repGroupUuid,
                   long repGroupVlsn,
                   RepNodeId repNodeId,
                   long repNodeLsn) {
        assert repGroupUuid != null;
        assert repGroupVlsn > 0 : repGroupVlsn;
        assert repNodeId != null;
        assert repNodeLsn != 0 && repNodeLsn != DbLsn.NULL_LSN : repNodeLsn;

        this.repGroupUuid = repGroupUuid;
        this.repGroupVlsn = repGroupVlsn;
        this.repNodeId = repNodeId;
        this.repNodeLsn = repNodeLsn;
    }

    /**
     * For internal use only.
     * @hidden
     *
     * FastExternalizable constructor.
     */
    public Version(ObjectInput in, short serialVersion)
        throws IOException {

        final long mostSig = in.readLong();
        final long leastSig = in.readLong();
        repGroupUuid = new UUID(mostSig, leastSig);
        repGroupVlsn = in.readLong();

        if (in.read() == 0) {
            repNodeId = null;
            repNodeLsn = 0;
            return;
        }

        /*
         * The serialized form supports ResourceIds of different subclasses,
         * but here we only allow a RepNodeId.
         */
        repNodeId = (RepNodeId) ResourceId.readFastExternal(in, serialVersion);

        repNodeLsn = in.readLong();
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

        out.writeLong(repGroupUuid.getMostSignificantBits());
        out.writeLong(repGroupUuid.getLeastSignificantBits());
        out.writeLong(repGroupVlsn);
        if (repNodeId == null) {
            out.write(0);
            return;
        }
        out.write(1);
        repNodeId.writeFastExternal(out, serialVersion);
        out.writeLong(repNodeLsn);
    }

    /**
     * Returns this Version as a serialized byte array, such that {@link
     * #fromByteArray} may be used to reconstitute the Version.
     */
    public byte[] toByteArray() {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream(50);
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
     * #toByteArray} and returns the resulting Version.
     */
    public static Version fromByteArray(byte[] keyBytes) {

        final ByteArrayInputStream bais = new ByteArrayInputStream(keyBytes);
        try {
            final ObjectInputStream ois = new ObjectInputStream(bais);

            final short serialVersion = ois.readShort();

            return new Version(ois, serialVersion);

        } catch (IOException e) {
            /* Should never happen. */
            throw new FaultException(e, false /*isRemote*/);
        }
    }

    /**
     * For internal use only.
     * @hidden
     */
    public UUID getRepGroupUUID() {
        return repGroupUuid;
    }

    /**
     * For internal use only.
     * @hidden
     */
    public long getVLSN() {
        return repGroupVlsn;
    }

    /**
     * For an implied Key associated with this Version, returns a unique
     * identifier for this Version.
     */
    public long getVersion() {
        return repGroupVlsn;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Version)) {
            return false;
        }
        final Version o = (Version) other;
        return repGroupVlsn == o.repGroupVlsn &&
               repGroupUuid.equals(o.repGroupUuid);
    }

    @Override
    public int hashCode() {
        return repGroupUuid.hashCode() + (int) repGroupVlsn;
    }

    @Override
    public String toString() {
        return "<Version repGroupUuid=" + repGroupUuid +
               " vlsn=" + (new VLSN(repGroupVlsn)).toString() +
               " repNodeId=" + repNodeId +
               " lsn=" + DbLsn.getNoFormatString(repNodeLsn) + '>';
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Returns true if the VLSN of this Version is equal to the given VLSN,
     * within the implied context of a replication group.  If false is
     * returned, it is certain that the versions are unequal.
     */
    public boolean sameLogicalVersion(long otherVlsn) {
        return repGroupVlsn == otherVlsn;
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Returns true if the node Id and LSN of this Version are equal to the
     * given node Id and LSN, within the implied context of a replication
     * group.  If false is returned, then it is not known whether the versions
     * are equal and sameLogicalVersion must be called to determine equality.
     */
    public boolean samePhysicalVersion(RepNodeId otherNodeId,
                                       long otherNodeLsn) {
        return repNodeLsn == otherNodeLsn &&
               repNodeId != null &&
               repNodeId.equals(otherNodeId);
    }
}
