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

package oracle.kv.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.Key;
import oracle.kv.KeyRange;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * @hidden
 */
public class KVInputSplit extends InputSplit implements Writable {

    private String kvStore;
    private String[] kvHelperHosts;
    private int kvPart;
    private Direction direction;
    private int batchSize;
    private Key parentKey;
    private KeyRange subRange;
    private Depth depth;
    private Consistency consistency;
    private long timeout;
    private TimeUnit timeoutUnit;
    private String[] locations = new String[0];
    private String formatterClassName;
    private String kvStoreSecurityFile;

    public KVInputSplit() {
    }

    /**
     * Get the size of the split, so that the input splits can be sorted by
     * size.
     *
     * @return the number of bytes in the split
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public long getLength()
        throws IOException, InterruptedException {

        /*
         * Always return 1 for now since partitions are assumed to be relatively
         * equal in size.
         */
        return 1;
    }

    /**
     * Get the list of nodes by name where the data for the split would be
     * local.  The locations do not need to be serialized.
     *
     * @return a new array of the node nodes.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public String[] getLocations()
        throws IOException, InterruptedException {

        return locations;
    }

    KVInputSplit setLocations(String[] locations) {
        this.locations = locations;
        return this;
    }

    KVInputSplit setDirection(Direction direction) {
        this.direction = direction;
        return this;
    }

    Direction getDirection() {
        return direction;
    }

    KVInputSplit setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    int getBatchSize() {
        return batchSize;
    }

    KVInputSplit setParentKey(Key parentKey) {
        this.parentKey = parentKey;
        return this;
    }

    Key getParentKey() {
        return parentKey;
    }

    KVInputSplit setSubRange(KeyRange subRange) {
        this.subRange = subRange;
        return this;
    }

    KeyRange getSubRange() {
        return subRange;
    }

    KVInputSplit setDepth(Depth depth) {
        this.depth = depth;
        return this;
    }

    Depth getDepth() {
        return depth;
    }

    KVInputSplit setConsistency(Consistency consistency) {
        this.consistency = consistency;
        return this;
    }

    Consistency getConsistency() {
        return consistency;
    }

    KVInputSplit setTimeout(long timeout) {
        this.timeout = timeout;
        return this;
    }

    long getTimeout() {
        return timeout;
    }

    KVInputSplit setTimeoutUnit(TimeUnit timeoutUnit) {
        this.timeoutUnit = timeoutUnit;
        return this;
    }

    TimeUnit getTimeoutUnit() {
        return timeoutUnit;
    }

    KVInputSplit setKVHelperHosts(String[] kvHelperHosts) {
        this.kvHelperHosts = kvHelperHosts;
        return this;
    }

    String[] getKVHelperHosts() {
        return kvHelperHosts;
    }

    KVInputSplit setKVStoreName(String kvStore) {
        this.kvStore = kvStore;
        return this;
    }

    String getKVStoreName() {
        return kvStore;
    }

    KVInputSplit setKVPart(int kvPart) {
        this.kvPart = kvPart;
        return this;
    }

    int getKVPart() {
        return kvPart;
    }

    KVInputSplit setFormatterClassName(String formatterClassName) {
        this.formatterClassName = formatterClassName;
        return this;
    }

    String getFormatterClassName() {
        return formatterClassName;
    }

    KVInputSplit setKVStoreSecurityFile(String securityFile) {
        this.kvStoreSecurityFile = securityFile;
        return this;
    }

    String getKVStoreSecurityFile() {
        return kvStoreSecurityFile;
    }

    /**
     * Serialize the fields of this object to <code>out</code>.
     *
     * @param out <code>DataOuput</code> to serialize this object into.
     * @throws IOException
     */
    @Override
    public void write(DataOutput out)
        throws IOException {

        out.writeInt(kvHelperHosts.length);
        for (int i = 0; i < kvHelperHosts.length; i++) {
            Text.writeString(out, kvHelperHosts[i]);
        }
        Text.writeString(out, kvStore);
        Text.writeString(out, "" + kvPart);
        Text.writeString(out, (direction == null ? "" : direction.name()));
        out.writeInt(batchSize);
        writeBytes(out, (parentKey == null ? null : parentKey.toByteArray()));
        writeBytes(out, (subRange == null ? null : subRange.toByteArray()));
        Text.writeString(out, (depth == null ? "" : depth.name()));
        writeBytes(out, (consistency == null ?
                         null :
                         consistency.toByteArray()));
        out.writeLong(timeout);
        Text.writeString(out, (timeoutUnit == null ? "" : timeoutUnit.name()));
        out.writeInt(locations.length);
        for (int i = 0; i < locations.length; i++) {
            Text.writeString(out, locations[i]);
        }
        Text.writeString(out, (formatterClassName == null ?
                               "" :
                               formatterClassName));
        Text.writeString(out, (kvStoreSecurityFile == null ?
                               "" :
                               kvStoreSecurityFile));
    }

    /**
     * Deserialize the fields of this object from <code>in</code>.
     *
     * <p>For efficiency, implementations should attempt to re-use storage in
     * the existing object where possible.</p>
     *
     * @param in <code>DataInput</code> to deseriablize this object from.
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in)
        throws IOException {

        int nHelperHosts = in.readInt();
        kvHelperHosts = new String[nHelperHosts];
        for (int i = 0; i < nHelperHosts; i++) {
            kvHelperHosts[i] = Text.readString(in);
        }

        kvStore = Text.readString(in);
        kvPart = Integer.parseInt(Text.readString(in));
        String dirStr = Text.readString(in);
        if (dirStr == null || dirStr.equals("")) {
            direction = Direction.FORWARD;
        } else {
            direction = Direction.valueOf(dirStr);
        }

        batchSize = in.readInt();

        byte[] pkBytes = readBytes(in);
        if (pkBytes == null) {
            parentKey = null;
        } else {
            parentKey = Key.fromByteArray(pkBytes);
        }

        byte[] srBytes = readBytes(in);
        if (srBytes == null) {
            subRange = null;
        } else {
            subRange = KeyRange.fromByteArray(srBytes);
        }

        String depthStr = Text.readString(in);
        if (depthStr == null || depthStr.equals("")) {
            depth = Depth.PARENT_AND_DESCENDANTS;
        } else {
            depth = Depth.valueOf(depthStr);
        }

        byte[] consBytes = readBytes(in);
        if (consBytes == null) {
            consistency = null;
        } else {
            consistency = Consistency.fromByteArray(consBytes);
        }

        timeout = in.readLong();

        String tuStr = Text.readString(in);
        if (tuStr == null || tuStr.equals("")) {
            timeoutUnit = null;
        } else {
            timeoutUnit = TimeUnit.valueOf(tuStr);
        }

        int len = in.readInt();
        locations = new String[len];
        for (int i = 0; i < len; i++) {
            locations[i] = Text.readString(in);
        }

        formatterClassName = Text.readString(in);
        if (formatterClassName == null || formatterClassName.equals("")) {
            formatterClassName = null;
        }

        kvStoreSecurityFile = Text.readString(in);
        if (kvStoreSecurityFile == null || kvStoreSecurityFile.equals("")) {
            kvStoreSecurityFile = null;
        }
    }

    private void writeBytes(DataOutput out, byte[] bytes)
        throws IOException {

        if (bytes == null) {
            out.writeInt(0);
            return;
        }
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private byte[] readBytes(DataInput in)
        throws IOException {

        int len = in.readInt();
        if (len == 0) {
            return null;
        }

        byte[] ret = new byte[len];
        in.readFully(ret);
        return ret;
    }
}
