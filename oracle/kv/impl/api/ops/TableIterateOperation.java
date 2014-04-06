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

package oracle.kv.impl.api.ops;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import oracle.kv.Direction;
import oracle.kv.impl.api.StoreIteratorParams;
import oracle.kv.impl.api.table.TargetTables;

/**
 * This is an intermediate class for a table iteration where the records
 * may or may not reside on the same partition.
 */
abstract public class TableIterateOperation extends MultiTableOperation {

    private final boolean majorComplete;
    private final Direction direction;
    private final int batchSize;
    private final byte[] resumeKey;

    public TableIterateOperation(OpCode opCode,
                                 StoreIteratorParams sip,
                                 TargetTables targetTables,
                                 boolean majorComplete,
                                 byte[] resumeKey) {
        super(opCode, sip.getParentKeyBytes(), targetTables, sip.getSubRange());
        this.majorComplete = majorComplete;
        this.direction = sip.getDirection();
        this.batchSize = sip.getBatchSize();
        this.resumeKey = resumeKey;
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    TableIterateOperation(OpCode opCode, ObjectInput in, short serialVersion)
        throws IOException {

        super(opCode, in, serialVersion);
        majorComplete = in.readBoolean();
        direction = Direction.getDirection(in.readUnsignedByte());
        batchSize = in.readInt();

        final int keyLen = in.readShort();
        if (keyLen < 0) {
            resumeKey = null;
        } else {
            resumeKey = new byte[keyLen];
            in.readFully(resumeKey);
        }
    }
    /**
     * FastExternalizable writer.  Must call superclass method first to write
     * common elements.
     */
    @Override
    public void writeFastExternal(ObjectOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);

        out.writeBoolean(majorComplete);
        out.writeByte(direction.ordinal());
        out.writeInt(batchSize);

        if (resumeKey == null) {
            out.writeShort(-1);
        } else {
            out.writeShort(resumeKey.length);
            out.write(resumeKey);
        }
    }

    Direction getDirection() {
        return direction;
    }

    int getBatchSize() {
        return batchSize;
    }

    byte[] getResumeKey() {
        return resumeKey;
    }

    boolean getMajorComplete() {
        return majorComplete;
    }
}
