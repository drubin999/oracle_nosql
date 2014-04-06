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

import oracle.kv.Depth;
import oracle.kv.KeyRange;
import oracle.kv.impl.api.ops.OperationHandler.KVAuthorizer;
import oracle.kv.impl.topo.PartitionId;

import com.sleepycat.je.Transaction;

/**
 * A multi-delete operation deletes records in the KV Store.
 */
public class MultiDelete extends MultiKeyOperation {

    /** LOB suffix is present at version 2 and greater. */
    private static final short LOB_SERIAL_VERSION = 2;

    /**
     * The LOB suffix bytes sent to the RN, so the RN can use them to ensure
     * that LOB objects in the range are not deleted.  Is null when an R1
     * client sends the request.
     */
    private final byte[] lobSuffixBytes;

    /**
     * Constructs a multi-delete operation.
     */
    public MultiDelete(byte[] parentKey,
                       KeyRange subRange,
                       Depth depth,
                       byte[] lobSuffixBytes) {
        super(OpCode.MULTI_DELETE, parentKey, subRange, depth);
        this.lobSuffixBytes = lobSuffixBytes;
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    MultiDelete(ObjectInput in, short serialVersion)
        throws IOException {

        super(OpCode.MULTI_DELETE, in, serialVersion);

        if (serialVersion >= LOB_SERIAL_VERSION) {
            final int suffixLen = in.readShort();
            if (suffixLen == 0) {
                lobSuffixBytes = null;
            } else {
                lobSuffixBytes = new byte[suffixLen];
                in.readFully(lobSuffixBytes);
            }
        } else {
            lobSuffixBytes = null;
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

        if (serialVersion >= LOB_SERIAL_VERSION) {
            if ((lobSuffixBytes != null) && (lobSuffixBytes.length > 0)) {
                out.writeShort(lobSuffixBytes.length);
                out.write(lobSuffixBytes);
            } else {
                out.writeShort(0);
            }
        }
    }

    @Override
    public Result execute(Transaction txn,
                          PartitionId partitionId,
                          OperationHandler operationHandler) {

        final KVAuthorizer kvAuth = checkPermission();

        final int result = operationHandler.multiDelete
            (txn, partitionId, getParentKey(), getSubRange(), getDepth(),
             lobSuffixBytes, kvAuth);

        return new Result.MultiDeleteResult(getOpCode(), result);
    }
}
