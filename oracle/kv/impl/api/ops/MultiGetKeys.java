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
import java.util.ArrayList;
import java.util.List;

import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.KeyRange;
import oracle.kv.impl.api.ops.OperationHandler.KVAuthorizer;
import oracle.kv.impl.topo.PartitionId;

import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Transaction;

/**
 * A multi-get-keys operation.
 */
public class MultiGetKeys extends MultiKeyOperation {

    /**
     * Construct a multi-get operation.
     */
    public MultiGetKeys(byte[] parentKey, KeyRange subRange, Depth depth) {
        super(OpCode.MULTI_GET_KEYS, parentKey, subRange, depth);
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    MultiGetKeys(ObjectInput in, short serialVersion)
        throws IOException {

        super(OpCode.MULTI_GET_KEYS, in, serialVersion);
    }

    @Override
    public Result execute(Transaction txn,
                          PartitionId partitionId,
                          OperationHandler operationHandler) {

        final KVAuthorizer kvAuth = checkPermission();

        final List<byte[]> results = new ArrayList<byte[]>();

        final boolean moreElements = operationHandler.iterateKeys
            (txn, partitionId, getParentKey(), true /*majorPathComplete*/,
             getSubRange(), getDepth(), Direction.FORWARD, 0 /*batchSize*/,
             null /*resumeKey*/, CursorConfig.DEFAULT, results, kvAuth);

        assert (!moreElements);

        return new Result.KeysIterateResult(getOpCode(), results,
                                            moreElements);
    }
}
