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

import oracle.kv.ReturnValueVersion;
import oracle.kv.Version;
import oracle.kv.impl.topo.PartitionId;

import com.sleepycat.je.Transaction;

/**
 * Inserts a key/data pair.
 */
public class DeleteIfVersion extends Delete {

    private final Version matchVersion;

    /**
     * Constructs a delete-if-version operation.
     */
    public DeleteIfVersion(byte[] keyBytes,
                           ReturnValueVersion.Choice prevValChoice,
                           Version matchVersion) {
        this(keyBytes, prevValChoice, matchVersion, 0);
    }

    /**
     * Constructs a delete-if-version operation with a table id.
     */
    public DeleteIfVersion(byte[] keyBytes,
                           ReturnValueVersion.Choice prevValChoice,
                           Version matchVersion,
                           long tableId) {
        super(OpCode.DELETE_IF_VERSION, keyBytes, prevValChoice, tableId);
        this.matchVersion = matchVersion;
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    DeleteIfVersion(ObjectInput in, short serialVersion)
        throws IOException {

        super(OpCode.DELETE_IF_VERSION, in, serialVersion);
        matchVersion = new Version(in, serialVersion);
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to write
     * common elements.
     */
    @Override
    public void writeFastExternal(ObjectOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        matchVersion.writeFastExternal(out, serialVersion);
    }

    @Override
    public Result execute(Transaction txn,
                          PartitionId partitionId,
                          OperationHandler operationHandler) {

        checkPermission();
        TableOperationHandler.checkTable(operationHandler, getTableId());

        final ReturnResultValueVersion prevVal =
            new ReturnResultValueVersion(getReturnValueVersionChoice());

        final boolean result = operationHandler.deleteIfVersion
            (txn, partitionId, getKeyBytes(), matchVersion, prevVal);

        return new Result.DeleteResult(getOpCode(), prevVal.getValueVersion(),
                                       result);
    }

    @Override
    public String toString() {
        return super.toString() + " MatchVersion: " + matchVersion;
    }
}
