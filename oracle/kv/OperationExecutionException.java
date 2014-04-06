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

/**
 * Used to indicate a failure to execute a sequence of operations.
 *
 * @see KVStore#execute execute
 */
public class OperationExecutionException extends ContingencyException {

    private static final long serialVersionUID = 1L;

    private final Operation failedOperation;
    private final int failedOperationIndex;
    private final OperationResult failedOperationResult;

    /**
     * For internal use only.
     * @hidden
     */
    public OperationExecutionException(Operation failedOperation,
                                       int failedOperationIndex,
                                       OperationResult failedOperationResult) {
        super("Failed operation: " + failedOperation +
              ", index: " + failedOperationIndex +
              ", result: " + failedOperationResult);
        this.failedOperation = failedOperation;
        this.failedOperationIndex = failedOperationIndex;
        this.failedOperationResult = failedOperationResult;
    }

    /**
     * The operation that caused the execution to be aborted.
     */
    public Operation getFailedOperation() {
        return failedOperation;
    }

    /**
     * The result of the operation that caused the execution to be aborted.
     */
    public OperationResult getFailedOperationResult() {
        return failedOperationResult;
    }

    /**
     * The list index of the operation that caused the execution to be aborted.
     */
    public int getFailedOperationIndex() {
        return failedOperationIndex;
    }
}
