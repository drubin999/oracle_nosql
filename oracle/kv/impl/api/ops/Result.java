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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import oracle.kv.Operation;
import oracle.kv.OperationExecutionException;
import oracle.kv.OperationResult;
import oracle.kv.Value;
import oracle.kv.Version;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.util.FastExternalizable;

/**
 * The result of running a request.  Result may contain a return value of the
 * request.  It may also contain an error, an update to some topology
 * information, or information about how the request was satisfied (such as the
 * forwarding path it took).
 */
public abstract class Result
    implements OperationResult, FastExternalizable {

    /**
     * The OpCode determines the result type for deserialization, and may be
     * useful for matching to the request OpCode.
     */
    private final OpCode opCode;

    /**
     * Constructs a request result that contains a value resulting from an
     * operation.
     */
    private Result(OpCode op) {
        opCode = op;
        assert op.checkResultType(this) :
        "Incorrect type " + getClass().getName() + " for " + op;
    }

    /**
     * FastExternalizable constructor.  Subclasses must call this constructor
     * before reading additional elements.
     *
     * The OpCode was read by readFastExternal.
     */
    Result(OpCode op,
           @SuppressWarnings("unused") ObjectInput in,
           @SuppressWarnings("unused") short serialVersion) {

        this(op);
    }

    /**
     * FastExternalizable factory for all Result subclasses.
     */
    public static Result readFastExternal(ObjectInput in,
                                          short serialVersion)
        throws IOException {

        final OpCode op = InternalOperation.getOpCode(in.readUnsignedByte());
        return op.readResult(in, serialVersion);
    }

    /**
     * FastExternalizable writer.  Subclasses must call this method before
     * writing additional elements.
     */
    @Override
    public void writeFastExternal(ObjectOutput out, short serialVersion)
        throws IOException {

        out.writeByte(opCode.ordinal());
    }

    /**
     * Gets the boolean result for all operations.
     *
     * @throws IllegalStateException if the result is the wrong type
     */
    @Override
    public abstract boolean getSuccess();

    /**
     * Gets the current Value result of a Get, Put or Delete operation.
     *
     * @throws IllegalStateException if the result is the wrong type
     */
    @Override
    public Value getPreviousValue() {
        throw new IllegalStateException
            ("result of type: " + getClass() + " does not contain a Value");
    }

    /**
     * Gets the current Version result of a Get, Put or Delete operation.
     *
     * @throws IllegalStateException if the result is the wrong type
     */
    @Override
    public Version getPreviousVersion() {
        throw new IllegalStateException
            ("result of type: " + getClass() +
             " does not contain a previous Version");
    }

    /**
     * Gets the new Version result of a Put operation.
     *
     * @throws IllegalStateException if the result is the wrong type
     */
    @Override
    public Version getNewVersion() {
        throw new IllegalStateException("result of type: " + getClass() +
                                        " does not contain a new Version");
    }

    /**
     * Gets the int result of a MultiDelete operation.
     *
     * @throws IllegalStateException if the result is the wrong type
     */
    public int getNDeletions() {
        throw new IllegalStateException
            ("result of type: " + getClass() + " does not contain a boolean");
    }

    /**
     * Gets the OperationExecutionException result of an Execute operation, or
     * null if no exception should be thrown.
     *
     * @throws IllegalStateException if the result is the wrong type
     */
    public OperationExecutionException
        getExecuteException(@SuppressWarnings("unused") List<Operation> ops) {

        throw new IllegalStateException
            ("result of type: " + getClass() +
             " does not contain an OperationExecutionException");
    }

    /**
     * Gets the OperationResult list result of an Execute operation, or null if
     * an OperationExecutionException should be thrown.
     *
     * @throws IllegalStateException if the result is the wrong type
     */
    public List<OperationResult> getExecuteResult() {
        throw new IllegalStateException
            ("result of type: " + getClass() +
             " does not contain a ExecuteResult");
    }

    /**
     * Gets the ResultKeyValueVersion list result of an iterate operation.
     *
     * @throws IllegalStateException if the result is the wrong type
     */
    public List<ResultKeyValueVersion> getKeyValueVersionList() {
        throw new IllegalStateException
            ("result of type: " + getClass() +
             " does not contain a ResultKeyValueVersion list");
    }

    /**
     * Gets the key list result of an iterate-keys operation.
     *
     * @throws IllegalStateException if the result is the wrong type
     */
    public List<byte[]> getKeyList() {
        throw new IllegalStateException
            ("result of type: " + getClass() +
             " does not contain a key list");
    }

    /**
     * Gets the ResultTableIndex list result of a table index iterate
     * operation.
     *
     * @throws IllegalStateException if the result is the wrong type
     */
    public List<ResultTableIndex> getTableIndexList() {
        throw new IllegalStateException
            ("result of type: " + getClass() +
             " does not contain a ResultTableIndex list");
    }

    /**
     * Gets the has-more-elements result of an iterate or iterate-keys
     * operation. True returned if the iteration is complete.
     *
     * @throws IllegalStateException if the result is the wrong type
     */
    public boolean hasMoreElements() {
        throw new IllegalStateException
            ("result of type: " + getClass() +
             " does not contain an iteration result");
    }

    /**
     * The number of records returned or processed as part of this operation.
     * Single operations only apply to one record, but the multi, iterate, or
     * execute operations will work on multiple records, and should override
     * this to provide the correct number of operations.
     */
    public int getNumRecords() {
        return 1;
    }

    /* The result of a Get operation. */
    static class GetResult extends ValueVersionResult {

        GetResult(OpCode opCode, ResultValueVersion valueVersion) {
            super(opCode, valueVersion);
        }

        /**
         * FastExternalizable constructor.  Must call superclass constructor
         * first to read common elements.
         */
        GetResult(OpCode opCode, ObjectInput in, short serialVersion)
            throws IOException {

            super(opCode, in, serialVersion);
        }

        @Override
        public boolean getSuccess() {
            return getPreviousValue() != null;
        }
    }

    /* The result of a Put operation. */
    static class PutResult extends ValueVersionResult {

        private final Version newVersion;

        PutResult(OpCode opCode,
                  ResultValueVersion prevVal,
                  Version newVersion) {
            super(opCode, prevVal);
            this.newVersion = newVersion;
        }

        /**
         * FastExternalizable constructor.  Must call superclass constructor
         * first to read common elements.
         */
        PutResult(OpCode opCode, ObjectInput in, short serialVersion)
            throws IOException {

            super(opCode, in, serialVersion);
            if (in.read() != 0) {
                newVersion = new Version(in, serialVersion);
            } else {
                newVersion = null;
            }
        }

        /**
         * FastExternalizable writer.  Must call superclass method first to
         * write common elements.
         */
        @Override
        public void writeFastExternal(ObjectOutput out, short serialVersion)
            throws IOException {

            super.writeFastExternal(out, serialVersion);
            if (newVersion != null) {
                out.write(1);
                newVersion.writeFastExternal(out, serialVersion);
            } else {
                out.write(0);
            }
        }

        @Override
        public Version getNewVersion() {
            return newVersion;
        }

        @Override
        public boolean getSuccess() {
            return newVersion != null;
        }
    }

    /* The result of a Delete operation. */
    static class DeleteResult extends ValueVersionResult {

        private final boolean success;

        DeleteResult(OpCode opCode,
                     ResultValueVersion prevVal,
                     boolean success) {
            super(opCode, prevVal);
            this.success = success;
        }

        /**
         * FastExternalizable constructor.  Must call superclass constructor
         * first to read common elements.
         */
        DeleteResult(OpCode opCode, ObjectInput in, short serialVersion)
            throws IOException {

            super(opCode, in, serialVersion);
            success = in.readBoolean();
        }

        /**
         * FastExternalizable writer.  Must call superclass method first to
         * write common elements.
         */
        @Override
        public void writeFastExternal(ObjectOutput out, short serialVersion)
            throws IOException {

            super.writeFastExternal(out, serialVersion);
            out.writeBoolean(success);
        }

        @Override
        public boolean getSuccess() {
            return success;
        }
    }

    /* The result of a Delete operation. */
    static class MultiDeleteResult extends Result {

        private final int nDeletions;

        MultiDeleteResult(OpCode opCode, int nDeletions) {
            super(opCode);
            this.nDeletions = nDeletions;
        }

        /**
         * FastExternalizable constructor.  Must call superclass constructor
         * first to read common elements.
         */
        MultiDeleteResult(OpCode opCode, ObjectInput in, short serialVersion)
            throws IOException {

            super(opCode, in, serialVersion);
            nDeletions = in.readInt();
        }

        /**
         * FastExternalizable writer.  Must call superclass method first to
         * write common elements.
         */
        @Override
        public void writeFastExternal(ObjectOutput out, short serialVersion)
            throws IOException {

            super.writeFastExternal(out, serialVersion);
            out.writeInt(nDeletions);
        }

        @Override
        public int getNDeletions() {
            return nDeletions;
        }

        @Override
        public boolean getSuccess() {
            return nDeletions > 0;
        }

        @Override
        public int getNumRecords() {
            return nDeletions;
        }
    }

    /* Base class for results with a Value and Version. */
    static abstract class ValueVersionResult extends Result {

        private final ResultValue resultValue;
        private final Version version;

        ValueVersionResult(OpCode op, ResultValueVersion valueVersion) {
            super(op);
            if (valueVersion != null) {
                resultValue = (valueVersion.getValueBytes() != null) ?
                    (new ResultValue(valueVersion.getValueBytes())) :
                    null;
                version = valueVersion.getVersion();
            } else {
                resultValue = null;
                version = null;
            }
        }

        /**
         * FastExternalizable constructor.  Must call superclass constructor
         * first to read common elements.
         */
        ValueVersionResult(OpCode op, ObjectInput in, short serialVersion)
            throws IOException {

            super(op, in, serialVersion);
            if (in.read() != 0) {
                resultValue = new ResultValue(in, serialVersion);
            } else {
                resultValue = null;
            }
            if (in.read() != 0) {
                version = new Version(in, serialVersion);
            } else {
                version = null;
            }
        }

        /**
         * FastExternalizable writer.  Must call superclass method first to
         * write common elements.
         */
        @Override
        public void writeFastExternal(ObjectOutput out, short serialVersion)
            throws IOException {

            super.writeFastExternal(out, serialVersion);
            if (resultValue != null) {
                out.write(1);
                resultValue.writeFastExternal(out, serialVersion);
            } else {
                out.write(0);
            }
            if (version != null) {
                out.write(1);
                version.writeFastExternal(out, serialVersion);
            } else {
                out.write(0);
            }
        }

        @Override
        public Value getPreviousValue() {
            return (resultValue == null) ? null : resultValue.getValue();
        }

        @Override
        public Version getPreviousVersion() {
            return version;
        }
    }

    static class NOPResult extends Result {

        NOPResult(ObjectInput in, short serialVersion) {
            super(OpCode.NOP, in, serialVersion);
        }

        NOPResult() {
            super(OpCode.NOP);
        }

        @Override
        public boolean getSuccess() {
            return true;
        }

        /* NOPs don't actually handle any records. */
        @Override
        public int getNumRecords() {
            return 0;
        }
    }

    /* The result of an Execute operation. */
    static class ExecuteResult extends Result {

        private final boolean success;
        private final List<Result> successResults;
        private final int failureIndex;
        private final Result failureResult;

        ExecuteResult(OpCode opCode, List<Result> successResults) {
            super(opCode);
            this.successResults = successResults;
            failureIndex = -1;
            failureResult = null;
            success = true;
        }

        ExecuteResult(OpCode opCode,
                      int failureIndex,
                      Result failureResult) {
            super(opCode);
            this.failureIndex = failureIndex;
            this.failureResult = failureResult;
            successResults = null;
            success = false;
        }

        /**
         * FastExternalizable constructor.  Must call superclass constructor
         * first to read common elements.
         */
        ExecuteResult(OpCode opCode, ObjectInput in, short serialVersion)
            throws IOException {

            super(opCode, in, serialVersion);
            success = in.readBoolean();
            if (success) {
                final int listSize = in.readInt();
                successResults = new ArrayList<Result>(listSize);
                for (int i = 0; i < listSize; i += 1) {
                    final Result result =
                        Result.readFastExternal(in, serialVersion);
                    successResults.add(result);
                }
                failureIndex = -1;
                failureResult = null;
            } else {
                failureIndex = in.readInt();
                failureResult = Result.readFastExternal(in, serialVersion);
                successResults = new ArrayList<Result>();
            }
        }

        /**
         * FastExternalizable writer.  Must call superclass method first to
         * write common elements.
         */
        @Override
        public void writeFastExternal(ObjectOutput out, short serialVersion)
            throws IOException {

            super.writeFastExternal(out, serialVersion);
            out.writeBoolean(success);
            if (success) {
                out.writeInt(successResults.size());
                for (final Result result : successResults) {
                    result.writeFastExternal(out, serialVersion);
                }
            } else {
                out.writeInt(failureIndex);
                failureResult.writeFastExternal(out, serialVersion);
            }
        }

        @Override
        public boolean getSuccess() {
            return success;
        }

        @Override
        public OperationExecutionException
            getExecuteException(List<Operation> ops) {

            if (success) {
                return null;
            }
            return new OperationExecutionException
                (ops.get(failureIndex), failureIndex, failureResult);
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public List<OperationResult> getExecuteResult() {
            if (!success) {
                return null;
            }
            /* Cast: a Result is an OperationResult. */
            return (List) Collections.unmodifiableList(successResults);
        }

        @Override
        public int getNumRecords() {
            if (!success) {
                return 0;
            }
            return successResults.size();
        }
    }

    /* The result of a MultiGetIterate or StoreIterate operation. */
    static class IterateResult extends Result {

        private final List<ResultKeyValueVersion> elements;
        private final boolean moreElements;

        IterateResult(OpCode opCode,
                      List<ResultKeyValueVersion> elements,
                      boolean moreElements) {
            super(opCode);
            this.elements = elements;
            this.moreElements = moreElements;
        }

        /**
         * FastExternalizable constructor.  Must call superclass constructor
         * first to read common elements.
         */
        IterateResult(OpCode opCode, ObjectInput in, short serialVersion)
            throws IOException {

            super(opCode, in, serialVersion);

            final int listSize = in.readInt();
            elements = new ArrayList<ResultKeyValueVersion>(listSize);
            for (int i = 0; i < listSize; i += 1) {
                elements.add(new ResultKeyValueVersion(in, serialVersion));
            }

            moreElements = in.readBoolean();
        }

        /**
         * FastExternalizable writer.  Must call superclass method first to
         * write common elements.
         */
        @Override
        public void writeFastExternal(ObjectOutput out, short serialVersion)
            throws IOException {

            super.writeFastExternal(out, serialVersion);

            out.writeInt(elements.size());
            for (final ResultKeyValueVersion elem : elements) {
                elem.writeFastExternal(out, serialVersion);
            }

            out.writeBoolean(moreElements);
        }

        @Override
        public boolean getSuccess() {
            return elements.size() > 0;
        }

        @Override
        public List<ResultKeyValueVersion> getKeyValueVersionList() {
            return elements;
        }

        @Override
        public boolean hasMoreElements() {
            return moreElements;
        }

        @Override
        public int getNumRecords() {
            return elements.size();
        }
    }

    /* The result of a MultiGetKeysIterate or StoreKeysIterate operation. */
    static class KeysIterateResult extends Result {

        private final List<byte[]> elements;
        private final boolean moreElements;

        KeysIterateResult(OpCode opCode,
                          List<byte[]> elements,
                          boolean moreElements) {
            super(opCode);
            this.elements = elements;
            this.moreElements = moreElements;
        }

        /**
         * FastExternalizable constructor.  Must call superclass constructor
         * first to read common elements.
         */
        KeysIterateResult(OpCode opCode, ObjectInput in, short serialVersion)
            throws IOException {

            super(opCode, in, serialVersion);

            final int listSize = in.readInt();
            elements = new ArrayList<byte[]>(listSize);
            for (int i = 0; i < listSize; i += 1) {
                final int keyLen = in.readShort();
                final byte[] key = new byte[keyLen];
                in.readFully(key);
                elements.add(key);
            }

            moreElements = in.readBoolean();
        }

        /**
         * FastExternalizable writer.  Must call superclass method first to
         * write common elements.
         */
        @Override
        public void writeFastExternal(ObjectOutput out, short serialVersion)
            throws IOException {

            super.writeFastExternal(out, serialVersion);

            out.writeInt(elements.size());
            for (final byte[] key : elements) {
                out.writeShort(key.length);
                out.write(key);
            }

            out.writeBoolean(moreElements);
        }

        @Override
        public boolean getSuccess() {
            return elements.size() > 0;
        }

        @Override
        public List<byte[]> getKeyList() {
            return elements;
        }

        @Override
        public boolean hasMoreElements() {
            return moreElements;
        }

        @Override
        public int getNumRecords() {
            return elements.size();
        }
    }

    /* The result of a table index iterate option. */
    static class TableIterateResult extends Result {

        private final List<ResultTableIndex> elements;
        private final boolean moreElements;

        TableIterateResult(OpCode opCode,
                           List<ResultTableIndex> elements,
                           boolean moreElements) {
            super(opCode);
            this.elements = elements;
            this.moreElements = moreElements;
        }

        /**
         * FastExternalizable constructor.  Must call superclass constructor
         * first to read common elements.
         */
        TableIterateResult(OpCode opCode, ObjectInput in, short serialVersion)
            throws IOException {

            super(opCode, in, serialVersion);

            final int listSize = in.readInt();
            elements = new ArrayList<ResultTableIndex>(listSize);
            for (int i = 0; i < listSize; i += 1) {
                elements.add(new ResultTableIndex(in, serialVersion));
            }

            moreElements = in.readBoolean();
        }

        /**
         * FastExternalizable writer.  Must call superclass method first to
         * write common elements.
         */
        @Override
        public void writeFastExternal(ObjectOutput out, short serialVersion)
            throws IOException {

            super.writeFastExternal(out, serialVersion);

            out.writeInt(elements.size());
            for (final ResultTableIndex elem : elements) {
                elem.writeFastExternal(out, serialVersion);
            }

            out.writeBoolean(moreElements);
        }

        @Override
        public boolean getSuccess() {
            return elements.size() > 0;
        }

        @Override
        public List<ResultTableIndex> getTableIndexList() {
            return elements;
        }

        @Override
        public boolean hasMoreElements() {
            return moreElements;
        }

        @Override
        public int getNumRecords() {
            return elements.size();
        }
    }
}
