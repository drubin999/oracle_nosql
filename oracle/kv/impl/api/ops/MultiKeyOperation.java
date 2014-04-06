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

import com.sleepycat.je.DatabaseEntry;

import oracle.kv.Depth;
import oracle.kv.KeyRange;
import oracle.kv.UnauthorizedException;
import oracle.kv.impl.api.ops.OperationHandler.KVAuthorizer;
import oracle.kv.impl.util.UserDataControl;

/**
 * A multi-key operation has a parent key, optional KeyRange and depth.
 */
abstract class MultiKeyOperation extends InternalOperation {

    private static final KVAuthorizer UNIVERSAL_AUTHORIZER =
        new KVAuthorizer() {
            @Override
            public boolean allowAccess(DatabaseEntry keyEntry) {
                return true;
            }

            @Override
            public boolean allowFullAccess() {
                return true;
            }
        };

    /**
     * The parent key, or null.
     */
    private final byte[] parentKey;

    /**
     * Sub-key range of traversal, or null.
     */
    private final KeyRange subRange;

    /**
     * Depth of traversal, always non-null.
     */
    private final Depth depth;

    /**
     * Constructs a multi-key operation.
     *
     * For subclasses, allows passing OpCode.
     */
    MultiKeyOperation(OpCode opCode,
                      byte[] parentKey,
                      KeyRange subRange,
                      Depth depth) {
        super(opCode);
        this.parentKey = parentKey;
        this.subRange = subRange;
        this.depth = depth;
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     *
     * For subclasses, allows passing OpCode.
     */
    MultiKeyOperation(OpCode opCode, ObjectInput in, short serialVersion)
        throws IOException {

        super(opCode, in, serialVersion);

        final int keyLen = in.readShort();
        if (keyLen < 0) {
            parentKey = null;
        } else {
            parentKey = new byte[keyLen];
            in.readFully(parentKey);
        }

        if (in.read() == 0) {
            subRange = null;
        } else {
            subRange = new KeyRange(in, serialVersion);
        }

        depth = Depth.getDepth(in.readUnsignedByte());
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to write
     * common elements.
     */
    @Override
    public void writeFastExternal(ObjectOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);

        if (parentKey == null) {
            out.writeShort(-1);
        } else {
            out.writeShort(parentKey.length);
            out.write(parentKey);
        }

        if (subRange == null) {
            out.write(0);
        } else {
            out.write(1);
            subRange.writeFastExternal(out, serialVersion);
        }

        out.writeByte(depth.ordinal());
    }

    /**
     * Perform an initial permission check based on the parent key.
     * If the parent key is within the private server space and this is not an
     * internal request, throw an UnauthorizedException.  Otherwise, return
     * a KVAuthorizer instance that will determine on a per-KV entry basis
     * whether entries are visible to the user. This implements a policy
     * that allows callers to iterate over the store without generating errors
     * if they come across something that they aren't allowed access to, but
     * if the specifically ask for something they aren't allowed to access,
     * we throw an exception.
     */
    KVAuthorizer checkPermission() {
        if (parentKey != null &&
            isPrivateAccess(parentKey) &&
            !isInternalRequestor()) {
                throw new UnauthorizedException(
                    "Illegal access to internal keyspace");
        }

        if ((parentKey != null && !mayBePrivateAccess(parentKey)) ||
            isInternalRequestor()) {

            /*
             * Entries either cannot possible fall into the server private
             * key space or else we have an internal requestor, so each access
             * is guaranteed to be authorized.
             */
            return UNIVERSAL_AUTHORIZER;
        }

        /*
         * We have a user-level requestor, and either the parent key is null or
         * the parent key is not null, but is too short to be sure that no
         * private access will result, so entries could possibly fall into the
         * server private key space.  Return an authorizer that will check
         * keys on each access.
         */
        return new KVAuthorizer() {
            @Override
            public boolean allowAccess(DatabaseEntry keyEntry) {
                return !isPrivateAccess(keyEntry.getData());
            }
            @Override
            public boolean allowFullAccess() {
                return false;
            }
        };
    }

    byte[] getParentKey() {
        return parentKey;
    }

    KeyRange getSubRange() {
        return subRange;
    }

    Depth getDepth() {
        return depth;
    }

    @Override
    public String toString() {
        return super.toString() + 
            " parentKey: " + UserDataControl.displayKey(parentKey) +
            " subRange: " + UserDataControl.displayKeyRange(subRange) +
            " depth: " + depth;
    }
}
