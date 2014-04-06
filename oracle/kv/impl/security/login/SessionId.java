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
package oracle.kv.impl.security.login;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Arrays;

import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.FastExternalizable;

/**
 * SessionId denotes the identity of a login session.
 */
public final class SessionId implements Serializable, FastExternalizable {

    /*
     * Maximum allowable size of a session id
     */
    public static final int SESSION_ID_MAX_SIZE = 127;

    private static final long serialVersionUID = 1;

    /* Bit mask for indication that the loginToken has an allocator encoded */
    private static final int HAS_ALLOCATOR = 0x1;

    /*
     * The scope of the idValue
     */
    private IdScope idValueScope;

    /*
     * The scope-local session identifier value
     */
    private byte[] idValue;

    /* Allocating resource for non-persistent tokens */
    private ResourceId allocator;

    /**
     * The scope of the ID. The scope generally depends on the allocator, and
     * the extent to which the session manager can be successfully referenced
     * by other Components.
     */
    public enum IdScope {

        /*
         * PERSISTENT scope refers to sessions which are stored persistently in
         * the KVStore.
         */
        PERSISTENT,

        /*
         * LOCAL scope refers to sessions which are stored
         * transiently within a KVStore component and which cannot be correctly
         * interpreted by components in SNs other than the one that created the
         * session.
         */
        LOCAL,

        /*
         * STORE scope refers to sessions which are stored
         * transiently within a KVStore component and which CAN be interpreted
         * by components in SNs other than the one that created the session,
         * provided that topology information is available.
         */
        STORE
    }

    private static IdScope getScope(int ordinal) {
        try {
            return IdScope.values()[ordinal];
        } catch (ArrayIndexOutOfBoundsException aioobe) {
            throw new IllegalArgumentException("invalid scope: " + ordinal);
        }
    }

    /**
     * Creates a session id for a persistent session.
     * @param idValue The session identifier value
     */
    public SessionId(byte[] idValue) {
        if (idValue.length > SESSION_ID_MAX_SIZE) {
            throw new IllegalArgumentException(
                "sessionId length exceeds limit");
        }
        this.idValueScope = IdScope.PERSISTENT;
        this.idValue = Arrays.copyOf(idValue, idValue.length);
    }

    /**
     * Creates a session id for a non-persistent session.
     * @param idValue The session identifier calue
     * @param idValueScope Must be LOCAL or NON_LOCAL
     * @param allocator The component that allocated this id
     */
    public SessionId(byte[] idValue,
                     IdScope idValueScope,
                     ResourceId allocator) {

        if (idValueScope == IdScope.PERSISTENT) {
            throw new IllegalArgumentException("invalid scope");
        }
        if (idValue.length > SESSION_ID_MAX_SIZE) {
            throw new IllegalArgumentException(
                "sessionId length exceeds limit");
        }
        this.idValueScope = idValueScope;
        this.idValue = Arrays.copyOf(idValue, idValue.length);
        this.allocator = allocator;
    }

    /* for FastExternalizable */
    public SessionId(ObjectInput in, short serialVersion)
        throws IOException {

        final int flagByte = in.readByte();
        idValueScope = getScope(in.readByte());

        final int valueLen = in.readByte();
        idValue = new byte[valueLen];
        in.read(idValue, 0, valueLen);
        if ((flagByte & HAS_ALLOCATOR) != 0) {
            allocator = ResourceId.readFastExternal(in, serialVersion);
        }
    }

    /**
     * Implementation of writeFastExternal for the FastExternalizable
     * interface.
     */
    @Override
    public void writeFastExternal(ObjectOutput out, short serialVersion)
        throws IOException {

        int flagByte = 0;
        if (allocator != null) {
            flagByte |= HAS_ALLOCATOR;
        }
        out.writeByte(flagByte);
        out.writeByte(idValueScope.ordinal());
        out.writeByte(idValue.length);
        out.write(idValue, 0, idValue.length);
        if (allocator != null) {
            allocator.writeFastExternal(out, serialVersion);
        }
    }

    /**
     * Return the session Id value for the token.
     */
    public byte[] getIdValue() {
        return idValue;
    }

    /**
     * Return the session scope for the token.
     */
    public IdScope getIdValueScope() {
        return idValueScope;
    }

    /**
     * Return the allocation scope. If the scope is PERSISTENT, this will
     * return null.
     */
    public ResourceId getAllocator() {
        return allocator;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || other.getClass() != SessionId.class) {
            return false;
        }

        final SessionId otherToken = (SessionId) other;
        if (idValueScope == otherToken.idValueScope &&
            Arrays.equals(idValue, otherToken.idValue) &&
            ((allocator == null && otherToken.allocator == null) ||
             (allocator != null && allocator.equals(otherToken.allocator)))) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(idValue);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("SessionId: scope=");
        sb.append(idValueScope);
        sb.append(", hashId()=");
        sb.append(hashId());
        sb.append(", allocator=");
        sb.append(allocator);
        return sb.toString();
    }

    /**
     * Computes a securely hashed identifier for the session id. The hash
     * values for two distinct ids are not guaranteed to be unique.
     */
    public int hashId() {
        return LoginSession.Id.hashId(idValue);
    }
}
