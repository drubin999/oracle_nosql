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

import oracle.kv.Value;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.UserDataControl;

/**
 * Holds a Value for a request, optimized to avoid array allocations/copies.
 * Value is serialized on the client from a Value object and deserialized on
 * the service into a byte array.
 */
class RequestValue implements FastExternalizable {

    /* The value field is non-null on the client and null on the service. */
    private final Value value;
    /* The bytes field is non-null on the service and null on the client. */
    private final byte[] bytes;

    /**
     * Used by the client.
     */
    RequestValue(Value value) {
        this.value = value;
        this.bytes = null;
    }

    /**
     * Deserialize into byte array.
     * Used by the service when deserializing a request.
     */
    public RequestValue(ObjectInput in, short serialVersion)
        throws IOException {

        value = null;
        bytes = Value.readFastExternal(in, serialVersion);
    }

    /**
     * FastExternalizable writer.
     * Used by the client when serializing a request.
     */
    @Override
    public void writeFastExternal(ObjectOutput out, short serialVersion)
        throws IOException {

        if (value != null) {
            value.writeFastExternal(out, serialVersion);
        } else {
            Value.writeFastExternal(out, serialVersion, bytes);
        }
    }

    /**
     * Used by the service.
     */
    byte[] getBytes() {
        if (bytes == null) {
            /* Only occurs in tests when RMI is bypassed. */
            return value.toByteArray();
        }
        return bytes;
    }

    @Override
    public String toString() {
        return UserDataControl.displayValue(value, bytes);
    }
}
