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
import oracle.kv.Version;
import oracle.kv.impl.util.FastExternalizable;

/**
 * Holds key and value as byte arrays to avoid conversion to Key and Value
 * objects on the service side.
 */
public class ResultKeyValueVersion implements FastExternalizable {

    private final byte[] keyBytes;
    private final ResultValue resultValue;
    private final Version version;

    public ResultKeyValueVersion(byte[] keyBytes,
                                 byte[] valueBytes,
                                 Version version) {
        this.keyBytes = keyBytes;
        this.resultValue = new ResultValue(valueBytes);
        this.version = version;
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor
     * first to read common elements.
     */
    public ResultKeyValueVersion(ObjectInput in, short serialVersion)
        throws IOException {

        final int keyLen = in.readShort();
        keyBytes = new byte[keyLen];
        in.readFully(keyBytes);
        resultValue = new ResultValue(in, serialVersion);
        version = new Version(in, serialVersion);
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to
     * write common elements.
     */
    @Override
    public void writeFastExternal(ObjectOutput out, short serialVersion)
        throws IOException {

        out.writeShort(keyBytes.length);
        out.write(keyBytes);
        resultValue.writeFastExternal(out, serialVersion);
        version.writeFastExternal(out, serialVersion);
    }

    public byte[] getKeyBytes() {
        return keyBytes;
    }

    public Value getValue() {
        return resultValue.getValue();
    }

    public Version getVersion() {
        return version;
    }
}
