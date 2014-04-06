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

import oracle.kv.impl.util.FastExternalizable;

/**
 * Holds results of table index iteration which includes primary key and index
 * key byte arrays.  This is all of the information that is available in a
 * single secondary scan without doing an additional database read of the
 * primary data.
 */
public class ResultTableIndex implements FastExternalizable {

    private final byte[] primaryKeyBytes;
    private final byte[] indexKeyBytes;

    public ResultTableIndex(byte[] primaryKeyBytes,
                            byte[] indexKeyBytes) {
        this.primaryKeyBytes = primaryKeyBytes;
        this.indexKeyBytes = indexKeyBytes;
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor
     * first to read common elements.
     */
    @SuppressWarnings("unused")
    public ResultTableIndex(ObjectInput in, short serialVersion)
        throws IOException {

        int keyLen = in.readShort();
        primaryKeyBytes = new byte[keyLen];
        in.readFully(primaryKeyBytes);
        keyLen = in.readShort();
        indexKeyBytes = new byte[keyLen];
        in.readFully(indexKeyBytes);
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to
     * write common elements.
     */
    @SuppressWarnings("unused")
    @Override
    public void writeFastExternal(ObjectOutput out, short serialVersion)
        throws IOException {

        out.writeShort(primaryKeyBytes.length);
        out.write(primaryKeyBytes);
        out.writeShort(indexKeyBytes.length);
        out.write(indexKeyBytes);
    }

    public byte[] getPrimaryKeyBytes() {
        return primaryKeyBytes;
    }

    public byte[] getIndexKeyBytes() {
        return indexKeyBytes;
    }
}
