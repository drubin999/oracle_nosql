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

package oracle.kv.impl.api.avro;

import java.util.EnumSet;

/**
 * Schema status values.  Stored as part of the schema key.
 */
public enum AvroSchemaStatus {

    /**
     * Schema is in use and accessible from the client.  This is intentionally
     * the first declared value (lowest ordinal) so that when enumerating an
     * EnumSet we will process it first; we rely on this in
     * SchemaAccessor.readSchema.
     */
    ACTIVE("A"),

    /**
     * Schema is disabled and not accessible to clients.  We disable rather
     * than delete schemas, so they can be reinstated if necessary.
     */
    DISABLED("D");

    private final String code;

    private AvroSchemaStatus(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    static final EnumSet<AvroSchemaStatus> ALL =
        EnumSet.allOf(AvroSchemaStatus.class);

    static AvroSchemaStatus fromCode(String code) {
        for (AvroSchemaStatus status : ALL) {
            if (code.equals(status.getCode())) {
                return status;
            }
        }
        return null;
    }
}
