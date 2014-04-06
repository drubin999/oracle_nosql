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

import java.io.Serializable;

/**
 * Holds the metadata that is stored along with a schema, including the schema
 * status that is part of the key.
 *
 * This serializable class is used in the admin CommandService RMI interface.
 * Fields may be added without adding new remote methods.
 */
public class AvroSchemaMetadata implements Serializable {

    private static final long serialVersionUID = 1L;

    private final AvroSchemaStatus status;
    private final long timeModified;
    private final String byUser;
    private final String fromMachine;

    public AvroSchemaMetadata(AvroSchemaStatus status,
                              long timeModified,
                              String byUser,
                              String fromMachine) {
        this.status = status;
        this.timeModified = timeModified;
        this.byUser = byUser;
        this.fromMachine = fromMachine;
    }
    
    public AvroSchemaStatus getStatus() {
        return status;
    }

    public long getTimeModified() {
        return timeModified;
    }
    
    public String getByUser() {
        return byUser;
    }

    public String getFromMachine() {
        return fromMachine;
    }
}
