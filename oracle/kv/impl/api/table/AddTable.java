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

package oracle.kv.impl.api.table;

import java.util.List;

/**
 * A TableChange to create/add a new table
 */
class AddTable extends TableChange {
    private static final long serialVersionUID = 1L;

    private final String name;
    private final String parentName;
    private final List<String> primaryKey;
    private final List<String> shardKey;
    private final FieldMap fields;
    private final boolean r2compat;
    private final int schemaId;
    private final String description;

    AddTable(TableImpl table, int seqNum) {
        super(seqNum);
        name = table.getName();
        final TableImpl parent = (TableImpl) table.getParent();
        parentName = (parent == null) ? null : parent.getFullName();
        primaryKey = table.getPrimaryKey();
        shardKey = table.getShardKey();
        fields = table.getFieldMap();
        r2compat = table.isR2compatible();
        schemaId = table.getSchemaId();
        description = table.getDescription();
    }

    @Override
    public boolean apply(TableMetadata md) {
        md.insertTable(name, parentName,
                       primaryKey, shardKey, fields,
                       r2compat, schemaId, description);
        return true;
    }
}
