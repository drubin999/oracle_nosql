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

import oracle.kv.table.FieldDef;

/**
 * Common builder for map and array.
 */
class CollectionBuilder extends TableBuilderBase {
    protected String description;
    protected FieldDef field;

    protected CollectionBuilder(String description) {
        this.description = description;
    }

    protected CollectionBuilder() {
        description = null;
    }

    @Override
    public TableBuilderBase addField(final FieldDef field1) {
        if (this.field != null) {
            throw new IllegalArgumentException
                ("Field for collection is already defined");
        }
        this.field = field1;
        return this;
    }

    @Override
    public TableBuilderBase setDescription(final String description) {
        this.description = description;
        return this;
    }

    @Override
    public boolean isCollectionBuilder() {
        return true;
    }
}
