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
import java.util.Map;

import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.RecordDef;

public class PrimaryKeyImpl extends RowImpl implements PrimaryKey {
    private static final long serialVersionUID = 1L;

    PrimaryKeyImpl(RecordDef field, TableImpl table) {
        super(field, table);
    }

    PrimaryKeyImpl(RecordDef field, TableImpl table,
                   Map<String, FieldValue> fields) {
        super(field, table, fields);
    }

    private PrimaryKeyImpl(PrimaryKeyImpl other) {
        super(other);
    }

    @Override
    public PrimaryKeyImpl clone() {
        return new PrimaryKeyImpl(this);
    }

    @Override
    public PrimaryKey asPrimaryKey() {
        return this;
    }

    @Override
    public boolean isPrimaryKey() {
        return true;
    }

    @Override
    FieldDef validateNameAndType(String name,
                                 FieldDef.Type type) {
        FieldDef def = super.validateNameAndType(name, type);
        if (!table.getPrimaryKey().contains(name)) {
            throw new IllegalArgumentException
                ("Field is not part of PrimaryKey: " + name);
        }
        return def;
    }

    @Override
    public boolean equals(Object other) {
        if (super.equals(other)) {
            return (other != null && other instanceof PrimaryKeyImpl);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    /**
     * Overrides for RecordValueImpl
     */
    @Override
    int getNumFields() {
        return table.getPrimaryKey().size();
    }

    @Override
    FieldDef getField(String fieldName) {
        FieldDef def = getDefinition().getField(fieldName);
        if (def != null && table.getPrimaryKey().contains(fieldName)) {
            return def;
        }
        return null;
    }

    @Override
    FieldMapEntry getFieldMapEntry(String fieldName) {
        FieldMapEntry fme = getDefinition().getFieldMapEntry(fieldName, false);
        if (fme != null && table.getPrimaryKey().contains(fieldName)) {
            return fme;
        }
        return null;
    }

    @Override
    List<String> getFields() {
        return table.getPrimaryKey();
    }

    @Override
    public int getDataSize() {
        throw new IllegalArgumentException
            ("It is not possible to get data size from a PrimaryKey");
    }

    /**
     * Validate the index key.  Rules:
     * 1. Fields must be in the index
     * 2. Fields must be specified in order.  If a field "to the right"
     * in the index definition is set, all fields to its "left" must also
     * be present.
     */
    @Override
    void validate() {
        List<String> key = table.getPrimaryKey();
        int numFound = 0;
        for (int i = 0; i < key.size(); i++) {
            String s = key.get(i);
            if (get(s) != null) {
                if (i != numFound) {
                    throw new IllegalArgumentException
                        ("PrimaryKey is missing fields more significant than" +
                         " field: " + s);
                }
                ++numFound;
            }
        }
        if (numFound != size()) {
           throw new IllegalArgumentException
               ("PrimaryKey contains a field that is not part of the key");
        }
    }
}



