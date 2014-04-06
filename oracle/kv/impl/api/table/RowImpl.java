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

import java.util.Iterator;
import java.util.Map;

import oracle.kv.Key;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.Version;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.RecordDef;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;

/**
 * RowImpl is a specialization of RecordValue to represent a single record,
 * or row, in a table.  It is a central object used in most table operations
 * defined in {@link TableAPI}.
 *<p>
 * Row objects are constructed by
 * {@link Table#createRow createRow} or implicitly when returned from table
 * operations.
 */
public class RowImpl extends RecordValueImpl implements Row {
    private static final long serialVersionUID = 1L;
    protected final TableImpl table;
    private Version version;
    private int tableVersion;

    RowImpl(RecordDef field, TableImpl table) {
        super(field);
        assert field != null && table != null;
        this.table = table;
        version = null;
        tableVersion = 0;
    }

    RowImpl(RecordDef field, TableImpl table, Map<String, FieldValue> fieldMap) {
        super(field, fieldMap);
        assert field != null && table != null;
        this.table = table;
        version = null;
        tableVersion = 0;
    }

    RowImpl(RowImpl other) {
        super(other);
        this.table = other.table;
        this.version = other.version;
        this.tableVersion = other.tableVersion;
    }

    /**
     * Return the Table associated with this row.
     */
    @Override
    public Table getTable() {
        return table;
    }

    @Override
    public boolean isRow() {
        return true;
    }

    @Override
    public Row asRow() {
        return this;
    }

    @Override
    public Version getVersion() {
        return version;
    }

    @Override
    public int getTableVersion() {
        return tableVersion;
    }

    void setTableVersion(final int tableVersion) {
        this.tableVersion = tableVersion;
    }

    void setVersion(Version version) {
        this.version = version;
    }

    @Override
    public RowImpl clone() {
        return new RowImpl(this);
    }

    @Override
    public PrimaryKey createPrimaryKey() {
        RowImpl impl = clone();
        impl.removeValueFields();
        return new PrimaryKeyImpl((RecordDef)field, table, impl.valueMap);
    }

    public int getDataSize() {
        return table.getDataSize(this);
    }

    public int getKeySize() {
        return table.getKeySize(this);
    }

    /**
     * Return the KVStore Key for the actual key/value record that this
     * object references.
     *
     * @param allowPartial set true if the primary key need not be complete.
     * This is the case for multi- and iterator-based methods.
     */
    Key getPrimaryKey(boolean allowPartial) {
        return table.createKey(this, allowPartial);
    }

    TableImpl getTableImpl() {
        return table;
    }

    /**
     * Return a new Row based on the ValueVersion returned from the store.
     * Make a copy of this object to hold the data.  It is assumed
     * that "this" holds the primary key.
     *
     * @param keyOnly set to true if only primary key fields should be
     * copied/cloned.
     */
    RowImpl rowFromValueVersion(ValueVersion vv, boolean keyOnly) {

        /*
         * Don't use clone to avoid creating a PrimaryKey
         */
        RowImpl row = new RowImpl(this);
        if (keyOnly) {
            removeValueFields();
        }
        return table.rowFromValueVersion(vv, row);
    }

    /**
     * Copy the primary key fields from row to this object.
     */
    void copyKeyFields(RowImpl row) {
        for (String keyField : table.getPrimaryKey()) {
            FieldValue val = row.get(keyField);
            if (val != null) {
                put(keyField, val);
            }
        }
    }

    /**
     * Create a Value from a Row
     */
    Value createValue() {
        return table.createValue(this);
    }

    @Override
    FieldDef validateNameAndType(String name,
                                 FieldDef.Type type) {
        return super.validateNameAndType(name, type);
    }

    @Override
    public boolean equals(Object other) {
        if (super.equals(other)) {
            if (other instanceof RowImpl) {
                return table.nameEquals(((RowImpl) other).table);
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        return super.hashCode() + table.getFullName().hashCode();
    }

    private void removeValueFields() {
        if (table.hasValueFields()) {
            /* remove non-key fields if present */
            Iterator<Map.Entry<String, FieldValue>> entries =
                valueMap.entrySet().iterator();
            while (entries.hasNext()) {
                Map.Entry<String, FieldValue> entry = entries.next();
                if (!table.isKeyComponent(entry.getKey())) {
                    entries.remove();
                }
            }
        }
    }
}
