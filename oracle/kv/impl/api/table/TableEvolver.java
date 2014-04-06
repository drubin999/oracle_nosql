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

import oracle.kv.table.Table;

/**
 * TableEvolver is a class used to evolve existing tables.  It has accessors
 * and modifiers for valid evolution operations and double checks them against
 * the current state of the class.
 * <p>
 * The general usage pattern is:
 * TableEvolver evolver = TableEvolver.createTableEvolver(Table);
 * ...do things...
 * Table newTable = evolver.evolveTable();
 * The resulting table can be passed to the TableMetadata.evolveTable()
 * method.
 * <p>
 * Schema evolution on tables allows the following transformations:
 * <ul>
 * <li> Add a non-key field to a table
 * <li> Remove a non-key field from a table (the system creates Avro defaults
 * for all fields which makes this possible)
 * <li> Change the nullable property of a field
 * <li> Change the description of a field
 * <li> Change a default value for a field for types with defaults
 * <li> Change a min/max value for a field for types with ranges
 * </ul>
 * These operations are not possible:
 * <ul>
 * <li> Modify/remove/add a field that is part of the primary key
 * <li> Change the type or name of any field
 * <li> NOTE: at some point it may be useful to allow some changes to
 * primary key fields, such as description, default value.  Changing
 * min/max will not be allowed because they can affect key size.  Primary
 * key fields cannot be nullable.
 * </ul>
 */
public class TableEvolver extends TableBuilderBase {
    private final TableImpl table;
    private final int evolvedVersion;

    TableEvolver(TableImpl table) {
        super(table.getFieldMap().clone());
        this.table = table;
        evolvedVersion = table.getTableVersion();
        if (evolvedVersion != table.numTableVersions()) {
            throw new IllegalArgumentException
                ("Table evolution must be performed on the latest version");
        }
    }

    public static TableEvolver createTableEvolver(Table table) {
        return new TableEvolver(((TableImpl)table).clone());
    }


    @Override
    public String getBuilderType() {
        return "Evolver";
    }

    /**
     * Accessors
     */
    public TableImpl getTable() {
        return table;
    }

    public int getTableVersion() {
        return evolvedVersion;
    }

    public RecordDefImpl getRecord(String fieldName) {
        return (RecordDefImpl) getField(fieldName);
    }

    public MapDefImpl getMap(String fieldName) {
        return (MapDefImpl) getField(fieldName);
    }

    public ArrayDefImpl getArray(String fieldName) {
        return (ArrayDefImpl) getField(fieldName);
    }

    public RecordEvolver createRecordEvolver(RecordDefImpl record) {
        if (record == null) {
            throw new IllegalArgumentException
                ("Null record passed to createRecordEvolver");
        }
        return new RecordEvolver(record);
    }

    /**
     * Do the evolution.  Reset the fields member to avoid accidental
     * updates to the live version of the table just evolved.  This way
     * this instance can be reused, which is helpful in testing.
     */
    public TableImpl evolveTable() {
        table.evolve(fields);

        /*
         * Reset the fields member to avoid accidental updates to the
         * live version of the table just evolved.
         */
        fields = fields.clone();
        return table;
    }

    /**
     * Show the current state of the table.
     */
    public String toJsonString(boolean pretty) {
        TableImpl t = table.clone();
        t.evolve(fields);
        return t.toJsonString(pretty);
    }

    @Override
    void validateFieldAddition(final String name) {
        super.validateFieldAddition(name);
        if (table.getField(name) != null) {
            throw new IllegalArgumentException
                ("Cannot add field, it already exists: " + name);
        }
    }

    /**
     * Fields cannot be removed if they are part of the primary
     * key or participate in an index.
     */
    @Override
    void validateFieldRemoval(String fieldName) {
        if (table.isKeyComponent(fieldName)) {
            throw new IllegalArgumentException
                ("Cannot remove a primary key field: " + fieldName);
        }
        if (table.isIndexKeyComponent(fieldName)) {
            throw new IllegalArgumentException
                ("Cannot remove an index key field: " + fieldName);
        }
    }
}
