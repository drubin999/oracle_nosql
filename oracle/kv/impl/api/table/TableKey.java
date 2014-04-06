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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import oracle.kv.Key;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldRange;
import oracle.kv.table.Row;
import oracle.kv.table.Table;

/**
 * A class to encapsulate a Key created from a Row in a Table.  The algorithm
 * is to iterate the target table's primary key adding fields and static
 * table ids as they are encountered, "left to right."  Addition of key
 * components ends when either (1) the entire primary key is added or (2)
 * a primary key field is missing from the Row.  In the latter case the
 * boolean allowPartial must be true allowing a partial primary key.
 *
 * In the case of partial keys it is important to get as many key components
 * as possible to get the most specific parent key for the iteration possible.
 */
public class TableKey {
    final private ArrayList<String> major;
    final private ArrayList<String> minor;
    final private Iterator<String> pkIterator;
    final private Iterator<String> majorIterator;
    final private boolean allowPartial;
    final private Row row;
    final private Table table;
    private Key key;
    private boolean majorComplete;
    private boolean keyComplete;
    private boolean done;
    private ArrayList<String> current;

    private TableKey(Table table, Row row, boolean allowPartial) {
        this.row = row;
        this.allowPartial = allowPartial;
        major = new ArrayList<String>();
        minor = new ArrayList<String>();
        pkIterator = table.getPrimaryKey().iterator();
        majorIterator = table.getShardKey().iterator();
        current = major;
        keyComplete = true;
        this.table = table;
    }

    /**
     * Create a Key for a record based on its value
     * (tableId, primaryKey)
     *
     * If record is empty this is a table iteration on a top-level
     * table -- validate that.
     */
    public static TableKey createKey(Table table, Row row,
                                     boolean allowPartial) {
        if (row.size() == 0) {
            if (!allowPartial) {
                throw new IllegalArgumentException("Primary key is empty");
            }
            return new TableKey(table, row, allowPartial).create();
        }
        return new TableKey(table, row, allowPartial).create();
    }

    public TableKey create() {
        validateKey();
        createPrimaryKey(table);
        key = Key.createKey(major, minor);
        return this;
    }

    public Table getTable() {
        return table;
    }

    public boolean getKeyComplete() {
        return keyComplete;
    }

    public boolean getMajorKeyComplete() {
        return majorComplete;
    }

    public Key getKey() {
        return key;
    }

    RowImpl getRow() {
        return (RowImpl) row;
    }

    public byte[] getKeyBytes() {
        return key.toByteArray();
    }

    /*
     * If the major key has been consumed, move to minor array.
     */
    private void incrementMajor() {
        if (majorIterator.hasNext()) {
            majorIterator.next();
        } else {
            current = minor;
            majorComplete = true;
        }
    }

    /*
     * Initialize the state of this object's major and minor lists
     * so that a Key can be created from them for use in table and
     * store operations.
     *
     * The key may be complete or partial (if allowPartial is true).
     * If partial the key fields are added "in order" left to right,
     * stopping when a missing field is encountered.
     */
    private void createPrimaryKey(Table currentTable) {
        if (currentTable.getParent() != null) {
            createPrimaryKey(currentTable.getParent());
        }
        if (done) {
            return;
        }
        List<String> primaryKey = currentTable.getPrimaryKey();

        /*
         * Add the table's static id and create a new primary key
         * iterator that applies to this table only.
         */
        current.add(((TableImpl)currentTable).getIdString());
        String lastKeyField = primaryKey.get(primaryKey.size() - 1);
        while (pkIterator.hasNext()) {
            String fieldName = pkIterator.next();
            FieldDef field = currentTable.getField(fieldName);

            incrementMajor();
            /*
             * Add the field to the appropriate key array.
             */
            FieldValueImpl value = (FieldValueImpl) row.get(fieldName);
            if (value != null) {
                current.add(value.formatForKey(field));
            } else {
                keyComplete = false;
                if (!allowPartial) {
                    throw new IllegalArgumentException
                        ("Missing primary key field: " + fieldName);
                }
                /* TODO: check for complete keys for parent tables */
                done = true;
                break;
            }
            /*
             * In the case of a parent table the parent's primary key will
             * run out before that of the target table.  Just consume the
             * fields that belong to the parent.
             */
            if (fieldName.equals(lastKeyField)) {
                incrementMajor();
                break;
            }
        }
    }

    /**
     * Ensure that if the FieldRange is non-null that the field is
     * the "next" one after the last field specified in the primary key.
     *
     * The size of the PrimaryKey (row) should be identical to the index
     * of the field in the primary key list.  That is, if the field is
     * the 3rd field of the key, the first two should be present making
     * the size of the row 2 -- the same as the index of the 3rd field.
     */
    void validateFieldOrder(FieldRange range) {
        if (range != null) {
            List<String> primaryKey = table.getPrimaryKey();
            int index = primaryKey.indexOf(range.getFieldName());
            if (index < 0) {
                throw new IllegalArgumentException
                    ("Field is not part of primary key: " +
                     range.getFieldName());
            }
            if (row.size() < index) {
                throw new IllegalArgumentException
                    ("PrimaryKey is missing fields more significant than" +
                     " field: " + range.getFieldName());
            }
            if (row.size() > index) {
                throw new IllegalArgumentException
                    ("PrimaryKey has extra fields beyond" +
                     " field: " + range.getFieldName());
            }
        }
    }

    /**
     * Validate the key.  If a partial primary key is specified it must be done
     * in order of the key fields.  For example a 2-component partial key can
     * have the first field only but not the second field only.
     */
    private void validateKey() {
        if (row.size() != 0) {
            boolean fieldMissing = false;
            for (String keyField : table.getPrimaryKey()) {
                if (row.get(keyField) == null) {
                    fieldMissing = true;
                } else {
                    /* found a field after a missing field... bad */
                    if (fieldMissing) {
                        throw new IllegalArgumentException
                            ("Primary key fields are specified out of order");
                    }
                }
            }
        }
    }
}
