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

import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.Index;
import oracle.kv.table.IndexKey;

public class IndexKeyImpl extends RecordValueImpl implements IndexKey {
    private static final long serialVersionUID = 1L;
    final IndexImpl index;

    /**
     * The RecordDef associated with an IndexKeyImpl is that of its table.
     */
    IndexKeyImpl(IndexImpl index) {
        super(new RecordDefImpl(index.getTableImpl().getName(),
                                index.getTableImpl().getFieldMap()));
        this.index = index;
    }

    private IndexKeyImpl(IndexKeyImpl other) {
        super(other);
        this.index = other.index;
    }

    /**
     * Return the Index associated with this key
     */
    @Override
    public Index getIndex() {
        return index;
    }

    @Override
    public IndexKeyImpl clone() {
        return new IndexKeyImpl(this);
    }

    @Override
    FieldDef validateNameAndType(String name,
                                 FieldDef.Type type) {
        FieldDef def = super.validateNameAndType(name, type);
        if (!index.containsField(name)) {
            throw new IllegalArgumentException
                ("Field is not part of Index: " + name);
        }
        return def;
    }

    @Override
    public IndexKey asIndexKey() {
        return this;
    }

    @Override
    public boolean isIndexKey() {
        return true;
    }

    @Override
    public boolean equals(Object other) {
        if (super.equals(other)) {
            return other instanceof IndexKeyImpl;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    int getNumFields() {
        return index.getFieldsInternal().size();
    }

    @Override
    FieldDef getField(String fieldName) {
        FieldDef def = getDefinition().getField(fieldName);
        if (def != null && index.containsField(fieldName)) {
            return def;
        }
        return null;
    }

    @Override
    FieldMapEntry getFieldMapEntry(String fieldName) {
        FieldMapEntry fme = getDefinition().getFieldMapEntry(fieldName, false);
        if (fme != null && index.containsField(fieldName)) {
            return fme;
        }
        return null;
    }

    @Override
    List<String> getFields() {
        return index.getFields();
    }

    public int getKeySize() {
        return index.serializeIndexKey(this).length;
    }

    TableImpl getTable() {
        return index.getTableImpl();
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
        int numFound = 0;
        int i = 0;
        for (String s : index.getFields()) {
            if (get(s) != null) {
                if (i != numFound) {
                    throw new IllegalArgumentException
                        ("IndexKey is missing fields more significant than" +
                         " field: " + s);
                }
                if (!index.containsField(s)) {
                    throw new IllegalArgumentException
                        ("Field is not part of Index: " + s);
                }
                ++numFound;
            }
            ++i;
        }
        if (numFound != size()) {
           throw new IllegalArgumentException
               ("IndexKey contains a field that is not part of the Index");
        }
    }

    public IndexImpl getIndexImpl() {
        return index;
    }

    /**
     * Return true if all fields in the index are specified.
     */
    boolean isComplete() {
        return (size() == getNumFields());
    }

    /**
     * This function behaves like adding "one" to the entire index key.  That
     * is, it increments the least significant field but if that field "wraps"
     * in that it's already at its maximum in terms of data type, such as
     * Integer.MAX_VALUE then increment the next more significant field and
     * set that field to its minimum value.
     *
     * If the value would wrap and there are no more significant fields then
     * return false, indicating that the "next" value is actually the end
     * of the index, period.
     *
     * This code is used to implement inclusive/exclusive semantics.
     */
    public boolean incrementIndexKey() {
        List<String> indexFields = index.getFieldsInternal();
        FieldValue[] values = new FieldValue[indexFields.size()];
        int fieldIndex = 0;
        for (String s : indexFields) {
            values[fieldIndex] = get(s);
            if (values[fieldIndex] == null) {
                break;
            }
            fieldIndex++;
        }

        /*
         * At least one field must exist.  Assert that and move back to the
         * target field.
         */
        assert fieldIndex > 0;
        --fieldIndex;

        /*
         * Increment and reset.  If the current field returns null, indicating
         * that it will wrap its value, set it to its minimum value and move to
         * the next more significant field.  If there are none, return false
         * indicating that there are no larger keys in the index that match the
         * key.
         */
        FieldValueImpl fvi = ((FieldValueImpl)values[fieldIndex]).getNextValue();
        while (fvi == null) {
            fvi = ((FieldValueImpl)values[fieldIndex]).getMinimumValue();
            put(indexFields.get(fieldIndex), fvi);

            /*
             * Move to next more significant field if it exists
             */
            --fieldIndex;
            if (fieldIndex >= 0) {
                fvi = ((FieldValueImpl)values[fieldIndex]).getNextValue();
            } else {
                /*
                 * Failed to increment
                 */
                return false;
            }
        }
        assert fvi != null && fieldIndex >= 0;
        put(indexFields.get(fieldIndex), fvi);
        return true;
    }
}


