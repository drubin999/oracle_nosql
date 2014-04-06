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

import static oracle.kv.impl.api.table.JsonUtils.FIELDS;
import static oracle.kv.impl.api.table.JsonUtils.NAME;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;

import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import com.sleepycat.persist.model.Persistent;

/**
 * FieldMap is a class to encapsulate a map of FieldDef objects that is
 * maintained in both declaration order and in a map for efficient lookup.  It
 * is used by TableImpl and RecordDefImpl as well as the builder utilities.
 * It uses a case-insensitive TreeMap<String, FieldMapEntry> in order to
 * implement case-insensitive, but case-preserving semantics.
 *
 * FieldMap also abstracts out the properties of FieldDef instances that are
 * specific to record types (TableImpl, RecordDefImpl) -- nullable and default
 * values.  That is why there exists an intermediate object, FieldMapEntry,
 * which encapsulates FieldDef and those additional properties.
 *
 * It is @Persistent but the comparator is not saved with the object.  This
 * is not a problem because in all cases a new FieldMap is constructed
 * from the raw Map when the deserialized FieldMap is used so order in
 * that case does not matter.
 *
 * LinkedList is used for the list to make removal simpler.
 *
 */
@Persistent(version=1)
public class FieldMap implements Cloneable, Serializable {

    private static final long serialVersionUID = 1L;
    private final Map<String, FieldMapEntry> fields;
    private final List<String> fieldOrder;

    FieldMap() {
        fields = new TreeMap<String, FieldMapEntry>(FieldComparator.instance);
        fieldOrder = new LinkedList<String>();
    }

    FieldMap(FieldMap other) {
        fields = copyMap(other.fields);
        fieldOrder = copyFieldOrder(other.fieldOrder);
    }

    FieldMap(Map<String, FieldMapEntry> fields,
             List<String> fieldOrder) {
        this.fields = copyMap(fields);
        this.fieldOrder = copyFieldOrder(fieldOrder);
    }

    /**
     * Utility method to deep copy a map
     */
    private static Map<String, FieldMapEntry>
        copyMap(Map<String, FieldMapEntry> map) {
        Map<String, FieldMapEntry> newMap =
            new TreeMap<String, FieldMapEntry>(FieldComparator.instance);
        for (Map.Entry<String, FieldMapEntry> entry : map.entrySet()) {
            newMap.put(entry.getKey(), entry.getValue().clone());
        }
        return newMap;
    }

    /**
     * Utility method to copy the field order list
     */
    private static List<String> copyFieldOrder(List<String> other) {
        List<String> newList = new LinkedList<String>();
        for (String s : other) {
            newList.add(s);
        }
        return newList;
    }

    Map<String, FieldMapEntry> getFields() {
        return fields;
    }

    List<String> getFieldOrder() {
        return fieldOrder;
    }

    FieldDef get(String name) {
        FieldMapEntry fme = fields.get(name);
        return (fme != null ? fme.getField() : null);
    }

    void put(String name, FieldMapEntry field) {
        fieldOrder.add(name);
        fields.put(name, field);
    }

    void put(String name, FieldDef field,
             boolean nullable, FieldValue defaultValue) {
        fieldOrder.add(name);
        put(name, new FieldMapEntry((FieldDefImpl) field,
                                    nullable,
                                    (FieldValueImpl) defaultValue));
    }

    boolean exists(String name) {
        return fields.containsKey(name);
    }

    FieldMapEntry remove(String name) {
        fieldOrder.remove(name);
        return fields.remove(name);
    }

    public boolean isEmpty() {
        return fieldOrder.isEmpty();
    }

    int size() {
        return fieldOrder.size();
    }

    @Override
    public FieldMap clone() {
        return new FieldMap(this);
    }

    /**
     * Compare equality.
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FieldMap) {
            FieldMap other = (FieldMap) obj;
            if (fieldOrder.size() == other.fieldOrder.size()) {
                for (int i = 0; i < fieldOrder.size(); i++) {
                    String name = fieldOrder.get(i);
                    String otherName = other.fieldOrder.get(i);
                    if (name.equals(otherName)) {
                        if (fields.get(name).equals(other.fields.get(name))) {
                            continue;
                        }
                    }
                    return false;
                }
                return true;
            }
        }
        return false;
    }

    /**
     * Unlike equals() the order of fields isn't required for hashCode.
     */
    @Override
    public int hashCode() {
        int code = fieldOrder.size();
        for (Map.Entry<String, FieldMapEntry> entry : fields.entrySet()) {
            code += entry.getKey().hashCode() + entry.getValue().hashCode();
        }
        return code;
    }

    /**
     * Puts the fields of this map into an ObjectNode for display as JSON.
     * This is called indirected from toJsonString() methods on tables and
     * records.  Output in declaration order.
     */
    void putFields(ObjectNode node) {
        ArrayNode array = node.putArray(FIELDS);
        for (String fname : getFieldOrder()) {
            FieldMapEntry entry = getFieldMapEntry(fname);
            ObjectNode fnode = array.addObject();
            fnode.put(NAME, fname);
            entry.toJson(fnode);
        }
    }

    FieldMapEntry getFieldMapEntry(String name) {
        return fields.get(name);
    }
}

