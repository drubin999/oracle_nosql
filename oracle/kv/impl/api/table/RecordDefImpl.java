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
import static oracle.kv.impl.api.table.JsonUtils.RECORD;
import static oracle.kv.impl.api.table.JsonUtils.TYPE;

import java.util.Collections;
import java.util.List;

import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.RecordDef;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import com.sleepycat.persist.model.Persistent;

/**
 * RecordDefImpl implements the RecordDef interface.
 */
@Persistent(version=1)
class RecordDefImpl extends FieldDefImpl
    implements RecordDef {

    private static final long serialVersionUID = 1L;
    final FieldMap fieldMap;
    final private String name;

    RecordDefImpl(final String name,
                  final FieldMap fieldMap,
                  final String description) {
        super(Type.RECORD, description);
        if (fieldMap == null || fieldMap.isEmpty()) {
            throw new IllegalArgumentException
                ("Record has no fields and cannot be built");
        }
        this.name  = name;
        this.fieldMap = fieldMap;
    }

    RecordDefImpl(final String name,
                  final FieldMap fieldMap) {
        this(name, fieldMap, null);
    }

    private RecordDefImpl(RecordDefImpl impl) {
        super(impl);
        this.name = impl.name;
        fieldMap = new FieldMap(impl.fieldMap);
    }

    @SuppressWarnings("unused")
    private RecordDefImpl() {
        fieldMap = null;
        name = null;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isRecord() {
        return true;
    }

    @Override
    public RecordDef asRecord() {
        return this;
    }

    @Override
    public List<String> getFields() {
        return Collections.unmodifiableList(fieldMap.getFieldOrder());
    }

    @Override
    public FieldDef getField(String name1) {
        return fieldMap.get(name1);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof RecordDefImpl) {
            RecordDefImpl otherDef = (RecordDefImpl) other;

            /* maybe avoid some work */
            if (this == otherDef) {
                return true;
            }

            /*
             * Perform field-by-field comparison if names match.
             */
            if (name.equals(otherDef.name)) {
                return fieldMap.equals(otherDef.fieldMap);
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        return fieldMap.hashCode() + name.hashCode();
    }

    @Override
    void toJson(ObjectNode node) {

        /*
         * Records always require a name because of Avro
         */
        node.put(NAME, name);
        super.toJson(node);
        fieldMap.putFields(node);
    }

    @Override
    public RecordDefImpl clone() {
        return new RecordDefImpl(this);
    }

    @Override
    public RecordValueImpl createRecord() {
        return new RecordValueImpl(this);
    }

    @Override
    public boolean isNullable(String fieldName) {
        FieldMapEntry fme = getFieldMapEntry(fieldName, true);
        return fme.isNullable();
    }

    @Override
    public FieldValue getDefaultValue(String fieldName) {
        FieldMapEntry fme = getFieldMapEntry(fieldName, true);
        return fme.getDefaultValue();
    }

    FieldMap getFieldMap() {
        return fieldMap;
    }

    int getNumFields() {
        return fieldMap.size();
    }

    List<String> getFieldsInternal() {
        return fieldMap.getFieldOrder();
    }

    /**
     * The containing ObjectNode has already been created.  What's needed
     * here is the name and type plus the fields array.  NOTE: the name of
     * the Record need not be the same as the name of the field that references
     * it.
     * {
     *  "name": "xxx",
     *  "type": {
     *    "name" : "xxx",
     *    "type" : "record",
     *    "fields" : [ {}, {}, ... {} ]
     *  }
     * }
     */
    @Override
    public JsonNode mapTypeToAvro(ObjectNode node) {
        if (node == null) {
            node = JsonUtils.createObjectNode();
        }
        node.put(TYPE, RECORD);
        node.put(NAME, name);
        ArrayNode array = node.putArray(FIELDS);
        for (String fname : fieldMap.getFieldOrder()) {
            FieldMapEntry entry = getFieldMapEntry(fname, true);
            ObjectNode fnode = array.addObject();
            fnode.put(NAME, fname);
            entry.createAvroTypeAndDefault(fnode);
        }
        return node;
    }

    FieldMapEntry getFieldMapEntry(String fieldName,
                                   boolean mustExist) {
        FieldMapEntry fme = fieldMap.getFieldMapEntry(fieldName);
        if (fme != null) {
            return fme;
        }
        if (mustExist) {
            throw new IllegalArgumentException
                ("Field does not exist in table definition: " + fieldName);
        }
        return null;
    }

    @Override
    FieldValueImpl createValue(JsonNode node) {
        if (node == null || node.isNull()) {
            return NullValueImpl.getInstance();
        }
        if (!node.isObject()) {
            throw new IllegalArgumentException
                ("Default value for type RECORD is not a record");
        }
        if (node.size() != 0) {
            throw new IllegalArgumentException
                ("Default value for record must be null or an empty record");
        }
        return createRecord();
    }
}

