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

import static oracle.kv.impl.api.table.JsonUtils.DEFAULT;
import static oracle.kv.impl.api.table.JsonUtils.NULL;
import static oracle.kv.impl.api.table.JsonUtils.NULLABLE;
import static oracle.kv.impl.api.table.JsonUtils.TYPE;

import java.io.Serializable;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import com.sleepycat.persist.model.Persistent;

/**
 * FieldMapEntry encapsulates the properties of FieldDef instances that are
 * specific to record types (TableImpl, RecordDefImpl) -- nullable and default
 * values.
 */
@Persistent(version=1)
class FieldMapEntry implements Cloneable, Serializable {

    private static final long serialVersionUID = 1L;
    private final FieldDefImpl field;
    private final boolean nullable;
    private final FieldValueImpl defaultValue;

    FieldMapEntry(FieldDefImpl field, boolean nullable,
                  FieldValueImpl defaultValue) {
        this.field = field;
        this.nullable = nullable;
        this.defaultValue = defaultValue;
        if (!nullable && defaultValue == null) {
            throw new IllegalArgumentException
                ("Not nullable fields require a default value");
        }
    }

    private FieldMapEntry(FieldMapEntry other) {
        this.field = other.field.clone();
        this.nullable = other.nullable;
        this.defaultValue = (other.defaultValue != null ?
                             other.defaultValue.clone() : null);
    }

    /* DPL */
    @SuppressWarnings("unused")
    private FieldMapEntry() {
        field = null;
        defaultValue = null;
        nullable = false;
    }

    FieldDefImpl getField() {
        return field;
    }

    boolean isNullable() {
        return nullable;
    }

    FieldValueImpl getDefaultValue() {
        return (defaultValue != null ? defaultValue :
                NullValueImpl.getInstance());
    }

    /**
     * Compare equality.
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FieldMapEntry) {
            FieldMapEntry other = (FieldMapEntry) obj;
            return field.equals(other.field) && nullable == other.nullable &&
                defaultsEqual(other);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return field.hashCode() + ((Boolean) nullable).hashCode() +
            getDefaultValue().hashCode();
    }

    @Override
    public FieldMapEntry clone() {
        return new FieldMapEntry(this);
    }

    /**
     * This function creates the "type" definition required by Avro for the
     * instance.  It is called from TableImpl and RecordDefImpl when generating
     * Avro schemas for named field types.  It abstracts out handling of
     * nullable fields and fields with default values.
     *
     * Fields that are nullable are implemented as an Avro union with the
     * "null" Avro value.  The problem is that default values for unions
     * require the type of the defaulted field to come first in the union
     * declaration. This means that the order of the fields in the union depend
     * on the presence and type of the default.
     *
     * All record fields in the Avro schema get default values for schema
     * evolution.  This is either user-defined or null.
     */
    final JsonNode createAvroTypeAndDefault(ObjectNode node) {
        if (isNullable()) {
            ArrayNode arrayNode = node.putArray(TYPE);
            if (getDefaultValue().isNull()) {
                arrayNode.add(NULL);
                arrayNode.add(field.mapTypeToAvroJsonNode());
            } else {
                arrayNode.add(field.mapTypeToAvroJsonNode());
                arrayNode.add(NULL);
            }
        } else {
            node.put(TYPE, field.mapTypeToAvroJsonNode());
        }
        node.put(DEFAULT, getDefaultValue().toJsonNode());
        return node;
    }

    /**
     * Outputs the state of the entry to the ObjectNode for display
     * as JSON.  This is called indirectly by toJsonString() methods.
     * First, output the information from the FieldDefImpl, then output
     * default and nullable state.
     */
    void toJson(ObjectNode node) {
        field.toJson(node);
        node.put(NULLABLE, nullable);
        node.put(DEFAULT, getDefaultValue().toJsonNode());
    }

    private boolean defaultsEqual(FieldMapEntry other) {
        return getDefaultValue().equals(other.getDefaultValue());
    }
}

