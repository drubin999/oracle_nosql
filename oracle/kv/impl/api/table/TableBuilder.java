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
import java.util.List;

import oracle.kv.table.Table;

import org.apache.avro.Schema;

/**
 * TableBuilder is a class used to construct Tables and complex data type
 * instances.  The instances themselves are immutable.  The pattern used
 * is
 * 1.  create TableBuilder
 * 2.  add state in terms of data types
 * 3.  build the desired object
 *
 * When constructing a child table the parent tables Table object is required.
 * This requirement makes it easy to add the necessary parent information to
 * the child table.  It is also less error prone than requiring the caller to
 * add the parent's key fields by hand.
 *
 * There is a special case is where the builder is created from a JSON string
 * which may already have the parent information encoded.  That area is a
 * TODO until a final decision on what JSON string format(s) are allowed for
 * this if it is even publicly supported.
 */
public class TableBuilder extends TableBuilderBase {
    private final String name;
    private String description;

    /* These apply only to tables */
    private final TableImpl parent;
    private List<String> primaryKey;
    private List<String> shardKey;
    private boolean r2compat;
    private boolean schemaAdded;
    private int schemaId;

    private TableBuilder(String name, String description,
                         Table parent, boolean copyParentInfo) {
        this.name = name;
        this.description = description;
        schemaAdded = false;
        r2compat = false;
        this.parent = (parent != null ? (TableImpl) parent : null);
        primaryKey = new ArrayList<String>();
        shardKey = new ArrayList<String>();
        if (parent != null && copyParentInfo) {
            addParentInfo();
        }
        TableImpl.validateComponent(name, true);
    }

    /**
     * Create a table builder
     */
    public static TableBuilder createTableBuilder(String name,
                                                  String description,
                                                  Table parent,
                                                  boolean copyParentInfo) {
        return new TableBuilder(name, description, parent,
                                copyParentInfo);
    }

    public static TableBuilder createTableBuilder(String name,
                                                  String description,
                                                  Table parent) {
        return new TableBuilder(name, description, parent, true);
    }

    public static TableBuilder createTableBuilder(String name) {
        return new TableBuilder(name, null, null, true);
    }

    /**
     * Creates an ArrayBuilder.
     *
     * @param description optional description.
     */
    public static ArrayBuilder createArrayBuilder(String description) {
        return new ArrayBuilder(description);
    }

    public static ArrayBuilder createArrayBuilder() {
        return new ArrayBuilder();
    }

    /**
     * Creates an MapBuilder.
     *
     * @param description optional description.
     */
    public static MapBuilder createMapBuilder(String description) {
        return new MapBuilder(description);
    }

    public static MapBuilder createMapBuilder() {
        return new MapBuilder();
    }

    /**
     * Creates an RecordBuilder.
     *
     * @param name the name of the record
     *
     * @param description optional description.
     */
    public static RecordBuilder createRecordBuilder(String name,
                                                    String description) {
        return new RecordBuilder(name, description);
    }

    public static RecordBuilder createRecordBuilder(String name) {
        return new RecordBuilder(name);
    }

    /**
     * Build a Table from its JSON format.
     */
    public static TableImpl fromJsonString(String jsonString,
                                           Table parent) {
        return JsonUtils.fromJsonString(jsonString, (TableImpl) parent);
    }

    /**
     * Accessors
     */
    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public TableImpl getParent() {
        return parent;
    }

    public List<String> getPrimaryKey() {
        return primaryKey;
    }

    public List<String> getShardKey() {
        return shardKey;
    }

    public boolean isR2compatible() {
        return r2compat;
    }

    public int getSchemaId() {
        return schemaId;
    }

    @Override
    public TableBuilderBase setDescription(String description) {
        this.description = description;
        return this;
    }

    /**
     * Table-only methods
     */
    @Override
    public TableBuilderBase primaryKey(String ... key) {
        for (String field : key) {
            if (primaryKey.contains(field)) {
                throw new IllegalArgumentException
                    ("The primary key field already exists: " + field);
            }
            primaryKey.add(field);
        }
        return this;
    }

    @Override
    public TableBuilderBase shardKey(String ... key) {
        if (parent != null) {
            throw new IllegalArgumentException
                ("Child tables cannot have a shard key.");
        }
        for (String field : key) {
            if (shardKey.contains(field)) {
                throw new IllegalArgumentException
                    ("The shard key field already exists: " + field);
            }
            shardKey.add(field);
        }
        return this;
    }

    @Override
    public TableBuilderBase primaryKey(final List<String> pKey) {
        this.primaryKey = pKey;
        return this;
    }

    @Override
    public TableBuilderBase shardKey(final List<String> mKey) {
        if (parent != null) {
            throw new IllegalArgumentException
                ("Child tables cannot have a shard key.");
        }
        this.shardKey = mKey;
        return this;
    }

    @Override
    public TableBuilderBase setR2compat(boolean value) {
        this.r2compat = value;
        return this;
    }

    /**
     * This implicitly sets r2compat to true as well.
     */
    @Override
    public TableBuilderBase setSchemaId(int id) {
        schemaId = id;
        r2compat = true;
        return this;
    }

    @Override
    public TableBuilderBase addSchema(String avroSchema) {
        if (schemaAdded) {
            throw new IllegalArgumentException
                ("Only one schema may be added to a table");
        }
        Schema schema = new Schema.Parser().parse(avroSchema);
        List<Schema.Field> schemaFields = schema.getFields();
        for (Schema.Field field : schemaFields) {
            generateAvroSchemaFields(field.schema(),
                                     field.name(),
                                     field.defaultValue(),
                                     field.doc());
        }
        schemaAdded = true;
        return this;
    }

    /**
     * Methods to perform the actual build
     */
    @Override
    public TableImpl buildTable() {
        /*
         * If the shard key is not provided it defaults to:
         * o the primary key if this is a top-level table
         * o the parent's shard key if this is a child table.
         */
        if (shardKey.isEmpty()) {
            if (parent != null) {
                shardKey = new ArrayList<String>(parent.getShardKey());
            } else {
                shardKey = primaryKey;
            }
        }

        return TableImpl.createTable(getName(),
                                     parent,
                                     getPrimaryKey(),
                                     getShardKey(),
                                     fields,
                                     r2compat,
                                     schemaId,
                                     getDescription(),
                                     true);
    }

    @Override
    void validateFieldAddition(final String fieldName) {
        super.validateFieldAddition(fieldName);
        /*
         * Cannot add a field who has duplicated name with
         * the primary key field of its parent table.
         */
        if (parent != null) {
            if (parent.getPrimaryKey().contains(fieldName)) {
                throw new IllegalArgumentException
                    ("Cannot add field, it already exists in primary key " +
                        "fields of its parent table: " + fieldName);
            }
        }
    }

    /*
     * Used to validate the state of the builder to ensure that it can be used
     * to build a table.  The simplest way to do this is to actually build one
     * and let TableImpl do the validation.  This method is used by tests and
     * the CLI.
     */
    @Override
    public TableBuilderBase validate() {
        buildTable();
        return this;
    }

    /*
     * Show the current state of the table.  The simplest way is to create a
     * not-validated table and display it.
     */
    public String toJsonString(boolean pretty) {
        TableImpl t = TableImpl.createTable(getName(),
                                            parent,
                                            getPrimaryKey(),
                                            getShardKey(),
                                            fields,
                                            r2compat,
                                            schemaId,
                                            getDescription(),
                                            false);
        return t.toJsonString(pretty);
    }

    /**
     * There is no need to go more than one level because the primary key of
     * each table includes the fields of its ancestors.
     */
    private void addParentInfo() {
        for (String fieldName : parent.getPrimaryKey()) {
            fields.put(fieldName, parent.getFieldMapEntry(fieldName, true));
            primaryKey.add(fieldName);
        }
    }
}
