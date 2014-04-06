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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import oracle.kv.Key;
import oracle.kv.Key.BinaryKeyIterator;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.util.SortableString;
import oracle.kv.table.EnumDef;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldRange;
import oracle.kv.table.FieldValue;
import oracle.kv.table.Index;
import oracle.kv.table.RecordValue;
import oracle.kv.table.ReturnRow;
import oracle.kv.table.Row;
import oracle.kv.table.Table;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import com.sleepycat.util.PackedInteger;

/**
 * TableImpl implements Table, which represents a table in Oracle NoSQL
 * Database.  It is an immutable object created from system metadata.
 *
 * Tables are defined in terms of several properties:
 * 1.  a map of {@link FieldDef} instances keyed by a String field name.  This
 * defines the fields (or "columns") of a table.
 * 2.  a list of fields that define the fields that participate in the
 * primary key for the table.  These fields turn into KV Key path
 * components in the store.
 * 3.  a list of fields that is a proper subset of the primary key fields
 * that defines the "shard key" for the table.  The shard key defines the
 * primary key fields that become part of the Key's major path.  The remaining
 * primary key fields become the Key's minor path.
 * 4.  optional indexes, defined in terms of fields in the table.
 * 5.  optional child tables, keyed by table name.  Child tables inherit the
 * table's primary key and shard key.
 *
 * If a table is a child table it also references its parent table.  When a
 * table is created the system generates a unique long to serve as an id for
 * the table.  The serialized form of this id serves a part of the table's
 * primary key to locate it in the store.  An id is used instead of the table
 * name to keep keys small.
 *
 * Tables can be created in {@code r2compat} mode which means that the table
 * name is used for keys instead of the id because the table overlays R2 data.
 * Such tables also write new records in a manner that is compatible with R2 by
 * avoiding adding the table version to the record data.
 *
 * Because a table can evolve the map of fields is maintained as a list of
 * maps of fields, indexed by table "version."  The initial table version
 * is 1 (but index 0).
 *
 * Tables can evolve in limited ways with schema evolution.  The only thing
 * that can be done is to add or remove non-key fields or change fields in
 * a way that does not affect their serialization.  Once r2compat tables have
 * been evolved they are no longer readable by R2 key/value code.
 */
public class TableImpl implements Table, MetadataInfo,
                                  Serializable, Cloneable {

    private static final long serialVersionUID = 1L;
    private final String name;
    private long id;
    private final TableImpl parent;
    private final TreeMap<String, Index> indexes;
    private final List<String> primaryKey;
    private final List<String> shardKey;
    private final String description;
    private final Map<String, Table> children;
    private final ArrayList<FieldMap> versions;
    private TableStatus status;

    /*
     * These next two are true, and non-zero, respectively, if this is
     * an overlay on R2 data with an Avro schema.  r2compat can be true
     * without a schemaId for a key-only table.  It affects the string used
     * as the table's key component (idString, below).
     */
    private final boolean r2compat;
    private final int schemaId;

    /*
     * transient, cached values
     */
    /* Cached Avro Schema for current version, unrelated to schemaId above */
    private transient Schema schema;
    /* The current version of this table instance */
    private transient int version;
    /* The number of components in a key for this table */
    private transient int numKeyComponents;
    /* The string used to key this table. */
    private transient String idString;

    public enum TableStatus {
        /** Table and its data is being deleted */
        DELETING() {
            @Override
            public boolean isDeleting() {
                return true;
            }
        },

        /** Table is ready for use */
        READY() {
            @Override
            public boolean isReady() {
                return true;
            }
        };

        /**
         * Returns true if this is the {@link #DELETING} type.
         * @return true if this is the {@link #DELETING} type
         */
	public boolean isDeleting() {
            return false;
        }

        /**
         * Returns true if this is the {@link #READY} type.
         * @return true if this is the {@link #READY} type
         */
	public boolean isReady() {
            return false;
        }
    }

    /*
     * String separator used to generate a globally unique name for a table.
     */
    public static final String SEPARATOR = ".";
    private static final int MAX_ID_LENGTH = 32;
    private static final int MAX_NAME_LENGTH = 64;
    private static final String SEPARATOR_REGEX = "\\.";
    static final String VALID_NAME_CHAR_REGEX = "^.*[^a-zA-Z0-9_].*$";
    private static final int INITIAL_TABLE_VERSION = 1;

    /**
     * Creates a TableImpl.
     * @param name the table name (required)
     * @param parent the parent table, or null
     * @param primaryKey the primary key fields (required)
     * @param shardKey the shard key (required)
     * @param fields the field definitions for the table (required)
     * @param r2compat if true create a release 2 compatible table which
     * means using the table name instead of its id in getIdString()
     * @param schemaId if a release 2 schema was used to construct the
     * fields this must be its schema id. It is only meaningful if r2compat
     * is true.
     * @param description a user-provided description of the table, or null
     * @param validate if true validate the fields and state of the table
     * upon construction
     */
    private TableImpl(final String name,
                      final TableImpl parent,
                      final List<String> primaryKey,
                      final List<String> shardKey,
                      final FieldMap fields,
                      boolean r2compat,
                      int schemaId,
                      final String description,
                      boolean validate) {
        this.name = name;
        this.parent = parent;
        this.description = description;
        this.primaryKey = primaryKey;
        this.shardKey = shardKey;
        this.status = TableStatus.READY;
        this.r2compat = r2compat;
        this.schemaId = schemaId;
        version = INITIAL_TABLE_VERSION;
        children = new TreeMap<String, Table>(FieldComparator.instance);
        indexes = new TreeMap<String, Index>(FieldComparator.instance);
        versions = new ArrayList<FieldMap>();
        versions.add(fields);
        if (validate) {
            validate();
            setSchema(true);
        }
        setIdString();
    }

    /*
     * This constructor is used by clone().  Some fields are copied by
     * reference:
     *  parent
     *  primaryKey, shardKey
     *  indexes (they are immutable)
     */
    private TableImpl(TableImpl t) {
        name = t.name;
        id = t.id;
        version = t.version;
        description = t.description;
        parent = t.parent;
        primaryKey = t.primaryKey;
        shardKey = t.shardKey;
        status = t.status;
        r2compat = t.r2compat;
        schemaId = t.schemaId;

        children = new TreeMap<String, Table>(FieldComparator.instance);
        for (Table table : t.children.values()) {
            children.put(table.getName(), ((TableImpl)table).clone());
        }

        versions = new ArrayList<FieldMap>(t.versions);
        /* this constructor uses the same Comparator as t.indexes */
        indexes = new TreeMap<String, Index>(t.indexes);
        setSchema(true);
        setIdString();
    }

    static TableImpl createTable(String name,
                                 Table parent,
                                 List<String> primaryKey,
                                 List<String> shardKey,
                                 FieldMap fields,
                                 boolean r2compat,
                                 int schemaId,
                                 String description,
                                 boolean validate) {
        return new TableImpl(name, (TableImpl)parent,
                             primaryKey,
                             shardKey, fields,
                             r2compat,
                             schemaId,
                             description,
                             validate);
    }

    private void readObject(java.io.ObjectInputStream in)
        throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        /*
         * Initialize transient fields not sent in the serialized object.
         */
        setSchema(false);
        getTableVersion();
        setIdString();
    }

    @Override
    public TableImpl clone() {
        return new TableImpl(this);
    }

    @Override
    public Table getChildTable(String tableName) {
        return children.get(tableName);
    }

    @Override
    public boolean childTableExists(String tableName) {
        return children.containsKey(tableName);
    }

    @Override
    public Table getVersion(int version1) {
        if (versions.size() < version1 || version1 < 0) {
            throw new IllegalArgumentException
                ("Table version " + version1 + " does not exist for table " +
                 getFullName());
        }
        TableImpl newTable = clone();
        newTable.version = version1;
        newTable.setSchema(true);
        return newTable;
    }

    @Override
    public Map<String, Table> getChildTables() {
        return Collections.unmodifiableMap(children);
    }

    @Override
    public Table getParent() {
        return parent;
    }

    public String getAvroSchema(boolean pretty) {
        return generateAvroSchema(version, pretty);
    }

    /**
     * Return the current version of this table.  Each time a table
     * is evolved its version number will increment.  A table starts out at
     * version 1.  Check for 0 because the field is transient and will not be
     * set from a deserialized instance.
     */
    @Override
    public int getTableVersion() {
        if (version == 0) {
            version = versions.size();
        }
        return version;
    }

    @Override
    public Index getIndex(String indexName) {
        return indexes.get(indexName);
    }

    @Override
    public Map<String, Index> getIndexes() {
        return Collections.unmodifiableMap(indexes);
    }

    @Override
    public String getName()  {
        return name;
    }

    /**
     * Get a unique string that identifies the table.  This
     * includes the name(s) of any parent tables.
     */
    @Override
    public String getFullName()  {
        StringBuilder sb = new StringBuilder();
        getTableNameInternal(sb);
        return sb.toString();
    }

    public long getId()  {
        return id;
    }

    public String getIdString()  {
        return idString;
    }

    @Override
    public String getDescription()  {
        return description;
    }

    @Override
    public List<String> getFields() {
        return Collections.unmodifiableList(getFieldOrder(version));
    }

    @Override
    public FieldDef getField(String fieldName) {
        FieldMapEntry fme = getFieldMapEntry(fieldName, false);
        if (fme != null) {
            return fme.getField();
        }
        return null;
    }

    @Override
    public boolean isNullable(String fieldName) {

        /* true means throw if the field doesn't exist */
        FieldMapEntry fme = getFieldMapEntry(fieldName, true);
        return fme.isNullable();
    }

    @Override
    public FieldValue getDefaultValue(String fieldName) {

        /* true means throw if the field doesn't exist */
        FieldMapEntry fme = getFieldMapEntry(fieldName, true);
        return fme.getDefaultValue();
    }

    @Override
    public List<String> getPrimaryKey() {
        return Collections.unmodifiableList(primaryKey);
    }

    @Override
    public List<String> getShardKey() {
        return Collections.unmodifiableList(shardKey);
    }

    @Override
    public RowImpl createRow() {
        return new RowImpl(new RecordDefImpl(getName(),
                                             getFieldMap()),
                           this);
    }

    @Override
    public RowImpl createRow(RecordValue value) {
        RowImpl row = new RowImpl(new RecordDefImpl(getName(),
                                                    getFieldMap()),
                                  this);
        populateRecord(row, value);
        return row;
    }

    @Override
    public RowImpl createRowWithDefaults() {
        RecordDefImpl def = new RecordDefImpl(getName(), getFieldMap());
        RowImpl row = new RowImpl(def, this);
        for (Map.Entry<String, FieldMapEntry> entry :
                 getFieldMap().getFields().entrySet()) {
            row.put(entry.getKey(), entry.getValue().getDefaultValue());
        }
        return row;
    }

    @Override
    public PrimaryKeyImpl createPrimaryKey() {
        return new PrimaryKeyImpl(new RecordDefImpl(getName(), getFieldMap()),
                                  this);
    }

    @Override
    public PrimaryKeyImpl createPrimaryKey(RecordValue value) {
        PrimaryKeyImpl key =
            new PrimaryKeyImpl(new RecordDefImpl(getName(), getFieldMap()),
                               this);
        populateRecord(key, value);
        return key;
    }

    @Override
    public ReturnRowImpl createReturnRow(ReturnRow.Choice returnChoice) {
        return new ReturnRowImpl(new RecordDefImpl(getName(), getFieldMap()),
                                 this,
                                 returnChoice);
    }

    @Override
    public Row createRowFromJson(String jsonInput, boolean exact) {
        return createRowFromJson
            (new ByteArrayInputStream(jsonInput.getBytes()), exact);
    }

    @Override
    public Row createRowFromJson(InputStream jsonInput, boolean exact) {
        RowImpl row = createRow();
        createFromJson(row, jsonInput, exact);
        return row;
    }

    @Override
    public PrimaryKeyImpl createPrimaryKeyFromJson(String jsonInput,
                                                   boolean exact) {
        return createPrimaryKeyFromJson
            (new ByteArrayInputStream(jsonInput.getBytes()), exact);
    }

    @Override
    public PrimaryKeyImpl createPrimaryKeyFromJson(InputStream jsonInput,
                                                   boolean exact) {
        PrimaryKeyImpl key = createPrimaryKey();
        createFromJson(key, jsonInput, exact);
        return key;
    }

    @Override
    public FieldRange createFieldRange(String fieldName) {
        FieldDef def = getField(fieldName);
        if (def == null) {
            throw new IllegalArgumentException
                ("Field does not exist in table definition: " + fieldName);
        }
        if (!primaryKey.contains(fieldName)) {
            throw new IllegalArgumentException
                ("Field does not exist in primary key: " + fieldName);
        }
        return new FieldRange(fieldName, def);
    }

    /**
     * Return true if ancestor is an ancestor of this table.   Match on
     * full name only.  Equality isn't needed here.
     */
    public boolean isAncestor(Table ancestor) {
        Table parentTable = getParent();
        String fullName = ancestor.getFullName();
        while (parentTable != null) {
            if (fullName.equals(parentTable.getFullName())) {
                return true;
            }
            parentTable = parentTable.getParent();
        }
        return false;
    }

    /**
     * Return the top-level for this table.
     */
    public TableImpl getTopLevelTable() {
        if (parent != null) {
            return parent.getTopLevelTable();
        }
        return this;
    }

    /**
     * Common implementation for row and primary key objects. The row may
     * be RowImpl or PrimaryKeyImpl (which is a RowImpl).
     */
    void createFromJson(RecordValueImpl row, InputStream jsonInput,
                        boolean exact) {
        JsonParser jp = null;
        try {
            jp = JsonUtils.createJsonParser(jsonInput);
            row.addJsonFields(jp, exact);
            row.validate();
        } catch (IOException ioe) {
            throw new IllegalArgumentException
                (("Failed to parse JSON input: " + ioe.getMessage()), ioe);
        } finally {
            if (jp != null) {
                try {
                    jp.close();
                } catch (IOException ignored) {
                    /* ignore failures on close */
                }
            }
        }
    }

    /**
     * Determine equality.  Use name, parentage and field definitions.
     */
    @Override
    public boolean equals(Object other) {
        if (other != null && other instanceof Table) {
            TableImpl otherDef = (TableImpl) other;
            if (getName().equals(otherDef.getName()) &&
                getId() == otherDef.getId()) {
                if (getParent() != null) {
                    if (!getParent().equals(otherDef.getParent())) {
                        return false;
                    }
                } else if (otherDef.getParent() != null) {
                    return false;
                }
                return (versionsEqual(otherDef) &&
                        getFieldMap().equals(otherDef.getFieldMap()));
            }
        }
        return false;
    }

    /**
     * More could be added, but this is enough to uniquely identify tables
     * users have obtained.
     */
    @Override
    public int hashCode() {
        return getFullName().hashCode() + versions.size() +
            getFieldMap().hashCode();
    }

    boolean nameEquals(TableImpl other) {
        return getFullName().equals(other.getFullName());
    }

    private boolean versionsEqual(TableImpl other) {
        int thisVersion = (version == 0 ? versions.size() : version);
        int otherVersion = (other.version == 0 ? other.versions.size() :
                            other.version);
        return (thisVersion == otherVersion);
    }

    @Override
    public int numTableVersions() {
        return versions.size();
    }

    public boolean hasChildren() {
        return (children.size() != 0);
    }

    /**
     * Return true if the table is an overlay over Avro key/value records.
     */
    public boolean isR2compatible() {
        return r2compat;
    }

    /**
     * Return the Avro schema ID if this table overlays an R2 table, 0
     * otherwise.
     */
    public int getSchemaId() {
        return schemaId;
    }

    /*
     * This is the only call that sets the table id.  It is called when a table
     * object is created in TableMetadata.
     */
    void setId(long id)  {
        this.id = id;
        setIdString();
    }

     private void setIdString() {
         if (id == 0 || r2compat) {
             idString = name;
         } else {
             int encodingLength = SortableString.encodingLength(id);
             idString = SortableString.toSortable(id, encodingLength);
         }
     }

    public FieldMap getFieldMap() {
        return getFieldMap(version);
    }

    /**
     * The number of key components for a unique primary key for this table.
     * This number is used to perform relatively efficient filtering of
     * keys on both client and server side as necessary.
     * NOTE: this could be made persistent but it's easily calculated and
     * cached.
     */
    public int getNumKeyComponents() {
        if (numKeyComponents == 0) {
            calculateNumKeys();
        }
        return numKeyComponents;
    }

    /*
     * This is separate from above so that setting the value is synchronized.
     * The number is:
     * 1.  The size of the primary key plus
     * 2.  One for each table in its hierarchy (including itself) plus
     */
    private synchronized void calculateNumKeys() {
        if (numKeyComponents == 0) {
            int num = primaryKey.size() + 1;
            TableImpl t = this;
            while (t.parent != null) {
                ++num;
                t = t.parent;
            }
            numKeyComponents = num;
        }
    }

    public TableStatus getStatus() {
        return status;
    }

    public synchronized void setStatus(TableStatus newStatus) {
        if ((status != newStatus) && status.isDeleting()) {
            throw new IllegalStateException("Table is being deleted, cannot " +
                                            "change status to " + newStatus);
        }
        status = newStatus;
    }

    Map<String, Table> getMutableChildTables() {
        return children;
    }

    FieldMapEntry getFieldMapEntry(String fieldName,
                                   boolean mustExist) {
        FieldMap fieldMap = getFieldMap();
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

    List<String> getMutablePrimaryKey() {
        return primaryKey;
    }

    public int getPrimaryKeySize() {
        return primaryKey.size();
    }

    List<String> getMutableShardKey() {
        return shardKey;
    }

    Map<String, Index> getMutableIndexes() {
        return indexes;
    }

    /**
     * If this table has a parent return its fully-qualified name, otherwise
     * null.
     */
    public String getParentName() {
        if (parent != null) {
            return parent.getFullName();
        }
        return null;
    }

    public Key createKey(Row row, boolean allowPartial) {
        setTableVersion(row);
        return TableKey.createKey(this, row, allowPartial).getKey();
    }

    /**
     * Create a Row object with all values for the primary key,
     * extracted from the byte[] array that is the store key.
     *
     * This method, and createPrimaryKeyFromBytes are lenient with
     * respect to failures and return null if they fail to match
     * a table.  This is necessary for mixed access between tables and
     * potentially matching key/value records.
     */
    RowImpl createRowFromKeyBytes(byte[] keyBytes) {
        return createFromKeyBytes(keyBytes, false);
    }

    /**
     * PrimaryKey version of createRowFromKeyBytes.
     */
    PrimaryKeyImpl createPrimaryKeyFromKeyBytes(byte[] keyBytes) {
        return (PrimaryKeyImpl) createFromKeyBytes(keyBytes, true);
    }

    /**
     * PrimaryKey version of createRowFromKeyBytes.
     */
    private RowImpl createFromKeyBytes(byte[] keyBytes,
                                       boolean createPrimaryKey) {
        BinaryKeyIterator keyIter = createBinaryKeyIterator(keyBytes);
        if (keyIter != null) {
            TableImpl targetTable = findTargetTable(keyIter);
            if (targetTable != null) {
                RowImpl row = (createPrimaryKey ?
                               targetTable.createPrimaryKey():
                               targetTable.createRow());
                keyIter.reset();
                if (initRowFromKeyBytes(row, keyIter, targetTable)) {
                    return row;
                }
            }
        }
        return null;
    }

    /**
     * Turn the server-side byte arrays into a Row for index
     * key extraction.
     *
     * If there is a failure of any sort return null.  This method
     * needs to be flexible to work with mixed KV and table access.
     * It also cannot throw an exception or the server would die.
     *
     * The only caller of this method is IndexImpl.extractIndexKey(s).
     * Because empty rows still have a format byte the valueBytes
     * array will always have at least one byte.
     *
     * Note that even such "empty" records need to call
     * initRowFromByteValue() in order to handle schema evolved
     * records that may contain default values not in the record value.
     */
    RowImpl createRowFromBytes(byte[] keyBytes,
                               byte[] valueBytes,
                               boolean keyOnly) {
        RowImpl fullKey = createRowFromKeyBytes(keyBytes);
        if (fullKey != null) {
            /*
             * The length check is pure paranoia, but doesn't hurt.
             * See header comment above.
             */
            if (keyOnly || valueBytes.length == 0) {
                return fullKey;
            }
            Value.Format format = Value.Format.fromFirstByte(valueBytes[0]);
            if (format == Value.Format.TABLE ||
                (format == Value.Format.AVRO && r2compat)) {
                int offset = 1;
                if (format == Value.Format.AVRO && r2compat) {
                    offset =
                        PackedInteger.getReadSortedIntLength(valueBytes, 0);
                }

                if (initRowFromByteValue(fullKey, valueBytes,
                                         format, offset)) {
                    return fullKey;
                }
            }
        }
        return null;
    }

    private boolean initRowFromKeyBytes(RowImpl row,
                                        BinaryKeyIterator keyIter,
                                        TableImpl targetTable) {
        Iterator<String> pkIter = targetTable.getPrimaryKey().iterator();
        return targetTable.fillInKeyForTable(row, keyIter, pkIter);
    }

    /**
     * Size of the value is the length of the serialized value plus
     * a format byte.
     *
     * TODO: if zero-length empty values are supported, don't add one.
     */
    int getDataSize(Row row) {
        Value value = createValue(row);
        return value.getValue().length + 1;
    }

    int getKeySize(Row row) {
        return createKey(row, true).toByteArray().length;
    }

    /**
     * Serialize the non-key fields into an Avro record.
     * Special cases:
     * 1. NullValue in a nullable field.  Avro wants these to be null entries
     * in the record.  Similarly, on reconstruction (rowFromValue) null Avro
     * record entries turn into NullValue instances in the Row.
     * 2. Default values.  If a field is both optional AND not set in the Row,
     * put its default value into the Avro record.  Required fields are just
     * that -- required.
     *
     * For now this code iterates the fields in the target Row and adds them
     * to an Avro GenericRecord, which is then serialized.  GenericRecord maps
     * specific Avro types to/from Java types.  The summary is on this page:
     *     http://avro.apache.org/docs/current/api/java/
     *                 org/apache/avro/generic/package-summary.html
     * Schema records are implemented as GenericRecord.
     * Schema enums are implemented as GenericEnumSymbol.
     * Schema arrays are implemented as Collection.
     * Schema maps are implemented as Map.
     * Schema fixed are implemented as GenericFixed.
     * Schema strings are implemented as CharSequence.
     * Schema bytes are implemented as ByteBuffer.
     * Schema ints are implemented as Integer.
     * Schema longs are implemented as Long.
     * Schema floats are implemented as Float.
     * Schema doubles are implemented as Double.
     * Schema booleans are implemented as Boolean.
     *
     * The appropriate mapping and copying of types is done by the various
     * FieldValueImpl subclasses in their toAvroValue() methods.  In the case
     * of complex types this is recursive.
     */
    @SuppressWarnings("unchecked")
    Value createValue(Row row) {
        setSchema(false);
        if (schema == null) {
            return Value.EMPTY_VALUE;
        }
        boolean isAvro = (schemaId != 0 && getTableVersion() == 1);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        /*
         * If this is a normal table, write the table/schema version to the
         * stream.
         *
         * If this is a table that overlays R2 (Avro) data and it has not been
         * evolved (which excludes direct KV access) then it must be
         * written using the AVRO Value.Format in order to be readable by
         * a pure key/value application doing mixed access.
         * Evolved R2 table overlays will have a table version > 1.
         */
        if (!isAvro) {
            int writeVersion = getTableVersion();
            outputStream.write(writeVersion);
            setTableVersion(row);
        } else {
            final int size =
                PackedInteger.getWriteSortedIntLength(schemaId);
            final byte[] buf = new byte[size];
            /* Copy in the schema ID. */
            PackedInteger.writeSortedInt(buf, 0, schemaId);
            outputStream.write(buf, 0, size);
            ((RowImpl)row).setTableVersion(1);
        }

        Encoder e = JsonUtils.getEncoderFactory().
            binaryEncoder(outputStream, null);
        GenericDatumWriter<GenericRecord> w =
            new GenericDatumWriter<GenericRecord>(schema);

        /*
         * Populate an Avro GenericRecord with the row data
         */
        GenericRecord r = new GenericData.Record(schema);

        for (Map.Entry<String, FieldMapEntry> entry :
                 getFields(version).entrySet()) {
            FieldMapEntry fme = entry.getValue();
            String fieldName = entry.getKey();
            if (!isKeyComponent(fieldName)) {
                FieldValueImpl fv = (FieldValueImpl) row.get(fieldName);
                if (fv == null) {
                    fv = fme.getDefaultValue();
                }
                if (fv.isNull()) {
                    if (!fme.isNullable()) {
                        throw new IllegalCommandException
                            ("The field can not be null: " + fieldName);
                    }
                    r.put(fieldName, null);
                } else {
                    r.put(fieldName,
                          fv.toAvroValue(schema.getField
                                         (fieldName).schema()));
                }
            }
        }
        try {
            // Encode
            w.write(r, e);
            e.flush();
        } catch (IOException ioe) {
            throw new IllegalCommandException("Failed to serialize Avro: " +
                                              ioe);
        }

        /*
         *
         */
        return Value.internalCreateValue
            (outputStream.toByteArray(),
             isAvro ? Value.Format.AVRO : Value.Format.TABLE);
    }

    /**
     * Deserialize the record value that is encoded in Avro.
     *
     * Offset is requires because on the client side the byte offset is 0 but
     * on the server side a "raw" database record is used which includes an
     * empty first byte added by the system.
     *
     * There is a special case where the table version cannot be acquired.
     * When a key-only table has a non-key field added (the only evolution
     * that can happen for key-only tables, really), there may be empty
     * records in which case the data array is empty.  In this case
     * there may be schema-evolved fields that need to be defaulted so
     * this method must be called regardless of data length.
     *
     * R2/KV compatibility NOTE:
     * If the table overlays R2 (KV) data, treat it specially because it
     * may not have a table version in the data.  Unevolved R2 overlays
     * will have table version 1 and the data will start with the encoded
     * schema id.  Evolved R2 overlays will have table version > 1 and
     * values may either (1) have the encoded schema id (first byte < 0) or
     * be newly-written values, which will have the table format (1) as
     * the first byte and table version used for write as the second byte.
     */
    private boolean initRowFromByteValue(RowImpl row, byte[] data,
                                         Value.Format format, int offset) {
        GenericRecord result = null;
        if (data.length >= (offset + 1)) {
            Schema writerSchema = schema;
            int tableVersion = (format == Value.Format.AVRO ? 1
                                : data[offset]);
            row.setTableVersion(tableVersion);
            /*
             * If table versions don't match get the writer schema unless
             * this table overlays KV records in which case there won't be
             * a valid table version in the first byte.
             */
            if (tableVersion != getTableVersion() &&
                tableVersion > numTableVersions()) {

                    /*
                     * Throw TableVersionException so the caller can
                     * get the appropriate metadata and retry or take
                     * other appropriate action.
                     */
                    throw new TableVersionException(tableVersion);
            }

            try {
                if (tableVersion != getTableVersion()) {
                    String schemaString =
                        generateAvroSchema(tableVersion, false);
                    writerSchema = new Schema.Parser().parse(schemaString);
                }
                /*
                 * If a "normal" table, or operating on the client side (offset
                 * 0), move the offset past table version byte.
                 */
                if (!(format == Value.Format.AVRO) || offset == 0) {
                    offset += 1;
                }
                DatumReader<GenericRecord> reader =
                    new GenericDatumReader<GenericRecord>(writerSchema, schema);

                Decoder decoder = JsonUtils.getDecoderFactory().binaryDecoder
                    (data, offset, (data.length - offset), null);

                result = reader.read(null, decoder);
            } catch (Exception e) {
                /*
                 * Exception is a big catch-all.  Consider splitting out
                 * the possibilities, but they all end up returning false.
                 */
                return false;
            }
        }

        /*
         * Use the fields from the current (expected) table version in
         * the record to construct the returned row.  This will add
         * default values for missing fields and implicitly remove
         * (ignore) fields that have been removed.
         */
        for (Map.Entry<String, FieldMapEntry> entry :
                 getFields(version).entrySet()) {
            FieldMapEntry fme = entry.getValue();
            String fieldName = entry.getKey();
            if (!isKeyComponent(fieldName)) {
                Object o = (result != null ? result.get(fieldName) : null);
                if (o != null) {
                    Schema fieldSchema = schema.getField(fieldName).schema();
                    row.put(fieldName, FieldValueImpl.
                            fromAvroValue(fme.getField(), o, fieldSchema));
                } else if (fme.isNullable()) {
                    row.putNull(fieldName);
                } else {
                    row.put(fieldName, fme.getDefaultValue());
                }
            }
        }
        return true;
    }

    /**
     * Create a Row from the Value.
     */
    RowImpl rowFromValueVersion(ValueVersion vv, RowImpl row) {

        assert row != null;

        /*
         * Set the Version for the Row
         */
        row.setVersion(vv.getVersion());
        byte[] data = vv.getValue().getValue();

        /*
         * If the value is not the correct format this is a non-table
         * record, skip it silently.  Empty table records will have the
         * TABLE format as well as data.length == 0.  Empty table records
         * (and empty KV records) are not distinguishable so let them pass.
         */
        Value.Format format = vv.getValue().getFormat();
        if ((format != Value.Format.TABLE) &&
            (format != Value.Format.AVRO || !r2compat) &&
            (data.length > 1)) {
            return null;
        }

        /*
         * Do the check for schema after the check for the correct format
         * to filter out non-table rows in the case where the table is key-only
         * and there is a KV key in the key space that doesn't belong to the
         * table.
         */
        if (setSchema(false) == null) {
            return row;
        }

        if (initRowFromByteValue(row, data, format, 0)) {
            return row;
        }
        return null;
    }

    /**
     * Evolve a table by adding a new version associated with a new set of
     * fields.  Evolutionary changes are limited to adding/removing non-key
     * fields.  Evolution is always relative to the latest version.
     *
     * When evolution occurs this method will be called twice.  The first time
     * is on the client side where the changes are made transiently.  The
     * second time is on the server when the metadata is to be updated.  That
     * is where the version check can fail.
     */
    void evolve(FieldMap newFields) {
        if (version == 255) {
            throw new IllegalCommandException
                ("Can't evolve the table any further; too many versions");
        }

        validateEvolution(newFields);

        /*
         * it's not legal to evolve a version other than the latest one
         */
        if (version != 0 && (version != versions.size())) {
            throw new IllegalCommandException
                ("Table evolution must be performed on the latest version");
        }
        versions.add(newFields);
        if (version != 0) {
            ++version;
        }
        setSchema(true);
    }

    /**
     * Does the table have a value or is it key-only?  Key-only tables
     * can avoid some unnecessary work.
     */
    boolean hasValueFields() {
        return schema != null;
    }

    /**
     * Validation of individual evolution steps is performed on the front end
     * when modifying fields. A few additional checks are done here.
     *
     * These operations are not allowed:
     * 1.  change fields in primary key
     * 2.  remove fields that participate in an index
     */
    private void validateEvolution(FieldMap newFields) {

        /*
         * Make sure primary key is intact.  Do this in a loop on primary
         * key fields vs above because it's more efficient.
         */
        for (String fieldName : primaryKey) {
            FieldDef oldDef = getField(fieldName);
            FieldDef newDef = newFields.get(fieldName);
            if (!oldDef.equals(newDef)) {
                throw new IllegalCommandException
                    ("Evolution cannot modify the primary key");
            }
        }

        /*
         * Keys need not be validated because they cannot be modified
         * at this time, but if minor modifications to primary key fields
         * are allowed (description, default value), this should be called
         * for extra safety:
         * validate();
         */

        /*
         * Make sure indexed fields are intact.
         */
        for (Index index : indexes.values()) {
            for (String field : index.getFields()) {
                FieldDef newDef = newFields.get(field);
                if (newDef == null) {
                    throw new IllegalCommandException
                        ("Evolution cannot remove indexed fields");
                }
                /* type cannot change either */
                if (!newDef.equals(getField(field))) {
                    throw new IllegalCommandException
                        ("Evolution cannot modify indexed fields");
                }
            }
        }
    }

    /**
     * Create a JSON representation of the table and format
     */
    public String toJsonString(boolean pretty) {
        ObjectWriter writer = JsonUtils.createWriter(pretty);
        ObjectNode o = JsonUtils.createObjectNode();
        o.put("type", "table");
        o.put("name", getName());
        if (r2compat) {
            o.put("r2compat", r2compat);
        }
        o.put("description", description);
        if (parent != null) {
            o.put("parent", parent.getName());
        }
        ArrayNode key = o.putArray("shardKey");
        for (String fieldName : shardKey) {
            key.add(fieldName);
        }
        key = o.putArray("primaryKey");
        for (String fieldName : primaryKey) {
            key.add(fieldName);
        }

        /*
         * Add child tables as an array of table names.
         */
        if (children.size() != 0) {
            ArrayNode childArray = o.putArray("children");
            for (Map.Entry<String, Table> childEntry : children.entrySet()) {
                childArray.add(childEntry.getKey());
            }
        }

        /*
         * Add the fields.
         */
        getFieldMap().putFields(o);

        /*
         * Add indexes
         */
        if (indexes.size() != 0) {
            ArrayNode indexArray = o.putArray("indexes");
            for (Map.Entry<String, Index> indexEntry : indexes.entrySet()) {
                IndexImpl impl = (IndexImpl) indexEntry.getValue();
                impl.toJsonNode(indexArray.addObject());
            }
        }

        /*
         * Format the JSON into a string
         */
        try {
            return writer.writeValueAsString(o);
        } catch (IOException ioe) {
            return ioe.toString();
        }
    }

    /**
     * Add Index objects during construction.  Check for the same indexed
     * fields in a different index name.  Do not allow this.
     */
    public void addIndex(Index index) {
        checkForDuplicateIndex(index);
        indexes.put(index.getName(), index);
    }

    /**
     * Remove an Index.
     */
    public Index removeIndex(String indexName) {
        return indexes.remove(indexName);
    }

    /**
     * Create and return a BinaryKeyIterator based on this table.  If this is
     * a top-level table the first component of the key must match the table
     * id.  If this is a child table it is assumed that the key is well-formed
     * and the parent's primary key is skipped and this child's id must match.
     *
     * If a match is not found null is returned.
     */
    BinaryKeyIterator createBinaryKeyIterator(byte[] key) {
        final BinaryKeyIterator keyIter =
            new BinaryKeyIterator(key);
        if (parent != null) {
            for (int i = 0; i < parent.getNumKeyComponents(); i++) {
                if (keyIter.atEndOfKey()) {
                    return null;
                }
                keyIter.skip();
            }
        }
        if (keyIter.atEndOfKey()) {
            return null;
        }
        final String tableId = keyIter.next();
        if (getIdString().equals(tableId)) {
            return keyIter;
        }
        return null;
    }

    /**
     * Convenience wrapper for createBinaryKeyIterator + findTargetTable.
     */
    public TableImpl findTargetTable(byte[] key) {
        BinaryKeyIterator iter = createBinaryKeyIterator(key);
        if (iter != null) {
            return findTargetTable(iter);
        }
        return null;
    }

    /**
     * Find the target table for this key in this table's hierarchy.
     * The caller has set the BinaryKeyIterator on this table's id
     * in the key and it matches.  At this point, consume key entries
     * until this table's primary key count is done.  The primary key
     * contribution from parent tables must be skipped.
     */
    TableImpl findTargetTable(BinaryKeyIterator keyIter) {
        int numPrimaryKeyComponentsToSkip = primaryKey.size();
        if (parent != null) {
            numPrimaryKeyComponentsToSkip -= parent.primaryKey.size();
        }

        /* Match up the primary keys with the input keys, in number only */
        for (int i = 0; i < numPrimaryKeyComponentsToSkip; i++) {
            /* If the key is short, no match */
            if (keyIter.atEndOfKey()) {
                return null;
            }
            keyIter.skip();
        }

        /* If both are done we have a match */
        if (keyIter.atEndOfKey()) {
            return this;
        }

        /* There is another component, check for a child table */
        final String childId = keyIter.next();
        for (Table table : children.values()) {
            if (((TableImpl)table).getIdString().equals(childId)) {
                return ((TableImpl)table).findTargetTable(keyIter);
            }
        }
        return null;
    }

    /*
     * Internal methods, some for the class, some for the package.
     */

    /**
     * Is the field part of the primary key?
     */
    boolean isKeyComponent(String fieldName) {
        for (String component : primaryKey) {
            if (fieldName.equals(component)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Is the field in an index on this table?
     */
    boolean isIndexKeyComponent(String fieldName) {
        for (Index index : indexes.values()) {
            if (((IndexImpl)index).containsField(fieldName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * List of versions is 0 indexed, actual versions start at 1, so
     * subtract when indexing.  0 means get the default (latest) version.
     */
    private Map<String, FieldMapEntry> getFields(final int version1) {
        return getFieldMap(version1).getFields();
    }

    private List<String> getFieldOrder(final int version1) {
        return getFieldMap(version1).getFieldOrder();
    }

    private FieldMap getFieldMap(final int version1) {
        if (versions.size() < version1 || version1 < 0) {
            throw new IllegalCommandException
                ("Table version " + version1 + " does not exist for table " +
                 name);
        }
        int versionToGet = (version1 == 0) ? versions.size() : version1;
        return versions.get(versionToGet - 1);
    }

    private void throwMissingState(String state) {
        throw new IllegalCommandException
            ("Table is missing state required for construction: " + state);
    }

    /**
     * Validate the parameters, primary key, and shard key.
     * IllegalCommandException is thrown vs IllegalArgumentException because
     * this could be run on the server side and IAE will cause the server to
     * crash.
     */
    private void validate() {

        if (primaryKey.isEmpty()) {
            throwMissingState("primary key");
        }

        if (name == null) {
            throwMissingState("table name");
        }

        FieldMap fields = getFieldMap(0);
        if (fields == null || fields.isEmpty()) {
            throwMissingState("no fields defined");
        }

        /**
         * Primary key for child tables has to have at least one
         * component in addition to parent's key.
         */
        if (parent != null) {
            if (!(primaryKey.size() > parent.primaryKey.size())) {
                throw new IllegalCommandException
                    ("Child table needs a primary key component");
            }
        }

        /**
         * Make sure that the shardKey is a strict subset of primary key
         */
        for (int i = 0; i < shardKey.size(); i++) {
            String pkField = primaryKey.get(i);
            if (pkField == null || !pkField.equals(shardKey.get(i))) {
                throw new IllegalCommandException
                    ("Shard key must be a subset of the primary key");
            }
        }

        /*
         * Validate the primary key fields.  The properties of nullable and
         * default values are not relevant to primary keys, so they are
         * ignored.
         */
        for (String pkField : primaryKey) {
            FieldDef field = getField(pkField);
            if (field == null){
                throw new IllegalCommandException
                    ("Primary key field is not a valid field: " +
                     pkField);
            }
            if (!field.isValidKeyField()) {
                throw new IllegalCommandException
                    ("Field type cannot be part of a primary key: " +
                     field.getType() + ", field name: " + pkField);
            }
        }
    }

    /**
     * Checks if the table has a schema to initialize and if so, the
     * schema is generated from the table metadata.
     *
     * This method is called whenver the schema is used because the serialized
     * form of the Table does not include the Avro schema so it needs
     * regenerating for newly-obtained tables.
     */
    private Schema setSchema(boolean flush) {
        if (schema == null || flush) {
            String schemaString = generateAvroSchema(version, true);
            if (schemaString == null) {
                schema = null;
            } else {
                schema = new Schema.Parser().parse(schemaString);
            }
        }
        return schema;
    }

    /*
     * Constructs the fully-qualified name for this table, including parent
     * tables.  It is a dot-separated format:
     *      parentName.childName.grandChildName
     *
     * Top-level tables have a single component.
     */
    private void getTableNameInternal(StringBuilder sb) {
        if (parent != null) {
            parent.getTableNameInternal(sb);
            sb.append(SEPARATOR);
        }
        sb.append(name);
    }

    /**
     * Use table schema (primary key) to create a Row record with values
     * from the key parameter (derived from Key).  The algorithm is:
     *  1. if table has a parent, call the parent to fill in its portion
     *  2. fill in portion for this table
     * This ensures that the key components are processed in order, making each
     * table responsible for its primary key components, skipping references,
     * which point to fields in ancestor tables.
     *
     * The List<String> from the Key is {TABLE1 pk1 pk2 TABLE2 pk3 pk4 ...}
     * For example a single top-level table with in Integer primary key:
     *   {"Users", "12345"}
     * Or a composite primary key of firstName, lastName:
     *   {"Users", "jane", "doe"}
     * Or a nested Email table, under Users with pk of "address":
     *   {"Users", "12345", "Email", "jane@foo.com"}
     *
     * NOTE: the above example uses table names in place of table ids and
     * string values for the key components.  The actual algorithm iterates
     * the byte[] form of the Key.
     *
     * This method should only be called for Key objects from the store so they
     * are well-formed in terms of the expected layout.  It does have to be
     * defensive in the face of keys that match a table in structure but
     * have values that can't be deserialized correctly.  This can happen
     * if there is mixed access between KV and table applications.  An example
     * is a too-long string that can't be turned into an integer.
     *
     * Unfortunately if the key really isn't supposed to be in the table AND
     * it deserializes without an exception this will succeed.  For this,
     * and other reasons, mixing keyspace for tables and non-tables is
     * not supported.
     *
     * @return true if the key was deserialized in full, false otherwise.
     *
     * This method must not throw exceptions.
     */
    private boolean fillInKeyForTable(Row keyRecord,
                                      BinaryKeyIterator keyIter,
                                      Iterator<String> pkIter) {
        if (parent != null) {
            if (!(parent).fillInKeyForTable(keyRecord, keyIter, pkIter)) {
                return false;
            }
        }
        assert !keyIter.atEndOfKey();

        setTableVersion(keyRecord);
        String keyComponent = keyIter.next();

        if (!keyComponent.equals(getIdString())) {
            return false;
        }

        /*
         * Fill in values for primary key components that belong to this
         * table.
         */
        String lastKeyField = primaryKey.get(primaryKey.size() - 1);
        while (pkIter.hasNext()) {
            assert !keyIter.atEndOfKey();
            String field = pkIter.next();
            String val = keyIter.next();
            try {
                keyRecord.put(field, createFromKey(val, getField(field)));
            } catch (Exception e) {
                return false;
            }
            if (field.equals(lastKeyField)) {
                break;
            }
        }
        return true;
    }

    /**
     * Create FieldValue instances from String formats for keys.
     */
    private FieldValue createFromKey(String value,
                                     FieldDef field) {
        switch (field.getType()) {
        case INTEGER:
            return new IntegerValueImpl(value);
        case LONG:
            return new LongValueImpl(value);
        case STRING:
            return new StringValueImpl(value);
        case DOUBLE:
            return new DoubleValueImpl(value);
        case FLOAT:
            return new FloatValueImpl(value);
        case ENUM:
            return EnumValueImpl.createFromKey((EnumDef)field, value);
        default:
            throw new IllegalCommandException("Type is not allowed in a key: " +
                                              field.getType());
        }
    }

    /**
     * Generate Avro schema from the table schema.
     *
     * Fields that are part of the key are Key components and not part of the
     * generated Avro schema
     *
     * Fields that are not part of the key are serialized via Avro, so they
     * are part of the schema.  Each FieldDef object knows how to generate Avro
     * schema definitions in JSON format using the Jackson interface.
     *
     * TODO: maybe use Avro's schema generation API when it's available
     *
     * @param versionToUse the table version to use.  Most callers used the
     * current version.
     *
     * @param pretty set to true to generate a pretty-printed JSON string
     *
     * @return the JSON string representing the schema, or null if there are
     * no serializable fields in the table, in which case this is a key-only
     * table.
     */
    private String generateAvroSchema(final int versionToUse, boolean pretty) {
        boolean hasSchema = false;
        ObjectWriter writer = JsonUtils.createWriter(pretty);
        ObjectNode sch = JsonUtils.createObjectNode();
        sch.put("type", "record");
        sch.put("name", getName());
        ArrayNode array = sch.putArray("fields");

        Map<String, FieldMapEntry> mapToUse = getFields(versionToUse);
        for (String fname : getFieldOrder(versionToUse)) {
            FieldMapEntry fme = mapToUse.get(fname);
            if (!isKeyComponent(fname)) {
                hasSchema = true;
                ObjectNode fnode = array.addObject();
                fnode.put("name", fname);
                /*
                 * Add default value and doc (description).
                 */
                fme.createAvroTypeAndDefault(fnode);
                if (fme.getField().getDescription() != null) {
                    fnode.put("doc", fme.getField().getDescription());
                }
            }
        }
        if (!hasSchema) {
            return null;
        }
        try {
            return writer.writeValueAsString(sch);
        } catch (IOException ioe) {
            /* this should not happen */
            throw new IllegalStateException
                ("IO Error writing Avro schema string", ioe);
        }
    }

    @Override
    public String toString() {
        return "Table[" + name + ", " + indexes.size() + ", " +
               children.size() + ", " + status + "]";
    }

    /**
     * Table, field, and index names are constrained to
     * alphanumeric characters plus "_".  They must also start with
     * a letter.  This is necessary for Avro schema, which only applies to
     * field names but it's simpler to enforce the restriction for all strings.
     */
    public static void validateComponent(String component, boolean isId) {
        if (component.matches(VALID_NAME_CHAR_REGEX)) {
            throw new IllegalCommandException
                ("Table, index and field names may contain only " +
                 "alphanumeric values plus the characters \"_\"");
        }

        if ((component.charAt(0) < 'A') || (component.charAt(0) > 'z') ||
            (component.charAt(0) == '_')) {
            throw new IllegalCommandException
                ("Table, index and field names " +
                 "must start with an alphabetic character");
        }

        if (isId && (component.length() > MAX_ID_LENGTH)) {
            throw new IllegalCommandException
                ("Table names must be less than or equal to " +
                 MAX_ID_LENGTH + " characters");
        }
        if (!isId && (component.length() > MAX_NAME_LENGTH)) {
            throw new IllegalCommandException
                ("Field and index names must be less than or equal to " +
                 MAX_NAME_LENGTH + " characters");
        }
    }

    static String[] parseFullName(String fullName) {
        return fullName.split(SEPARATOR_REGEX);
    }

    /*
     * MetadataInfo
     */
    @Override
    public MetadataType getType() {
        return MetadataType.TABLE;
    }

    @Override
    public int getSourceSeqNum() {
        return versions.size();
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    static void  populateRecord(RecordValueImpl record,
                                RecordValue value) {
        for (String s : record.getFields()) {
            FieldValue v = value.get(s);
            if (v != null) {
                record.put(s, v);
            }
        }
    }

    void checkForDuplicateIndex(Index index) {
        for (Map.Entry<String, Index> entry : indexes.entrySet()) {
            if (index.getFields().equals(entry.getValue().getFields())) {
                throw new IllegalCommandException
                    ("Index is a duplicate of an existing index with " +
                     "another name.  Existing index name: " + entry.getKey() +
                     ", new index name: " + index.getName());
            }
        }
    }

    private void setTableVersion(Row row) {
        ((RowImpl)row).setTableVersion(getTableVersion());
    }
}

