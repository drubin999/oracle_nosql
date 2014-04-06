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
import java.io.InputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldRange;
import oracle.kv.table.FieldValue;
import oracle.kv.table.Index;
import oracle.kv.table.IndexKey;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Table;

import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

/**
 * Implementation of the Index interface.
 */
public class IndexImpl implements Index, Serializable {

    private static final long serialVersionUID = 1L;
    private final String name;
    private final String description;
    private final TableImpl table;
    private final List<String> fields;
    private IndexStatus status;

    public enum IndexStatus {
        /** Index is transient */
        TRANSIENT() {
            @Override
            public boolean isTransient() {
                return true;
            }
        },

        /** Index is being populated */
        POPULATING() {
            @Override
            public boolean isPopulating() {
                return true;
            }
        },

        /** Index is populated and ready for use */
        READY() {
            @Override
            public boolean isReady() {
                return true;
            }
        };

        /**
         * Returns true if this is the {@link #TRANSIENT} type.
         * @return true if this is the {@link #TRANSIENT} type
         */
        public boolean isTransient() {
            return false;
        }

        /**
         * Returns true if this is the {@link #POPULATING} type.
         * @return true if this is the {@link #POPULATING} type
         */
	public boolean isPopulating() {
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

    public IndexImpl(String name, TableImpl table, List<String> fields,
                     String description) {
        this.name = name;
        this.table = table;
        this.fields = fields;
        this.description = description;
        status = IndexStatus.TRANSIENT;
        validate();
    }

    @Override
    public Table getTable() {
        return table;
    }

    @Override
    public String getName()  {
        return name;
    }

    @Override
    public List<String> getFields() {
        return Collections.unmodifiableList(fields);
    }

    @Override
    public String getDescription()  {
        return description;
    }

    @Override
    public IndexKeyImpl createIndexKey() {
        return new IndexKeyImpl(this);
    }

    @Override
    public IndexKeyImpl createIndexKey(RecordValue value) {
        IndexKeyImpl key = new IndexKeyImpl(this);
        TableImpl.populateRecord(key, value);
        return key;
    }

    @Override
    public IndexKey createIndexKeyFromJson(String jsonInput, boolean exact) {
        return createIndexKeyFromJson
            (new ByteArrayInputStream(jsonInput.getBytes()), exact);
    }

    @Override
    public IndexKey createIndexKeyFromJson(InputStream jsonInput,
                                           boolean exact) {
        IndexKeyImpl key = createIndexKey();
        table.createFromJson(key, jsonInput, exact);
        return key;
    }

    @Override
    public FieldRange createFieldRange(String fieldName) {
        FieldDef def = table.getField(fieldName);
        if (def == null) {
            throw new IllegalArgumentException
                ("Field does not exist in table definition: " + fieldName);
        }
        if (!containsField(fieldName)) {
            throw new IllegalArgumentException
                ("Field does not exist in index: " + fieldName);
        }
        return new FieldRange(fieldName, def);
    }

    /**
     * Returns true if the index comprises only fields from the table's primary
     * key.
     */
    public boolean isKeyOnly() {
        for (String field : fields) {
            if (!table.isKeyComponent(field)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Return true if this index has multiple keys/record.  This happens if
     * there is an array in the index.  An index can only contain one array.
     */
    public boolean isMultiKey() {
        return (findArray() != null);
    }

    public IndexStatus getStatus() {
        return status;
    }

    public void setStatus(IndexStatus status) {
        this.status = status;
    }

    public TableImpl getTableImpl() {
        return table;
    }

    List<String> getFieldsInternal() {
        return fields;
    }

    /**
     * If there's an array in the index return its name.
     */
    private String findArray() {
        for (String fieldName : fields) {
            if (table.getField(fieldName).isArray()) {
                return fieldName;
            }
        }
        return null;
    }

    /**
     * Extract an index key from the key and data for this
     * index.  The key has already matched this index.
     *
     * While not likely it is possible that the record is not actually  a
     * table record and the key pattern happens to match.  Such records
     * will fail to be deserialized and throw an exception.  Rather than
     * treating this as an error, silently ignore it.
     *
     * TODO: maybe make this faster.  Right now it turns the key and data
     * into a Row and extracts from that object which is a relatively
     * expensive operation, including full Avro deserialization.
     */
    public byte[] extractIndexKey(byte[] key,
                                  byte[] data,
                                  boolean keyOnly) {
        RowImpl row = table.createRowFromBytes(key, data, keyOnly);
        if (row != null) {
            return serializeIndexKey(row);
        }
        return null;
    }

    /**
     * Extract multiple index keys from a single record.  This is used if
     * one of the indexed fields is an array.  Only one array is allowed
     * in an index.
     *
     * While not likely it is possible that the record is not actually  a
     * table record and the key pattern happens to match.  Such records
     * will fail to be deserialized and throw an exception.  Rather than
     * treating this as an error, silently ignore it.
     *
     * TODO: can this be done without reserializing to Row?  It'd be
     * faster but more complex.
     *
     * 1.  Deserialize to RowImpl
     * 2.  Find the array value and get its size
     * 3.  for each array entry (size), serialize a key using that entry
     */
    public List<byte[]> extractIndexKeys(byte[] key,
                                         byte[] data,
                                         boolean keyOnly) {

        RowImpl row = table.createRowFromBytes(key, data, keyOnly);
        if (row != null) {
            String arrayField = findArray();
            assert arrayField != null;

            int arraySize = row.get(arrayField).asArray().size();
            ArrayList<byte[]> returnList = new ArrayList<byte[]>(arraySize);
            for (int i = 0; i < arraySize; i++) {
                returnList.add(serializeIndexKey(row, i));
            }
            return returnList;
        }
        return null;
    }

    public void toJsonNode(ObjectNode node) {
        node.put("name", name);
        node.put("description", description);
        ArrayNode fieldArray = node.putArray("fields");
        for (String s : fields) {
            fieldArray.add(s);
        }
    }

    /**
     * Validate that the name, fields, and types of the index match
     * the table.
     */
    private void validate() {
        TableImpl.validateComponent(name, false);
        boolean hasArray = false;
        if (fields.isEmpty()) {
            throw new IllegalCommandException
                ("Index requires at least one field");
        }
        for (String field : fields) {
            if (field == null || field.length() == 0) {
                throw new IllegalCommandException
                    ("Invalid (null or empty) index field name");
            }

            FieldDef def = table.getField(field);
            if (def == null) {
                throw new IllegalCommandException
                    ("Index field not found in table: " + field);
            }
            if (!def.isValidIndexField()) {
                throw new IllegalCommandException
                    ("Field type is not valid in an index: " +
                     def.getType() + ", field name: " + field);
            }
            if (def.isArray()) {
                if (hasArray) {
                    throw new IllegalCommandException
                        ("Indexes may contain only one array field");
                }
                hasArray = true;
            }
        }
        table.checkForDuplicateIndex(this);
    }

    @Override
    public String toString() {
        return "Index[" + name + ", " + table.getId() + ", " + status + "]";
    }

    /**
     * Serialize the index fields from the RecordValueImpl argument.
     * Fields are extracted in index order.  It is assumed that the caller has
     * validated the record and that if it is an IndexKey that user-provided
     * fields are correct and in order.
     *
     * @param record the record to extract.  This may be an IndexKeyImpl or
     * RowImpl.  In both cases the caller can vouch for the validity of the
     * object.
     *
     * @param arrayIndex if an array is involved use this index to get the
     * appropriate value for indexing.
     *
     * TODO:
     *  - navigation into complex types to indexed simple types
     */
    public byte[] serializeIndexKey(RecordValueImpl record, int arrayIndex) {
        TupleOutput out = null;
        try {
            out = new TupleOutput();
            for (String s : fields) {
                FieldValue val = record.get(s);
                if (val == null) {
                    break; /* done with fields */
                }
                FieldDef def = table.getField(s);

                /*
                 * If the field is an array use its type, which must be simple,
                 * and indexable.
                 */
                if (def.getType() == FieldDef.Type.ARRAY) {
                    def = ((ArrayDefImpl)def).getElement();
                    val = ((ArrayValueImpl)val).get(arrayIndex);
                }

                switch (def.getType()) {
                case INTEGER:
                    out.writeSortedPackedInt(val.asInteger().get());
                    break;
                case STRING:
                    out.writeString(val.asString().get());
                    break;
                case LONG:
                    out.writeSortedPackedLong(val.asLong().get());
                    break;
                case DOUBLE:
                    out.writeSortedDouble(val.asDouble().get());
                    break;
                case FLOAT:
                    out.writeSortedFloat(val.asFloat().get());
                    break;
                case ENUM:
                    /* enumerations are sorted by declaration order */
                    out.writeSortedPackedInt(val.asEnum().getIndex());
                    break;
                case ARRAY:
                case BINARY:
                case BOOLEAN:
                case FIXED_BINARY:
                case MAP:
                case RECORD:
                    throw new IllegalStateException
                        ("Type not supported in indexes: " +
                         def.getType());
                }
            }
            return out.toByteArray();
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException ioe) {
            }
        }
    }

    /**
     * This is the version used by most callers.
     */
    public byte[] serializeIndexKey(RecordValueImpl record) {
        return serializeIndexKey(record, 0);
    }

    /**
     * Deserialize an index key into IndexKey.  The caller will also have
     * access to the primary key bytes which can be turned into a PrimaryKey
     * and combined with the IndexKey for the returned KeyPair.
     *
     * Arrays -- if there is an array index the index key returned will
     * be the serialized value of a single array entry and not the array
     * itself. This value needs to be deserialized back into a single-value
     * array.
     *
     * @param data the bytes
     * @param partialOK true if not all fields must be in the data stream.
     */
    public IndexKeyImpl rowFromIndexKey(byte[] data, boolean partialOK) {
        IndexKeyImpl ikey = createIndexKey();
        TupleInput input = null;
        try {
            input = new TupleInput(data);
            for (String s : fields) {
                if (input.available() <= 0) {
                    break;
                }
                FieldDef def = table.getField(s);
                switch (def.getType()) {
                case INTEGER:
                    ikey.put(s, input.readSortedPackedInt());
                    break;
                case STRING:
                    ikey.put(s, input.readString());
                    break;
                case LONG:
                    ikey.put(s, input.readSortedPackedLong());
                    break;
                case DOUBLE:
                    ikey.put(s, input.readSortedDouble());
                    break;
                case FLOAT:
                    ikey.put(s, input.readSortedFloat());
                    break;
                case ENUM:
                    ikey.putEnum(s, input.readSortedPackedInt());
                    break;
                case ARRAY:
                    ArrayValueImpl array = ikey.putArray(s);
                    readArrayElement(array, input);
                    break;
                case BINARY:
                case BOOLEAN:
                case FIXED_BINARY:
                case MAP:
                case RECORD:
                    throw new IllegalStateException
                        ("Type not supported in indexes: " +
                         def.getType());
                }
            }
            if (!partialOK && (ikey.size() != fields.size())) {
                throw new IllegalStateException
                    ("Missing fields from index data");
            }
            return ikey;
        } finally {
            try {
                if (input != null) {
                    input.close();
                }
            } catch (IOException ioe) {
            }
        }
    }

    private void readArrayElement(ArrayValueImpl array,
                                  TupleInput input) {
        switch (array.getDefinition().getElement().getType()) {
        case INTEGER:
            array.add(input.readSortedPackedInt());
            break;
        case STRING:
            array.add(input.readString());
            break;
        case LONG:
            array.add(input.readSortedPackedLong());
            break;
        case DOUBLE:
            array.add(input.readSortedDouble());
            break;
        case FLOAT:
            array.add(input.readSortedFloat());
            break;
        case ENUM:
            array.addEnum(input.readSortedPackedInt());
            break;
        default:
            throw new IllegalStateException("Type not supported in indexes: ");
        }
    }

    boolean containsField(String fieldName) {
        for (String s : fields) {
            if (s.equals(fieldName)) {
                return true;
            }
        }
        return false;
    }
}

