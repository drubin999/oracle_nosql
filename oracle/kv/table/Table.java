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

package oracle.kv.table;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * Table is a handle on a table in the Oracle NoSQL Database.  Tables are
 * created in a store using administrative interfaces.  The Table handle is
 * obtained using {@link TableAPI#getTable} and {@link Table#getChildTable}.
 * Table contains immutable metadata for a table and acts as a factory for
 * objects used in the {@link TableAPI} interface.
 * <p>
 * Tables are defined in terms of named fields where each field is defined by
 * an instance of {@link FieldDef}.  A single record in a table is called a
 * {@link Row} and is uniquely identified by the table's {@link PrimaryKey}.
 * The primary key is defined upon construction by an ordered list of fields.
 * Tables also have a {@code shardKey} which is a proper subset of the primary
 * key.  A shard key has the property that all rows in a table that share the
 * same shard key values are stored in the same partition and can be accessed
 * transactionally.  If not otherwise defined the shard key is the same as the
 * table's primary key.
 * <p>
 * A table may be a "top-level" table, with no parent table, or a "child"
 * table, which has a parent.  A child table shares its parent table's
 * primary key but adds its own fields to its primary key to uniquely
 * identify child table rows.  It is not possible to define a shard key on
 * a child table.  It inherits its parent table's shard key.
 *
 * @since 3.0
 */
public interface Table {

    /**
     * Gets the named child table if it exists.  The name must specify a
     * direct child of this table.  Null is returned if the table does
     * not exist.
     *
     * @param name the table name for the desired table
     *
     * @return the child table or null
     */
    Table getChildTable(String name);

    /**
     * Returns true if the named child table exists, false otherwise.
     * The name must specify a direct child of this table.
     *
     * @param name the table name for the desired table
     *
     * @return true if the child table exists, false otherwise
     */
    boolean childTableExists(String name);

    /**
     * Gets all child tables of this table.  Only direct child tables
     * are returned.  If no child tables exist an empty map is returned.
     *
     * @return the map of child tables, which may be empty
     */
    Map<String, Table> getChildTables();

    /**
     * Returns the parent table, or null if this table has no parent.
     *
     * @return the parent table or null
     */
    Table getParent();

    /**
     * Returns the current version of the table metadata.  Each time a table
     * is evolved its version number will increment.  A table starts out at
     * version 1.
     *
     * @return the version of the table
     */
    int getTableVersion();

    /**
     * Gets the specified version of the table.  This allows an application to
     * use an older version of a table if it is not prepared to use the latest
     * version (the default).  Table versions change when a table is evolved by
     * adding or removing fields.  Most applications should use the default
     * (latest) version if at all possible.
     *<p>
     * The version must be a valid version between 0 and the value of
     * {@link #numTableVersions}.  If the version is 0 the latest
     * version is used.  Actual version numbers begin with 1.
     *
     * @param version the version to use
     *
     * @return the requested table
     *
     * @throws IllegalArgumentException if the requested version number is
     * negative or out of range of the known versions
     */
    Table getVersion(int version);

    /**
     * Returns the number of versions of this table that exist.  Versions are
     * identified by integer, starting with 1.  Versions are added if the table
     * is modified (evolved) with each new version getting the next integer.
     * <p>
     * By default when a table is accessed its current (latest) version will be
     * used.  It is possible to see an earlier or later version using
     * {@link #getVersion}.
     *
     * @return the number of table versions
     */
    int numTableVersions();

    /**
     * Gets the named index if it exists, null otherwise.
     *
     * @param indexName the name of the index
     *
     * @return the index or null
     */
    Index getIndex(String indexName);

    /**
     * Returns a map of all of the indexes on this table.  If there are no
     * indexes defined an empty map is returned.
     *
     * @return a map of indexes
     */
    Map<String, Index> getIndexes();

    /**
     * Gets the name of the table.
     *
     * @return the table name
     */
    String getName();

    /**
     * Gets the table's fully-qualified name which includes its ancestor tables
     * in a dot (".") separated path.  For top-level tables this value is the
     * same as the table name.
     *
     * @return the full table name
     */
    String getFullName();

    /**
     * Gets the table's description if present, otherwise null.  This is a
     * description of the table that is optionally supplied during
     * definition of the table.
     *
     * @return the description or null
     */
    String getDescription();

    /**
     * Returns the field names of the table in declaration order which is
     * done during table definition.  This will never be empty.
     *
     * @return the fields
     */
    List<String> getFields();

    /**
     * Returns the named field from the table definition, or null if the field
     * does not exist.
     *
     * @return the field or null
     */
    FieldDef getField(String name);

    /**
     * Returns true if the named field is nullable.
     *
     * @param name the name of the field
     *
     * @return true if the named field is nullable
     *
     * @throws IllegalArgumentException if the named field does not exist
     * in the table definition
     */
    boolean isNullable(String name);

    /**
     * Creates an instance using the default value for the named field.
     * The return value is {@link FieldValue} and not a more specific type
     * because in the case of nullable fields the default will be a null value,
     * which is a special value that returns true for {@link #isNullable}.
     *
     * @param name the name of the field
     *
     * @return a default value
     *
     * @throws IllegalArgumentException if the named field does not exist
     * in the table definition
     */
    FieldValue getDefaultValue(String name);

    /**
     * Returns a list of the field names that comprise the primary key for the
     * table.  This will never be null.
     *
     * @return the list of fields
     */
    List<String> getPrimaryKey();

    /**
     * Returns the shard key fields.  This is a strict subset of the
     * primary key.  This will never be null.
     *
     * @return the list of fields
     */
    List<String> getShardKey();

    /**
     * Creates an empty Row for the table that can hold any field value.
     *
     * @return an empty row
     */
    Row createRow();

    /**
     * Creates a Row for the table populated with relevant fields from the
     * {@code RecordValue} parameter.  Only fields that belong in
     * this table are added.  Other fields are silently ignored.
     *
     * @param value a RecordValue instance containing fields that may or
     * may not be applicable to this table
     *
     * @return the row
     */
    Row createRow(RecordValue value);

    /**
     * Creates a Row using the default values for all fields.  This includes
     * the primary key fields which are not allowed to be defaulted in put
     * operations.
     *
     * @return a new row
     */
    Row createRowWithDefaults();

    /**
     * Creates an empty {@code PrimaryKey} for the table that can only hold
     * fields that are part of the primary key for the table.  Other fields
     * will be rejected if an attempt is made to set them on the returned
     * object.
     *
     * @return an empty primary key
     */
    PrimaryKey createPrimaryKey();

    /**
     * Creates a {@code PrimaryKey} for the table populated with relevant
     * fields from the {@code RecordValue} parameter.  Only fields that belong
     * in this primary key are added.  Other fields are silently ignored.
     *
     * @param value a {@code RecordValue} instance containing fields that may
     * or may not be applicable to this table's primary key.  Only fields that
     * belong in the key are added.
     */
    PrimaryKey createPrimaryKey(RecordValue value);

    /**
     * Creates a ReturnRow object for the ReturnRow parameter in
     * table put and delete methods in {@link TableAPI} such as
     * {@link TableAPI#put} and {@link TableAPI#delete}.
     *
     * @param returnChoice describes the state to return
     *
     * @return a {@code ReturnRow} containing the choice passed in the
     * parameter
     */
    ReturnRow createReturnRow(ReturnRow.Choice returnChoice);

    /**
     * Creates a Row based on JSON string input.  If the {@code exact}
     * parameter is true the input string must contain an exact match to
     * the table row, including all fields, required or not.  It must not
     * have additional data.  If false, only matching fields will be added
     * and the input may have additional, unrelated data.
     *
     * @param jsonInput a JSON string
     *
     * @param exact set to true for an exact match.  See above
     *
     * @throws IllegalArgumentException if exact is true and a field is
     * missing or extra.  It will also be thrown if a field type or value is
     * not correct
     *
     * @throws IllegalArgumentException if the input is malformed
     */
    Row createRowFromJson(String jsonInput, boolean exact);

    /**
     * Creates a Row based on JSON input.  If the {@code exact}
     * parameter is true the input string must contain an exact match to
     * the table row, including all fields, required or not.  It must not
     * have additional data.  If false, only matching fields will be added
     * and the input may have additional, unrelated data.
     *
     * @param jsonInput a JSON string
     *
     * @param exact set to true for an exact match.  See above
     *
     * @throws IllegalArgumentException if exact is true and a field is
     * missing or extra.  It will also be thrown if a field type or value is
     * not correct
     *
     * @throws IllegalArgumentException if the input is malformed
     */
    Row createRowFromJson(InputStream jsonInput, boolean exact);

    /**
     * Creates a {@code PrimaryKey} based on JSON input.  If the
     * {@code exact}
     * parameter is true the input string must contain an exact match to
     * the primary key, including all fields, required or not.  It must not
     * have additional data.  If false, only matching fields will be added
     * and the input may have additional, unrelated data.
     *
     * @param jsonInput a JSON string
     *
     * @param exact set to true for an exact match.  See above
     *
     * @throws IllegalArgumentException if exact is true and a field is
     * missing or extra.  It will also be thrown if a field type or value is
     * not correct
     *
     * @throws IllegalArgumentException if the input is malformed
     */
    PrimaryKey createPrimaryKeyFromJson(String jsonInput,
                                        boolean exact);

    /**
     * Creates a {@code PrimaryKey} based on JSON input.  If the
     * {@code exact}
     * parameter is true the input string must contain an exact match to
     * the primary key, including all fields, required or not.  It must not
     * have additional data.  If false, only matching fields will be added
     * and the input may have additional, unrelated data.
     *
     * @param jsonInput a JSON string
     *
     * @param exact set to true for an exact match.  See above
     *
     * @throws IllegalArgumentException if exact is true and a field is
     * missing or extra.  It will also be thrown if a field type or value is
     * not correct
     *
     * @throws IllegalArgumentException if the input is malformed
     */
    PrimaryKey createPrimaryKeyFromJson(InputStream jsonInput,
                                        boolean exact);

    /**
     * Creates a {@code FieldRange} object used to specify a value range for use
     * in a table iteration operation in {@link TableAPI}.
     *
     * @param fieldName the name of the field from the PrimaryKey to use for
     * the range
     *
     * @throws IllegalArgumentException if the field is not defined in the
     * table's primary key
     *
     * @return an empty {@code FieldRange} based on the table and field
     */
    FieldRange createFieldRange(String fieldName);

    /**
     * Clone the table.
     *
     * @return a deep copy of the table
     */
    Table clone();
}

