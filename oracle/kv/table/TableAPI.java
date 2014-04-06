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

import java.util.List;
import java.util.Map;

import oracle.kv.Consistency;
import oracle.kv.ConsistencyException;
import oracle.kv.Direction;
import oracle.kv.Durability;
import oracle.kv.DurabilityException;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.OperationExecutionException;
import oracle.kv.RequestTimeoutException;
import oracle.kv.Version;

/**
 * TableAPI is a handle for the table interface to an Oracle NoSQL
 * store. Tables are an independent layer implemented on top
 * of the {@link KVStore key/value interface}.  While the two interfaces
 * are not incompatible, in general applications will use one or the other.
 * To create a TableAPI instance use {@link KVStore#getTableAPI getTableAPI()}.
 * <p>
 * The table interface is required to use secondary indexes and supported data
 * types.
 * <p>
 * Tables are similar to tables in a relational database.  They are named and
 * contain a set of strongly typed records, called rows.  Rows in an Oracle
 * NoSQL Database table are analogous to rows in a relational system and each
 * row has one or more named, typed data values.  These fields can be compared
 * to a relational database column.  A single top-level row in a table is
 * contained in a {@link Row} object.  Row is used as return value for TableAPI
 * get operations as well as a key plus value object for TableAPI put
 * operations. All rows in a given table have the same fields.  Tables have a
 * well-defined primary key which comprises one or more of its fields, in
 * order.  Primary key fields must be simple (single-valued) data types.
 * <p>
 * The data types supported in tables are well-defined and include simple
 * single-valued types such as Integer, String, Date, etc., in addition to
 * several complex, multi-valued types -- Array, Map, and Record.  Complex
 * objects allow for creation of arbitrarily complex, nested rows.
 * <p>
 * All operations on this interface include parameters that supply optional
 * arguments to control non-default behavior.  The types of these parameter
 * objects varies depending on whether the operation is a read, update,
 * or a multi-read style operation returning more than one result or an
 * iterator.
 * <p>
 * In order to control, and take advantage of sharding across partitions tables
 * may be defined in a hierarchy.  A top-level table is one without a parent
 * and may be defined in a way such that its primary key spreads the table rows
 * across partitions.  The primary key for this sort of table has a
 * <em>complete</em> shard key but an empty minor key.  Tables with parents
 * always have a primary key with a minor key.  The primary key of a child
 * table comprises the primary key of its immediate parent plus the fields
 * defined in the child table as being part of its primary key.  This means
 * that the fields of a child table implicitly include the primary key fields
 * of all of its ancestors.
 * <p>
 * Some of the methods in this interface include {@link MultiRowOptions} which
 * can be used to cause operations to return not only rows from the target
 * table but from its ancestors and descendant tables as well.  This allows
 * for efficient and transactional mechanisms to return related groups of rows.
 * The MultiRowOptions object is also used to specify value ranges that apply
 * to the operation.
 *
 * @since 3.0
 */
public interface TableAPI {

    /**
     * Gets an instance of a table.  This method can be retried in the event
     * that the specified table is not yet fully initialized.  This call will
     * typically go to a server node to find the requested metadata and/or
     * verify that it is current.
     * <p>
     * This interface will only retrieve top-level tables -- those with no
     * parent table.  Child tables are retrieved using
     * {@link Table#getChildTable}.
     *
     * @param tableName the name of the target table
     *
     * @return the table or null if the table does not exist
     *
     * @throws FaultException if the operation fails to communicate with a
     * server node that has the table metadata
     */
    Table getTable(String tableName) throws FaultException;

    /**
     * Gets all known tables.  Only top-level tables -- those without parent
     * tables -- are returned. Child tables of a parent are retrieved using
     * {@link Table#getChildTables}.
     *
     * @return the map of tables
     *
     * @throws FaultException if the operation fails to communicate with a
     * server node that has the table metadata
     */
    Map<String, Table> getTables() throws FaultException;

    /**
     * Gets the {@code Row} associated with the primary key.
     *
     * @param key the primary key for a table.  It must be a complete primary
     * key, with all fields set.
     *
     * @param readOptions non-default options for the operation or null to
     * get default behavior
     *
     * @return the matching Row, or null if not found
     *
     * @throws ConsistencyException if the specified {@link Consistency} cannot
     * be satisfied
     *
     * @throws RequestTimeoutException if the request timeout interval was
     * exceeded
     *
     * @throws FaultException if the operation cannot be completed for any
     * reason
     *
     * @throws IllegalArgumentException if the primary key is not complete
     */
    Row get(PrimaryKey key,
            ReadOptions readOptions)
        throws ConsistencyException, RequestTimeoutException, FaultException;

    /**
     * Returns the rows associated with a partial primary key in an
     * atomic manner.  Rows are returned in primary key order.  The key used
     * must contain all of the fields defined for the table's shard key.
     *
     * @param key the primary key for the operation.  It may be partial or
     * complete.
     *
     * @param getOptions a {@code MultiRowOptions} object used to control
     * ranges in the operation and whether ancestor and descendant tables are
     * included in the results.  It may be null.  The table used to construct
     * the {@code PrimaryKey} parameter is always included as a target.
     *
     * @param readOptions non-default options for the operation or null to
     * get default behavior
     *
     * @return a list of matching rows, one for each selected record, or an
     * empty list if no rows are matched
     *
     * @throws ConsistencyException if the specified {@link Consistency} cannot
     * be satisfied
     *
     * @throws RequestTimeoutException if the request timeout interval was
     * exceeded
     *
     * @throws FaultException if the operation cannot be completed for any
     * reason
     *
     * @throws IllegalArgumentException if the primary key is malformed or
     * does not contain the required fields
     */
    List<Row> multiGet(PrimaryKey key,
                       MultiRowOptions getOptions,
                       ReadOptions readOptions)
        throws ConsistencyException, RequestTimeoutException, FaultException;

    /**
     * Return the rows associated with a partial primary key in an
     * atomic manner.  Keys are returned in primary key order.  The key used
     * must contain all of the fields defined for the table's shard key.
     *
     * @param key the primary key for the operation.  It may be partial or
     * complete
     *
     * @param getOptions a {@code MultiRowOptions} object used to control
     * ranges in the operation and whether ancestor and descendant tables are
     * included in the results.  It may be null.  The table used to construct
     * the {@code PrimaryKey} parameter is always included as a target.
     *
     * @param readOptions non-default options for the operation or null to
     * get default behavior
     *
     * @return a list of matching keys, one for each selected row, or an
     * empty list if no rows are matched
     *
     * @throws ConsistencyException if the specified {@link Consistency} cannot
     * be satisfied
     *
     * @throws RequestTimeoutException if the request timeout interval was
     * exceeded
     *
     * @throws FaultException if the operation cannot be completed for any
     * reason
     *
     * @throws IllegalArgumentException if the primary key is malformed or
     * does not contain the required fields
     */
    List<PrimaryKey> multiGetKeys(PrimaryKey key,
                                  MultiRowOptions getOptions,
                                  ReadOptions readOptions)
        throws ConsistencyException, RequestTimeoutException, FaultException;

    /**
     * Returns an iterator over the rows associated with a partial primary key.
     *
     * @param key the primary key for the operation.  It may be partial or
     * complete shard key.  If the key contains a partial shard key the
     * iteration goes to all partitions in the store.  If the key contains a
     * complete shard key the operation is restricted to the target partition.
     * If the key has no fields set the entire table is matched.
     *
     * @param getOptions a {@code MultiRowOptions} object used to control
     * ranges in the operation and whether ancestor and descendant tables are
     * included in the results.  It may be null.  The table used to construct
     * the {@code PrimaryKey} parameter is always included as a target.
     *
     * @param iterateOptions the non-default arguments for consistency of the
     * operation and to control the iteration or null to get default behavior.
     * The default Direction in {@code TableIteratorOptions} is
     * {@link Direction#UNORDERED}. If the primary key contains a complete
     * shard key both {@link Direction#FORWARD} and {@link Direction#REVERSE}
     * are allowed.
     *
     * @return an iterator over the matching rows, or if none match an empty
     * iterator
     *
     * @throws ConsistencyException if the specified {@link Consistency} cannot
     * be satisfied
     *
     * @throws RequestTimeoutException if the request timeout interval was
     * exceeded
     *
     * @throws FaultException if the operation cannot be completed for any
     * reason
     *
     * @throws IllegalArgumentException if the primary key is malformed
     */
    TableIterator<Row> tableIterator(PrimaryKey key,
                                     MultiRowOptions getOptions,
                                     TableIteratorOptions iterateOptions)
        throws ConsistencyException, RequestTimeoutException, FaultException;

    /**
     * Returns an iterator over the keys associated with a partial primary key.
     *
     * @param key the primary key for the operation.  It may be partial or
     * complete shard key.  If the key contains a partial shard key the
     * iteration goes to all partitions in the store.  If the key contains a
     * complete shard key the operation is restricted to the target partition.
     * If the key has no fields set the entire table is matched.
     *
     * @param getOptions a {@code MultiRowOptions} object used to control
     * ranges in the operation and whether ancestor and descendant tables are
     * included in the results.  It may be null.  The table used to construct
     * the {@code PrimaryKey} parameter is always included as a target.
     *
     * @param iterateOptions the non-default arguments for consistency of the
     * operation and to control the iteration or null to get default behavior.
     * The default Direction in {@code TableIteratorOptions} is
     * {@link Direction#UNORDERED}. If the primary key contains a complete
     * shard key both {@link Direction#FORWARD} and {@link Direction#REVERSE}
     * are allowed.
     *
     * @return an iterator over the keys for the matching rows, or if none
     * match an empty iterator
     *
     * @throws ConsistencyException if the specified {@link Consistency} cannot
     * be satisfied
     *
     * @throws RequestTimeoutException if the request timeout interval was
     * exceeded
     *
     * @throws FaultException if the operation cannot be completed for any
     * reason
     *
     * @throws IllegalArgumentException if the primary key is malformed
     */
    TableIterator<PrimaryKey> tableKeysIterator
        (PrimaryKey key,
         MultiRowOptions getOptions,
         TableIteratorOptions iterateOptions)
        throws ConsistencyException, RequestTimeoutException, FaultException;

    /**
     * Returns an iterator over the rows associated with an index key.
     * This method requires an additional database read on the server side
     * to get row information for matching rows.  Ancestor table rows for
     * matching index rows may be returned as well if specified in the
     * {@code getOptions} paramter.  Index operations may not specify the
     * return of child table rows.
     *
     * @param key the index key for the operation.  It may be partial or
     * complete.  If the key has no fields set the entire index is matched.
     *
     * @param getOptions a {@code MultiRowOptions} object used to control
     * ranges in the operation and whether ancestor and descendant tables are
     * included in the results.  It may be null.  The table on which the
     * index is defined is always included as a target.  Child tables cannot
     * be included for index operations.
     *
     * @param iterateOptions the non-default arguments for consistency of the
     * operation and to control the iteration or null to get default behavior.
     * The default Direction in {@code TableIteratorOptions} is
     * {@link Direction#FORWARD}.
     *
     * @return an iterator over the matching rows, or if none match an empty
     * iterator
     *
     * @throws ConsistencyException if the specified {@link Consistency} cannot
     * be satisfied
     *
     * @throws RequestTimeoutException if the request timeout interval was
     * exceeded
     *
     * @throws FaultException if the operation cannot be completed for any
     * reason
     *
     * @throws IllegalArgumentException if the primary key is malformed
     *
     * @throws UnsupportedOperationException if the {@code getOptions}
     * parameter specifies the return of child tables
     */
    TableIterator<Row> tableIterator(IndexKey key,
                                     MultiRowOptions getOptions,
                                     TableIteratorOptions iterateOptions)
        throws ConsistencyException, RequestTimeoutException, FaultException;

    /**
     * Return the keys for matching rows associated with an index key.  The
     * iterator returned only references information directly available from
     * the index.  No extra fetch operations are performed.  Ancestor table
     * keys for matching index keys may be returned as well if specified in the
     * {@code getOptions} paramter.  Index operations may not specify the
     * return of child table keys.
     *
     * @param key the index key for the operation.  It may be partial or
     * complete.  If the key has no fields set the entire index is matched.
     *
     * @param getOptions a {@code MultiRowOptions} object used to control
     * ranges in the operation and whether ancestor and descendant tables are
     * included in the results.  It may be null.  The table on which the
     * index is defined is always included as a target.  Child tables cannot
     * be included for index operations.
     *
     * @param iterateOptions the non-default arguments for consistency of the
     * operation and to control the iteration or null to get default behavior.
     * The default Direction in {@code TableIteratorOptions} is
     * {@link Direction#FORWARD}.
     *
     * @return an iterator over {@code KeyPair} objects, which provide access
     * to both the {@link PrimaryKey} associated with a match but the values
     * in the matching {@link IndexKey} as well without an additional fetch of
     * the Row itself.
     *
     * @throws ConsistencyException if the specified {@link Consistency} cannot
     * be satisfied
     *
     * @throws RequestTimeoutException if the request timeout interval was
     * exceeded
     *
     * @throws FaultException if the operation cannot be completed for any
     * reason
     *
     * @throws IllegalArgumentException if the primary key is malformed
     *
     * @throws UnsupportedOperationException if the {@code getOptions}
     * parameter specifies the return of child tables
     */
    TableIterator<KeyPair> tableKeysIterator
        (IndexKey key,
         MultiRowOptions getOptions,
         TableIteratorOptions iterateOptions)
        throws ConsistencyException, RequestTimeoutException, FaultException;

    /**
     * Puts a row into a table.  The row must contain a complete primary
     * key and all required fields.
     *
     * @param row the row to put
     *
     * @param prevRow a {@code ReturnRow} object to contain the previous row
     * value and version associated with the given row, or null if they should
     * not be returned.  If a previous row does not exist, or the {@link
     * ReturnRow.Choice} specifies that they should not be returned, the
     * version in this object is set to null and none of the row's fields are
     * available.
     *
     * @param writeOptions non-default arguments controlling the
     * durability of the operation, or null to get default behavior.
     * See {@code WriteOptions} for more information.
     *
     * @return the version of the new row value
     *
     * @throws DurabilityException if the specified {@link Durability} cannot
     * be satisfied
     *
     * @throws RequestTimeoutException if the request timeout interval was
     * exceeded
     *
     * @throws FaultException if the operation cannot be completed for any
     * reason
     *
     * @throws IllegalArgumentException if the <code>row</code> does not have
     * a complete primary key or is otherwise invalid
     */
    Version put(Row row,
                ReturnRow prevRow,
                WriteOptions writeOptions)
        throws DurabilityException, RequestTimeoutException, FaultException;

    /**
     * Puts a row into a table, but only if the row does not exist.  The row
     * must contain a complete primary key and all required fields.
     *
     * @param row the row to put
     *
     * @param prevRow a {@code ReturnRow} object to contain the previous row
     * value and version associated with the given row, or null if they should
     * not be returned.  If a previous row does not exist, or the {@link
     * ReturnRow.Choice} specifies that they should not be returned, the
     * version in this object is set to null and none of the row's fields are
     * available.
     *
     * @param writeOptions non-default arguments controlling the
     * durability of the operation, or null to get default behavior.
     *
     * @return the version of the new value, or null if an existing value is
     * present and the put is unsuccessful
     *
     * @throws DurabilityException if the specified {@link Durability} cannot
     * be satisfied
     *
     * @throws RequestTimeoutException if the request timeout interval was
     * exceeded
     *
     * @throws FaultException if the operation cannot be completed for any
     * reason
     *
     * @throws IllegalArgumentException if the <code>row</code> does not have
     * a complete primary key or is otherwise invalid
     */
    Version putIfAbsent(Row row,
                        ReturnRow prevRow,
                        WriteOptions writeOptions)
        throws DurabilityException, RequestTimeoutException, FaultException;

    /**
     * Puts a row into a table, but only if the row already exists.  The row
     * must contain a complete primary key and all required fields.
     *
     * @param row the row to put
     *
     * @param prevRow a {@code ReturnRow} object to contain the previous row
     * value and version associated with the given row, or null if they should
     * not be returned.  If a previous row does not exist, or the {@link
     * ReturnRow.Choice} specifies that they should not be returned, the
     * version in this object is set to null and none of the row's fields are
     * available.
     *
     * @param writeOptions non-default arguments controlling the
     * durability of the operation, or null to get default behavior.
     *
     * @return the version of the new value, or null if there is no existing
     * row and the put is unsuccessful
     *
     * @throws DurabilityException if the specified {@link Durability} cannot
     * be satisfied
     *
     * @throws RequestTimeoutException if the request timeout interval was
     * exceeded
     *
     * @throws FaultException if the operation cannot be completed for any
     * reason
     *
     * @throws IllegalArgumentException if the {@code Row} does not have
     * a complete primary key or is otherwise invalid.
     */
    Version putIfPresent(Row row,
                         ReturnRow prevRow,
                         WriteOptions writeOptions)
        throws DurabilityException, RequestTimeoutException, FaultException;

    /**
     * Puts a row, but only if the version of the existing row matches the
     * matchVersion argument. Used when updating a value to ensure that it has
     * not changed since it was last read.  The row must contain a complete
     * primary key and all required fields.
     *
     * @param row the row to put
     *
     * @param matchVersion the version to match
     *
     * @param prevRow a {@code ReturnRow} object to contain the previous row
     * value and version associated with the given row, or null if they should
     * not be returned.  If a previous row does not exist, or the {@link
     * ReturnRow.Choice} specifies that they should not be returned, the
     * version in this object is set to null and none of the row's fields are
     * available.
     *
     * @param writeOptions non-default arguments controlling the
     * durability of the operation, or null to get default behavior.
     *
     * @return the version of the new value, or null if the versions do not
     * match and the put is unsuccessful
     *
     * @throws DurabilityException if the specified {@link Durability} cannot
     * be satisfied
     *
     * @throws RequestTimeoutException if the request timeout interval was
     * exceeded
     *
     * @throws FaultException if the operation cannot be completed for any
     * reason
     *
     * @throws IllegalArgumentException if the {@code Row} does not have
     * a complete primary key or is otherwise invalid
     */
    Version putIfVersion(Row row,
                         Version matchVersion,
                         ReturnRow prevRow,
                         WriteOptions writeOptions)
        throws DurabilityException, RequestTimeoutException, FaultException;

    /**
     * Deletes a row from a table.
     *
     * @param key the primary key for the row to delete
     *
     * @param prevRow a {@code ReturnRow} object to contain the previous row
     * value and version associated with the given row, or null if they should
     * not be returned.  If a previous row does not exist, or the {@link
     * ReturnRow.Choice} specifies that they should not be returned, the
     * version in this object is set to null and none of the row's fields are
     * available.
     *
     * @param writeOptions non-default arguments controlling the
     * durability of the operation, or null to get default behavior.
     *
     * @return true if the row existed and was deleted, false otherwise
     *
     * @throws DurabilityException if the specified {@link Durability} cannot
     * be satisfied
     *
     * @throws RequestTimeoutException if the request timeout interval was
     * exceeded
     *
     * @throws FaultException if the operation cannot be completed for any
     * reason
     *
     * @throws IllegalArgumentException if the primary key is not complete
     */
    boolean delete(PrimaryKey key,
                   ReturnRow prevRow,
                   WriteOptions writeOptions)
        throws DurabilityException, RequestTimeoutException, FaultException;

    /**
     * Deletes a row from a table but only if its version matches the one
     * specified in matchVersion.
     *
     * @param key the primary key for the row to delete
     *
     * @param matchVersion the version to match
     *
     * @param prevRow a {@code ReturnRow} object to contain the previous row
     * value and version associated with the given row, or null if they should
     * not be returned.  If a previous row does not exist, or the {@link
     * ReturnRow.Choice} specifies that they should not be returned, or
     * the matchVersion parameter matches the existing value and the delete is
     * successful, the version in this object is set to null and none of the
     * row's fields are available.
     *
     * @param writeOptions non-default arguments controlling the
     * durability of the operation, or null to get default behavior.
     *
     * @return true if the row existed and its version matched matchVersion
     * and was successfully deleted, false otherwise
     *
     * @throws DurabilityException if the specified {@link Durability} cannot
     * be satisfied
     *
     * @throws RequestTimeoutException if the request timeout interval was
     * exceeded
     *
     * @throws FaultException if the operation cannot be completed for any
     * reason
     *
     * @throws IllegalArgumentException if the primary key is not complete
     */
    boolean deleteIfVersion(PrimaryKey key,
                            Version matchVersion,
                            ReturnRow prevRow,
                            WriteOptions writeOptions)
        throws DurabilityException, RequestTimeoutException, FaultException;


    /**
     * Deletes multiple rows from a table in an atomic operation.  The
     * key used may be partial but must contain all of the fields that are
     * in the shard key.
     *
     * @param key the primary key for the row to delete
     *
     * @param getOptions a {@code MultiRowOptions} object used to control
     * ranges in the operation and whether ancestor and descendant tables are
     * included in the operation. It may be null.  The table used to construct
     * the {@code PrimaryKey} parameter is always included as a target.
     *
     * @param writeOptions non-default arguments controlling the
     * durability of the operation, or null to get default behavior.
     *
     * @return the number of rows deleted from the table
     *
     * @throws DurabilityException if the specified {@link Durability} cannot
     * be satisfied
     *
     * @throws RequestTimeoutException if the request timeout interval was
     * exceeded
     *
     * @throws FaultException if the operation cannot be completed for any
     * reason
     *
     * @throws IllegalArgumentException if the primary key is malformed or does
     * not contain all shard key fields
     */
    int multiDelete(PrimaryKey key,
                    MultiRowOptions getOptions,
                    WriteOptions writeOptions)
        throws DurabilityException, RequestTimeoutException, FaultException;

    /**
     * Returns a {@code TableOperationFactory} to create operations passed
     * to {@link #execute}.  Not all operations must use the same table but
     * they must all use the same shard portion of the primary key.
     *
     * @return an empty {@code TableOperationFactory}
     */
    TableOperationFactory getTableOperationFactory();

    /**
     * This method provides an efficient and transactional mechanism for
     * executing a sequence of operations associated with tables that share the
     * same <em>shard key</em> portion of their primary keys. The efficiency
     * results from the use of a single network interaction to accomplish the
     * entire sequence of operations.
     * <p>
     * The operations passed to this method are created using an {@link
     * TableOperationFactory}, which is obtained from the {@link
     * #getTableOperationFactory} method.
     * </p>
     * <p>
     * All the {@code operations} specified are executed within the scope of a
     * single transaction that effectively provides serializable isolation.
     * The transaction is started and either committed or aborted by this
     * method.  If the method returns without throwing an exception, then all
     * operations were executed atomically, the transaction was committed, and
     * the returned list contains the result of each operation.
     * </p>
     * <p>
     * If the transaction is aborted for any reason, an exception is thrown.
     * An abort may occur for two reasons:
     * <ol>
     *   <li>An operation or transaction results in an exception that is
     *   considered a fault, such as a durability or consistency error, a
     *   failure due to message delivery or networking error, etc. A {@link
     *   FaultException} is thrown.</li>
     *   <li>An individual operation returns normally but is unsuccessful as
     *   defined by the particular operation (e.g., a delete operation for a
     *   non-existent key) <em>and</em> {@code true} was passed for the {@code
     *   abortIfUnsuccessful} parameter when the operation was created using
     *   the {@link TableOperationFactory}.
     *   An {@link OperationExecutionException}
     *   is thrown, and the exception contains information about the failed
     *   operation.</li>
     * </ol>
     * </p>
     * <p>
     * Operations are not executed in the sequence they appear the {@code
     * operations} list, but are rather executed in an internally defined
     * sequence that prevents deadlocks.  Additionally, if there are two
     * operations for the same key, their relative order of execution is
     * arbitrary; this should be avoided.
     * </p>
     *
     * @param operations the list of operations to be performed. Note that all
     * operations in the list must specify primary keys with the same
     * complete shard key.
     *
     * @param writeOptions non-default arguments controlling the
     * durability of the operation, or null to get default behavior.
     *
     * @return the sequence of results associated with the operation. There is
     * one entry for each TableOperation in the operations argument list.  The
     * returned list is in the same order as the operations argument list.
     *
     * @throws OperationExecutionException if an operation is not successful as
     * defined by the particular operation (e.g., a delete operation for a
     * non-existent key) <em>and</em> {@code true} was passed for the {@code
     * abortIfUnsuccessful} parameter when the operation was created using the
     * {@link TableOperationFactory}.
     *
     * @throws DurabilityException if the specified {@link Durability} cannot
     * be satisfied
     *
     * @throws RequestTimeoutException if the request timeout interval was
     * exceeded
     *
     * @throws FaultException if the operation cannot be completed for any
     * reason
     *
     * @throws IllegalArgumentException if operations is null or empty, or not
     * all operations operate on primary keys with the same shard key, or more
     * than one operation has the same primary key, or any of the primary keys
     * are incomplete.
     */
    List<TableOperationResult> execute(List<TableOperation> operations,
                                       WriteOptions writeOptions)
        throws OperationExecutionException,
               DurabilityException,
               FaultException;
}
