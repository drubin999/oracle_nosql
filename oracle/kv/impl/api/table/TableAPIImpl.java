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

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.Durability;
import oracle.kv.DurabilityException;
import oracle.kv.FaultException;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.Operation;
import oracle.kv.OperationExecutionException;
import oracle.kv.OperationFactory;
import oracle.kv.OperationResult;
import oracle.kv.ReturnValueVersion;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.Version;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.Request;
import oracle.kv.impl.api.ops.MultiDeleteTable;
import oracle.kv.impl.api.ops.MultiGetTable;
import oracle.kv.impl.api.ops.MultiGetTableKeys;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.api.ops.ResultKeyValueVersion;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.table.FieldRange;
import oracle.kv.table.IndexKey;
import oracle.kv.table.KeyPair;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.ReadOptions;
import oracle.kv.table.ReturnRow;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableIteratorOptions;
import oracle.kv.table.TableOperation;
import oracle.kv.table.TableOperationFactory;
import oracle.kv.table.TableOperationResult;
import oracle.kv.table.WriteOptions;


/**
 * Implementation of the TableAPI interface.  It also manages materialization
 * of tables from metadata and caches retrieved tables.
 *
 * TableAPIImpl maintains a cache of TableImpl tables that have been explicitly
 * fetched by TableImpl because of schema evolution.  If TableImpl encounters
 * a table version higher than its own then it will fetch that version so it
 * can deserialize records written from a later version.  It is assumed that
 * this cache will be small and is not used for user calls to getTable().
 */
public class TableAPIImpl implements TableAPI {
    final static String TABLE_PREFIX = "Tables";
    final static String SCHEMA_PREFIX = "Schemas";
    final static String SEPARATOR = ".";
    private final KVStoreImpl store;
    private final OpFactory opFactory;

    /*
     * Cache a random RepNodeAdminAPI handle.  Refresh as necessary.
     */
    private RepNodeAdminAPI repNodeAdmin;

    /*
     * Cache of TableImpl that have been fetched because the user's
     * table version is older than the latest table version.
     */
    final private ConcurrentHashMap<String, TableImpl> fetchedTables;

    /*
     * This must be public for KVStoreImpl to use it.
     */
    public TableAPIImpl(KVStoreImpl store) {
        this.store = store;
        opFactory = new OpFactory(store.getOperationFactory());
        fetchedTables = new ConcurrentHashMap<String, TableImpl>();
    }

    /*
     * Table metadata methods
     */
    @Override
    public Table getTable(String tableName)
    throws FaultException {

        RepNodeAdminAPI rnai = getRepNodeAdmin(false);
        int retry = 1;
        Exception cause = null;
        while (retry >= 0) {
            try {
                return (TableImpl) rnai.getMetadata
                    (MetadataType.TABLE,
                     new TableMetadata.TableMetadataKey
                     (tableName).
                     getMetadataKey(), 0);
            } catch (RemoteException e) {
                /* refresh the RepNodeAdmin handle */
                rnai = getRepNodeAdmin(true);
                --retry;
                cause = e;
            }
        }
        String message = "Unable to get table";
        if (cause != null) {
            message += ": " + cause.getMessage();
        }
        throw new FaultException(message, cause, false);
    }

    @Override
    public Map<String, Table> getTables()
        throws FaultException {

        RepNodeAdminAPI rnai = getRepNodeAdmin(false);
        int retry = 1;
        Exception cause = null;
        while (retry >= 0) {
            try {
                final TableMetadata md = (TableMetadata) rnai.
                    getMetadata(MetadataType.TABLE);
                if (md != null) {
                    return md.getTables();
                }
                return Collections.<String, Table>emptyMap();
            } catch (RemoteException e) {
                /* refresh the RepNodeAdmin handle */
                rnai = getRepNodeAdmin(true);
                --retry;
                cause = e;
            }
        }
        String message = "Unable to get tables";
        if (cause != null) {
            message += ": " + cause.getMessage();
        }
        throw new FaultException(message, cause, false);
    }

    /*
     * Runtime interfaces
     */

    @Override
    public Row get(PrimaryKey rowKeyArg,
                   ReadOptions readOptions)
        throws FaultException {

        PrimaryKeyImpl rowKey = (PrimaryKeyImpl) rowKeyArg;
        Key key = rowKey.getPrimaryKey(false);
        ValueVersion vv = store.getInternal(key,
                                            rowKey.getTableImpl().getId(),
                                            getConsistency(readOptions),
                                            getTimeout(readOptions),
                                            getTimeoutUnit(readOptions));
        if (vv == null) {
            return null;
        }
        return getRowFromValueVersion(vv, rowKey, true);
    }

    @Override
    public Version put(Row rowArg,
                       ReturnRow prevRowArg,
                       WriteOptions writeOptions)
        throws FaultException {

        RowImpl row = (RowImpl) rowArg;
        ReturnValueVersion rvv = makeRVV(prevRowArg);
        Key key = row.getPrimaryKey(false);
        Value value = row.createValue();
        Version version = store.putInternal(key, value, rvv,
                                            row.getTableImpl().getId(),
                                            getDurability(writeOptions),
                                            getTimeout(writeOptions),
                                            getTimeoutUnit(writeOptions));
        initReturnRow(prevRowArg, row, rvv);
        return version;
    }

    @Override
    public Version putIfAbsent(Row rowArg,
                               ReturnRow prevRowArg,
                               WriteOptions writeOptions)
        throws FaultException {

        RowImpl row = (RowImpl) rowArg;
        ReturnValueVersion rvv = makeRVV(prevRowArg);
        Key key = row.getPrimaryKey(false);
        Value value = row.createValue();
        Version version =
            store.putIfAbsentInternal(key, value, rvv,
                                      row.getTableImpl().getId(),
                                      getDurability(writeOptions),
                                      getTimeout(writeOptions),
                                      getTimeoutUnit(writeOptions));
        initReturnRow(prevRowArg, row, rvv);
        return version;
    }

    @Override
    public Version putIfPresent(Row rowArg,
                                ReturnRow prevRowArg,
                                WriteOptions writeOptions)
        throws FaultException {

        RowImpl row = (RowImpl) rowArg;
        ReturnValueVersion rvv = makeRVV(prevRowArg);
        Key key = row.getPrimaryKey(false);
        Value value = row.createValue();
        Version version =
            store.putIfPresentInternal(key, value, rvv,
                                       row.getTableImpl().getId(),
                                       getDurability(writeOptions),
                                       getTimeout(writeOptions),
                                       getTimeoutUnit(writeOptions));
        initReturnRow(prevRowArg, row, rvv);
        return version;
    }

    @Override
    public Version putIfVersion(Row rowArg,
                                Version matchVersion,
                                ReturnRow prevRowArg,
                                WriteOptions writeOptions)
        throws FaultException {

        RowImpl row = (RowImpl) rowArg;
        ReturnValueVersion rvv = makeRVV(prevRowArg);
        Key key = row.getPrimaryKey(false);
        Value value = row.createValue();
        Version version =
            store.putIfVersionInternal(key, value,
                                       matchVersion, rvv,
                                       row.getTableImpl().getId(),
                                       getDurability(writeOptions),
                                       getTimeout(writeOptions),
                                       getTimeoutUnit(writeOptions));
        initReturnRow(prevRowArg, row, rvv);
        return version;
    }

    /*
     * Multi/iterator ops
     */
    @Override
    public List<Row> multiGet(PrimaryKey rowKeyArg,
                              MultiRowOptions getOptions,
                              ReadOptions readOptions)
        throws FaultException {

        boolean hasAncestorTables = false;
        PrimaryKeyImpl rowKey = (PrimaryKeyImpl) rowKeyArg;
        Table table = rowKey.getTable();
        TableKey key = TableKey.createKey(table, rowKey, true);
        if (!key.getMajorKeyComplete()) {
            throw new IllegalArgumentException
                ("Cannot perform multiGet on a primary key without a " +
                 "complete major path");
        }

        if (getOptions != null) {
            validateMultiRowOptions(getOptions, table, false);
            if (getOptions.getIncludedParentTables() != null) {
                hasAncestorTables = true;
            }
        }
        /* Execute request. */
        final byte[] parentKeyBytes =
            store.getKeySerializer().toByteArray(key.getKey());
        final PartitionId partitionId =
            store.getDispatcher().getPartitionId(parentKeyBytes);
        final MultiGetTable get =
            new MultiGetTable(parentKeyBytes,
                              makeTargetTables(table, getOptions),
                              makeKeyRange(key, getOptions));
        final Request req =
            store.makeReadRequest(get, partitionId,
                                  getConsistency(readOptions),
                                  getTimeout(readOptions),
                                  getTimeoutUnit(readOptions));
        final Result result = store.executeRequest(req);

        /*
         * Convert results to List<Row>
         */
        return processMultiResults(table, result, hasAncestorTables);
    }

    @Override
    public List<PrimaryKey> multiGetKeys(PrimaryKey rowKeyArg,
                                         MultiRowOptions getOptions,
                                         ReadOptions readOptions)
        throws FaultException {

        boolean hasAncestorTables = false;
        PrimaryKeyImpl rowKey = (PrimaryKeyImpl) rowKeyArg;
        Table table = rowKey.getTable();
        TableKey key = TableKey.createKey(table, rowKey, true);
        if (!key.getMajorKeyComplete()) {
            throw new IllegalArgumentException
                ("Cannot perform multiGet on a primary key without a " +
                 "complete major path");
        }

        if (getOptions != null) {
            validateMultiRowOptions(getOptions, table, false);
            if (getOptions.getIncludedParentTables() != null) {
                hasAncestorTables = true;
            }
        }
        /* Execute request. */
        final byte[] parentKeyBytes =
            store.getKeySerializer().toByteArray(key.getKey());
        final PartitionId partitionId =
            store.getDispatcher().getPartitionId(parentKeyBytes);
        final MultiGetTableKeys get =
            new MultiGetTableKeys(parentKeyBytes,
                                  makeTargetTables(table, getOptions),
                                  makeKeyRange(key, getOptions));
        final Request req =
            store.makeReadRequest(get, partitionId,
                                  getConsistency(readOptions),
                                  getTimeout(readOptions),
                                  getTimeoutUnit(readOptions));
        final Result result = store.executeRequest(req);

        /*
         * Convert byte[] keys to Key objects.
         */
        final List<byte[]> byteKeyResults = result.getKeyList();
        assert result.getSuccess() == (!byteKeyResults.isEmpty());
        return processMultiResults(table, byteKeyResults, hasAncestorTables);
    }

    @Override
    public TableIterator<Row> tableIterator(PrimaryKey rowKeyArg,
                                            MultiRowOptions getOptions,
                                            TableIteratorOptions iterateOptions)
        throws FaultException {

        final PrimaryKeyImpl rowKey = (PrimaryKeyImpl) rowKeyArg;
        final Table table = rowKey.getTable();
        final TableKey key = TableKey.createKey(table, rowKey, true);


        if (getOptions != null) {
            validateMultiRowOptions(getOptions, table, false);
        }
        return TableScan.
            createTableIterator(this, key, getOptions, iterateOptions);
    }

    @Override
    public TableIterator<PrimaryKey> tableKeysIterator
        (PrimaryKey rowKeyArg,
         MultiRowOptions getOptions,
         TableIteratorOptions iterateOptions)
        throws FaultException {

        final PrimaryKeyImpl rowKey = (PrimaryKeyImpl) rowKeyArg;
        final Table table = rowKey.getTable();
        final TableKey key = TableKey.createKey(table, rowKey, true);


        if (getOptions != null) {
            validateMultiRowOptions(getOptions, table, false);
        }
        return TableScan.
            createTableKeysIterator(this, key, getOptions, iterateOptions);
    }

    @Override
    public boolean delete(PrimaryKey rowKeyArg,
                          ReturnRow prevRowArg,
                          WriteOptions writeOptions)
        throws FaultException {

        PrimaryKeyImpl rowKey = (PrimaryKeyImpl) rowKeyArg;
        ReturnValueVersion rvv = makeRVV(prevRowArg);
        Key key = rowKey.getPrimaryKey(false);
        boolean retval = store.deleteInternal(key, rvv,
                                              getDurability(writeOptions),
                                              getTimeout(writeOptions),
                                              getTimeoutUnit(writeOptions),
                                              rowKey.getTableImpl().getId());
        initReturnRow(prevRowArg, rowKey, rvv);
        return retval;
    }

    @Override
    public boolean deleteIfVersion(PrimaryKey rowKeyArg,
                                   Version matchVersion,
                                   ReturnRow prevRowArg,
                                   WriteOptions writeOptions)
        throws FaultException {

        PrimaryKeyImpl rowKey = (PrimaryKeyImpl) rowKeyArg;
        ReturnValueVersion rvv = makeRVV(prevRowArg);
        Key key = rowKey.getPrimaryKey(false);
        boolean retval = store.deleteIfVersionInternal
            (key, matchVersion, rvv,
             getDurability(writeOptions),
             getTimeout(writeOptions),
             getTimeoutUnit(writeOptions),
             rowKey.getTableImpl().getId());

        initReturnRow(prevRowArg, rowKey, rvv);
        return retval;
    }

    @Override
    public int multiDelete(PrimaryKey rowKeyArg,
                           MultiRowOptions getOptions,
                           WriteOptions writeOptions)
        throws FaultException {

        PrimaryKeyImpl rowKey = (PrimaryKeyImpl) rowKeyArg;
        Table table = rowKey.getTable();
        TableKey key = TableKey.createKey(table, rowKey, true);
        if (!key.getMajorKeyComplete()) {
            throw new IllegalArgumentException
                ("Cannot perform multiDelete on a primary key without a " +
                 "complete major path.  Key: " + rowKey.toJsonString(false));
        }

        if (getOptions != null) {
            validateMultiRowOptions(getOptions, table, false);
        }
        final KeyRange keyRange = makeKeyRange(key, getOptions);

        /* Execute request. */
        final byte[] parentKeyBytes =
            store.getKeySerializer().toByteArray(key.getKey());
        final PartitionId partitionId =
            store.getDispatcher().getPartitionId(parentKeyBytes);
        final MultiDeleteTable del =
            new MultiDeleteTable(parentKeyBytes,
                                 makeTargetTables(table, getOptions),
                                 keyRange);
        final Request req =
            store.makeWriteRequest(del, partitionId,
                                   getDurability(writeOptions),
                                   getTimeout(writeOptions),
                                   getTimeoutUnit(writeOptions));
        final Result result = store.executeRequest(req);
        return result.getNDeletions();
    }

    /*
     * Index iterator operations
     */
    @Override
    public TableIterator<Row> tableIterator(IndexKey indexKeyArg,
                                            MultiRowOptions getOptions,
                                            TableIteratorOptions iterateOptions)
        throws FaultException {
        final  IndexKeyImpl indexKey = (IndexKeyImpl)indexKeyArg;
        if (getOptions != null) {
            validateMultiRowOptions(getOptions, indexKey.getTable(), true);
        }
        return IndexScan.createTableIterator(this,
                                             indexKey,
                                             getOptions,
                                             iterateOptions);
    }

    @Override
    public TableIterator<KeyPair>
            tableKeysIterator(IndexKey indexKeyArg,
                              MultiRowOptions getOptions,
                              TableIteratorOptions iterateOptions)
        throws FaultException {
        final  IndexKeyImpl indexKey = (IndexKeyImpl)indexKeyArg;
        if (getOptions != null) {
            validateMultiRowOptions(getOptions, indexKey.getTable(), true);
        }
        return IndexScan.createTableKeysIterator(this,
                                                 indexKey,
                                                 getOptions,
                                                 iterateOptions);
    }

    @Override
    public TableOperationFactory getTableOperationFactory() {
        return opFactory;
    }

    /**
     * All of the TableOperations can be directly mapped to simple KV operations
     * so do that.
     */
    @Override
    public List<TableOperationResult> execute(List<TableOperation> operations,
                                              WriteOptions writeOptions)
        throws OperationExecutionException,
               DurabilityException,
               FaultException {

        ArrayList<Operation> opList =
            new ArrayList<Operation>(operations.size());
        for (TableOperation op : operations) {
            opList.add(((OpWrapper)op).getOperation());
        }
        List<OperationResult> results =
            store.execute(opList,
                          getDurability(writeOptions),
                          getTimeout(writeOptions),
                          getTimeoutUnit(writeOptions));
        List<TableOperationResult> tableResults =
            new ArrayList<TableOperationResult>(results.size());
        int index = 0;
        for (OperationResult opRes : results) {
            PrimaryKey pkey = operations.get(index).getPrimaryKey();
            tableResults.add(new OpResultWrapper(this, opRes, pkey));
            ++index;
        }
        return tableResults;
    }

    /**
     * Creates a Row from the Value with a retry in the case of a
     * TableVersionException.
     */
    RowImpl getRowFromValueVersion(ValueVersion vv, RowImpl row,
                                   boolean keyOnly) {
        int requiredVersion = 0;
        try {
            return row.rowFromValueVersion(vv, keyOnly);
        } catch (TableVersionException tve) {
            requiredVersion = tve.getRequiredVersion();
            assert requiredVersion > row.getTable().getTableVersion();
        }

        /*
         * Fetch the required table, create a new row from the existing
         * row and try again.  The fetch will throw if the table and version
         * can't be found.
         */
        TableImpl newTable = fetchTable(row.getTable().getFullName(),
                                        requiredVersion);
        assert requiredVersion == newTable.getTableVersion();

        /*
         * Set the version of the table to the original version to ensure that
         * deserialization does the right thing with added and removed fields.
         */
        newTable =
            (TableImpl) newTable.getVersion(row.getTable().getTableVersion());
        RowImpl newRow = newTable.createRow(row);
        return newRow.rowFromValueVersion(vv, keyOnly);
    }

    TableImpl fetchTable(String tableName, int tableVersion) {
        TableImpl table = fetchedTables.get(tableName);
        if (table != null && table.numTableVersions() >= tableVersion) {
            return (TableImpl) table.getVersion(tableVersion);
        }

        /*
         * Either the table is not in the cache or it is not sufficiently
         * recent.  Go to the server.
         */
        table = (TableImpl) getTable(tableName);
        if (table != null && table.numTableVersions() >= tableVersion) {

            /*
             * Cache the table.  If an intervening operation cached the
             * table, make sure that the cache has the lastest version.
             */
            TableImpl t = fetchedTables.putIfAbsent(tableName, table);
            if (t != null && table.numTableVersions() > t.numTableVersions()) {
                fetchedTables.put(tableName, table);
            }
            return (TableImpl) table.getVersion(tableVersion);
        }
        throw new IllegalArgumentException
            ("Table or version does not exist.  It may have been removed: " +
             tableName + ", version " + tableVersion);
    }

    KVStoreImpl getStore() {
        return store;
    }

    /**
     * The next classes implement mapping of TableOperation and
     * TableOperationFactory to the KVStore Operation and OperationFactory.
     */
    private static class OpWrapper implements TableOperation {
        private final Operation op;
        private final TableOperation.Type type;
        private final RowImpl record;

        private OpWrapper(Operation op, TableOperation.Type type,
                          final RowImpl record) {
            this.op = op;
            this.type = type;
            this.record = record;
        }

        @Override
        public Row getRow() {
            return record;
        }

        @Override
        public PrimaryKey getPrimaryKey() {
            if (record instanceof PrimaryKey) {
                return (PrimaryKey) record;
            }
            return record.createPrimaryKey();
        }

        @Override
        public TableOperation.Type getType() {
            return type;
        }

        @Override
        public boolean getAbortIfUnsuccessful() {
            return op.getAbortIfUnsuccessful();
        }

        private Operation getOperation() {
            return op;
        }
    }

    private static class OpResultWrapper implements TableOperationResult {
        private final TableAPIImpl impl;
        private final OperationResult opRes;
        private final PrimaryKey key;

        private OpResultWrapper(TableAPIImpl impl,
                                OperationResult opRes, PrimaryKey key) {
            this.impl = impl;
            this.opRes = opRes;
            this.key = key;
        }

        @Override
        public Version getNewVersion() {
            return opRes.getNewVersion();
        }

        @Override
        public Row getPreviousRow() {
            Value value = opRes.getPreviousValue();
            if (value != null && key != null) {
                return impl.getRowFromValueVersion
                    (new ValueVersion(value, null), (RowImpl) key, true);
            }
            return null;
        }

        @Override
        public Version getPreviousVersion() {
            return opRes.getPreviousVersion();
        }

        @Override
        public boolean getSuccess() {
            return opRes.getSuccess();
        }
    }

    private static class OpFactory implements TableOperationFactory {
        private final OperationFactory factory;

        private OpFactory(final OperationFactory factory) {
            this.factory = factory;
        }

        @Override
        public TableOperation createPut(Row rowArg,
                                        ReturnRow.Choice prevReturn,
                                        boolean abortIfUnsuccessful) {

            RowImpl row = (RowImpl) rowArg;
            ReturnValueVersion.Choice choice =
                ReturnRowImpl.mapChoice(prevReturn);
            Key key = row.getPrimaryKey(false);
            Value value = row.createValue();
            Operation op = factory.createPut(key, value, choice,
                                             abortIfUnsuccessful);
            return new OpWrapper(op, TableOperation.Type.PUT, row);
        }

        @Override
        public TableOperation createPutIfAbsent(Row rowArg,
                                                ReturnRow.Choice prevReturn,
                                                boolean abortIfUnsuccessful) {

            RowImpl row = (RowImpl) rowArg;
            ReturnValueVersion.Choice choice =
                ReturnRowImpl.mapChoice(prevReturn);
            Key key = row.getPrimaryKey(false);
            Value value = row.createValue();
            Operation op = factory.createPutIfAbsent(key, value, choice,
                                                     abortIfUnsuccessful);
            return new OpWrapper(op, TableOperation.Type.PUT_IF_ABSENT, row);
        }

        @Override
        public TableOperation createPutIfPresent(Row rowArg,
                                                 ReturnRow.Choice prevReturn,
                                                 boolean abortIfUnsuccessful) {

            RowImpl row = (RowImpl) rowArg;
            ReturnValueVersion.Choice choice =
                ReturnRowImpl.mapChoice(prevReturn);
            Key key = row.getPrimaryKey(false);
            Value value = row.createValue();
            Operation op = factory.createPutIfPresent(key, value, choice,
                                                     abortIfUnsuccessful);
            return new OpWrapper(op, TableOperation.Type.PUT_IF_PRESENT, row);
        }

        @Override
        public TableOperation createPutIfVersion(Row rowArg,
                                                 Version versionMatch,
                                                 ReturnRow.Choice prevReturn,
                                                 boolean abortIfUnsuccessful) {

            RowImpl row = (RowImpl) rowArg;
            ReturnValueVersion.Choice choice =
                ReturnRowImpl.mapChoice(prevReturn);
            Key key = row.getPrimaryKey(false);
            Value value = row.createValue();
            Operation op = factory.createPutIfVersion(key, value,
                                                      versionMatch, choice,
                                                      abortIfUnsuccessful);
            return new OpWrapper(op, TableOperation.Type.PUT_IF_VERSION, row);
        }

        @Override
        public TableOperation createDelete
            (PrimaryKey keyArg,
             ReturnRow.Choice prevReturn,
             boolean abortIfUnsuccessful) {

            PrimaryKeyImpl rowKey = (PrimaryKeyImpl) keyArg;
            ReturnValueVersion.Choice choice =
                ReturnRowImpl.mapChoice(prevReturn);
            Key key = rowKey.getPrimaryKey(false);
            Operation op = factory.createDelete(key, choice,
                                                abortIfUnsuccessful);
            return new OpWrapper(op, TableOperation.Type.DELETE, rowKey);
        }

        @Override
        public TableOperation createDeleteIfVersion
            (PrimaryKey keyArg,
             Version versionMatch,
             ReturnRow.Choice prevReturn,
             boolean abortIfUnsuccessful) {

            PrimaryKeyImpl rowKey = (PrimaryKeyImpl) keyArg;
            ReturnValueVersion.Choice choice =
                ReturnRowImpl.mapChoice(prevReturn);
            Key key = rowKey.getPrimaryKey(false);
            Operation op = factory.createDeleteIfVersion(key, versionMatch,
                                                         choice,
                                                         abortIfUnsuccessful);
            return new OpWrapper
                (op, TableOperation.Type.DELETE_IF_VERSION, rowKey);
        }
    }

    /************* end runtime methods **************/

    /*
     * Internal utilities
     */

    /**
     * Get a RepNodeAdminAPI instance from any RepNode that can be found.
     * This interface is used to fetch metadata.
     */
    private synchronized RepNodeAdminAPI getRepNodeAdmin(boolean force) {
        if (repNodeAdmin != null && !force) {
            return repNodeAdmin;
        }
        try {
            repNodeAdmin = null;
            Topology topo =
                store.getDispatcher().getTopologyManager().getTopology();
            RegistryUtils regUtils = store.getDispatcher().getRegUtils();
            for (RepNodeId rnid : topo.getRepNodeIds()) {
                RepNodeAdminAPI rnai = regUtils.getRepNodeAdmin(rnid);
                if (rnai != null) {
                    repNodeAdmin = rnai;
                    break;
                }
            }
            return repNodeAdmin;
        } catch (Exception e) {
            throw new IllegalStateException("Unable to find metadata: " +
                                            e.getMessage(), e);
        }
    }

    private ReturnValueVersion makeRVV(ReturnRow rr) {
        if (rr != null) {
            return ((ReturnRowImpl)rr).makeReturnValueVersion();
        }
        return null;
    }

    private void initReturnRow(ReturnRow rr, RowImpl key,
                               ReturnValueVersion rvv) {
        if (rr != null) {
            ((ReturnRowImpl)rr).init(this, rvv, key);
        }
    }

    static KeyRange makeKeyRange(TableKey key, MultiRowOptions getOptions) {
        if (getOptions != null) {
            FieldRange range = getOptions.getFieldRange();
            if (range != null) {
                if (key.getKeyComplete()) {
                    throw new IllegalArgumentException
                        ("Cannot specify a FieldRange with a complete " +
                         "primary key");
                }
                key.validateFieldOrder(range);

                String start = null;
                String end = null;
                boolean startInclusive = true;
                boolean endInclusive = true;
                if (range.getStart() != null) {
                    start = ((FieldValueImpl)range.getStart()).
                        formatForKey(range.getDefinition());
                    startInclusive = range.getStartInclusive();
                }
                if (range.getEnd() != null) {
                    end = ((FieldValueImpl)range.getEnd()).
                        formatForKey(range.getDefinition());
                    endInclusive = range.getEndInclusive();
                }
                return new KeyRange(start, startInclusive, end, endInclusive);
            }
        } else {
            key.getRow().validate();
        }
        return null;
    }

    /**
     * Turn a List<byte[]> of keys into List<PrimaryKey>
     */
    private List<PrimaryKey>
        processMultiResults(Table table,
                            List<byte[]> keys,
                            boolean hasAncestorTables) {
        List<PrimaryKey> list = new ArrayList<PrimaryKey>(keys.size());
        TableImpl t = (TableImpl) table;
        if (hasAncestorTables) {
            t = t.getTopLevelTable();
        }
        for (byte[] key : keys) {
            PrimaryKeyImpl pk =
                t.createPrimaryKeyFromKeyBytes(key);
            if (pk != null) {
                list.add(pk);
            }
        }
        return list;
    }

    /**
     * Turn a List<ResultKeyValueVersion> of results into List<Row>
     */
    private List<Row>
        processMultiResults(Table table,
                            Result result,
                            boolean hasAncestorTables) {
        final List<ResultKeyValueVersion> resultList =
            result.getKeyValueVersionList();
        List<Row> list = new ArrayList<Row>(resultList.size());
        TableImpl t = (TableImpl) table;
        if (hasAncestorTables) {
            t = t.getTopLevelTable();
        }

        for (ResultKeyValueVersion rkvv : result.getKeyValueVersionList()) {
            RowImpl row = t.createRowFromKeyBytes(rkvv.getKeyBytes());
            if (row != null) {
                ValueVersion vv = new ValueVersion(rkvv.getValue(),
                                                   rkvv.getVersion());
                list.add(getRowFromValueVersion(vv, row, false));
            }
        }
        return list;
    }

    /**
     * Validate the ancestor and child tables, if set against the target table.
     */
    private static void validateMultiRowOptions(MultiRowOptions mro,
                                                Table targetTable,
                                                boolean isIndex) {
        if (mro.getIncludedParentTables() != null) {
            for (Table t : mro.getIncludedParentTables()) {
                if (!((TableImpl)targetTable).isAncestor(t)) {
                    throw new IllegalArgumentException
                        ("Ancestor table \"" + t.getFullName() + "\" is not " +
                         "an ancestor of target table \"" +
                         targetTable.getFullName() + "\"");
                }
            }
        }
        if (mro.getIncludedChildTables() != null) {
            if (isIndex) {
                throw new UnsupportedOperationException
                    ("Child table returns are not supported for index " +
                     "scan operations");
            }
            for (Table t : mro.getIncludedChildTables()) {
                if (!((TableImpl)t).isAncestor(targetTable)) {
                    throw new IllegalArgumentException
                        ("Child table \"" + t.getFullName() + "\" is not a " +
                         "descendant of target table \"" +
                         targetTable.getFullName() + "\"");
                }

            }
        }
    }

    static Consistency getConsistency(ReadOptions opts) {
        return (opts != null ? opts.getConsistency() : null);
    }

    static long getTimeout(ReadOptions opts) {
        return (opts != null ? opts.getTimeout() : 0);
    }

    static TimeUnit getTimeoutUnit(ReadOptions opts) {
        return (opts != null ? opts.getTimeoutUnit() : null);
    }

    static Direction getDirection(TableIteratorOptions opts,
                                  TableKey key) {
        if (opts == null) {
           return key.getMajorKeyComplete() ? Direction.FORWARD :
                                              Direction.UNORDERED;
        }
        if (!key.getMajorKeyComplete() &&
            (opts.getDirection() != Direction.UNORDERED)) {
                throw new IllegalArgumentException
                    ("Direction must be Direction.UNORDERED if " +
                     "major key is not complete");
        }
        return opts.getDirection();
    }

    static int getBatchSize(TableIteratorOptions opts) {
        return ((opts != null && opts.getResultsBatchSize() != 0) ?
                opts.getResultsBatchSize():
                KVStoreImpl.DEFAULT_ITERATOR_BATCH_SIZE);
    }

    static Durability getDurability(WriteOptions opts) {
        return (opts != null ? opts.getDurability() : null);
    }

    static long getTimeout(WriteOptions opts) {
        return (opts != null ? opts.getTimeout() : 0);
    }

    static TimeUnit getTimeoutUnit(WriteOptions opts) {
        return (opts != null ? opts.getTimeoutUnit() : null);
    }

    static TargetTables makeTargetTables(Table target,
                                         MultiRowOptions getOptions) {
        List<Table> childTables =
            getOptions != null ? getOptions.getIncludedChildTables() : null;
        List<Table> ancestorTables =
            getOptions != null ? getOptions.getIncludedParentTables() : null;

        return new TargetTables(target, childTables, ancestorTables);
    }
}



