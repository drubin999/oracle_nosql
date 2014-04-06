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

package oracle.kv.impl.rep.table;

import static oracle.kv.impl.rep.PartitionManager.DB_OPEN_RETRY_MS;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.Key;
import oracle.kv.Key.BinaryKeyIterator;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.fault.EnvironmentTimeoutException;
import oracle.kv.impl.fault.RNUnavailableException;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.rep.MetadataManager;
import oracle.kv.impl.rep.PartitionManager;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.RepNodeService.Params;
import oracle.kv.impl.rep.StateTracker;
import oracle.kv.impl.rep.table.MaintenanceThread.PopulateThread;
import oracle.kv.impl.rep.table.MaintenanceThread.PrimaryCleanerThread;
import oracle.kv.impl.rep.table.MaintenanceThread.SecondaryCleanerThread;
import oracle.kv.impl.rep.table.SecondaryInfoMap.DeletedTableInfo;
import oracle.kv.impl.rep.table.SecondaryInfoMap.SecondaryInfo;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.table.Index;
import oracle.kv.table.Table;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.SecondaryAssociation;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.NoConsistencyRequiredPolicy;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.StateChangeEvent;

/**
 * Manages the secondary database handles for the rep node.
 */
public class TableManager extends MetadataManager<TableMetadata>
                          implements SecondaryAssociation {

    /*
     * Create the name of the secondary DB based on the specified index
     * and table names.  Use index name first.  TODO: is this best order?
     */
    static String createDbName(String indexName,
                               String tableName) {
        StringBuilder sb = new StringBuilder();
        sb.append(indexName);
        sb.append(".");
        sb.append(tableName);
        return sb.toString();
    }

    private final Params params;

    private final StateTracker stateTracker;

    /*
     * The table metadata. TableMetada is not thread safe. The instance
     * referenced by tableMetadata should be treated as immutable. Updates
     * to the table metadata must be made to a copy first and then replace the
     * reference with the copy.
     */
    private volatile TableMetadata tableMetadata = null;

    /*
     * Map of secondary database handles. Modification must be made with
     * the threadLock held.
     */
    private final Map<String, SecondaryDatabase> secondaryDbMap =
                                new HashMap<String, SecondaryDatabase>();

    /* Thread used to asynchronously open secondary database handles */
    private UpdateThread updateThread = null;

    /*
     * Thread used to preform maintenance operations such as populating
     * secondary DBs. Only one thread is run at a time.
     */
    private MaintenanceThread maintenanceThread = null;

    /*
     * Lock controlling updateThread, maintenanceThread, and modification to
     * secondaryDbMap. If object synchronization and the threadLock is needed
     * the synchronization should be first.
     */
    private final ReentrantLock threadLock = new ReentrantLock();

    /*
     * The map is an optimization for secondary DB lookup. This lookup happens
     * on every operation and so should be as efficient as possible. This map
     * parallels the Table map in TableMetadata except that it only contains
     * entries for tables which contain indexes (and therefore reference
     * secondary DBs). The map is reconstructed whenever the table MD is
     * updated.
     *
     * If the map is null, the table metadata has not yet been initialized and
     * operations should not be allowed.
     *
     * Note that secondaryLookupMap may be out-of-date relitive to the
     * non-synchronized tableMetadata.
     */
    private volatile Map<String, TableEntry> secondaryLookupMap = null;

    /*
     * Map to lookup a table via its ID.
     * 
     * If the map is null, the table metadata has not yet been initialized and
     * operations should not be allowed.
     *
     * Note that idLookupMap may be out-of-date relitive to the non-synchronized
     * tableMetadata.
     */
    private volatile Map<Long, TableImpl> idLookupMap = null;

    public TableManager(RepNode repNode, Params params) {
        super(repNode, params);
        this.params = params;
        logger.log(Level.INFO, "Table manager created");// TODO - FINE?
        stateTracker = new TableManagerStateTracker(logger);
    }

    @Override
    public void shutdown() {
        stateTracker.shutdown();
        threadLock.lock();
        try {
            stopUpdateThread();
            stopMaintenanceThread();
        } finally {
            threadLock.unlock();
        }
        super.shutdown();
    }

    /**
     * Returns the table metadata object. Returns null if there was an error.
     *
     * @return the table metadata object or null
     */
    public TableMetadata getTableMetadata() {

        if (tableMetadata == null) {

            synchronized (this) {
                if (tableMetadata == null) {
                    try {
                        tableMetadata = fetchMetadata();
                    } catch (EnvironmentTimeoutException ete) {
                        /* RN not ready, ignore */
                        return  null;
                    }
                    /*
                     * If the DB is empty, we create a new instance so
                     * that we don't keep re-reading.
                     */
                    if (tableMetadata == null) {
                        tableMetadata = new TableMetadata(true);
                    }
                }
            }
        }
        return tableMetadata;
    }
    
    /* -- public index APIs -- */

    /**
     * Returns true if the specified index has been successfully added.
     *
     * @param indexName the index ID
     * @param tableName the fully qualified table name
     * @return true if the specified index has been successfully added
     */
    public boolean addIndexComplete(String indexName,
                                    String tableName) {
        final ReplicatedEnvironment repEnv = repNode.getEnv(1);

        if (repEnv == null) {
            return false;
        }

        final SecondaryInfoMap secondaryInfoMap = getSecondaryInfoMap(repEnv);

        /* If there is an issue reading the info object, punt */
        if (secondaryInfoMap == null) {
            return false;
        }

        final String dbName = createDbName(indexName, tableName);

        final SecondaryInfo info = secondaryInfoMap.getSecondaryInfo(dbName);

        logger.log(Level.FINE, "addIndexComplete({0}) returning {1}",
                   new Object[]{dbName, info});
        return (info == null) ? false : !info.needsPopulating();
    }

    /**
     * Returns true if the data associated with the specified table has been
     * removed from the store.
     *
     * @param tableName the fully qualified table name
     * @return true if the table data has been removed
     */
    public boolean removeTableDataComplete(String tableName) {
        final ReplicatedEnvironment repEnv = repNode.getEnv(1);

        if (repEnv == null) {
            return false;
        }

        final SecondaryInfoMap secondaryInfoMap = getSecondaryInfoMap(repEnv);
        final DeletedTableInfo info =
                                secondaryInfoMap.getDeletedTableInfo(tableName);
        if (info != null) {
            return info.isDone();
        }
        final TableMetadata md = getTableMetadata();

        return (md == null) ? false : (md.getTable(tableName) == null);
    }

   /**
    * Gets the table instance for the specified ID. If no table is defined
    * null is returned.
    * 
    * @param tableId a table ID
    * @return the table instance
    * @throws RNUnavailableException is the table metadata is not yet
    * initialized
    */
    public TableImpl getTable(long tableId) {
        final Map<Long, TableImpl> map = idLookupMap;
        if (map == null) {
            /* Throwing RNUnavailableException should cause a retry */
            throw new RNUnavailableException(
                                "Table metadata is not yet initialized");
        }
        return map.get(tableId);
    }

    /**
     * Gets the params used to construct this instance.
     */
    Params getParams() {
        return params;
    }

    /**
     * Gets the secondaryInfoMap for read-only. Returns null if there was an
     * error.
     */
    private SecondaryInfoMap getSecondaryInfoMap(ReplicatedEnvironment repEnv) {
        try {
            return SecondaryInfoMap.fetch(repEnv);
        } catch (RuntimeException re) {
            PartitionManager.handleException(re, logger, "index populate info");
        }
        return null;
    }

    /* -- Secondary DB methods -- */

    /**
     * Gets the secondary database of the specified name. Returns null if the
     * secondary database does not exist.
     *
     * @param dbName the name of a secondary database
     * @return a secondary database or null
     */
    SecondaryDatabase getSecondaryDb(String dbName) {
        return secondaryDbMap.get(dbName);
    }

    /**
     * Gets the secondary database for the specified index. Returns null if the
     * secondary database does not exist.
     *
     * @param indexName the index name
     * @param tableName the table name
     * @return a secondary database or null
     */
    public SecondaryDatabase getIndexDB(String indexName, String tableName) {
        return getSecondaryDb(createDbName(indexName, tableName));
    }

    /**
     * Closes all secondary DB handles.
     */
    @Override
    public void closeDbHandles() {
        logger.log(Level.INFO, "Closing secondary database handles");

        threadLock.lock();
        try {
            stopUpdateThread();
            stopMaintenanceThread();

            final Iterator<SecondaryDatabase> itr =
                                        secondaryDbMap.values().iterator();
            while (itr.hasNext()) {

                final SecondaryDatabase db = itr.next();
                if (db == null) {
                    continue;
                }

                if (!closeSecondaryDb(db)) {
                    /* Break out on an env failure */
                    return;
                }
                itr.remove();
            }
        } finally {
            threadLock.unlock();
            super.closeDbHandles();
        }
    }

    /**
     * Closes the specified secondary DB. Returns false if the environment is
     * closed or invalid, otherwise true.
     *
     * @param db secondary database to close
     * @return false if the environment is closed
     */
    private boolean closeSecondaryDb(SecondaryDatabase db) {
        logger.log(Level.FINE, "Closing secondary DB {0}",
                   db.getDatabaseName());

        final Environment env = db.getEnvironment();

        if ((env == null) || !env.isValid()) {
            return false;
        }
        TxnUtil.close(logger, db, "secondary");
        return true;
    }

    /* -- From SecondaryAssociation -- */

    @Override
    public boolean isEmpty() {

        /*
         * This method is called on every operation. It must be as fast
         * as possible.
         */
        final Map<String, TableEntry> map = secondaryLookupMap;
        if (map == null) {
            /* Throwing RNUnavailableException should cause a retry */
            throw new RNUnavailableException(
                                    "Table metadata is not yet initialized");
        }
        return map.isEmpty();
    }

    @Override
    public Database getPrimary(DatabaseEntry primaryKey) {
        return repNode.getPartitionDB(primaryKey.getData());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Collection<SecondaryDatabase>
        getSecondaries(DatabaseEntry primaryKey) {

        /*
         * This is not synchronized so the map may have nulled since isEmpty()
         * was called.
         */
        final Map<String, TableEntry> map = secondaryLookupMap;
        if (map == null) {
            /* Throwing RNUnavailableException should cause a retry */
            throw new RNUnavailableException(
                                "Table metadata is not yet initialized");
        }

        /* Start by looking up the top level table for this key */
        final BinaryKeyIterator keyIter =
                                    new BinaryKeyIterator(primaryKey.getData());

        /* The first element of the key will be the top level table */
        final String rootId = keyIter.next();
        final TableEntry entry = map.get(rootId);

        /* The entry could be null if the table doesn't have indexes */
        if (entry == null) {
            return Collections.EMPTY_SET;
        }

        /* We have a table with indexes, match the rest of the key. */
        final Collection<String> matchedIndexes = entry.matchIndexes(keyIter);

        /* This could be null if the key did not match any table with indexes */
        if (matchedIndexes == null) {
            return Collections.EMPTY_SET;
        }
        final List<SecondaryDatabase> secondaries =
                        new ArrayList<SecondaryDatabase>(matchedIndexes.size());

        /*
         * Get the DB for each of the matched DB. Opening of the DBs is async
         * so we may not be able to complete the list. In this case thow an
         * exception and hopefully the operation will be retried.
         */
        for (String dbName : matchedIndexes) {
            final SecondaryDatabase db = secondaryDbMap.get(dbName);
            if (db == null) {
                /* Throwing RNUnavailableException should cause a retry */
                throw new RNUnavailableException("Secondary db not yet opened "+
                                                 dbName);
            }
            secondaries.add(db);
        }
        return secondaries;
    }

    /* -- Metadata update methods -- */

    /**
     * Updates the table metadata with the specified metadata object. Returns
     * true if the update is successful.
     *
     * @param newMetadata a new metadata
     * @return true if the update is successful
     */
    public synchronized boolean updateMetadata(Metadata<?> newMetadata) {
        if (!(newMetadata instanceof TableMetadata)) {
            throw new IllegalStateException("Bad metadata?" + newMetadata);
        }
        
        /* If no env, then we can't do an update */
        final ReplicatedEnvironment repEnv = repNode.getEnv(1);
        if ((repEnv == null)) {
            return false;
        }
        
        /*
         * If not the master, then we can't but shouldn't be called again, so
         * report back success.
         */
        if (!repEnv.getState().isMaster()) {
            return true;
        }
        final TableMetadata md = getTableMetadata();

        /* Can't update if we can't read it */
        if (md == null) {
            return false;
        }
        /* If the current md is up-to-date or newer, exit */
        if (md.getSequenceNumber() >= newMetadata.getSequenceNumber()) {
            return true;
        }
        logger.log(Level.INFO, "Updating table metadata with {0}", newMetadata);
        return update((TableMetadata)newMetadata, repEnv);
    }

    /**
     * Updates the table metadata with the specified metadata info object.
     * Returns the sequence number of the table metadata at the end of the
     * update.
     *
     * @param metadataInfo a table metadata info object
     * @return the post update sequence number of the table metadata
     */
    public synchronized int updateMetadata(MetadataInfo metadataInfo) {
        final ReplicatedEnvironment repEnv = repNode.getEnv(1);
        
        /* If no env or error then report back that we don't have MD */
        if (repEnv == null) {
            return Metadata.EMPTY_SEQUENCE_NUMBER;
        }
        final TableMetadata md = getTableMetadata();
        if (md == null) {
            return Metadata.EMPTY_SEQUENCE_NUMBER;
        }
        
        /* Only a master can actually update the metadata */
        if (repEnv.getState().isMaster()) {
            final TableMetadata newMetadata = md.getCopy();
            try {
                if (newMetadata.update(metadataInfo)) {
                    logger.log(Level.INFO, "Updating table metadata with {0}",
                               metadataInfo);
                    if (update(newMetadata, repEnv)) {
                        return newMetadata.getSequenceNumber();
                    }
                }
            } catch (Exception e) {
                logger.log(Level.WARNING,
                           "Error updating table metadata with " +
                           metadataInfo, e);
            }
        }
        /* Update failed, return the current seq # */
        return md.getSequenceNumber();
    }

    /**
     * Persists the specified table metadata and updates the secondary DB
     * handles. The metadata is persisted only if this node is the master.
     * 
     * @return true if the update was successful, or false if in shutdown
     */
    private boolean update(final TableMetadata newMetadata,
                           ReplicatedEnvironment repEnv) {
        assert Thread.holdsLock(this);
        assert repEnv != null;

        /* Only the master can persistMetadata the metadata */
        if (repEnv.getState().isMaster()) {
            if (!persistMetadata(newMetadata)) {
                return true;    /* In shutdown */
            }
        }
        
        /* Update the cached version and update the DB handles */
        tableMetadata = newMetadata;
        updateDbHandles(repEnv, true);
        return true;
    }

    /**
     * Updates the secondary database handles based on the  current table
     * metadata. If reuseExistingHandles is true existing handles are reused.
     * Otherwise new handles are open for each secondary database. Update of
     * the handles is done asynchronously. If an update thread is already
     * running it is stopped and a new thread is started. 
     * 
     * This is called by the RN when 1) the handles have to be renewed
     * due to an environment change, reuseExistingHandles == false, 2) when the
     * topology changes, reuseExistingHandles == true, and 3) when the table
     * metadata is updated.
     * 
     * The table maps are also set.
     *
     * @param repEnv the replicated environment handle
     * @param reuseExistingHandles true if any current db handles are valid
     * and should be reused
     */
    public synchronized void updateDbHandles(ReplicatedEnvironment repEnv,
                                             boolean reuseExistingHandles) {
        assert repEnv != null;
        super.updateDbHandles(repEnv);
        
        final TableMetadata tableMd = getTableMetadata();
        boolean succeeded = updateTableMaps(tableMd, repEnv);
        
        threadLock.lock();
        try {
            stopUpdateThread();

            /*
             * If the handles are being refreshed, or the update failed,
             * or we are not the master, shudown the maintenance thread.
             */
            if (!reuseExistingHandles || !succeeded ||
                !repEnv.getState().isMaster()) {
                stopMaintenanceThread();
            }
            if (!succeeded) {
                return;
            }
            updateThread = new UpdateThread(tableMd, repEnv,
                                            reuseExistingHandles);
            updateThread.start();
        } finally {
            threadLock.unlock();
        }
    }

    /**
     * Rebuilds the secondaryLookupMap and idLookupMap maps. Returns true if
     * the update was successful. If false is returned the table maps have been
     * set such that operations will be disabled.
     * 
     * @return true if the update was successful
     */
    private synchronized boolean updateTableMaps(TableMetadata tableMd,
                                                 ReplicatedEnvironment repEnv) {
        assert repEnv != null;
        
        /*
         * If env is invalid, or tableMD null, disable ops
         */
        if (!repEnv.isValid() || (tableMd == null)) {
            secondaryLookupMap = null;
            idLookupMap = null;
            return false;
        }
        
        /*
         * If empty, then a quick return. Note that we return true so that
         * the update thread runs because there may have been tables/indexes
         * that need cleaning.
         */
        if (tableMd.isEmpty()) {
            secondaryLookupMap = Collections.emptyMap();
            idLookupMap = Collections.emptyMap();
            return true;
        }
        
        // TODO - optimize if the MD has not changed?

        final Map<String, TableEntry> slm = new HashMap<String, TableEntry>();
        final Map<Long, TableImpl> ilm = new HashMap<Long, TableImpl>();

        for (Table table : tableMd.getTables().values()) {
            final TableImpl tableImpl = (TableImpl)table;
            
            /*
             * Add an entry for each table that has indexes somewhere in its
             * hierarchy.
             */
            final TableEntry entry = new TableEntry(tableImpl);

            if (entry.hasSecondaries()) {
                slm.put(tableImpl.getIdString(), entry);
            }

            /*
             * The id map has an entry for each table, so descend into its
             * hierarchy.
             */
            addToMap(tableImpl, ilm);
        }
        secondaryLookupMap = slm;
        idLookupMap = ilm;
        return true;
    }

    private void addToMap(TableImpl tableImpl, Map<Long, TableImpl> map) {
        map.put(tableImpl.getId(), tableImpl);
        for (Table child : tableImpl.getChildTables().values()) {
            addToMap((TableImpl)child, map);
        }
    }

    /**
     * Starts the state tracker
     * TODO - Perhaps start the tracker on-demand in noteStateChange()?
     */
    public void startTracker() {
        stateTracker.start();
    }

    /**
     * Notes a state change in the replicated environment. The actual
     * work to change state is made asynchronously to allow a quick return.
     */
    public void noteStateChange(StateChangeEvent stateChangeEvent) {
        stateTracker.noteStateChange(stateChangeEvent);
    }

    @Override
    protected MetadataType getType() {
        return MetadataType.TABLE;
    }

    /**
     * Refreshes the table metadata due to it being updated by the master.
     * Called from the database trigger.
     */
    @Override
    protected synchronized void update(ReplicatedEnvironment repEnv) {
        /*
         * This will force the tableMetadata to be re-read from the db (in
         * updateDBHandles)
         */
        tableMetadata = null;
        updateDbHandles(repEnv, true);
    }

    /**
     * Thread to manage replicated environment state changes.
     */
    private class TableManagerStateTracker extends StateTracker {
        TableManagerStateTracker(Logger logger) {
            super(TableManagerStateTracker.class.getSimpleName(),
                  repNode, logger);
        }

        @Override
        protected void doNotify(StateChangeEvent sce) {

            final ReplicatedEnvironment repEnv = repNode.getEnv(1);
            if (repEnv == null) {
                return;
            }
            logger.log(Level.INFO, "Table manager change state to {0}",
                       sce.getState());

            Database infoDb = null;
            threadLock.lock();
            try {
                if (sce.getState().isMaster()) {
                    infoDb = SecondaryInfoMap.openDb(repEnv);
                    checkMaintenanceThreads(repEnv, infoDb);
                } else {
                    stopMaintenanceThread();
                }
            } catch (RuntimeException re) {
                // TODO - should this be fatal - if so need to separate non-fatal
                // database exceptions.
                logger.log(Level.SEVERE,
                           "Table manager state change failed", re);
            } finally {
                if (infoDb != null) {
                    TxnUtil.close(logger, infoDb, "secondary info db");
                }
                threadLock.unlock();
            }
        }
    }

    /**
     * A container class for quick lookup of secondary DBs.
     */
    private static class TableEntry {
        private final int keySize;
        private final Set<String> secondaries = new HashSet<String>();
        private final Map<String, TableEntry> children =
                                            new HashMap<String, TableEntry>();

        TableEntry(TableImpl table) {
            /* For child tables subtract the key count from parent */
            keySize = (table.getParent() == null ?
                       table.getPrimaryKeySize() :
                       table.getPrimaryKeySize() -
                       ((TableImpl)table.getParent()).getPrimaryKeySize());

            /* For each index, save the secondary DB name */
            for (Index index : table.getIndexes().values()) {
                secondaries.add(createDbName(index.getName(),
                                             index.getTable().getFullName()));
            }

            /* Add only children which have indexes */
            for (Table child : table.getChildTables().values()) {
                final TableEntry entry = new TableEntry((TableImpl)child);

                if (entry.hasSecondaries()) {
                    children.put(((TableImpl)child).getIdString(), entry);
                }
            }
        }

        private boolean hasSecondaries() {
            return !secondaries.isEmpty() || !children.isEmpty();
        }

        private Collection<String> matchIndexes(BinaryKeyIterator keyIter) {
            /* Match up the primary keys with the input keys, in number only */
            for (int i = 0; i < keySize; i++) {
                /* If the key is short, then punt */
                if (keyIter.atEndOfKey()) {
                    return null;
                }
                keyIter.skip();
            }

            /* If both are done we have a match */
            if (keyIter.atEndOfKey()) {
                return secondaries;
            }

            /* There is another component, check for a child table */
            final String childId = keyIter.next();
            final TableEntry entry = children.get(childId);
            return (entry == null) ? null : entry.matchIndexes(keyIter);
        }
    }

    /**
     * Stops the update thread if one is running and waits for it to exit.
     */
    private void stopUpdateThread() {
        assert threadLock.isHeldByCurrentThread();

        if (updateThread != null) {
            updateThread.waitForStop();
            updateThread = null;
        }
    }

    /**
     * Stops the maintenance thread that may be running, waiting for it to exit.
     */
    private void stopMaintenanceThread() {
        assert threadLock.isHeldByCurrentThread();

        if (maintenanceThread != null) {
            maintenanceThread.waitForStop();
            maintenanceThread = null;
        }
    }

    /**
     * Checks whether a maintenance thread need to be started. This method
     * will attempt to get the threadLock. If the lock is unavailable, no checks
     * are made and false is returned.
     *
     * The priority of the maintenance threads is:
     *
     * 1. Populate
     * 2. Primary cleaning
     * 3. Secondary cleaning
     *
     * This method will stop a lower priority thread before starting a higher
     * priority thread.
     * Note that 1. Populate and 2. Primary cleaning are waited on by plans
     * and should not be needed at the same time.
     *
     * @return true if the check was successful
     */
    boolean checkMaintenanceThreads(ReplicatedEnvironment repEnv,
                                    Database infoDb) {
        assert infoDb != null;

        if (!repEnv.getState().isMaster()) {
            return true;
        }
        logger.fine("Checking maintenance threads");

        /*
         * We only try to obtain the lock to avoid deadlock.
         */
        if (!threadLock.tryLock()) {
            logger.fine("Failed to acquire maintenance thread lock");
            return false;
        }

        try {
            stopMaintenanceThread();

            final SecondaryInfoMap secondaryInfoMap =
                                                SecondaryInfoMap.fetch(infoDb);
            if (secondaryInfoMap == null) {
                return false;
            }

            if (secondaryInfoMap.secondaryNeedsPopulate()) {
                maintenanceThread =
                        new PopulateThread(this, repNode, repEnv, logger);
                maintenanceThread.start();
                return true;
            }

            if (secondaryInfoMap.tableNeedDeleting()) {
                maintenanceThread =
                        new PrimaryCleanerThread(this, repNode, repEnv, logger);
                maintenanceThread.start();
                return true;
            }

            if (secondaryInfoMap.secondaryNeedsCleaning()) {
                maintenanceThread =
                      new SecondaryCleanerThread(this, repNode, repEnv, logger);
                maintenanceThread.start();
                return true;
            }
        } finally {
            threadLock.unlock();
        }
        return true;
    }

    /**
     * Checks if a new maintenance thread needs to be started. Called when a
     * running maintenance thread is finished and about to exit.
     */
    void maintenanceThreadExit(ReplicatedEnvironment repEnv, Database infoDb) {
        assert maintenanceThread != null;

        /*
         * Only try to obtain lock to prevent deadlocks. Note that if
         * someone else has the lock, then things are stopping or there
         * is an update, which will check for maintenance threads.
         */
        if (!threadLock.tryLock()) {
            return;
        }
        try {
            if (!maintenanceThread.isStopped()) {
                return;
            }

            /*
             * Clear the reference to the current thread to prevent
             * checkMaintenanceThreads() from waiting for it to exit.
             */
            maintenanceThread = null;
            checkMaintenanceThreads(repEnv, infoDb);
        } finally {
            threadLock.unlock();
        }
    }

    public void notifyRemoval(PartitionId partitionId) {
        if (secondaryDbMap.isEmpty()) {
            return;
        }
        logger.log(Level.INFO, "{0} has been removed, removing obsolete " +
                   "records from secondaries", partitionId);

        final ReplicatedEnvironment repEnv = repNode.getEnv(1);

        if (repEnv == null) {
            return; // TODO - humm - lost info?
        }
        /* This will call back here to start the primary cleaner thread */
        SecondaryInfoMap.markForSecondaryCleaning(this, repEnv, logger);
    }
    
    @Override
    public String toString() {
        return "TableManager[" +
               ((tableMetadata == null) ? "-" :
                                          tableMetadata.getSequenceNumber()) +
               ", " + secondaryDbMap.size() + "]";
    }

    /**
     * Thread to update the secondary database handles. When run, the table
     * metadata is scanned looking for indexes. Each index will have a
     * corresponding secondary DB. If the index is new, (and this is the master)
     * a new secondary DB is created. Populating the new secondary DB is done
     * in a separate thread.
     *
     * The table MD is also checked for changes, such as indexes dropped and
     * tables that need their data deleted.
     */
    private class UpdateThread extends Thread {

        private final TableMetadata tableMd;
        private final ReplicatedEnvironment repEnv;
        private final boolean reuseExistingHandles;

        /* Filled in only when master */
        private Database infoDb = null;

        private volatile boolean stop = false;

        UpdateThread(TableMetadata tableMd,
                     ReplicatedEnvironment repEnv,
                     boolean reuseExistingHandles) {
            super("KV secondary handle updater");
            assert repEnv != null;
            assert tableMd != null;
            this.tableMd = tableMd;
            this.repEnv = repEnv;
            this.reuseExistingHandles = reuseExistingHandles;
            setDaemon(true);
            setUncaughtExceptionHandler(repNode.getExceptionHandler());
        }

        @Override
        public void run() {
            logger.log(Level.FINE, "Starting {0}", this);
            try {
                /* Retry as long as there are errors */
                while (update()) {
                    try {
                        Thread.sleep(DB_OPEN_RETRY_MS);
                    } catch (InterruptedException ie) {
                        /* Should not happen. */
                        throw new IllegalStateException(ie);
                    }
                }

                /* If infoDB == null, we are not the master, so we are done */
                if (infoDb == null) {
                    return;
                }

                /* Check to see if any maintenance threads can be started. */
                while (!stop && repEnv.isValid() &&
                       repEnv.getState().isMaster()) {

                    if (checkMaintenanceThreads(repEnv, infoDb)) {
                        return;
                    }
                    try {
                        Thread.sleep(DB_OPEN_RETRY_MS);
                    } catch (InterruptedException ie) {
                        /* Should not happen. */
                        throw new IllegalStateException(ie);
                    }
                }
            } finally {
                if (infoDb != null) {
                    TxnUtil.close(logger, infoDb, "secondary info db");
                }
            }
        }

        /**
         * Updates the partition database handles.
         *
         * @return true if there was an error and the update should be retried
         */
        private boolean update() {

            if (stop || !repEnv.isValid()) {
                return false;
            }
            logger.log(Level.INFO,  // FINE
                       "Establishing secondary database handles, " +
                       "table seq#: {0}",
                       tableMd.getSequenceNumber());

            /*
             * If we are the master, get the populate info in case we
             * find a new index or we need to restart an ongoing populate.
             */
            if (repEnv.getState().isMaster() && (infoDb == null)) {
                try {
                    infoDb = SecondaryInfoMap.openDb(repEnv);
                    assert infoDb != null;
                } catch (Exception e) {
                    logger.log(Level.INFO, "Failed to open info map DB", e);
                    return !stop;
                }
            }

            /*
             * Map of seondary DB name -> indexes that are defined in the table
             * metadata. This is used to determine what databases to open and if
             * an index has been dropped.
             */
            final Map<String, IndexImpl> indexes =
                                            new HashMap<String, IndexImpl>();

            /*
             * Set tables which are being removed but first need their data
             * deleted.
             */
            final Set<TableImpl> deletedTables = new HashSet<TableImpl>();
            final Map<String, Table> currentTables = tableMd.getTables();

            for (Table table : tableMd.getTables().values()) {
                scanTable((TableImpl)table, indexes, deletedTables);
            }
            logger.log(Level.INFO, "Found {0} indexes and {1} tables marked " +
                       "for deletion in {2}",
                       new Object[]{indexes.size(), deletedTables.size(),
                                    tableMd});

            /*
             * Update the secondary map with any indexes that have been dropped
             * and removed tables.
             */
            if (infoDb != null) {
                SecondaryInfoMap.check(currentTables, indexes, deletedTables,
                                       infoDb, logger);
            }

            int errors = 0;

            /* Remove secondary DBs for dropped indexes */
            final Iterator<Entry<String, SecondaryDatabase>> itr =
                                           secondaryDbMap.entrySet().iterator();

            while (itr.hasNext()) {
                if (stop || !repEnv.isValid()) {
                    return false;
                }
                final Entry<String, SecondaryDatabase> entry = itr.next();
                final String dbName = entry.getKey();

                if (!indexes.containsKey(dbName)) {
                    final SecondaryDatabase db = entry.getValue();

                    if (db != null) {
                        try {
                            closeSecondaryDb(db);

                            if (repEnv.getState().isMaster()) {
                                logger.log(Level.INFO,
                                       "Secondary database {0} is not " +
                                       "defined in table metadata " +
                                       "seq# {1} and is being removed.",
                                       new Object[]{dbName,
                                                  tableMd.getSequenceNumber()});
                                repEnv.removeDatabase(null, dbName);
                            }
                        } catch (DatabaseNotFoundException ignore) {
                            /* Already gone */
                        } catch (RuntimeException re) {
                            PartitionManager.handleException(re, logger,
                                                             dbName);
                            /* Log the exception and go on, leaving the db in
                             * the map for next time.
                             */
                            errors++;
                            continue;
                        }
                    }
                    itr.remove();
                }
            }

            /*
             * For each index open its secondary DB
             */
            for (Entry<String, IndexImpl> entry : indexes.entrySet()) {

                /* Exit if the updater has been stopped or the env is bad */
                if (stop || !repEnv.isValid()) {
                    logger.log(Level.INFO,
                               "Update terminated, established {0} " +
                               "secondary database handles",
                               secondaryDbMap.size());
                    return false;   // Will cause thread to exit
                }

                final String dbName = entry.getKey();
                SecondaryDatabase db = secondaryDbMap.get(dbName);
                if ((db == null) || !reuseExistingHandles) {
                    try {
                        final IndexImpl index = entry.getValue();
                        db = openSecondaryDb(dbName, index);
                        assert db != null;
                        secondaryDbMap.put(dbName, db);
                    } catch (RuntimeException re) {
                        if (PartitionManager.handleException(re, logger,
                                                             dbName)) {
                            errors++;
                        }
                    }
                } else {
                    updateIndexKeyCreator(db, entry.getValue());
                }
            }

            /*
             * If there have been errors return true (unless the update has been
             * stopped) which will cause the update to be retried.
             */
            if (errors > 0) {
                logger.log(Level.INFO,
                           "Established {0} secondary database handles, " +
                           "will retry in {1}ms",
                           new Object[] {secondaryDbMap.size(),
                                         DB_OPEN_RETRY_MS});
                return !stop;
            }
            logger.log(Level.INFO, "Established {0} secondary database handles",
                       secondaryDbMap.size());

            return false;   // Done
        }

        /*
         * Update the index in the secondary's key creator.
         */
        private void updateIndexKeyCreator(SecondaryDatabase db,
                                           IndexImpl index) {
            logger.log(Level.FINE,
                       "Updating index metadata for index {0} in table {1}",
                       new Object[]{index.getName(),
                                    index.getTable().getFullName()});
            final SecondaryConfig config = db.getConfig();
            final IndexKeyCreator keyCreator = (IndexKeyCreator)
                        (index.isMultiKey() ? config.getMultiKeyCreator() :
                                              config.getKeyCreator());
            assert keyCreator != null;
            keyCreator.setIndex(index);
        }
        
        /*
         * Check if the specified table has any indexes or if the table is
         * deleted (and needs it data deleted). If it has indexes, add them to
         * indexes map. If the table is marked for deletion add that to the
         * deleteingTables map.
         *
         * If the table has children, recursively check those.
         */
        private void scanTable(TableImpl table,
                               Map<String, IndexImpl> indexes,
                               Set<TableImpl> deletedTables) {

            if (table.getStatus().isDeleting()) {

                // TODO - should we check for consistency? If so, exception?
                if (!table.getChildTables().isEmpty()) {
                    throw new IllegalStateException("Table " + table +
                                                " is deleted but has children");
                }
                if (!table.getIndexes().isEmpty()) {
                    throw new IllegalStateException("Table " + table +
                                                 " is deleted but has indexes");
                }
                deletedTables.add(table);
                return;
            }
            for (Table child : table.getChildTables().values()) {
                scanTable((TableImpl)child, indexes, deletedTables);
            }

            for (Index index : table.getIndexes().values()) {
                indexes.put(createDbName(index.getName(),
                                         table.getFullName()),
                            (IndexImpl)index);
            }
        }

        /**
         * Opens the specified secondary database.
         */
        private SecondaryDatabase openSecondaryDb(String dbName,
                                                  IndexImpl index) {
            // TODO - fine
            logger.log(Level.INFO, "Open secondary DB {0}", dbName);

            final IndexKeyCreator keyCreator = new IndexKeyCreator(index);

            /*
             * Use NO_CONSISTENCY so that the handle establishment is not
             * blocked trying to reach consistency particularly when the env is
             * in the unknown state and we want to permit read access.
             */
            final TransactionConfig txnConfig = new TransactionConfig().
               setConsistencyPolicy(NoConsistencyRequiredPolicy.NO_CONSISTENCY);
            
            final SecondaryConfig dbConfig = new SecondaryConfig();
            dbConfig.setExtractFromPrimaryKeyOnly(keyCreator.primaryKeyOnly()).
                     setSecondaryAssociation(TableManager.this).
                     setTransactional(true).
                     setAllowCreate(true).
                     setDuplicateComparator(Key.BytesComparator.class).
                     setSortedDuplicates(true);

            if (keyCreator.isMultiKey()) {
                dbConfig.setMultiKeyCreator(keyCreator);
            } else {
                dbConfig.setKeyCreator(keyCreator);
            }

            Transaction txn = null;
            try {
                txn = repEnv.beginTransaction(null, txnConfig);
                final SecondaryDatabase db =
                      repEnv.openSecondaryDatabase(txn, dbName, null, dbConfig);
                
                /*
                 * If we are the master, add the info record for this secondary.
                 */
                if (infoDb != null) {
                    if (SecondaryInfoMap.add(dbName, infoDb, txn, logger)) {
                        db.startIncrementalPopulation();
                    } else {
                        db.endIncrementalPopulation();
                    }
                }
                txn.commit();
                txn = null;
                return db;
            } catch (IllegalStateException e) {

                /*
                 * The exception was most likely thrown because the environment
                 * was closed.  If it was thrown for another reason, though,
                 * then invalidate the environment so that the caller will
                 * attempt to recover by reopening it.
                 */
                if (repEnv.isValid()) {
                    EnvironmentFailureException.unexpectedException(
                        DbInternal.getEnvironmentImpl(repEnv), e);
                }
                throw e;

            } finally {
               TxnUtil.abort(txn);
            }
        }

        /**
         * Stops the updater and waits for the thread to exit.
         */
        void waitForStop() {
            assert Thread.currentThread() != this;

            stop = true;

            try {
                join();
            } catch (InterruptedException ie) {
                /* Should not happen. */
                throw new IllegalStateException(ie);
            }
        }
    }
}
