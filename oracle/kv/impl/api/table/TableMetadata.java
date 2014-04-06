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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.api.table.TableImpl.TableStatus;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.metadata.MetadataKey;
import oracle.kv.table.Index;
import oracle.kv.table.Table;

/**
 * This is internal implementation that wraps Table and Index metadata
 * operations such as table/index creation, etc.
 *
 * TableMetadata stores tables in a tree.  The top level is a map from
 * name (String) to Table and contains top-level tables only.  Each top-level
 * table may or may not contain child tables.  When this class is serialized
 * the entire tree of Table objects, along with their contained Index objects,
 * is serialized.
 *
 * When a table lookup is performed it must be done top-down.  First the lookup
 * walks to the "root" of the metadata structure, which is the map contained in
 * this instance.  For top-level tables the lookup is a simple get.  For child
 * tables the code unwinds down the stack of parents to get the child.
 *
 * When a table is first inserted into TableMetadata it is assigned a
 * numeric id. Ids are allocated from the keyId member.
 *
 * Note that this implementation is not synchronized. If multiple threads
 * access a table metadata instance concurrently, and at least one of the
 * threads modifies the table metadata structurally, it must be synchronized
 * externally.
 */
public class TableMetadata implements Metadata<TableChangeList>, Serializable {

    private static final long serialVersionUID = 1L;
    private final Map<String, Table> tables =
        new TreeMap<String, Table>(FieldComparator.instance);

    private int seqNum = Metadata.EMPTY_SEQUENCE_NUMBER;
    private long keyId = INITIAL_KEY_ID;
    private static final int INITIAL_KEY_ID = 1;

    /*
     * Record of changes to the metadata. If null no changes will be kept.
     */
    private final List<TableChange> changeHistory;

    /**
     * Construct a table metadata object. If keepChanges is true any changes
     * made to are recorded and can be accessed through the getMetadataInfo()
     * interface.
     *
     * @param keepChanges
     */
    public TableMetadata(boolean keepChanges) {
        changeHistory = keepChanges ? new LinkedList<TableChange>() : null;
    }

    public TableImpl addTable(String name,
                              String parentName,
                              List<String> primaryKey,
                              List<String> shardKey,
                              FieldMap fieldMap,
                              boolean r2compat,
                              int schemaId,
                              String description) {
        TableImpl table = insertTable(name, parentName,
                                      primaryKey, shardKey,
                                      fieldMap,
                                      r2compat, schemaId,
                                      description);
        bumpSeqNum();
        if (changeHistory != null) {
            changeHistory.add(new AddTable(table, seqNum));
        }
        return table;
    }

    /**
     * Drops a table. If the table has indexes or child tables an
     * IllegalArgumentException is thrown. If markForDelete is true the table's
     * status is set to DELETING and is not removed.
     *
     * @param tableName the table name
     * @param markForDelete if true mark the table as DELETING
     *
     * @returns the removed table
     */
    public void dropTable(String tableName, boolean markForDelete) {
        removeTable(tableName, markForDelete);

        bumpSeqNum();
        if (changeHistory != null) {
            changeHistory.add(new DropTable(tableName,
                                            markForDelete, seqNum));
        }
    }

    /**
     * Evolves a table using new fields but only if it's not already been done
     * and if the supplied version indicates that the evolution started with
     * the latest table version.
     *
     * If this operation was retried the evolution may have already been
     * applied.  Check field equality and if equal, consider the evolution
     * done.
     *
     * @return true if the evolution happens, false otherwise
     *
     * @throws IllegalCommandException if an attempt is made to evolve a version
     * other than the latest table version
     */
    public boolean evolveTable(TableImpl table, int tableVersion,
                               FieldMap fieldMap) {

        if (fieldMap.equals(table.getFieldMap())) {
            return false;
        }

        if (tableVersion != table.numTableVersions()) {
            throw new IllegalCommandException
                ("Table evolution must be performed on the latest version");
        }

        table.evolve(fieldMap);
        bumpSeqNum();
        if (changeHistory != null) {
            changeHistory.add(new EvolveTable(table, seqNum));
        }
        return true;
    }

    public void addIndex(String indexName,
                         String tableName,
                         List<String> fields,
                         String description) {
        final IndexImpl index = insertIndex(indexName, tableName,
                                            fields, description);
        bumpSeqNum();
        if (changeHistory != null) {
            changeHistory.add(new AddIndex(index, seqNum));
        }
    }

    public void dropIndex(String indexName, String tableName) {
        if (removeIndex(indexName, tableName)) {
            bumpSeqNum();
            if (changeHistory != null) {
                changeHistory.add(new DropIndex(indexName,
                                                tableName,
                                                seqNum));
            }
        }
    }

    public boolean updateIndexStatus(String indexName,
                                     String tableName,
                                     IndexImpl.IndexStatus status) {
        final IndexImpl index = changeIndexStatus(indexName, tableName,
                                                  status);
        if (index != null) {
            bumpSeqNum();
            if (changeHistory != null) {
                changeHistory.add(new UpdateIndexStatus(index, seqNum));
            }
            return true;
        }
        return false;
    }

    /*
     * Add the table described.  It must not exist or an exception is thrown.
     * If it has a parent the parent must exist.
     */
    TableImpl insertTable(String name,
                          String parentName,
                          List<String> primaryKey,
                          List<String> shardKey,
                          FieldMap fields,
                          boolean r2compat,
                          int schemaId,
                          String description) {

        TableImpl table = null;

        if (parentName != null) {
            final TableImpl parent = getTable(parentName,
                                              true);
            if (parent.childTableExists(name)) {
                throw new IllegalArgumentException
                    ("Cannot create table.  Table exists: " +
                     makeQualifiedName(name, parentName));
            }
            table = TableImpl.createTable(name, parent,
                                          primaryKey, shardKey,
                                          fields, r2compat, schemaId,
                                          description, true);
            table.setId(allocateId());
            parent.getMutableChildTables().put(name, table);
        } else {
            if (tables.containsKey(name)) {
                throw new IllegalArgumentException
                    ("Cannot create table.  Table exists: " + name);
            }
            table = TableImpl.createTable(name, null,
                                          primaryKey, shardKey,
                                          fields, r2compat, schemaId,
                                          description, true);
            table.setId(allocateId());
            tables.put(name, table);
        }
        return table;

    }

    /*
     * Evolve the table described.  It must not exist or an exception is thrown.
     */
    TableImpl evolveTable(String tableName, FieldMap fields) {
        final TableImpl table = getTable(tableName, true);
        table.evolve(fields);
        return table;
    }

    /**
     * Removes a table. If the table has indexes or child tables an
     * IllegalArgumentException is thrown. If markForDelete is true the table's
     * status is set to DELETING and is not removed.
     *
     * @param tableName the table name
     * @param markForDelete if true mark the table as DELETING
     *
     * @return the removed table
     */
    Table removeTable(String tableName, boolean markForDelete) {
        final TableImpl table = checkForRemove(tableName, true);
        if (markForDelete) {
            table.setStatus(TableStatus.DELETING);
            return table;
        }
        Table parent = table.getParent();
        if (parent != null) {
            ((TableImpl)parent).getMutableChildTables().remove(table.getName());
        } else {
            /* a top-level table */
            tables.remove(table.getName());
        }
        return table;
    }

    /**
     * Called to see if it is ok to remove this table.  If mustExist
     * is true then throw if the table does not exists.
     */
    public TableImpl checkForRemove(String tableName,
                                    boolean mustExist) {
        final TableImpl table = getTable(tableName, mustExist);
        String qname = makeQualifiedName(null, tableName);
        if (table != null) {
            if (!table.getChildTables().isEmpty()) {
                throw new IllegalCommandException
                    ("Cannot remove " + qname +
                     ", it is still referenced by " +
                     "child tables");
            }
        }
        return table;
    }

    IndexImpl insertIndex(String indexName,
                          String tableName,
                          List<String> fields,
                          String description) {
        final TableImpl table = getTable(tableName, true);

        if (table.getIndex(indexName) != null) {
            throw new IllegalArgumentException
                ("Index exists: " + indexName + " on table: " +
                 makeQualifiedName(null, tableName));
        }
        IndexImpl index = new IndexImpl(indexName, table, fields,
                                        description);
        index.setStatus(IndexImpl.IndexStatus.POPULATING);
        table.addIndex(index);
        return index;
    }

    boolean removeIndex(String indexName, String tableName) {
        final TableImpl table = getTable(tableName, true);

        if (table.getIndex(indexName) == null) {
            throw new IllegalArgumentException
                ("Index does not exist: " + indexName + " on table: " +
                 makeQualifiedName(null, tableName));
        }
        table.removeIndex(indexName);
        return true;
    }

    /*
     * Update the index status to the desired status.  If a change was made
     * return the Index, if the status is unchanged return null, allowing
     * this operation to be an idempotent no-op.
     */
    IndexImpl changeIndexStatus(String indexName,
                                String tableName,
                                IndexImpl.IndexStatus status) {
        final TableImpl table = getTable(tableName, true);

        IndexImpl index = (IndexImpl) table.getIndex(indexName);
        if (index == null) {
            throw new IllegalArgumentException
                ("Index does not exist: " + indexName + " on table: " +
                 makeQualifiedName(null, tableName));
        }
        if (index.getStatus() == status) {
            return null;
        }
        index.setStatus(status);
        return index;
    }

    /**
     * Return the named table.
     *
     * @param tableName is a "." separated path to the table name, e.g.
     * parent.child.target.  For top-level tables it is a single
     * component
     */
    public TableImpl getTable(String tableName, boolean mustExist) {
        String path[] = TableImpl.parseFullName(tableName);
        String firstKey = path[0];
        TableImpl targetTable = findTable(firstKey);
        if (path.length > 1) {
            for (int i = 1; i < path.length && targetTable != null; i++) {
                try {
                    targetTable = getChildTable(path[i], targetTable);
                } catch (IllegalArgumentException ignored) {
                    targetTable = null;
                    break;
                }
            }
        }
        if (targetTable == null && mustExist) {
            throw new IllegalArgumentException
               ("Table: " + makeQualifiedName(null, tableName) +
                " does not exist in " + this);
        }
        return targetTable;
    }

    public TableImpl getTable(String tableName) {
        return getTable(tableName, false);
    }

    public boolean tableExists(String name, String tableName) {
        StringBuilder sb = new StringBuilder();
        if (tableName != null) {
            sb.append(tableName);
            sb.append(TableImpl.SEPARATOR);
        }
        if (name != null) {
            sb.append(name);
        }
        return (getTable(sb.toString()) != null);
    }

    public static String makeQualifiedName(TableImpl table) {
        return makeQualifiedName(null, table.getFullName());
    }

    /**
     * Create a string that uniquely identifies a table for use in error
     * messages.  Format is [parentName][.]name
     */
    public static String makeQualifiedName(String name,
                                           String parentName) {
        StringBuilder sb = new StringBuilder();
        if (parentName != null) {
            sb.append(parentName);
            if (name != null) {
                sb.append(TableImpl.SEPARATOR);
            }
        }
        if (name != null) {
            sb.append(name);
        }
        return sb.toString();
    }

    /**
     * Return the named child table.
     */
    public TableImpl getChildTable(String tableName, Table parent) {
        return (TableImpl) parent.getChildTable(tableName);
    }

    /*
     * Get a table from TableMetadataKey.  This is used by RepNodes to return
     * tables requested by clients.  In this path it's necessary to filter out
     * created, but not-yet-populated indexes.
     */
    public TableImpl getTable(TableMetadataKey mdKey) {
        TableImpl table = getTable(mdKey.getTableName());
        if (table != null && table.getIndexes().size() > 0) {
            /* clone, filter */
            table = table.clone();
            for (Map.Entry<String, Index> entry :
                     table.getIndexes().entrySet()) {
                if (!((IndexImpl)entry.getValue()).getStatus().isReady()) {
                    table.removeIndex(entry.getKey());
                }
            }
        }
        return table;
    }

    /**
     * Return all top-level tables.
     */
    public Map<String, Table> getTables() {
        return tables;
    }

    /**
     * Returns true if there are no tables defined.
     * 
     * @return true if there are no tables defined
     */
    public boolean isEmpty() {
        return tables.isEmpty();
    }
    
    /*
     * Find the named top-level table.
     */
    private TableImpl findTable(String key) {
        return (TableImpl) tables.get(key);
    }

    private void bumpSeqNum() {
        seqNum++;
    }

    /*
     * Bump and return a new table id
     */
    private long allocateId() {
        return ++keyId;
    }

    /* -- From Metadata -- */

    @Override
    public MetadataType getType() {
        return MetadataType.TABLE;
    }

    @Override
    public int getSequenceNumber() {
        return seqNum;
    }

    @Override
    public TableChangeList getChangeInfo(int startSeqNum) {
        return new TableChangeList(seqNum, getChanges(startSeqNum));
    }

    /* -- Change support methods -- */

    private List<TableChange> getChanges(int startSeqNum) {

        /* Skip if we are out of date, or don't have changes */
        if ((startSeqNum >= seqNum) ||
            (changeHistory == null) ||
            changeHistory.isEmpty()) {
            return null;
        }

        /* Also skip if they are way out of date (or not initialized) */
        if (startSeqNum < changeHistory.get(0).getSequenceNumber()) {
            return null;
        }

        List<TableChange> list = null;

        for (TableChange change : changeHistory) {
            if (change.getSequenceNumber() > startSeqNum) {
                if (list == null) {
                    list = new LinkedList<TableChange>();
                }
                list.add(change);
            }
        }
        return list;
    }

    /**
     * Updates the metadata data from an info object. Returns true
     * if the table metadata was modified.
     *
     * @param metadataInfo info object to update from
     * @return true if the table metadata was modified
     */
    public boolean update(MetadataInfo metadataInfo) {

        if (metadataInfo instanceof TableChangeList) {
            return apply((TableChangeList)metadataInfo);
        }
        throw new IllegalArgumentException("Unknow metadata info: " +
                                           metadataInfo);
    }

    private boolean apply(TableChangeList changeList) {
        if (changeList.isEmpty()) {
            return false;
        }

        final int origSeqNum = seqNum;

        for (TableChange change : changeList) {
            if (change.getSequenceNumber() <= seqNum) {
                break;
            }
            if (change.getSequenceNumber() > (seqNum + 1)) {
                break;
            }
            if (!change.apply(this)) {
                break;
            }
            seqNum = change.getSequenceNumber();
            if (changeHistory != null) {
                changeHistory.add(change);
            }
        }
        return origSeqNum != seqNum;
    }

    /**
     * Creates a copy of this TableMetadata object.
     *
     * @return the new TableMetadata instance
     */
    public TableMetadata getCopy() {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream() ;
            ObjectOutputStream oos = new ObjectOutputStream(bos) ;
            oos.writeObject(this);
            oos.close();

            ByteArrayInputStream bis =
                new ByteArrayInputStream(bos.toByteArray()) ;
            ObjectInputStream ois = new ObjectInputStream(bis);

            return (TableMetadata)ois.readObject();
        } catch (IOException ioe) {
            throw new IllegalStateException("Unexpected exception", ioe);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Unexpected exception", e);
        }
    }

    @Override
    public String toString() {
        return "TableMetadata[" + seqNum + ", " + tables.size() + ", " +
            ((changeHistory == null) ? "-" : changeHistory.size()) + "]";
    }

    public static class TableMetadataKey implements MetadataKey, Serializable {
        private static final long serialVersionUID = 1L;
        private final String tableName;

        public TableMetadataKey(final String tableName) {
            this.tableName = tableName;
        }

        public String getTableName() {
            return tableName;
        }

        public MetadataKey getMetadataKey() {
            return this;
        }

        /*
         * For debugging
         */
        @Override
        public String toString() {
            return "TableMetadataKey[" +
                   (tableName != null ? tableName : "null") + "]";
        }
    }
}
