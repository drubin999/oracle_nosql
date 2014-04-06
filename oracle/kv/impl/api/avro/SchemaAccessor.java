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

package oracle.kv.impl.api.avro;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.Durability;
import oracle.kv.KVVersion;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.KeyValueVersion;
import oracle.kv.KVStore;
import oracle.kv.Operation;
import oracle.kv.OperationExecutionException;
import oracle.kv.OperationFactory;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.impl.api.KVStoreImpl;

import org.apache.avro.Schema;

/**
 * Provides query and update operations for reading and writing schema records
 * in the KVStore.  Used by the administration interface for writing schemas,
 * and by the SchemaCache (which is used by clients) for reading schemas.
 * <p>
 * The schema key format is //sch/-/status/schemaID where status is a one
 * letter abbreviation ('A' for active or 'D' for disabled) and schemaID is
 * an 8 digit hex number.  The data stored under each such key is the schema
 * text itself.
 * <p>
 * The status is placed in the key, preceding the schema ID, in order to query
 * only all active schemas when populating the cache.  This ensures that cache
 * population has the mimimal system impact, because disabled schemas are not
 * queried.
 * <p>
 * When a schema is disabled it is effectively deleted (hidden from clients) by
 * changing its key. The active key is deleted and the schema is re-inserted
 * under a disabled key.  It can be reinstated, however, by changing the key
 * back to an active key.  The deletion and insertion are done atomically using
 * a transaction (using KVStore.execute).
 * <p>
 * The root key for all schemas, //sch, is used to store the last assigned
 * schema ID.  This kv pair is updated atomically in a transaction (using
 * KVStore.execute) along with each newly inserted schema kv pair.
 */
class SchemaAccessor {

    static final int FIRST_SCHEMA_ID = 1;
    private static final int MAX_WRITE_RETRIES = 100;
    
    private static final String KEY_TAG = "sch";
    private static final Key ROOT_PARENT_KEY =
        Key.createKey(Arrays.asList("", KEY_TAG));
    private static final Key ACTIVE_PARENT_KEY =
        Key.createKey(ROOT_PARENT_KEY.getMajorPath(),
                      AvroSchemaStatus.ACTIVE.getCode());
    private static final String ID_FORMAT = "%08x";

    private static final long CONSISTENCY_LAG_MS_1 = 5 * 60 * 1000;
    private static final long CONSISTENCY_LAG_MS_2 = 30 * 1000;

    /**
     * The consistency timeout is very short to minimize the impact on overall
     * operation time when retries occur.
     */
    private static final int CONSISTENCY_TIMEOUT_MS = 1;

    /**
     * String format versions
     *   - initial: not versioned, shortLenght always >= 0
     *   - FORMAT_VERSION_1 = -1:  version + intLength + Utf8String
     */
    private static final short FORMAT_VERSION_1 = -1;
    private static final KVVersion FORMAT_VERSION_1_COMPARE =
        KVVersion.R3_0; /* R3.0 Q1/2014 */

    /**
     * Used to read and write schema kv pairs.  This internal handle provides
     * access to the internal keyspace (//).
     */
    private final KVStore store;

    /** See {@link #getConsistencyRamp}. */
    private final List<Consistency> consistencyRamp;


    SchemaAccessor(KVStore store) {

        /* Get access to the internal keyspace. */
        this.store = KVStoreImpl.makeInternalHandle(store);

        /* Add the retry consistency ramp up values. */
        consistencyRamp = new ArrayList<Consistency>(4);
        consistencyRamp.add(Consistency.NONE_REQUIRED);
        consistencyRamp.add(createTimeConsistency(CONSISTENCY_LAG_MS_1));
        consistencyRamp.add(createTimeConsistency(CONSISTENCY_LAG_MS_2));
        consistencyRamp.add(Consistency.ABSOLUTE);
    }

    private Consistency.Time createTimeConsistency(long lagMs) {
        return new Consistency.Time(lagMs, TimeUnit.MILLISECONDS,
                                    CONSISTENCY_TIMEOUT_MS,
                                    TimeUnit.MILLISECONDS);
    }

    /**
     * Inserts a schema, assigning it the next available schema ID, which is
     * always greater than any existing schema ID.
     *
     * @return the newly assigned schema ID.
     */
    int insertSchema(SchemaData data, KVVersion version) {

        final Value value = schemaDataToValue(data, version);
        final OperationFactory opFactory = store.getOperationFactory();
        OperationExecutionException lastOpException = null;

        for (int i = 0; i < MAX_WRITE_RETRIES; i += 1) {

            final List<Operation> ops = new ArrayList<Operation>(2);
            final int id;

            /*
             * If a root record exists, get the last assigned ID from it and
             * update it to contain the ID following it.  Otherwise, use
             * FIRST_SCHEMA_ID and insert the root record.
             *
             * For simplicity, use absolute consistency (rather than the
             * consistency ramp) to read the root record.  We intend to update
             * it and this operation is very rare.
             */
            final ValueVersion rootVv =
                store.get(ROOT_PARENT_KEY, Consistency.ABSOLUTE, 0L, null);
            if (rootVv == null) {
                id = FIRST_SCHEMA_ID;
                ops.add(opFactory.createPutIfAbsent
                    (ROOT_PARENT_KEY, schemaIdToValue(id), null, true));
            } else {
                id = valueToSchemaId(rootVv.getValue()) + 1;
                ops.add(opFactory.createPutIfVersion
                    (ROOT_PARENT_KEY, schemaIdToValue(id), rootVv.getVersion(),
                     null, true));
            }

            /* Insert the schema. */
            ops.add(opFactory.createPutIfAbsent
                (schemaIdToKey(id, data.getMetadata().getStatus()), value,
                 null, true));

            try {
                store.execute(ops, Durability.COMMIT_SYNC, 0L, null);
                /* Success. */
                return id;
            } catch (OperationExecutionException e) {

                /*
                 * If the operation fails because a schema was added by another
                 * thread, retry.
                 */
                lastOpException = e;
                continue;
            }
        }

        throw new IllegalStateException
            ("Max retries (" + MAX_WRITE_RETRIES +
             ") exceeded attempting to insert schema",
             lastOpException);
    }

    /**
     * Changes the status of a schema.  This involves deleting the old schema
     * and inserting a new one, because the status is part of the key.  This is
     * atomically in a KVStore.execute transaction.
     *
     * @throws IllegalArgumentException if the given schema ID does not exist.
     */
    boolean updateSchemaStatus(int schemaId,
                               AvroSchemaMetadata newMeta,
                               KVVersion version) {

        final AvroSchemaStatus newStatus = newMeta.getStatus();
        final OperationFactory opFactory = store.getOperationFactory();
        OperationExecutionException lastOpException = null;

        retries: for (int i = 0; i < MAX_WRITE_RETRIES; i += 1) {

            /*
             * Read the schema by trying each status in EnumSet order, so that
             * ACTIVE (the most common) is queried first.  See readSchema.
             */
            for (final AvroSchemaStatus status : AvroSchemaStatus.ALL) {
                final Key key = schemaIdToKey(schemaId, status);

                /*
                 * For simplicity, use absolute consistency (rather than the
                 * consistency ramp) to read the root record.  This operation
                 * is very rare.
                 */
                final ValueVersion vv = store.get(key, Consistency.ABSOLUTE,
                                                  0L, null);
                if (vv == null) {
                    /* Try another status key. */
                    continue;
                }

                if (status == newStatus) {
                    /* Short-circuit if status is already set. */
                    return false;
                }

                /* Create new key/value objects. */
                final Key newKey = schemaIdToKey(schemaId, newStatus);
                final SchemaData oldData =
                    valueToSchemaData(vv.getValue(), status);
                final SchemaData newData =
                    new SchemaData(newMeta, oldData.getSchema());
                final Value newValue = schemaDataToValue(newData, version);

                /* Execute ops to delete old key and insert new key. */
                final List<Operation> ops = new ArrayList<Operation>(2);
                ops.add(opFactory.createDeleteIfVersion(key, vv.getVersion(),
                                                        null, true));
                ops.add(opFactory.createPutIfAbsent(newKey, newValue,
                                                    null, true));
                try {
                    store.execute(ops, Durability.COMMIT_SYNC, 0L, null);
                    /* Success. */
                    return true;
                } catch (OperationExecutionException e) {

                    /*
                     * If the operation fails because the schema was
                     * modified by another thread, retry.
                     */
                    lastOpException = e;
                    continue retries;
                }
            }

            /* Not found under any status key. */
            throw new IllegalArgumentException("Schema ID " + schemaId +
                                               " does not exist");
        }

        throw new IllegalStateException
            ("Max retries (" + MAX_WRITE_RETRIES +
             ") exceeded attempting to insert schema",
             lastOpException);
    }

    /**
     * Returns a map of all schema kv pairs, including disabled schemas.
     *
     * @param includeDisabled whether to include disabled schemas; active
     * schemas are always included.
     *
     * @param consistency is the consistency used for the query; see {@link
     * #getConsistencyRamp}.
     *
     * @return the query results.
     */
    SortedMap<Integer, SchemaData> readAllSchemas(boolean includeDisabled,
                                                  Consistency consistency) {
        if (includeDisabled) {
            return readSchemas(ROOT_PARENT_KEY, null, consistency);
        }
        return readSchemas(ACTIVE_PARENT_KEY, null, consistency);
    }

    /**
     * Returns a map of all active schema kv pairs, in schema ID order, with
     * IDs starting with a given schema ID (i.e., that were inserted at a later
     * time).
     *
     * @param startSchemaId used as the start field of the KeyRange for
     * querying schemas.
     *
     * @param includeStart used as the startInclusive field of the KeyRange for
     * querying schemas.
     *
     * @param consistency is the consistency used for the query; see {@link
     * #getConsistencyRamp}.
     *
     * @return the query results.
     */
    SortedMap<Integer, SchemaData> readActiveSchemas(int startSchemaId,
                                                     boolean includeStart,
                                                     Consistency consistency) {

        final KeyRange subRange = new KeyRange
            (String.format(ID_FORMAT, startSchemaId), includeStart,
             null, false);

        return readSchemas(ACTIVE_PARENT_KEY, subRange, consistency);
    }

    /**
     * Read schemas internally using a given parent key and range.
     */
    private SortedMap<Integer, SchemaData>
        readSchemas(Key parentKey,
                    KeyRange subRange,
                    Consistency consistency) {

        final SortedMap<Integer, SchemaData> results =
            new TreeMap<Integer, SchemaData>();

        final Iterator<KeyValueVersion> iter = store.multiGetIterator
            (Direction.FORWARD, 0, parentKey, subRange, Depth.DESCENDANTS_ONLY,
             consistency, 0L, null);

        while (iter.hasNext()) {
            final KeyValueVersion kvv = iter.next();
            final int id = keyToSchemaId(kvv.getKey());
            final AvroSchemaStatus status = keyToSchemaStatus(kvv.getKey());
            final SchemaData data = valueToSchemaData(kvv.getValue(), status);
            results.put(id, data);
        }

        return results;
    }

    /**
     * Returns the schema data for a given ID.
     *
     * @throws IllegalArgumentException if the given schema ID does not exist.
     */
    SchemaData readSchema(int schemaId, Consistency consistency) {

        /*
         * Because the schema key contains the status, preceding the ID, we
         * must query for each status in turn.  Because it is most likely that
         * a schema status is ACTIVE (it is very rare to query a DISABLED
         * schema), we try status values in declared order (as returned by
         * EnumSet), and rely on the fact that ACTIVE is declared first.
         */
        for (final AvroSchemaStatus status : AvroSchemaStatus.ALL) {
            final ValueVersion vv = store.get(schemaIdToKey(schemaId, status),
                                              consistency, 0L, null);
            if (vv != null) {
                return valueToSchemaData(vv.getValue(), status);
            }
        }

        /* Not found under any status key. */
        throw new IllegalArgumentException("Schema ID " + schemaId +
                                           " does not exist");
    }

    /**
     * Deletes all schemas.
     *
     * @throws IllegalArgumentException if the given schema ID does not exist.
     */
    void deleteAllSchemas() {
        store.multiDelete(ROOT_PARENT_KEY, null, Depth.DESCENDANTS_ONLY,
                          Durability.COMMIT_SYNC, 0L, null);
    }

    /**
     * Returns an iterable sequence of Consistency values that should be used
     * for querying schemas, when it is expected that a particular schema has
     * been stored.  The first iterated value should be used initially for the
     * query, and if an expected schema is not found then the next value should
     * be used to retry the query, and so on.
     * <p>
     * Normally the lowest consistency is sufficient for finding schemas, since
     * they are stored very infrequently and therefore all replicas (on the
     * shard containing schemas) should contain all written schemas.  In the
     * rare case where a replica does not containing a schema that is expected
     * to be stored, higher consistency levels are used to query the schema.
     *
     * @see SchemaCache
     */
    Iterable<Consistency> getConsistencyRamp() {
        return consistencyRamp;
    }

    /**
     * Returns the lowest consistency level in the consistency ramp, for use
     * when initialing the schema cache, for example.
     */
    Consistency getLowestConsistency() {
        return consistencyRamp.get(0);
    }

    /**
     * Returns the highest consistency level in the consistency ramp.
     */
    Consistency getHighestConsistency() {
        return consistencyRamp.get(consistencyRamp.size() - 1);
    }

    /**
     * Creates a key that uniquely identifies a schema with the given ID.
     */
    private Key schemaIdToKey(int id, AvroSchemaStatus status) {

        final List<String> minorPath =
            Arrays.asList(status.getCode(), String.format(ID_FORMAT, id));

        return Key.createKey(ROOT_PARENT_KEY.getMajorPath(), minorPath);
    }

    /**
     * Extracts the schema ID from a schema key.
     */
    private int keyToSchemaId(Key key) {
        final List<String> minor = key.getMinorPath();
        if (minor.size() == 2) {
            try {
                return Integer.parseInt(minor.get(1), 16);
            } catch (NumberFormatException e) {
                /* Fall through. */
            }
        }
        throw new IllegalStateException("Invalid internal schema key: " + key);
    }

    /**
     * Extracts the schema status from a schema key.
     */
    private AvroSchemaStatus keyToSchemaStatus(Key key) {
        final List<String> minor = key.getMinorPath();
        if (minor.size() == 2) {
            final AvroSchemaStatus status =
                AvroSchemaStatus.fromCode(minor.get(0));
            if (status != null) {
                return status;
            }
        }
        throw new IllegalStateException("Invalid internal schema key: " + key);
    }

    /**
     * Serializes SchemaData as a Value.  If the metadata time-modified
     * property is zero, the current time will be used.
     * <p>
     * It is possible that additional metadata fields may be added in the
     * future.  The versioning approach must take into account old code that
     * reads a new version of the data, and new code that reads an old version
     * of the data.
     * <ul>
     *   <li>
     *   The format and position of existing fields will not be changed when
     *   new fields are added, and fields that follow the known fields are
     *   ignored.  This ensures that old code can read new data.
     *   <li>
     *   New fields will be added at the end of the serialized format.  After
     *   reading the initial field (the schema text), new code will call
     *   InputStream.available() to determine whether additional fields are
     *   present.  This ensures that new code can read old data.
     * </ul>
     */
    private Value schemaDataToValue(SchemaData data, KVVersion version) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream(1000);
        final DataOutputStream dos = new DataOutputStream(baos);
        final AvroSchemaMetadata metadata = data.getMetadata();
        try {
            writeLongString(dos, data.getSchema().toString(), version);
            final long time = (metadata.getTimeModified() != 0) ?
                metadata.getTimeModified() :
                System.currentTimeMillis();
            dos.writeLong(time);
            dos.writeUTF(metadata.getByUser());
            dos.writeUTF(metadata.getFromMachine());
            dos.flush();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return Value.createValue(baos.toByteArray());
    }

    /**
     * Deserializes SchemaData from a Value.  See {@link #schemaDataToValue}
     * for the versioning approach.
     */
    private SchemaData valueToSchemaData(Value value,
                                         AvroSchemaStatus status) {

        final DataInputStream dis =
            new DataInputStream(new ByteArrayInputStream(value.getValue()));

        final String text;
        final long timeModified;
        final String byUser;
        final String fromMachine;
        try {
            text = readLongString(dis);
            timeModified = dis.readLong();
            byUser = dis.readUTF();
            fromMachine = dis.readUTF();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        final Schema schema;
        try {
            schema = new Schema.Parser().parse(text);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot parse stored schema: " +
                                            text);
        }

        final AvroSchemaMetadata metadata =
            new AvroSchemaMetadata(status, timeModified, byUser, fromMachine);

        return new SchemaData(metadata, schema);
    }

    /** Serializes schema ID, for writing with the root key. */
    private Value schemaIdToValue(int id) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream(1000);
        final DataOutputStream dos = new DataOutputStream(baos);
        try {
            dos.writeInt(id);
            dos.flush();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return Value.createValue(baos.toByteArray());
    }

    /** Deserializes a schema ID, after reading with the root key. */
    private int valueToSchemaId(Value value) {
        final DataInputStream dis =
            new DataInputStream(new ByteArrayInputStream(value.getValue()));
        try {
            return dis.readInt();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Writes a versioned string in UTF format. Only non-null str are
     * allowed. 
     * <pre>
     * If version <= R3.0 Q1/2014
     *    format of string is: shortLength + stringByteArray
     *    shortLength is always >= 0
     * else
     *    format of string is: version + intLength + stringByteArray
     *    version is always < 0
     * </pre>
     */
    static void writeLongString(DataOutputStream out, String str,
                         KVVersion version)
        throws IOException {

        if ( version.compareTo(FORMAT_VERSION_1_COMPARE) < 0 ) {
            if ( str!= null && str.length() > Short.MAX_VALUE )
                throw new IllegalArgumentException("String length too long " +
                    "for serialization in this version, please upgrade to " +
                    "next version.");
            /* this is the initial version FORMAT_VERSION_0 */
            out.writeUTF(str);
        } else {
            /* this is the second version FORMAT_VERSION_1 */
            out.writeShort(FORMAT_VERSION_1);
            final byte[] utf = str.getBytes("UTF8");
            out.writeInt(utf.length);
            out.write(utf);
        }
    }

    /**
     * Reads a versioned string in UTF format.
     * @see #writeLongString(java.io.DataOutputStream, String, oracle.kv.KVVersion)
     * writeLongString for format of the string
     */
    static String readLongString(DataInputStream in)
        throws IOException {

        final short shortLength = in.readShort();
        if ( shortLength >= 0 ) {
            /* this is the initial version FORMAT_VERSION_0 */
            final byte[] utf = new byte[shortLength];
            in.readFully(utf);
            return new String(utf, "UTF8");
        }
        if ( shortLength==FORMAT_VERSION_1 ) {
            /* this is the second version FORMAT_VERSION_1 */
            final int len = in.readInt();
            final byte[] utf = new byte[len];
            in.readFully(utf);
            return new String(utf, "UTF8");
        }
        throw new IllegalStateException("Unknown format version.");
    }
}
