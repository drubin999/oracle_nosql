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

import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;

import oracle.kv.Consistency;
import oracle.kv.Value;
import oracle.kv.avro.UndefinedSchemaException;
import oracle.kv.impl.test.TestHook;

import org.apache.avro.Schema;

/**
 * Keeps a cache of all schemas for use by clients that use the Avro bindings,
 * and (in the future) for use by queries and indexers running on an RN.
 * <p>
 * The cache uses a copy-on-write approach for all cached data, to avoid any
 * blocking among threads using the cache as long as there are no cache misses.
 * Copy-on-write is used rather than a ReadWriteLock or concurrent collections
 * for several reasons:
 * <ul>
 *   <li>
 *   Cache hits vastly outnumber misses/updates.
 *   <li>
 *   Cache reads are very small/quick operations and the added overhead of
 *   synchronization on read might be noticeable.
 *   <li>
 *   The cost of copying the cache is low.  It is not expected to be large and
 *   only a shallow copy is needed because Schema objects are immutable.
 * </ul>
 *
 * <h3>Stored Schemas</h3>
 *
 * There are two types of cached information, stored schemas and user schemas.
 * Stored schemas are queried using the SchemaAccessor and cached in two maps,
 * one by schema ID and the other by schema name.
 * <p>
 * When there is a cache miss, we query any recently added schema kv pairs in
 * the store, while synchronized on the cache object itself.  The expectation
 * is that cache misses are infrequent and cache updates even less frequent
 * (because schema changes are so infrequent), so blocking will normally only
 * occur when the cache is initially populated.  Cache updates, when necessary,
 * are performed while synchronized to prevent multiple threads from reading
 * the schema kv pairs concurrently, since this would be wasteful and could
 * impact performance on the RN holding the schema kv pairs.
 * <p>
 * Blocking and schema kv pair queries may occur frequently if multiple caller
 * threads repeatedly try to use a schema that is undefined in the store.  This
 * should be unusual and is considered a programming error, so it not worth
 * trying to optimize.  There is a warning to this effect in the
 * UndefinedSchemaException javadoc.
 *
 * <h3>User Schemas</h3>
 *
 * Users pass Schema objects to the binding APIs for use with Avro as writer
 * schemas and reader schemas.  We must ensure those schemas are known
 * (stored).  To do this we maintain an identity map from user schemas to
 * stored schemas.  This allows us to quickly discover whether a user specified
 * schema is known, but allows users to pass arbitrary schema objects to the
 * binding APIs.  Users typically create schema objects using Avro.
 * <p>
 * Before adding the association between a user's schema and a stored schema to
 * the identity map, we ensure the user's schema is equal to the stored schema.
 * This is considered a lookup by schema value, since a deep comparison between
 * schemas is performed.  When a schema has multiple stored versions, multiple
 * schemas may need to be compared to find the version specified by the user.
 * Once an association has been added to the identity map, a lookup of the user
 * schema is very quick and does not require a schema comparison.
 * <p>
 * Although the map containing user schemas is updated when a user specifies a
 * new schema, rather than by querying stored schemas that were recently added,
 * the same copy-on-write approach and synchronization (on the cache object) is
 * used.  Potential blocking could be reduced by synchronizing on two different
 * objects -- one for updating the stored schemas and another for updating user
 * schemas -- but this would add complexity and potential ordering issues.
 * Both types of cache updates are so infrequent that this is not worth the
 * trouble.
 * <p>
 * Blocking while adding a user schema may occur frequently if the user creates
 * new schema objects often, e.g., for every operation.  This may also use
 * large amounts of memory for caching the user schemas and may eventually fill
 * the JVM heap.  This is considered a programming error and is not explicitly
 * handled.  There is a warning to this effect in the AvroCatalog javadoc.
 */
class SchemaCache {

    /** Used to read schema kv pairs from the store. */
    private final SchemaAccessor accessor;

    /** For use by Avro C API. */
    private final CBindingBridge cBindingBridge;

    /**
     * Current cache contents. This field is reassigned with a new Contents
     * object when there is a change, and the assignment is performed while
     * synchronized.
     */
    private volatile Contents contents;

    private TestHook<Void> cacheMissHook;

    /**
     * Initializes the cache with all currently stored schemas.  Invoked when
     * the AvroCatalog is first opened by a client app.
     */
    SchemaCache(SchemaAccessor accessor) {
        this.accessor = accessor;
        cBindingBridge = new CBindingBridgeImpl();
        contents = new Contents().updateStoredSchemas
            (accessor, accessor.getLowestConsistency());
    }

    /**
     * Updates the cache with stored schemas added since the cache was last
     * initialized or updated.  Invoked when a client calls
     * AvroCatalog.refreshSchemaCache.
     * <p>
     * Calling this method often from multiple threads may cause blocking, and
     * calling it often (even from one thread) could have an impact on store
     * performance.  The AvroCatalog.refreshSchemaCache method javadoc contains
     * warnings to this effect.
     */
    void updateStoredSchemas(Consistency consistency) {

        synchronized (this) {
            /* Update the cache while synchronized. */
            contents = contents.updateStoredSchemas(accessor, consistency);
        }
    }

    /**
     * Returns a map of stored schemas by name.  The most recent version of
     * each schema is contained in the map, according to the current contents
     * of the cache.  The cache is not updated by this method.
     */
    Map<String, Schema> getCurrentSchemas() {
        return contents.currentSchemas;
    }

    /**
     * Gets a stored schema by ID.  If a schema with the given ID is not
     * present in the cache, try updating the cache.  If no such ID is known,
     * return null.
     */
    SchemaInfo getSchemaInfoById(int schemaId) {

        /* First check for a cache hit without any synchronization. */
        SchemaInfo info = contents.byId.get(schemaId);
        if (info != null) {
            return info;
        }

        if (cacheMissHook != null) {
            cacheMissHook.doHook(null);
        }

        /* Synchronize when there is a cache miss. */
        synchronized (this) {

            /*
             * Return cached schema if another thread added the schema to the
             * cache while we waited to get the mutex.  The double-check is
             * safe because the contents field is volatile.
             */
            info = contents.byId.get(schemaId);
            if (info != null) {
                return info;
            }

            /* Update the cache while synchronized. */
            for (Consistency consistency : accessor.getConsistencyRamp()) {
                contents = contents.updateStoredSchemas(accessor, consistency);
                info = contents.byId.get(schemaId);
                if (info != null) {
                    return info;
                }
            }

            /*
             * Final attempt refreshes all schemas from scratch when an older
             * schema ID has recently been enabled.
             */
            contents = contents.refreshStoredSchemas
                (accessor, accessor.getHighestConsistency());
            info = contents.byId.get(schemaId);
            if (info != null) {
                return info;
            }
        }

        return null;
    }

    /**
     * Gets a stored schema by value, using a given Schema for comparision.
     * Returns a stored schema that is equal to the given schema, where
     * equality is the same as Schema.equals with an important exception: Avro
     * string type properties are disregarded.  If such a schema is not present
     * in the cache, try updating the cache.  If no such schema is known,
     * return null.
     */
    SchemaInfo getSchemaInfoByValue(Schema schemaValue) {

        /* First check for a cache hit without any synchronization. */
        SchemaInfo info = contents.byValue.get(schemaValue);
        if (info != null) {
            return info;
        }

        if (cacheMissHook != null) {
            cacheMissHook.doHook(null);
        }

        /* Synchronize when there is a cache miss. */
        synchronized (this) {

            /*
             * Return cached schema if another thread added the schema to the
             * cache while we waited to get the mutex.  The double-check is
             * safe because the contents field is volatile.
             */
            info = contents.byValue.get(schemaValue);
            if (info != null) {
                return info;
            }


            /*
             * Update the cache while synchronized.  First try updating the
             * by-value cache using the cached stored schemas.  If that fails,
             * try updating the stored schemas and then the by-value cache.
             */
            contents = contents.updateUserSchemas(schemaValue);
            info = contents.byValue.get(schemaValue);
            if (info != null) {
                return info;
            }
            for (Consistency consistency : accessor.getConsistencyRamp()) {
                contents = contents.updateStoredSchemas(accessor, consistency);
                contents = contents.updateUserSchemas(schemaValue);
                info = contents.byValue.get(schemaValue);
                if (info != null) {
                    return info;
                }
            }

            /*
             * Final attempt refreshes all schemas from scratch when an older
             * schema ID has recently been enabled.
             */
            contents = contents.refreshStoredSchemas
                (accessor, accessor.getHighestConsistency());
            contents = contents.updateUserSchemas(schemaValue);
            info = contents.byValue.get(schemaValue);
            if (info != null) {
                return info;
            }
        }

        return null;
    }

    /**
     * Gets a stored schema by value like getSchemaInfoByValue.  Unlike
     * getSchemaInfoByValue, does not update the byValue map since the given
     * Schema is coming from the C API and may be a temporary object.  Updates
     * the SchemaInfo to contain the given cSchema, unless another thread gets
     * in first and updates it.  If alwaysCacheCSchema is true, the given
     * cSchema is always added to the byCSchema map, regardless of whether the
     * SchemaInfo already has a non-zero cSchema.
     */
    private SchemaInfo getByValueAndUpdateCSchema(Schema schemaValue,
                                                  long cSchema,
                                                  boolean alwaysCacheCSchema) {
        /*
         * This operation takes place after a cache miss.  Do all checks while
         * synchronized.
         */
        synchronized (this) {

            /*
             * First get the SchemaInfo by value.  Call findByValue to do a
             * lookup without updating the byValue map.
             */
            SchemaInfo info = contents.findByValue(schemaValue, true);
            if (info == null) {
                /* Try updating the stored schemas. */
                for (Consistency consistency : accessor.getConsistencyRamp()) {
                    contents =
                        contents.updateStoredSchemas(accessor, consistency);
                    info = contents.findByValue(schemaValue, true);
                    if (info != null) {
                        break;
                    }
                }
                if (info == null) {

                    /*
                     * Final attempt refreshes all schemas from scratch when an
                     * older schema ID has recently been enabled.
                     */
                    contents = contents.refreshStoredSchemas
                        (accessor, accessor.getHighestConsistency());
                    info = contents.findByValue(schemaValue, true);
                    if (info == null) {
                        /* Schema is not present in the store. */
                        return null;
                    }
                }
            }

            /*
             * We have a SchemaInfo.  Now update its cSchema and add the
             * cSchema to the byCSchema map.
             */
            contents = contents.updateCSchema(cSchema, info,
                                              alwaysCacheCSchema);
            return info;
        }
    }

    /** See CBindingBridge. */
    public CBindingBridge getCBindingBridge() {
        return cBindingBridge;
    }

    /** See CBindingBridge. */
    private class CBindingBridgeImpl implements CBindingBridge {

        @Override
        public Schema getJavaSchema(long cSchema) {
            final SchemaInfo info = contents.byCSchema.get(cSchema);
            if (info == null) {
                return null;
            }
            return info.getSchema();
        }

        @Override
        public Schema putSchema(String schemaText, long cSchema)
            throws UndefinedSchemaException, IllegalArgumentException {

            final Schema javaSchema;
            try {
                javaSchema = new Schema.Parser().parse(schemaText);
            } catch (RuntimeException e) {
                throw new IllegalArgumentException("Error parsing schema", e);
            }
            final SchemaInfo info = getByValueAndUpdateCSchema
                (javaSchema, cSchema, true /*alwaysCacheCSchema*/);
            if (info == null) {
                throw AvroCatalogImpl.newUndefinedSchemaException(javaSchema);
            }
            return info.getSchema();
        }

        @Override
        public long getCSchema(Schema javaSchema)
            throws UndefinedSchemaException {

            final SchemaInfo info = getSchemaInfoByValue(javaSchema);
            if (info == null) {
                throw AvroCatalogImpl.newUndefinedSchemaException(javaSchema);
            }
            return info.getCSchema();
        }

        @Override
        public long putSchema(Schema javaSchema, long cSchema)
            throws UndefinedSchemaException {

            final SchemaInfo info = getByValueAndUpdateCSchema
                (javaSchema, cSchema, false /*alwaysCacheCSchema*/);
            if (info == null) {
                throw AvroCatalogImpl.newUndefinedSchemaException(javaSchema);
            }
            return info.getCSchema();
        }

        @Override
        public long[] getCachedCSchemas() {
            final Map<Long, SchemaInfo> map = contents.byCSchema;
            final long[] array = new long[map.size()];
            int i = 0;
            for (final long x : map.keySet()) {
                array[i++] = x;
            }
            return array;
        }

        @Override
        public int getValueRawDataOffset(Value value) {
            return RawBinding.getValueRawDataOffset(value);
        }

        @Override
        public Schema getValueSchema(Value value)
            throws IllegalArgumentException {

            return RawBinding.getValueSchema(value, SchemaCache.this);
        }

        @Override
        public Value allocateValue(Schema schema, int rawDataSize)
            throws UndefinedSchemaException {

            return RawBinding.allocateValue(schema, rawDataSize,
                                            SchemaCache.this);
        }
    }

    /**
     * An immutable object containing the contents of the cache.
     */
    private static class Contents {

        /**
         * Map of full schema name to current schema info, which is the head of
         * a chain of schemas (different versions) with the same name.
         */
        final Map<String, SchemaInfo> byName;

        /** Map of schema ID to schema info, for every schema version. */
        final Map<Integer, SchemaInfo> byId;

        /** Map of user schema to stored schema. */
        final Map<Schema, SchemaInfo> byValue;

        /** Map of schema pointer in C API to stored schema. */
        final Map<Long, SchemaInfo> byCSchema;

        /** Map of full schema name to current schema.  Derived from byName. */
        final Map<String, Schema> currentSchemas;

        /** Next schema ID available, i.e., one more than highest known ID. */
        final int nextSchemaId;

        /** Constructor to initialize an empty Contents object. */
        Contents() {
            byName = Collections.emptyMap();
            byId = Collections.emptyMap();
            byValue = Collections.emptyMap();
            byCSchema = Collections.emptyMap();
            currentSchemas = Collections.emptyMap();
            nextSchemaId = SchemaAccessor.FIRST_SCHEMA_ID;
        }

        /**
         * Copy constructor that allows optionally specifying each field value.
         * If a parameter is zero/false/null, the field is copied from
         * prevContents; otherwise it is set to the given arg value.
         */
        @SuppressWarnings("null")
        private Contents(Contents prevContents,
                         Map<String, SchemaInfo> byName,
                         Map<Integer, SchemaInfo> byId,
                         Map<Schema, SchemaInfo> byValue,
                         Map<Long, SchemaInfo> byCSchema,
                         boolean deriveCurrentSchemas,
                         int nextSchemaId) {

            this.byName = (byName != null) ? byName : prevContents.byName;
            this.byId = (byId != null) ? byId : prevContents.byId;
            this.byValue = (byValue != null) ? byValue : prevContents.byValue;
            this.byCSchema =
                (byCSchema != null) ? byCSchema : prevContents.byCSchema;
            this.nextSchemaId =
                (nextSchemaId != 0) ? nextSchemaId : prevContents.nextSchemaId;

            if (deriveCurrentSchemas) {

                final Map<String, Schema> newCurrentSchemas =
                    new HashMap<String, Schema>(byName.size());

                for (final Map.Entry<String, SchemaInfo> entry :
                     byName.entrySet()) {
                    newCurrentSchemas.put(entry.getKey(),
                                          entry.getValue().getSchema());
                }

                this.currentSchemas =
                    Collections.unmodifiableMap(newCurrentSchemas);
            } else {
                this.currentSchemas = prevContents.currentSchemas;
            }
        }

        /**
         * Returns a new Contents object containing the schemas in this
         * Contents object plus any schemas that have been added via the admin
         * interface since the cache was updated.  If no new schemas are
         * available, the new Contents object only has an updated timestamp.
         */
        Contents updateStoredSchemas(SchemaAccessor accessor,
                                     Consistency consistency) {

            /*
             * Read schemas that have been added since we last called
             * readActiveSchemas.  If none, no update is needed.
             */
            final SortedMap<Integer, SchemaData> newSchemas =
                accessor.readActiveSchemas
                (nextSchemaId, true /*includeStart*/, consistency);
            if (newSchemas.isEmpty()) {
                return this;
            }

            return addSchemas(newSchemas);
        }

        /**
         * Returns a new Contents object containing the schemas in this
         * Contents object plus any schemas that have been added or re-enabled
         * via the admin interface since the cache was updated.  If no new or
         * re-enabled schemas are available, the new Contents object only has
         * an updated timestamp.
         */
        Contents refreshStoredSchemas(SchemaAccessor accessor,
                                      Consistency consistency) {

            /*
             * Read all schemas. If all schema IDs match, no update is needed.
             */
            final SortedMap<Integer, SchemaData> allSchemas =
                accessor.readActiveSchemas
                (SchemaAccessor.FIRST_SCHEMA_ID, true /*includeStart*/,
                 consistency);
            if (allSchemas.keySet().equals(byId.keySet())) {
                return this;
            }

            /*
             * If schema IDs do not match, check to see whether a full cache
             * refresh is needed.  The newIds set below contains the IDs just
             * queried that are not currently in the cache.  A full refresh is
             * needed in two cases:
             *  + newIds is empty, which means the set of available schemas has
             *    been reduced by disabling one or more schemas;
             *  + the first new ID is less than nextSchemaId, which means an
             *    older schema has been disabled.
             * These cases should be extremely rare so we don't mind starting
             * from scratch.
             */
            final SortedSet<Integer> newIds =
                new TreeSet<Integer>(allSchemas.keySet());
            newIds.removeAll(byId.keySet());

            if (newIds.isEmpty() || newIds.first() < nextSchemaId) {
                /* Full refresh is needed. */
                return new Contents().addSchemas(allSchemas);
            }
            /* Only add new IDs. */
            return addSchemas(allSchemas.tailMap(nextSchemaId));
        }

        /**
         * Common method for adding schemas to an existing Contents or
         * refreshing from scratch (when this Contents is empty).
         */
        private Contents
            addSchemas(SortedMap<Integer, SchemaData> newSchemas) {

            /*
             * Copy this byName and byId maps, and add new stored schemas.
             */
            final Map<String, SchemaInfo> newByName =
                new HashMap<String, SchemaInfo>(byName);
            final Map<Integer, SchemaInfo> newById =
                new HashMap<Integer, SchemaInfo>(byId);

            for (final Map.Entry<Integer, SchemaData> entry :
                 newSchemas.entrySet()) {

                final Integer id = entry.getKey();
                final Schema schema = entry.getValue().getSchema();
                final String name = schema.getFullName();
                final SchemaInfo prevVersion = newByName.get(name);
                final SchemaInfo info = new SchemaInfo(schema, id,
                                                       prevVersion);
                newByName.put(name, info);
                newById.put(id, info);
            }

            /* Update all fields except for byValue and byCSchema. */
            return new Contents(this, Collections.unmodifiableMap(newByName),
                                Collections.unmodifiableMap(newById),
                                null, null, true, newSchemas.lastKey() + 1);
        }

        /**
         * Returns a new Contents object containing the schemas in this
         * Contents object plus a byValue mapping for the given schemaValue.
         * If a stored schema matching schemaValue cannot be found, this
         * Contents object is returned without modification.
         */
        Contents updateUserSchemas(Schema schemaValue) {

            /* Find by value. If no match, return the unmodified contents. */
            final SchemaInfo info = findByValue(schemaValue, false);
            if (info == null) {
                return this;
            }

            /* Copy this byValue map and add new user schema. */
            final Map<Schema, SchemaInfo> newByValue =
                new IdentityHashMap<Schema, SchemaInfo>(byValue);

            newByValue.put(schemaValue, info);

            /* Update only the byValue field. */
            return new Contents(this, null, null,
                                Collections.unmodifiableMap(newByValue),
                                null, false, 0);
        }

        /**
         * Update the given SchemaInfo's cSchema and add the cSchema to the
         * byCSchema map.  The SchemaInfo is not updated if it already contains
         * a non-zero cSchema because another thread got in first.  In that
         * case, if alwaysCacheCSchema is false then the given cSchema is not
         * added to the byCSchema map.
         */
        Contents updateCSchema(long cSchema,
                               SchemaInfo info,
                               boolean alwaysCacheCSchema) {

            if (info.getCSchema() == 0) {
                info.setCSchema(cSchema);
            } else {
                if (!alwaysCacheCSchema) {
                    return this;
                }
            }

            /*
             * We've decided to add cSchema to the byCSchema map, if it's not
             * already present.
             */
            if (byCSchema != null && byCSchema.containsKey(cSchema)) {
                return this;
            }

            /* Copy this byCSchema map and add cSchema mapping. */
            final Map<Long, SchemaInfo> newByCSchema =
                (new HashMap<Long, SchemaInfo>(byCSchema));

            newByCSchema.put(cSchema, info);

            /* Update only the byCSchema field. */
            return new Contents(this, null, null, null,
                                Collections.unmodifiableMap(newByCSchema),
                                false, 0);
        }

        /**
         * Find by value, examining each schema version with the same name
         * as the given schema.
         */
        SchemaInfo findByValue(Schema schemaValue, boolean allowNullDefault) {
            SchemaInfo info = byName.get(schemaValue.getFullName());
            while (info != null) {
                if (SchemaChecker.equalSerializationWithDefault
                    (schemaValue, info.getSchema(), allowNullDefault)) {
                    return info;
                }
                info = info.getPreviousVersion();
            }
            return null;
        }
    }

    /** For testing. */
    int getByIdSize() {
        return contents.byId.size();
    }

    /** For testing. */
    int getByNameSize() {
        return contents.byName.size();
    }

    /** For testing. */
    int getByValueSize() {
        return contents.byValue.size();
    }

    /** For testing. */
    int getByCSchemaSize() {
        return contents.byCSchema.size();
    }

    /** For testing. */
    void setCacheMissHook(TestHook<Void> hook) {
        cacheMissHook = hook;
    }
}
