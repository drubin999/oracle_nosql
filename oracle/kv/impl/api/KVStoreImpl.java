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

package oracle.kv.impl.api;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.AuthenticationRequiredException;
import oracle.kv.Consistency;
import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.Durability;
import oracle.kv.FaultException;
import oracle.kv.KVSecurityException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.KeyValueVersion;
import oracle.kv.LoginCredentials;
import oracle.kv.Operation;
import oracle.kv.OperationExecutionException;
import oracle.kv.OperationFactory;
import oracle.kv.OperationResult;
import oracle.kv.ParallelScanIterator;
import oracle.kv.ReauthenticateHandler;
import oracle.kv.RequestTimeoutException;
import oracle.kv.ReturnValueVersion;
import oracle.kv.StoreIteratorConfig;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.Version;
import oracle.kv.avro.AvroCatalog;
import oracle.kv.impl.api.avro.AvroCatalogImpl;
import oracle.kv.impl.api.lob.KVLargeObjectImpl;
import oracle.kv.impl.api.ops.Delete;
import oracle.kv.impl.api.ops.DeleteIfVersion;
import oracle.kv.impl.api.ops.Execute;
import oracle.kv.impl.api.ops.Execute.OperationImpl;
import oracle.kv.impl.api.ops.Get;
import oracle.kv.impl.api.ops.InternalOperation;
import oracle.kv.impl.api.ops.MultiDelete;
import oracle.kv.impl.api.ops.MultiGet;
import oracle.kv.impl.api.ops.MultiGetIterate;
import oracle.kv.impl.api.ops.MultiGetKeys;
import oracle.kv.impl.api.ops.MultiGetKeysIterate;
import oracle.kv.impl.api.ops.Put;
import oracle.kv.impl.api.ops.PutIfAbsent;
import oracle.kv.impl.api.ops.PutIfPresent;
import oracle.kv.impl.api.ops.PutIfVersion;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.api.ops.ResultKeyValueVersion;
import oracle.kv.impl.api.ops.StoreIterate;
import oracle.kv.impl.api.ops.StoreKeysIterate;
import oracle.kv.impl.api.parallelscan.ParallelScan;
import oracle.kv.impl.api.parallelscan.ParallelScanHook;
import oracle.kv.impl.api.parallelscan.StoreIteratorMetricsImpl;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.login.RepNodeLoginManager;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.lob.InputStreamVersion;
import oracle.kv.stats.KVStats;
import oracle.kv.table.TableAPI;

import com.sleepycat.je.utilint.PropUtil;

public class KVStoreImpl implements KVStore, Cloneable {

    /* TODO: Is this the correct default value? */
    private static int DEFAULT_TTL = 5;

    /* TODO: Is this the correct default value? */
    public static int DEFAULT_ITERATOR_BATCH_SIZE = 100;

    /** A request handler to use for making all requests.  */
    private final RequestDispatcher dispatcher;

    /**
     * Indicates whether the dispatcher is owned by this instance. If we own
     * the dispatcher, our LoginManager is used to gain access to Topology
     * maintenance API methods, and so login changes should be propagated to
     * the dispatcher.
     */
    private final boolean isDispatcherOwner;

    /** Default request timeout in millis, used when timeout param is zero. */
    private final int defaultRequestTimeoutMs;

    /** Socket read timeout for requests. */
    private final int readTimeoutMs;

    /** Default Consistency, used when Consistency param is null. */
    private final Consistency defaultConsistency;

    /** Default Durability, used when Durability param is null. */
    private final Durability defaultDurability;

    /** @see KVStoreConfig#getLOBTimeout() */
    private final long defaultLOBTimeout;

    /** @see KVStoreConfig#getLOBSuffix() */
    private final String defaultLOBSuffix;

    /** @see KVStoreConfig#getLOBVerificationBytes() */
    private final long defaultLOBVerificationBytes;

    /** @see KVStoreConfig#getChunksPerPartition() */
    private final int defaultChunksPerPartition;

    /** @see KVStoreConfig#getChunkSize() */
    private final int defaultChunkSize;

    /** Implementation of OperationFactory. */
    private final OperationFactory operationFactory;

    /** Max partition ID.  This value is immutable for a store. */
    private final int nPartitions;

    /** Translates between byte arrays and Keys */
    private final KeySerializer keySerializer;

    /** The component implementing large object support. */
    final KVLargeObjectImpl largeObjectImpl;

    /** The storeIteratorMetrics. */
    private final StoreIteratorMetricsImpl storeIteratorMetrics;

    /** Debugging and unit test hook for Parallel Scan. */
    private ParallelScanHook parallelScanHook;

    /** LoginManager - may be null*/
    private volatile LoginManager loginMgr;

    /** Login/Logout locking handle */
    private final Object loginLock = new Object();

    /** Optional reauthentication handler */
    private final ReauthenticateHandler reauthHandler;

    /**
     * Holds lazily created AvroCatalog.  In addition to acting as a volatile
     * field (see getAvroCatalog), we use an AtomicReference so the catalog is
     * shared among handles when the internal copy constructor is used (see
     * KVStoreImpl(KVStoreImpl, boolean)).
     */
    private final AtomicReference<AvroCatalog> avroCatalogRef;

    /* The default logger used on the client side. */
    private final Logger logger;

    /**
     * The KVStoreInternalFactory constructor
     */
    public KVStoreImpl(Logger logger,
                       RequestDispatcher dispatcher,
                       KVStoreConfig config,
                       LoginManager loginMgr) {

        this(logger, dispatcher, config, loginMgr,
             (ReauthenticateHandler) null,
             false /* isDispatcherOwner */);
    }

    /**
     * The KVStoreFactory constructor
     */
     public KVStoreImpl(Logger logger,
                        RequestDispatcher dispatcher,
                       KVStoreConfig config,
                       LoginManager loginMgr,
                       ReauthenticateHandler reauthHandler) {

        this(logger, dispatcher, config, loginMgr, reauthHandler,
             true /* isDispatcherOwner */);
    }

    public KVStoreImpl(Logger logger,
                       RequestDispatcher dispatcher,
                       KVStoreConfig config,
                       LoginManager loginMgr,
                       ReauthenticateHandler reauthHandler,
                       boolean isDispatcherOwner) {
        this.logger = logger;
        this.dispatcher = dispatcher;
        this.isDispatcherOwner = isDispatcherOwner;
        this.loginMgr = loginMgr;
        this.reauthHandler = reauthHandler;
        this.defaultRequestTimeoutMs =
            (int) config.getRequestTimeout(TimeUnit.MILLISECONDS);
        this.readTimeoutMs =
            (int) config.getSocketReadTimeout(TimeUnit.MILLISECONDS);
        this.defaultConsistency = config.getConsistency();
        this.defaultDurability = config.getDurability();
        this.keySerializer = KeySerializer.PROHIBIT_INTERNAL_KEYSPACE;
        this.operationFactory =
            new Execute.OperationFactoryImpl(keySerializer);
        this.nPartitions = dispatcher.getTopologyManager().
                                      getTopology().
                                      getPartitionMap().
                                      getNPartitions();

        this.defaultLOBTimeout = config.getLOBTimeout(TimeUnit.MILLISECONDS);
        this.defaultLOBSuffix = config.getLOBSuffix();
        this.defaultLOBVerificationBytes = config.getLOBVerificationBytes();
        this.defaultChunksPerPartition = config.getLOBChunksPerPartition();
        this.defaultChunkSize = config.getLOBChunkSize();
        this.largeObjectImpl = new KVLargeObjectImpl();

        this.avroCatalogRef = new AtomicReference<AvroCatalog>(null);
        this.storeIteratorMetrics = new StoreIteratorMetricsImpl();

        /*
         * Only invoke this after all ivs have been initialized, since it
         * creates an internal handle.
         */
        largeObjectImpl.setKVSImpl(this);
    }

    public KVLargeObjectImpl getLargeObjectImpl() {
        return largeObjectImpl;
    }

    /**
     * Clones a handle for internal use, to provide access to the internal
     * keyspace (//) that is used for internal metadata.  This capability is
     * not exposed in published classes -- KVStoreConfig or KVStoreFactory --
     * in order to provide an extra safeguard against use of the internal
     * keyspace by user applications.  See KeySerializer for more information.
     * <p>
     * The new instance created by this method should never be explicitly
     * closed.  It will be discarded when the KVStoreImpl it is created from is
     * closed and discarded.
     * <p>
     * The new instance created by this method shares the KVStoreConfig
     * settings with the KVStoreImpl it is created from.  If specific values
     * are desired for consistency, durability or timeouts, these parameters
     * should be passed explicitly to the operation methods.
     */
    public static KVStore makeInternalHandle(KVStore other) {
        return new KVStoreImpl((KVStoreImpl) other,
                               true /*allowInternalKeyspace*/) {
            @Override
            public void close() {
                throw new UnsupportedOperationException();
            }
            @Override
            public void logout() {
                throw new UnsupportedOperationException();
            }
            @Override
            public void login(LoginCredentials creds) {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Returns the current LoginManager for a KVStore instance.
     */
    public static LoginManager getLoginManager(KVStore store) {
        final KVStoreImpl impl = (KVStoreImpl) store;
        synchronized (impl.loginLock) {
            return impl.loginMgr;
        }
    }

    /**
     * A predicate to determine whether this is an internal handle KVS handle.
     */
    private boolean isInternalHandle() {
        return keySerializer == KeySerializer.ALLOW_INTERNAL_KEYSPACE;
    }

    /**
     * Note that this copy constructor could be modified to allow overriding
     * KVStoreConfig settings, if multiple handles with difference settings are
     * needed in the future.
     */
    private KVStoreImpl(KVStoreImpl other, boolean allowInternalKeyspace) {
        this.logger = other.logger;
        this.loginMgr = getLoginManager(other);
        this.dispatcher = other.dispatcher;
        this.isDispatcherOwner = false;
        this.defaultRequestTimeoutMs = other.defaultRequestTimeoutMs;
        this.readTimeoutMs = other.readTimeoutMs;
        this.defaultConsistency = other.defaultConsistency;
        this.defaultDurability = other.defaultDurability;
        this.keySerializer = allowInternalKeyspace ?
            KeySerializer.ALLOW_INTERNAL_KEYSPACE :
            KeySerializer.PROHIBIT_INTERNAL_KEYSPACE;
        this.operationFactory =
            new Execute.OperationFactoryImpl(keySerializer);
        this.nPartitions = other.nPartitions;

        this.defaultLOBTimeout = other.defaultLOBTimeout;
        this.defaultLOBSuffix = other.defaultLOBSuffix;
        this.defaultLOBVerificationBytes = other.defaultLOBVerificationBytes;
        this.defaultChunksPerPartition = other.defaultChunksPerPartition;
        this.defaultChunkSize = other.defaultChunkSize;
        this.largeObjectImpl = other.largeObjectImpl;
        this.reauthHandler = other.reauthHandler;

        if (largeObjectImpl == null) {
            throw new IllegalStateException("null large object impl");
        }

        this.avroCatalogRef = other.avroCatalogRef;
        this.storeIteratorMetrics = new StoreIteratorMetricsImpl();
    }

    public Logger getLogger() {
        return logger;
    }

    public KeySerializer getKeySerializer() {
        return keySerializer;
    }

    public StoreIteratorMetricsImpl getStoreIteratorMetrics() {
        return storeIteratorMetrics;
    }

    public int getNPartitions() {
        return nPartitions;
    }

    public RequestDispatcher getDispatcher() {
        return dispatcher;
    }

    public void setParallelScanHook(ParallelScanHook parallelScanHook) {
        this.parallelScanHook = parallelScanHook;
    }

    public ParallelScanHook getParallelScanHook() {
        return parallelScanHook;
    }

    public int getDefaultRequestTimeoutMs() {
        return defaultRequestTimeoutMs;
    }

    public int getReadTimeoutMs() {
        return readTimeoutMs;
    }

    @Override
    public ValueVersion get(Key key)
        throws FaultException {

        return get(key, null, 0, null);
    }

    @Override
    public ValueVersion get(Key key,
                            Consistency consistency,
                            long timeout,
                            TimeUnit timeoutUnit)
        throws FaultException {

        return getInternal(key, 0, consistency, timeout, timeoutUnit);
    }

    public ValueVersion getInternal(Key key,
                                    long tableId,
                                    Consistency consistency,
                                    long timeout,
                                    TimeUnit timeoutUnit)
        throws FaultException {

        final byte[] keyBytes = keySerializer.toByteArray(key);
        final PartitionId partitionId = dispatcher.getPartitionId(keyBytes);
        final Get get = new Get(keyBytes, tableId);
        final Request req = makeReadRequest(get, partitionId, consistency,
                                            timeout, timeoutUnit);
        final Result result = executeRequest(req);
        final Value value = result.getPreviousValue();
        if (value == null) {
            assert !result.getSuccess();
            return null;
        }
        assert result.getSuccess();
        final ValueVersion ret = new ValueVersion();
        ret.setValue(value);
        ret.setVersion(result.getPreviousVersion());
        return ret;
    }

    @Override
    public SortedMap<Key, ValueVersion> multiGet(Key parentKey,
                                                 KeyRange subRange,
                                                 Depth depth)
        throws FaultException {

        return multiGet(parentKey, subRange, depth, null, 0, null);
    }

    @Override
    public SortedMap<Key, ValueVersion> multiGet(Key parentKey,
                                                 KeyRange subRange,
                                                 Depth depth,
                                                 Consistency consistency,
                                                 long timeout,
                                                 TimeUnit timeoutUnit)
        throws FaultException {

        if (depth == null) {
            depth = Depth.PARENT_AND_DESCENDANTS;
        }

        /* Execute request. */
        final byte[] parentKeyBytes = keySerializer.toByteArray(parentKey);
        final PartitionId partitionId =
            dispatcher.getPartitionId(parentKeyBytes);
        final MultiGet get = new MultiGet(parentKeyBytes, subRange, depth);
        final Request req = makeReadRequest(get, partitionId, consistency,
                                            timeout, timeoutUnit);
        final Result result = executeRequest(req);

        /* Convert byte[] keys to Key objects. */
        final List<ResultKeyValueVersion> byteKeyResults =
            result.getKeyValueVersionList();
        final SortedMap<Key, ValueVersion> stringKeyResults =
            new TreeMap<Key, ValueVersion>();
        for (ResultKeyValueVersion entry : byteKeyResults) {
            stringKeyResults.put
                (keySerializer.fromByteArray(entry.getKeyBytes()),
                 new ValueVersion(entry.getValue(), entry.getVersion()));
        }
        assert result.getSuccess() == (!stringKeyResults.isEmpty());
        return stringKeyResults;
    }

    @Override
    public SortedSet<Key> multiGetKeys(Key parentKey,
                                       KeyRange subRange,
                                       Depth depth)
        throws FaultException {

        return multiGetKeys(parentKey, subRange, depth, null, 0, null);
    }

    @Override
    public SortedSet<Key> multiGetKeys(Key parentKey,
                                       KeyRange subRange,
                                       Depth depth,
                                       Consistency consistency,
                                       long timeout,
                                       TimeUnit timeoutUnit)
        throws FaultException {

        if (depth == null) {
            depth = Depth.PARENT_AND_DESCENDANTS;
        }

        /* Execute request. */
        final byte[] parentKeyBytes = keySerializer.toByteArray(parentKey);
        final PartitionId partitionId =
            dispatcher.getPartitionId(parentKeyBytes);
        final MultiGetKeys get =
            new MultiGetKeys(parentKeyBytes, subRange, depth);
        final Request req = makeReadRequest(get, partitionId, consistency,
                                            timeout, timeoutUnit);
        final Result result = executeRequest(req);

        /* Convert byte[] keys to Key objects. */
        final List<byte[]> byteKeyResults = result.getKeyList();
        final SortedSet<Key> stringKeySet = new TreeSet<Key>();
        for (byte[] entry : byteKeyResults) {
            stringKeySet.add(keySerializer.fromByteArray(entry));
        }
        assert result.getSuccess() == (!stringKeySet.isEmpty());
        return stringKeySet;
    }

    @Override
    public Iterator<KeyValueVersion> multiGetIterator(Direction direction,
                                                      int batchSize,
                                                      Key parentKey,
                                                      KeyRange subRange,
                                                      Depth depth)
        throws FaultException {

        return multiGetIterator(direction, batchSize, parentKey, subRange,
                                depth, null, 0, null);
    }

    @Override
    public Iterator<KeyValueVersion>
        multiGetIterator(final Direction direction,
                         final int batchSize,
                         final Key parentKey,
                         final KeyRange subRange,
                         final Depth depth,
                         final Consistency consistency,
                         final long timeout,
                         final TimeUnit timeoutUnit)
        throws FaultException {

        if (direction != Direction.FORWARD &&
            direction != Direction.REVERSE) {
            throw new IllegalArgumentException
                ("Only Direction.FORWARD and REVERSE are supported, got: " +
                 direction);
        }

        final Depth useDepth =
            (depth != null) ? depth : Depth.PARENT_AND_DESCENDANTS;

        final int useBatchSize =
            (batchSize > 0) ? batchSize : DEFAULT_ITERATOR_BATCH_SIZE;

        final byte[] parentKeyBytes = keySerializer.toByteArray(parentKey);

        final PartitionId partitionId =
            dispatcher.getPartitionId(parentKeyBytes);

        return new ArrayIterator<KeyValueVersion>() {
            private boolean moreElements = true;
            private byte[] resumeKey = null;

            @Override
            KeyValueVersion[] getMoreElements() {

                /* Avoid round trip if we know there are no more elements. */
                if (!moreElements) {
                    return null;
                }

                /* Execute request. */
                final MultiGetIterate get = new MultiGetIterate
                    (parentKeyBytes, subRange, useDepth, direction,
                     useBatchSize, resumeKey);
                final Request req = makeReadRequest
                    (get, partitionId, consistency, timeout, timeoutUnit);
                final Result result = executeRequest(req);

                /* Get results and save resume key. */
                moreElements = result.hasMoreElements();
                final List<ResultKeyValueVersion> byteKeyResults =
                    result.getKeyValueVersionList();
                if (byteKeyResults.size() == 0) {
                    assert (!moreElements);
                    return null;
                }
                resumeKey = byteKeyResults.get
                    (byteKeyResults.size() - 1).getKeyBytes();

                /* Convert byte[] keys to Key objects. */
                final KeyValueVersion[] stringKeyResults =
                    new KeyValueVersion[byteKeyResults.size()];
                for (int i = 0; i < stringKeyResults.length; i += 1) {
                    final ResultKeyValueVersion entry = byteKeyResults.get(i);
                    stringKeyResults[i] = new KeyValueVersion
                        (keySerializer.fromByteArray(entry.getKeyBytes()),
                         entry.getValue(), entry.getVersion());
                }
                return stringKeyResults;
            }
        };
    }

    @Override
    public Iterator<Key> multiGetKeysIterator(Direction direction,
                                              int batchSize,
                                              Key parentKey,
                                              KeyRange subRange,
                                              Depth depth)
        throws FaultException {

        return multiGetKeysIterator(direction, batchSize, parentKey, subRange,
                                    depth, null, 0, null);
    }

    @Override
    public Iterator<Key> multiGetKeysIterator(final Direction direction,
                                              final int batchSize,
                                              final Key parentKey,
                                              final KeyRange subRange,
                                              final Depth depth,
                                              final Consistency consistency,
                                              final long timeout,
                                              final TimeUnit timeoutUnit)
        throws FaultException {

        if (direction != Direction.FORWARD &&
            direction != Direction.REVERSE) {
            throw new IllegalArgumentException
                ("Only Direction.FORWARD and REVERSE are supported, got: " +
                 direction);
        }

        final Depth useDepth =
            (depth != null) ? depth : Depth.PARENT_AND_DESCENDANTS;

        final int useBatchSize =
            (batchSize > 0) ? batchSize : DEFAULT_ITERATOR_BATCH_SIZE;

        final byte[] parentKeyBytes = keySerializer.toByteArray(parentKey);

        final PartitionId partitionId =
            dispatcher.getPartitionId(parentKeyBytes);

        return new ArrayIterator<Key>() {
            private boolean moreElements = true;
            private byte[] resumeKey = null;

            @Override
            Key[] getMoreElements() {

                /* Avoid round trip if we know there are no more elements. */
                if (!moreElements) {
                    return null;
                }

                /* Execute request. */
                final MultiGetKeysIterate get = new MultiGetKeysIterate
                    (parentKeyBytes, subRange, useDepth, direction,
                     useBatchSize, resumeKey);
                final Request req = makeReadRequest
                    (get, partitionId, consistency, timeout, timeoutUnit);
                final Result result = executeRequest(req);

                /* Get results and save resume key. */
                moreElements = result.hasMoreElements();
                final List<byte[]> byteKeyResults = result.getKeyList();
                if (byteKeyResults.size() == 0) {
                    assert (!moreElements);
                    return null;
                }
                resumeKey = byteKeyResults.get(byteKeyResults.size() - 1);

                /* Convert byte[] keys to Key objects. */
                final Key[] stringKeyResults = new Key[byteKeyResults.size()];
                for (int i = 0; i < stringKeyResults.length; i += 1) {
                    final byte[] entry = byteKeyResults.get(i);
                    stringKeyResults[i] = keySerializer.fromByteArray(entry);
                }
                return stringKeyResults;
            }
        };
    }

    @Override
    public Iterator<KeyValueVersion> storeIterator(Direction direction,
                                                   int batchSize)
        throws FaultException {

        return storeIterator(direction, batchSize, null, null, null,
                             null, 0, null);
    }

    @Override
    public Iterator<KeyValueVersion> storeIterator(Direction direction,
                                                   int batchSize,
                                                   Key parentKey,
                                                   KeyRange subRange,
                                                   Depth depth)
        throws FaultException {

        return storeIterator(direction, batchSize, parentKey, subRange, depth,
                             null, 0, null);
    }

    @Override
    public Iterator<KeyValueVersion>
        storeIterator(final Direction direction,
                      final int batchSize,
                      final Key parentKey,
                      final KeyRange subRange,
                      final Depth depth,
                      final Consistency consistency,
                      final long timeout,
                      final TimeUnit timeoutUnit)
        throws FaultException {

        return storeIterator(direction, batchSize, 1, nPartitions, parentKey,
                             subRange, depth, consistency, timeout,
                             timeoutUnit);
    }

    @Override
    public ParallelScanIterator<KeyValueVersion>
        storeIterator(final Direction direction,
                      final int batchSize,
                      final Key parentKey,
                      final KeyRange subRange,
                      final Depth depth,
                      final Consistency consistency,
                      final long timeout,
                      final TimeUnit timeoutUnit,
                      final StoreIteratorConfig storeIteratorConfig)
        throws FaultException {

        if (storeIteratorConfig == null) {
            throw new IllegalArgumentException
                ("The StoreIteratorConfig argument must be supplied.");
        }

        return ParallelScan.
            createParallelScan(this, direction, batchSize, parentKey, subRange,
                               depth, consistency, timeout, timeoutUnit,
                               storeIteratorConfig);
    }

    /**
     * Internal use only.  Iterates using the same rules as storeIterator,
     * but over the single, given partition.
     *
     * @param direction unlike storeIterator, the direction must currently be
     * {@link Direction#FORWARD}, and keys are always returned in forward order
     * for the given partition.  In the future we may support a faster,
     * unordered iteration.
     */
    public Iterator<KeyValueVersion>
        partitionIterator(final Direction direction,
                          final int batchSize,
                          final int partition,
                          final Key parentKey,
                          final KeyRange subRange,
                          final Depth depth,
                          final Consistency consistency,
                          final long timeout,
                          final TimeUnit timeoutUnit)
        throws FaultException {

        if (direction != Direction.FORWARD) {
            throw new IllegalArgumentException
                ("Only Direction.FORWARD is currently supported, got: " +
                 direction);
        }

        return storeIterator(Direction.UNORDERED, batchSize, partition,
                             partition, parentKey, subRange, depth,
                             consistency, timeout, timeoutUnit);
    }

    private Iterator<KeyValueVersion>
        storeIterator(final Direction direction,
                      final int batchSize,
                      final int firstPartition,
                      final int lastPartition,
                      final Key parentKey,
                      final KeyRange subRange,
                      final Depth depth,
                      final Consistency consistency,
                      final long timeout,
                      final TimeUnit timeoutUnit)
        throws FaultException {

        if (direction != Direction.UNORDERED) {
            throw new IllegalArgumentException
                ("Only Direction.UNORDERED is currently supported, got: " +
                 direction);
        }

        if ((parentKey != null) && (parentKey.getMinorPath().size()) > 0) {
            throw new IllegalArgumentException
                ("Minor path of parentKey must be empty");
        }

        final Depth useDepth =
            (depth != null) ? depth : Depth.PARENT_AND_DESCENDANTS;

        final int useBatchSize =
            (batchSize > 0) ? batchSize : DEFAULT_ITERATOR_BATCH_SIZE;

        final byte[] parentKeyBytes =
            (parentKey != null) ? keySerializer.toByteArray(parentKey) : null;

        /* Prohibit iteration of internal keyspace (//). */
        final KeyRange useRange =
            keySerializer.restrictRange(parentKey, subRange);

        return new ArrayIterator<KeyValueVersion>() {
            private boolean moreElements = true;
            private byte[] resumeKey = null;
            private PartitionId partitionId = new PartitionId(firstPartition);

            @Override
            KeyValueVersion[] getMoreElements() {

                while (true) {
                    /* If no more in one partition, move to the next. */
                    if ((!moreElements) &&
                        (partitionId.getPartitionId() < lastPartition)) {
                        partitionId =
                            new PartitionId(partitionId.getPartitionId() + 1);
                        moreElements = true;
                        resumeKey = null;
                    }

                    /* Avoid round trip when there are no more elements. */
                    if (!moreElements) {
                        return null;
                    }

                    /* Execute request. */
                    final StoreIterate get = new StoreIterate
                        (parentKeyBytes, useRange, useDepth, Direction.FORWARD,
                         useBatchSize, resumeKey);
                    final Request req = makeReadRequest
                        (get, partitionId, consistency, timeout, timeoutUnit);
                    final Result result = executeRequest(req);

                    /* Get results and save resume key. */
                    moreElements = result.hasMoreElements();
                    final List<ResultKeyValueVersion> byteKeyResults =
                        result.getKeyValueVersionList();
                    if (byteKeyResults.size() == 0) {
                        assert (!moreElements);
                        continue;
                    }
                    resumeKey = byteKeyResults.get
                        (byteKeyResults.size() - 1).getKeyBytes();

                    /* Convert byte[] keys to Key objects. */
                    final KeyValueVersion[] stringKeyResults =
                        new KeyValueVersion[byteKeyResults.size()];
                    for (int i = 0; i < stringKeyResults.length; i += 1) {
                        final ResultKeyValueVersion entry =
                            byteKeyResults.get(i);
                        stringKeyResults[i] = new KeyValueVersion
                            (keySerializer.fromByteArray(entry.getKeyBytes()),
                             entry.getValue(), entry.getVersion());
                    }
                    return stringKeyResults;
                }
            }
        };
    }

    @Override
    public Iterator<Key> storeKeysIterator(Direction direction,
                                           int batchSize)
        throws FaultException {

        return storeKeysIterator(direction, batchSize, null, null, null,
                                 null, 0, null);
    }

    @Override
    public Iterator<Key> storeKeysIterator(Direction direction,
                                           int batchSize,
                                           Key parentKey,
                                           KeyRange subRange,
                                           Depth depth)
        throws FaultException {

        return storeKeysIterator(direction, batchSize, parentKey, subRange,
                                 depth, null, 0, null);
    }

    @Override
    public Iterator<Key> storeKeysIterator(final Direction direction,
                                           final int batchSize,
                                           final Key parentKey,
                                           final KeyRange subRange,
                                           final Depth depth,
                                           final Consistency consistency,
                                           final long timeout,
                                           final TimeUnit timeoutUnit)
        throws FaultException {

        if (direction != Direction.UNORDERED) {
            throw new IllegalArgumentException
                ("Only Direction.UNORDERED is currently supported, got: " +
                 direction);
        }

        if ((parentKey != null) && (parentKey.getMinorPath().size()) > 0) {
            throw new IllegalArgumentException
                ("Minor path of parentKey must be empty");
        }

        final Depth useDepth =
            (depth != null) ? depth : Depth.PARENT_AND_DESCENDANTS;

        final int useBatchSize =
            (batchSize > 0) ? batchSize : DEFAULT_ITERATOR_BATCH_SIZE;

        final byte[] parentKeyBytes =
            (parentKey != null) ? keySerializer.toByteArray(parentKey) : null;

        /* Prohibit iteration of internal keyspace (//). */
        final KeyRange useRange =
            keySerializer.restrictRange(parentKey, subRange);

        return new ArrayIterator<Key>() {
            private boolean moreElements = true;
            private byte[] resumeKey = null;
            private PartitionId partitionId = new PartitionId(1);

            @Override
            Key[] getMoreElements() {

                while (true) {
                    /* If no more in one partition, move to the next. */
                    if ((!moreElements) &&
                        (partitionId.getPartitionId() < nPartitions)) {
                        partitionId =
                            new PartitionId(partitionId.getPartitionId() + 1);
                        moreElements = true;
                        resumeKey = null;
                    }

                    /* Avoid round trip when there are no more elements. */
                    if (!moreElements) {
                        return null;
                    }

                    /* Execute request. */
                    final StoreKeysIterate get = new StoreKeysIterate
                        (parentKeyBytes, useRange, useDepth, Direction.FORWARD,
                         useBatchSize, resumeKey);
                    final Request req = makeReadRequest
                        (get, partitionId, consistency, timeout, timeoutUnit);
                    final Result result = executeRequest(req);

                    /* Get results and save resume key. */
                    moreElements = result.hasMoreElements();
                    final List<byte[]> byteKeyResults = result.getKeyList();
                    if (byteKeyResults.size() == 0) {
                        assert (!moreElements);
                        continue;
                    }
                    resumeKey = byteKeyResults.get(byteKeyResults.size() - 1);

                    /* Convert byte[] keys to Key objects. */
                    final Key[] stringKeyResults =
                        new Key[byteKeyResults.size()];
                    for (int i = 0; i < stringKeyResults.length; i += 1) {
                        final byte[] entry = byteKeyResults.get(i);
                        stringKeyResults[i] =
                            keySerializer.fromByteArray(entry);
                    }
                    return stringKeyResults;
                }
            }
        };
    }

    @Override
    public ParallelScanIterator<Key>
        storeKeysIterator(final Direction direction,
                          final int batchSize,
                          final Key parentKey,
                          final KeyRange subRange,
                          final Depth depth,
                          final Consistency consistency,
                          final long timeout,
                          final TimeUnit timeoutUnit,
                          final StoreIteratorConfig storeIteratorConfig)
        throws FaultException {

        if (storeIteratorConfig == null) {
            throw new IllegalArgumentException
                ("The StoreIteratorConfig argument must be supplied.");
        }

        return ParallelScan.
            createParallelKeyScan(this, direction, batchSize,
                                  parentKey, subRange,
                                  depth, consistency, timeout, timeoutUnit,
                                  storeIteratorConfig);
    }

    private abstract class ArrayIterator<E> implements Iterator<E> {

        private E[] elements = null;
        private int nextElement = 0;

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasNext() {
            if (elements != null && nextElement < elements.length) {
                return true;
            }
            elements = getMoreElements();
            if (elements == null) {
                return false;
            }
            assert (elements.length > 0);
            nextElement = 0;
            return true;
        }

        @Override
        public E next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return elements[nextElement++];
        }

        /**
         * Returns more elements or null if there are none.  May not return a
         * zero length array.
         */
        abstract E[] getMoreElements();
    }

    @Override
    public Version put(Key key,
                       Value value)
        throws FaultException {

        return put(key, value, null, null, 0, null);
    }

    @Override
    public Version put(Key key,
                       Value value,
                       ReturnValueVersion prevValue,
                       Durability durability,
                       long timeout,
                       TimeUnit timeoutUnit)
        throws FaultException {

        return putInternal(key, value, prevValue, 0,
                           durability, timeout, timeoutUnit);
    }

    public Version putInternal(Key key,
                               Value value,
                               ReturnValueVersion prevValue,
                               long tableId,
                               Durability durability,
                               long timeout,
                               TimeUnit timeoutUnit)
        throws FaultException {

        final byte[] keyBytes = keySerializer.toByteArray(key);
        final PartitionId partitionId = dispatcher.getPartitionId(keyBytes);
        final ReturnValueVersion.Choice prevValChoice = (prevValue != null) ?
            prevValue.getReturnChoice() :
            ReturnValueVersion.Choice.NONE;
        final Put put = new Put(keyBytes, value, prevValChoice, tableId);
        final Request req = makeWriteRequest(put, partitionId, durability,
                                             timeout, timeoutUnit);
        final Result result = executeRequest(req);
        if (prevValue != null) {
            prevValue.setValue(result.getPreviousValue());
            prevValue.setVersion(result.getPreviousVersion());
        }
        assert result.getSuccess() == (result.getNewVersion() != null);
        return result.getNewVersion();
    }

    @Override
    public Version putIfAbsent(Key key,
                               Value value)
        throws FaultException {

        return putIfAbsent(key, value, null, null, 0, null);
    }

    @Override
    public Version putIfAbsent(Key key,
                               Value value,
                               ReturnValueVersion prevValue,
                               Durability durability,
                               long timeout,
                               TimeUnit timeoutUnit)
        throws FaultException {

        return putIfAbsentInternal(key, value, prevValue, 0,
                                   durability, timeout, timeoutUnit);
    }

    public Version putIfAbsentInternal(Key key,
                                       Value value,
                                       ReturnValueVersion prevValue,
                                       long tableId,
                                       Durability durability,
                                       long timeout,
                                       TimeUnit timeoutUnit)
        throws FaultException {

        final byte[] keyBytes = keySerializer.toByteArray(key);
        final PartitionId partitionId = dispatcher.getPartitionId(keyBytes);
        final ReturnValueVersion.Choice prevValChoice = (prevValue != null) ?
            prevValue.getReturnChoice() :
            ReturnValueVersion.Choice.NONE;
        final Put put =
            new PutIfAbsent(keyBytes, value, prevValChoice, tableId);
        final Request req = makeWriteRequest(put, partitionId, durability,
                                             timeout, timeoutUnit);
        final Result result = executeRequest(req);
        if (prevValue != null) {
            prevValue.setValue(result.getPreviousValue());
            prevValue.setVersion(result.getPreviousVersion());
        }
        assert result.getSuccess() == (result.getNewVersion() != null);
        return result.getNewVersion();
    }

    @Override
    public Version putIfPresent(Key key,
                                Value value)
        throws FaultException {

        return putIfPresent(key, value, null, null, 0, null);
    }

    @Override
    public Version putIfPresent(Key key,
                                Value value,
                                ReturnValueVersion prevValue,
                                Durability durability,
                                long timeout,
                                TimeUnit timeoutUnit)
        throws FaultException {

        return putIfPresentInternal(key, value, prevValue, 0,
                                    durability, timeout, timeoutUnit);
    }

    public Version putIfPresentInternal(Key key,
                                        Value value,
                                        ReturnValueVersion prevValue,
                                        long tableId,
                                        Durability durability,
                                        long timeout,
                                        TimeUnit timeoutUnit)
        throws FaultException {

        final byte[] keyBytes = keySerializer.toByteArray(key);
        final PartitionId partitionId = dispatcher.getPartitionId(keyBytes);
        final ReturnValueVersion.Choice prevValChoice = (prevValue != null) ?
            prevValue.getReturnChoice() :
            ReturnValueVersion.Choice.NONE;
        final Put put =
            new PutIfPresent(keyBytes, value, prevValChoice, tableId);
        final Request req = makeWriteRequest(put, partitionId, durability,
                                             timeout, timeoutUnit);
        final Result result = executeRequest(req);
        if (prevValue != null) {
            prevValue.setValue(result.getPreviousValue());
            prevValue.setVersion(result.getPreviousVersion());
        }
        assert result.getSuccess() == (result.getNewVersion() != null);
        return result.getNewVersion();
    }

    @Override
    public Version putIfVersion(Key key,
                                Value value,
                                Version matchVersion)
        throws FaultException {

        return putIfVersion(key, value, matchVersion, null, null, 0, null);
    }

    @Override
    public Version putIfVersion(Key key,
                                Value value,
                                Version matchVersion,
                                ReturnValueVersion prevValue,
                                Durability durability,
                                long timeout,
                                TimeUnit timeoutUnit)
        throws FaultException {

        return putIfVersionInternal(key, value, matchVersion, prevValue, 0,
                                    durability, timeout, timeoutUnit);
    }

    public Version putIfVersionInternal(Key key,
                                        Value value,
                                        Version matchVersion,
                                        ReturnValueVersion prevValue,
                                        long tableId,
                                        Durability durability,
                                        long timeout,
                                        TimeUnit timeoutUnit)
        throws FaultException {

        final byte[] keyBytes = keySerializer.toByteArray(key);
        final PartitionId partitionId = dispatcher.getPartitionId(keyBytes);
        final ReturnValueVersion.Choice prevValChoice = (prevValue != null) ?
            prevValue.getReturnChoice() :
            ReturnValueVersion.Choice.NONE;
        final Put put = new PutIfVersion(keyBytes, value, prevValChoice,
                                         matchVersion, tableId);
        final Request req = makeWriteRequest(put, partitionId, durability,
                                             timeout, timeoutUnit);
        final Result result = executeRequest(req);
        if (prevValue != null) {
            prevValue.setValue(result.getPreviousValue());
            prevValue.setVersion(result.getPreviousVersion());
        }
        assert result.getSuccess() == (result.getNewVersion() != null);
        return result.getNewVersion();
    }

    @Override
    public boolean delete(Key key)
        throws FaultException {

        return delete(key, null, null, 0, null);
    }

    @Override
    public boolean delete(Key key,
                          ReturnValueVersion prevValue,
                          Durability durability,
                          long timeout,
                          TimeUnit timeoutUnit)
        throws FaultException {

        return deleteInternal(key, prevValue, durability,
                              timeout, timeoutUnit, 0);
    }

    public boolean deleteInternal(Key key,
                                  ReturnValueVersion prevValue,
                                  Durability durability,
                                  long timeout,
                                  TimeUnit timeoutUnit,
                                  long tableId)
        throws FaultException {

        final byte[] keyBytes = keySerializer.toByteArray(key);
        final PartitionId partitionId = dispatcher.getPartitionId(keyBytes);
        final ReturnValueVersion.Choice prevValChoice = (prevValue != null) ?
            prevValue.getReturnChoice() :
            ReturnValueVersion.Choice.NONE;
        final Delete del = new Delete(keyBytes, prevValChoice, tableId);
        final Request req = makeWriteRequest(del, partitionId, durability,
                                             timeout, timeoutUnit);
        final Result result = executeRequest(req);
        if (prevValue != null) {
            prevValue.setValue(result.getPreviousValue());
            prevValue.setVersion(result.getPreviousVersion());
        }
        return result.getSuccess();
    }

    @Override
    public boolean deleteIfVersion(Key key,
                                   Version matchVersion)
        throws FaultException {

        return deleteIfVersion(key, matchVersion, null, null, 0, null);
    }

    @Override
    public boolean deleteIfVersion(Key key,
                                   Version matchVersion,
                                   ReturnValueVersion prevValue,
                                   Durability durability,
                                   long timeout,
                                   TimeUnit timeoutUnit)
        throws FaultException {

        return deleteIfVersionInternal(key, matchVersion, prevValue,
                                       durability, timeout, timeoutUnit, 0);
    }

    public boolean deleteIfVersionInternal(Key key,
                                           Version matchVersion,
                                           ReturnValueVersion prevValue,
                                           Durability durability,
                                           long timeout,
                                           TimeUnit timeoutUnit,
                                           long tableId)
        throws FaultException {

        final byte[] keyBytes = keySerializer.toByteArray(key);
        final PartitionId partitionId = dispatcher.getPartitionId(keyBytes);
        final ReturnValueVersion.Choice prevValChoice = (prevValue != null) ?
            prevValue.getReturnChoice() :
            ReturnValueVersion.Choice.NONE;
        final Delete del = new DeleteIfVersion(keyBytes, prevValChoice,
                                               matchVersion, tableId);
        final Request req = makeWriteRequest(del, partitionId, durability,
                                             timeout, timeoutUnit);
        final Result result = executeRequest(req);
        if (prevValue != null) {
            prevValue.setValue(result.getPreviousValue());
            prevValue.setVersion(result.getPreviousVersion());
        }
        return result.getSuccess();
    }

    @Override
    public int multiDelete(Key parentKey,
                           KeyRange subRange,
                           Depth depth)
        throws FaultException {

        return multiDelete(parentKey, subRange, depth, null, 0, null);
    }

    @Override
    public int multiDelete(Key parentKey,
                           KeyRange subRange,
                           Depth depth,
                           Durability durability,
                           long timeout,
                           TimeUnit timeoutUnit)
        throws FaultException {

        if (depth == null) {
            depth = Depth.PARENT_AND_DESCENDANTS;
        }

        final byte[] parentKeyBytes = keySerializer.toByteArray(parentKey);
        final PartitionId partitionId =
            dispatcher.getPartitionId(parentKeyBytes);
        final MultiDelete del =
            new MultiDelete(parentKeyBytes, subRange,
                            depth,
                            largeObjectImpl.getLOBSuffixBytes());
        final Request req = makeWriteRequest(del, partitionId, durability,
                                             timeout, timeoutUnit);
        final Result result = executeRequest(req);
        return result.getNDeletions();
    }

    @Override
    public List<OperationResult> execute(List<Operation> operations)
        throws OperationExecutionException,
               FaultException {

        return execute(operations, null, 0, null);
    }

    @Override
    public List<OperationResult> execute(List<Operation> operations,
                                         Durability durability,
                                         long timeout,
                                         TimeUnit timeoutUnit)
        throws OperationExecutionException,
               FaultException {

        /* Validate operations. */
        final List<OperationImpl> ops = OperationImpl.downcast(operations);
        if (ops == null || ops.size() == 0) {
            throw new IllegalArgumentException
                ("operations must be non-null and non-empty");
        }
        final OperationImpl firstOp = ops.get(0);
        final List<String> firstMajorPath = firstOp.getKey().getMajorPath();
        final Set<Key> keySet = new HashSet<Key>();
        keySet.add(firstOp.getKey());
        checkLOBKeySuffix(firstOp.getInternalOp());
        for (int i = 1; i < ops.size(); i += 1) {
            final OperationImpl op = ops.get(i);
            final Key opKey = op.getKey();
            if (!opKey.getMajorPath().equals(firstMajorPath)) {
                throw new IllegalArgumentException
                    ("Two operations have different major paths, first: " +
                     firstOp.getKey() + " other: " + opKey);
            }
            if (keySet.add(opKey) == false) {
                throw new IllegalArgumentException
                    ("More than one operation has the same Key: " + opKey);
            }
            checkLOBKeySuffix(op.getInternalOp());
        }

        /* Execute the execute. */
        final PartitionId partitionId =
            dispatcher.getPartitionId(firstOp.getInternalOp().getKeyBytes());
        final Execute exe = new Execute(ops);
        final Request req = makeWriteRequest(exe, partitionId, durability,
                                             timeout, timeoutUnit);
        final Result result = executeRequest(req);
        final OperationExecutionException exception =
            result.getExecuteException(operations);
        if (exception != null) {
            throw exception;
        }
        return result.getExecuteResult();
    }

    @Override
    public OperationFactory getOperationFactory() {
        return operationFactory;
    }

    @Override
    public void close() {
        synchronized(loginLock) {
            if (loginMgr != null) {
                logout();
            }
        }
        dispatcher.shutdown(null);
    }

    public Request makeWriteRequest(InternalOperation op,
                                    PartitionId partitionId,
                                    Durability durability,
                                    long timeout,
                                    TimeUnit timeoutUnit) {
        checkLOBKeySuffix(op);

        return makeRequest
            (op, partitionId, null /*repGroupId*/, true /*write*/,
             ((durability != null) ? durability : defaultDurability),
             null /*consistency*/, timeout, timeoutUnit);
    }

    /**
     * Perform any LOB key specific checks when using non-internal handles.
     *
     * @param op the operation whose key is to be checked
     */
    private void checkLOBKeySuffix(InternalOperation op) {

        if (isInternalHandle()) {
            return;
        }

        final byte[] keyBytes =
            op.checkLOBSuffix(largeObjectImpl.getLOBSuffixBytes());

        if (keyBytes == null) {
            return;
        }

        final String msg =
            "Operation: " + op.getOpCode() +
            " Illegal LOB key argument: " +
             Key.fromByteArray(keyBytes) +
            ". Use LOB-specific APIs to modify a LOB key/value pair.";

        throw new IllegalArgumentException(msg);
    }

    public Request makeReadRequest(InternalOperation op,
                                   PartitionId partitionId,
                                   Consistency consistency,
                                   long timeout,
                                   TimeUnit timeoutUnit) {
        return makeRequest
            (op, partitionId, null /*repGroupId*/,
             false /*write*/, null /*durability*/,
             ((consistency != null) ? consistency : defaultConsistency),
             timeout, timeoutUnit);
    }

    public Request makeReadRequest(InternalOperation op,
                                   RepGroupId repGroupId,
                                   Consistency consistency,
                                   long timeout,
                                   TimeUnit timeoutUnit) {
        return makeRequest
            (op, null /*partitionId*/, repGroupId,
             false /*write*/, null /*durability*/,
             ((consistency != null) ? consistency : defaultConsistency),
             timeout, timeoutUnit);
    }

    private Request makeRequest(InternalOperation op,
                                PartitionId partitionId,
                                RepGroupId repGroupId,
                                boolean write,
                                Durability durability,
                                Consistency consistency,
                                long timeout,
                                TimeUnit timeoutUnit) {

        int requestTimeoutMs = defaultRequestTimeoutMs;
        if (timeout > 0) {
            requestTimeoutMs = PropUtil.durationToMillis(timeout, timeoutUnit);
            if (requestTimeoutMs > readTimeoutMs) {
                String format = "Request timeout parameter: %,d ms exceeds " +
                                "socket read timeout: %,d ms";
                throw new IllegalArgumentException
                    (String.format(format, requestTimeoutMs, readTimeoutMs));
            }
        }

        return (partitionId != null) ?
          new Request
            (op, partitionId, write, durability, consistency, DEFAULT_TTL,
             dispatcher.getTopologyManager().getTopology().getSequenceNumber(),
             dispatcher.getDispatcherId(), requestTimeoutMs,
             !write ? dispatcher.getReadZoneIds() : null) :
          new Request
            (op, repGroupId, write, durability, consistency, DEFAULT_TTL,
             dispatcher.getTopologyManager().getTopology().getSequenceNumber(),
             dispatcher.getDispatcherId(), requestTimeoutMs,
             !write ? dispatcher.getReadZoneIds() : null);
    }

    /**
     * Invokes a request through the request handler
     *
     * @param request the request to run
     * @return the result of the request
     */
    public Result executeRequest(Request request)
        throws FaultException {

        final LoginManager requestLoginMgr = this.loginMgr;
        try {
            return dispatcher.execute(request, loginMgr).getResult();
        } catch (AuthenticationRequiredException are) {
            if (!tryReauthenticate(requestLoginMgr)){
                throw are;
            }

            /*
             * If the authentication completed, we assume we are ready to
             * retry the operation.  No retry on the authentication here.
             */
            return dispatcher.execute(request, loginMgr).getResult();
        }
    }

    /* (non-Javadoc)
     * @see oracle.kv.KVStore#getStats(com.sleepycat.je.StatsConfig)
     */
    @Override
    public KVStats getStats(boolean clear) {
        return new KVStats(clear, dispatcher, storeIteratorMetrics);
    }

    @Override
    public AvroCatalog getAvroCatalog() {

        /* First check for an existing catalog without any synchronization. */
        AvroCatalog catalog = avroCatalogRef.get();
        if (catalog != null) {
            return catalog;
        }

        /*
         * Catalog creation is fairly expensive because it queries all schemas
         * from the store. We use synchronization (rather than compareAndSet)
         * to avoid the potential cost of creating two or more catalogs if
         * multiple threads initially call this method concurrently.
         *
         * Note that if there are multiple handles (created via the copy
         * constructor) they will share the same AtomicReference instance. So
         * multiple threads will always synchronize on the same instance.
         */
        synchronized (avroCatalogRef) {

            /*
             * Return catalog if another thread created it while we waited to
             * get the mutex.  The double-check is safe because the
             * AtomicReference is effectively volatile.
             */
            catalog = avroCatalogRef.get();
            if (catalog != null) {
                return catalog;
            }

            /*
             * Create the catalog and update the AtomicReference while
             * synchronized.
             */
            catalog = new AvroCatalogImpl(this);
            avroCatalogRef.set(catalog);
            return catalog;
        }
    }

    @Override
    public void login(LoginCredentials creds)
        throws RequestTimeoutException, AuthenticationFailureException,
               FaultException {

        if (creds == null) {
            throw new IllegalArgumentException("No credentials provided");
        }

        final LoginManager priorLoginMgr;
        synchronized (loginLock) {
            /*
             * If there is an existing login, the new creds must be for the
             * same username.
             */
            if (loginMgr != null) {
                if ((loginMgr.getUsername() == null &&
                     creds.getUsername() != null) ||
                    (loginMgr.getUsername() != null &&
                     !loginMgr.getUsername().equals(creds.getUsername()))) {
                    throw new AuthenticationFailureException(
                        "Logout required prior to logging in with new " +
                        "user identity.");
                }
            }

            final RepNodeLoginManager rnlm =
                new RepNodeLoginManager(creds.getUsername(), true);
            rnlm.setTopology(dispatcher.getTopologyManager());
            rnlm.login(creds);

            /* login succeeded - establish new login */
            priorLoginMgr = loginMgr;
            if (isDispatcherOwner) {
                dispatcher.setRegUtilsLoginManager(rnlm);
            }
            this.loginMgr = rnlm;
        }

        if (priorLoginMgr != null) {
            Exception logException = null;
            try {
                priorLoginMgr.logout();
            } catch (SessionAccessException re) {
                /* ok */
                logException = re;
            } catch (AuthenticationRequiredException are) {
                /* ok */
                logException = are;
            }

            if (logException != null) {
                logger.info(logException.getMessage());
            }
        }
    }

    @Override
    public void logout()
        throws RequestTimeoutException, FaultException {

        synchronized(loginLock) {
            if (loginMgr == null) {
                throw new AuthenticationRequiredException(
                    "The KVStore handle has no associated login",
                    false /* isReturnSignal */);
            }

            try {
                loginMgr.logout();
            } catch (SessionAccessException sae) {
                logger.fine(sae.getMessage());
                /* ok */
            } finally {
                if (isDispatcherOwner) {
                    dispatcher.setRegUtilsLoginManager(null);
                }
            }
        }
    }

    /** For testing. */
    public boolean isAvroCatalogPopulated() {
        return (avroCatalogRef.get() != null);
    }

    /**
     * For unit test and debugging assistance.
     */
    public PartitionId getPartitionId(Key key) {
        final byte[] keyBytes = keySerializer.toByteArray(key);
        return dispatcher.getPartitionId(keyBytes);
    }

    public long getDefaultLOBTimeout() {
        return defaultLOBTimeout;
    }

    public String getDefaultLOBSuffix() {
        return defaultLOBSuffix;
    }

    public long getDefaultLOBVerificationBytes() {
        return defaultLOBVerificationBytes;
    }

    public Consistency getDefaultConsistency() {
        return defaultConsistency;
    }

    public Durability getDefaultDurability() {
        return defaultDurability;
    }

    public int getDefaultChunksPerPartition() {
        return defaultChunksPerPartition;
    }

    public int getDefaultChunkSize() {
        return defaultChunkSize;
    }

    @Override
    public Version putLOB(Key lobKey,
                          InputStream lobStream,
                          Durability durability,
                          long lobTimeout,
                          TimeUnit timeoutUnit)
        throws IOException {

        return largeObjectImpl.putLOB(lobKey, lobStream,
                                      durability, lobTimeout, timeoutUnit);
    }

    @Override
    public boolean deleteLOB(Key lobKey,
                             Durability durability,
                             long lobTimeout,
                             TimeUnit timeoutUnit) {
        return largeObjectImpl.deleteLOB(lobKey, durability,
                                         lobTimeout, timeoutUnit);
    }

    @Override
    public InputStreamVersion getLOB(Key lobKey,
                                     Consistency consistency,
                                     long lobTimeout,
                                     TimeUnit timeoutUnit) {
        return largeObjectImpl.getLOB(lobKey, consistency,
                                      lobTimeout, timeoutUnit);
    }

    @Override
    public Version putLOBIfAbsent(Key lobKey,
                                  InputStream lobStream,
                                  Durability durability,
                                  long lobTimeout,
                                  TimeUnit timeoutUnit)
        throws IOException {

        return largeObjectImpl.
            putLOBIfAbsent(lobKey, lobStream,
                           durability, lobTimeout, timeoutUnit);
    }

    @Override
    public Version putLOBIfPresent(Key lobKey,
                                   InputStream lobStream,
                                   Durability durability,
                                   long lobTimeout,
                                   TimeUnit timeoutUnit)
        throws IOException {

        return largeObjectImpl.
            putLOBIfPresent(lobKey, lobStream,
                            durability, lobTimeout, timeoutUnit);
    }

    @Override
    public TableAPI getTableAPI() {
        return new TableAPIImpl(this);
    }

    @Override
    public Version appendLOB(Key lobKey,
                             InputStream lobAppendStream,
                             Durability durability,
                             long lobTimeout,
                             TimeUnit timeoutUnit)
        throws IOException {

        return largeObjectImpl.
            appendLOB(lobKey, lobAppendStream,
                      durability, lobTimeout, timeoutUnit);
    }

    /**
     * Attempt reauthentication, if possible.
     * @param requestLoginMgr the LoginManager in effect at the time of
     *   the request execution.
     * @return true if reauthentication has succeeded
     */
    private boolean tryReauthenticate(LoginManager requestLoginMgr)
        throws FaultException {

        if (reauthHandler == null) {
            return false;
        }

        synchronized (loginLock) {
            /*
             * If multiple threads are concurrently accessing the kvstore at
             * the time of an AuthenticationRequiredException, there is the
             * possibility of a flood of AuthenticationRequiredExceptions
             * occuring, with a flood of reauthentication attempts following.
             * Because of the synchronization on loginLock, only one thread
             * will be able to re-authenticate at a time, so by the time a
             * reauthentication request makes it here, another thread may
             * already have completed the authentication.
             */
            if (this.loginMgr == requestLoginMgr) {
                try {
                    reauthHandler.reauthenticate(this);
                } catch (KVSecurityException kvse) {
                    logger.fine(kvse.getMessage());
                    return false;
                }
            }
        }

        return true;
    }

}
