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

package oracle.kv;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import oracle.kv.impl.api.lob.ChunkConfig;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.lob.KVLargeObject;

import com.sleepycat.je.utilint.PropUtil;

/**
 * Represents the configuration parameters used to create a handle to an
 * existing KV store.
 */
public class KVStoreConfig implements Serializable, Cloneable,
                                      KVSecurityConstants {

    private static final long serialVersionUID = 1L;

    /**
     * The default timeout in ms associated with KVStore requests.
     */
    public static final int DEFAULT_REQUEST_TIMEOUT = 5 * 1000;

    /**
     * The default open timeout in ms associated with the sockets used to make
     * KVStore requests.
     */
    public static final int DEFAULT_OPEN_TIMEOUT = 5 * 1000;

    /**
     * The default read timeout in ms associated with the sockets used to make
     * KVStore requests.
     * <p>
     * The default read timeout value must be larger than
     * {@link #DEFAULT_REQUEST_TIMEOUT} to ensure that read requests are not
     * timed out by the socket.
     */
    public static final int DEFAULT_READ_TIMEOUT = 30 * 1000;

    /**
     * @hidden
     * The default open timeout in ms associated with the sockets used to make
     * registry requests.
     */
    public static final int DEFAULT_REGISTRY_OPEN_TIMEOUT = 5 * 1000;

    /**
     * @hidden
     *
     * The default read timeout associated with the sockets used to make
     * registry requests.
     */
    public static final int DEFAULT_REGISTRY_READ_TIMEOUT = 10 * 1000;

    private static final Consistency DEFAULT_CONSISTENCY =
        Consistency.NONE_REQUIRED;

    private static final Durability DEFAULT_DURABILITY =
        Durability.COMMIT_NO_SYNC;

    /**
     * The default LOB suffix ({@value}) used to identify keys associated with
     * Large Objects.
     *
     * @since 2.0
     */
    static final String DEFAULT_LOB_SUFFIX = ".lob";

    /**
     * The default number of trailing bytes ({@value}) of a partial LOB that
     * must be verified against the user supplied LOB stream when resuming a
     * LOB <code>put</code> operation.
     *
     * @see KVLargeObject#putLOB
     *
     * @since 2.0
     */
    static final long DEFAULT_LOB_VERIFICATION_BYTES = 1024;

    /* TODO: add bean properties file. */
    /* TODO: add load/save to Properties object. */

    private String storeName;
    private String[] helperHosts;

    /* The current set of merged security properties */
    private Properties securityProps;

    /*
     * The set of security properties provided through a file specified by the
     * oracle.kv.security system property.  These are read at construction time
     * and applied dynamically as an overlay on top of caller-specified
     * properties.
     */
    private final Properties masterSecurityProps;

    /* Socket related timeouts. */
    private int openTimeout;
    private int readTimeout;
    private int registryOpenTimeout;
    private int registryReadTimeout;

    private int requestTimeout;
    private Consistency consistency;
    private Durability durability;

    /* LOB related properties. */
    private long lobVerificationBytes;
    private String lobSuffix;
    private int lobTimeout;

    private RequestLimitConfig requestLimitConfig;

    private ChunkConfig lobConfig = new ChunkConfig();

    /**
     * The zones in which nodes must be located to be used for read operations,
     * or {@code null} if read operations can be performed on nodes in any
     * zone.
     *
     * @since 3.0
     */
    private String[] readZones = null;

    /**
     * The default timeout value ({@value} ms) associated with internal
     * LOB access during operations on LOBs.
     *
     * @since 2.0
     */
    public static final int DEFAULT_LOB_TIMEOUT = 10000;

    /**
     * Creates a config object with the minimum required properties.
     *
     * @param storeName the name of the KVStore.  The store name is used to
     * guard against accidental use of the wrong host or port.  The store name
     * must consist entirely of upper or lowercase, letters and digits.
     *
     * @param helperHostPort one or more strings containing the host and port
     * of an active node in the KVStore. Each string has the format:
     * hostname:port. It is good practice to pass multiple hosts so that if
     * one host is down, the system will attempt to open the next one, and
     * so on.
     *
     * @throws IllegalArgumentException if an argument has an illegal value.
     * This may be thrown if the
     * {@value oracle.kv.KVSecurityConstants#SECURITY_FILE_PROPERTY} property
     * is set and an error occurs while attempting to read that file.
     */
    public KVStoreConfig(String storeName, String... helperHostPort)
        throws IllegalArgumentException {
        setStoreName(storeName);
        setHelperHosts(helperHostPort);

        openTimeout = DEFAULT_OPEN_TIMEOUT;
        readTimeout = DEFAULT_READ_TIMEOUT;

        registryOpenTimeout = DEFAULT_REGISTRY_OPEN_TIMEOUT;
        registryReadTimeout = DEFAULT_REGISTRY_READ_TIMEOUT;

        requestTimeout = DEFAULT_REQUEST_TIMEOUT;

        consistency = DEFAULT_CONSISTENCY;
        durability = DEFAULT_DURABILITY;

        lobVerificationBytes = DEFAULT_LOB_VERIFICATION_BYTES;
        lobSuffix = DEFAULT_LOB_SUFFIX;
        lobTimeout = DEFAULT_LOB_TIMEOUT;

        requestLimitConfig = RequestLimitConfig.getDefault();

        masterSecurityProps = readSecurityProps();
        securityProps = mergeSecurityProps(null, masterSecurityProps);
    }

    @Override
    public KVStoreConfig clone() {
        try {
            KVStoreConfig clone = (KVStoreConfig) super.clone();
            clone.lobConfig = clone.lobConfig.clone();
            return clone;
        } catch (CloneNotSupportedException neverHappens) {
            return null;
        }
    }

    /**
     * Configures the store name.
     *
     * @param storeName the name of the KVStore.  The store name is used to
     * guard against accidental use of the wrong host or port.  The store name
     * must consist entirely of upper or lowercase, letters and digits.
     *
     * @return this
     */
    public KVStoreConfig setStoreName(String storeName)
        throws IllegalArgumentException {

        setStoreNameVoid(storeName);
        return this;
    }

    /**
     * The void return setter for use by Bean editors.
     * @hidden
     */
    public void setStoreNameVoid(String storeName)
        throws IllegalArgumentException {

        if (storeName == null) {
            throw new IllegalArgumentException("Store name may not be null");
        }
        this.storeName = storeName;
    }

    /**
     * Returns the store name.
     *
     * <p>If it is not overridden by calling {@link #setStoreName}, the
     * default value is the one specified to the {@link KVStoreConfig}
     * constructor.</p>
     *
     * @return the store name.
     */
    public String getStoreName() {
        return storeName;
    }

    /**
     * Configures the helper host/port pairs.
     *
     * @param helperHostPort one or more strings containing the host and port
     * of an active node in the KVStore. Each string has the format:
     * hostname:port. It is good practice to pass multiple hosts so that if
     * one host is down, the system will attempt to open the next one, and
     * so on.
     *
     * @return this
     *
     * @throws IllegalArgumentException if no helperHostPort is specified.
     */
    public KVStoreConfig setHelperHosts(String... helperHostPort)
        throws IllegalArgumentException {

        setHelperHostsVoid(helperHostPort);
        return this;
    }

    /**
     * The void return setter for use by Bean editors.
     * @hidden
     */
    public void setHelperHostsVoid(String... helperHostPort)
        throws IllegalArgumentException {

        if (helperHostPort.length == 0) {
            throw new IllegalArgumentException("No helperHostPort specified");
        }
        for (final String s : helperHostPort) {
            if (s == null) {
                throw new IllegalArgumentException("helperHostPort is null");
            }
        }
        helperHosts = helperHostPort;
    }

    /**
     * Returns the helper host/port pairs.
     *
     * <p>If it is not overridden by calling {@link #setHelperHosts}, the
     * default value is the one specified to the {@link KVStoreConfig}
     * constructor.</p>
     *
     * @return the helper hosts.
     */
    public String[] getHelperHosts() {
        return helperHosts;
    }

    /**
     * Configures the open timeout used when establishing sockets used to make
     * client requests. Shorter timeouts result in more rapid failure detection
     * and recovery. The default open timeout ({@value #DEFAULT_OPEN_TIMEOUT}
     * milliseconds) should be adequate for most applications.
     * <p>
     * The client does not directly open sockets when making requests. KVStore
     * manages the network connections used to make client requests opening and
     * closing connections as needed.
     * <p>
     * Please note that the socket timeout applies to any duplicate KVStore
     * handles to the same KVStore within this process.
     *
     * @param timeout the socket open timeout.
     *
     * @param unit the {@code TimeUnit} of the timeout value.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the timeout value is negative or
     * zero.
     *
     * @see #DEFAULT_OPEN_TIMEOUT
     *
     * @since 2.0
     */
    public KVStoreConfig setSocketOpenTimeout(long timeout, TimeUnit unit)
        throws IllegalArgumentException {

        setSocketOpenTimeoutVoid(timeout, unit);
        return this;
    }

    /**
     * @deprecated replaced by {@link #setSocketOpenTimeout}
     */
    @Deprecated
    public KVStoreConfig setOpenTimeout(long timeout, TimeUnit unit)
        throws IllegalArgumentException {

        setSocketOpenTimeoutVoid(timeout, unit);
        return this;
    }

    /**
     * The void return setter for use by Bean editors.
     * @hidden
     */
    public void setSocketOpenTimeoutVoid(long timeout, TimeUnit unit)
        throws IllegalArgumentException {

        if (timeout <= 0) {
            throw new IllegalArgumentException
                ("Timeout may not be negative or zero");
        }
        openTimeout = PropUtil.durationToMillis(timeout, unit);
    }

    /**
     * Returns the socket open timeout.
     *
     * <p>If it is not overridden by calling {@link #setOpenTimeout}, the
     * default value is {@value #DEFAULT_OPEN_TIMEOUT} milliseconds.</p>
     *
     * @param unit the {@code TimeUnit} of the returned value. May not be null.
     *
     * @return The socket open timeout.
     *
     * @since 2.0
     */
    public long getSocketOpenTimeout(TimeUnit unit) {
        return PropUtil.millisToDuration(openTimeout, unit);
    }

    /**
     * @deprecated replaced by {@link #getSocketOpenTimeout}
     */
    @Deprecated
    public long getOpenTimeout(TimeUnit unit) {
        return getSocketOpenTimeout(unit);
    }

    /**
     * Configures the read timeout associated with the underlying sockets used
     * to make client requests. Shorter timeouts result in more rapid failure
     * detection and recovery. However, this timeout should be sufficiently
     * long so as to allow for the longest timeout associated with a request.
     * <p>
     * The client does not directly manage sockets when making requests.
     * KVStore manages the network connections used to make client requests
     * opening and closing connections as needed.
     * <p>
     * Please note that the socket timeout applies to any duplicate KVStore
     * handles to the same KVStore within this process.
     *
     * @param timeout the socket read timeout
     * @param unit the {@code TimeUnit} of the timeout value
     * @return this
     *
     * @throws IllegalArgumentException if the timeout is invalid
     *
     * @since 2.0
     */
    public KVStoreConfig setSocketReadTimeout(long timeout, TimeUnit unit)
        throws IllegalArgumentException {

        setReadTimeoutVoid(timeout, unit);
        return this;
    }

    /**
     * The void return setter for use by Bean editors.
     * @hidden
     */
    public void setReadTimeoutVoid(long timeout, TimeUnit unit)
        throws IllegalArgumentException {

        if (timeout <= 0) {
            throw new IllegalArgumentException
                ("Timeout may not be negative or zero");
        }
        readTimeout = PropUtil.durationToMillis(timeout, unit);
    }

    /**
     * Returns the read timeout associated with the sockets used to
     * make requests.
     *
     * <p>If it is not overridden by calling {@link #setSocketReadTimeout}, the
     * default value is {@value #DEFAULT_READ_TIMEOUT} milliseconds.</p>
     *
     * @param unit the {@code TimeUnit} of the returned value. May not be null.
     *
     * @return The socket read timeout
     *
     * @since 2.0
     */
    public long getSocketReadTimeout(TimeUnit unit) {
        return PropUtil.millisToDuration(readTimeout, unit);
    }

    /**
     * Configures the default request timeout.
     *
     * @param timeout the default request timeout.
     *
     * @param unit the {@code TimeUnit} of the timeout value.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the timeout value is negative or
     * zero.
     */
    public KVStoreConfig setRequestTimeout(long timeout, TimeUnit unit)
        throws IllegalArgumentException {

        setRequestTimeoutVoid(timeout, unit);
        return this;
    }

    /**
     * The void return setter for use by Bean editors.
     * @hidden
     */
    public void setRequestTimeoutVoid(long timeout, TimeUnit unit)
        throws IllegalArgumentException {

        if (timeout <= 0) {
            throw new IllegalArgumentException
                ("Timeout may not be zero or negative");
        }
        requestTimeout = PropUtil.durationToMillis(timeout, unit);
    }

    /**
     * Returns the default request timeout.
     *
     * <p>If it is not overridden by calling {@link #setRequestTimeout}, the
     * default value is {@link #DEFAULT_REQUEST_TIMEOUT}.</p>
     *
     * @param unit the {@code TimeUnit} of the returned value. May not be null.
     *
     * @return The transaction timeout.
     */
    public long getRequestTimeout(TimeUnit unit) {
        return PropUtil.millisToDuration(requestTimeout, unit);
    }

    /**
     * Configures the default read Consistency to be used when a Consistency is
     * not specified for a particular read operation.
     *
     * @param consistency the default read Consistency.
     *
     * @return this
     */
    public KVStoreConfig setConsistency(Consistency consistency)
        throws IllegalArgumentException {

        setConsistencyVoid(consistency);
        return this;
    }

    /**
     * The void return setter for use by Bean editors.
     * @hidden
     */
    public void setConsistencyVoid(Consistency consistency)
        throws IllegalArgumentException {

        if (consistency == null) {
            throw new IllegalArgumentException("Consistency may not be null");
        }
        this.consistency = consistency;
    }

    /**
     * Returns the default read Consistency.
     *
     * <p>If it is not overridden by calling {@link #setConsistency}, the
     * default value is {@link Consistency#NONE_REQUIRED}.</p>
     *
     * @return the default read Consistency.
     */
    public Consistency getConsistency() {
        return consistency;
    }

    /**
     * Configures the default write Durability to be used when a Durability is
     * not specified for a particular write operation.
     *
     * @param durability the default write Durability.
     *
     * @return this
     */
    public KVStoreConfig setDurability(Durability durability)
        throws IllegalArgumentException {

        setDurabilityVoid(durability);
        return this;
    }

    /**
     * The void return setter for use by Bean editors.
     * @hidden
     */
    public void setDurabilityVoid(Durability durability)
        throws IllegalArgumentException {

        if (durability == null) {
            throw new IllegalArgumentException("Durability may not be null");
        }
        this.durability = durability;
    }

    /**
     * Returns the default write Durability.
     *
     * <p>If it is not overridden by calling {@link #setDurability}, the
     * default value is {@link Durability#COMMIT_NO_SYNC}.</p>
     *
     * @return the default write Durability.
     */
    public Durability getDurability() {
        return durability;
    }

    /**
     * Configures the maximum number of requests that can be active for a node
     * in the KVStore. Limiting requests in this way helps minimize the
     * possibility of thread starvation in situations where one or more nodes
     * in the KVStore exhibits long service times and as a result retains
     * threads, making them unavailable to service requests to other reachable
     * and healthy nodes.
     * <p>
     * The long service times can be due to problems at the node itself, or in
     * the network path to that node. The KVStore request dispatcher will,
     * whenever possible, minimize use of nodes with long service times
     * automatically, by re-routing requests to other nodes that can handle
     * them. So this mechanism provides an additional margin of safety when
     * such re-routing of requests is not possible.
     *
     * @return this
     */
    public KVStoreConfig
        setRequestLimit(RequestLimitConfig requestLimitConfig) {

        if (requestLimitConfig == null) {
            throw new IllegalArgumentException
                ("requestLimitConfig may not be null");
        }
        this.requestLimitConfig = requestLimitConfig;
        return this;
    }

    /**
     * Returns the configuration describing how the number of active requests
     * to a node are limited.
     * <p>
     * It returns the default value, documented in {@link RequestLimitConfig},
     * if it was not overridden by calling {@link #setRequestLimit}.
     *
     * @return this
     */
    public RequestLimitConfig getRequestLimit() {
        return requestLimitConfig;
    }

    /**
     * @hidden
     * Configures the connect/open timeout used when making RMI registry
     * lookup requests.
     * <p>
     * Note that the setting of these registry timeouts is global and is not
     * KVS-specific like the other timeouts. The API itself could be static,
     * but that would make it stand out relative to the other timeout apis.
     * Revisit this if we ever make these registry apis public.
     *
     * @param timeout the open timeout.
     *
     * @param unit the {@code TimeUnit} of the timeout value.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the timeout value is negative or
     * zero.
     */
    public KVStoreConfig setRegistryOpenTimeout(long timeout, TimeUnit unit)
        throws IllegalArgumentException {

        setRegistryOpenTimeoutVoid(timeout, unit);
        return this;
    }

    /**
     * The void return setter for use by Bean editors.
     * @hidden
     */
    public void setRegistryOpenTimeoutVoid(long timeout, TimeUnit unit)
        throws IllegalArgumentException {

        if (timeout <= 0) {
            throw new IllegalArgumentException
                ("Timeout may not be negative or zero");
        }
        registryOpenTimeout = PropUtil.durationToMillis(timeout, unit);
    }

    /**
     * @hidden
     * Returns the socket open timeout associated with sockets used to access
     * an RMI registry.
     *
     * <p>
     * If it is not overridden by calling {@link #setRegistryOpenTimeout}, the
     * default value is {@link #DEFAULT_REGISTRY_OPEN_TIMEOUT}.
     * </p>
     *
     * @param unit the {@code TimeUnit} of the returned value. May not be null.
     *
     * @return The open timeout.
     */
    public long getRegistryOpenTimeout(TimeUnit unit) {
        return PropUtil.millisToDuration(registryOpenTimeout, unit);
    }

    /**
     * @hidden
     * Configures the read timeout associated with sockets used to make RMI
     * registry requests. Shorter timeouts result in more rapid failure
     * detection and recovery. However, this timeout should be sufficiently
     * long so as to allow for the longest timeout associated with a request.
     * <p>
     * The default value {@link #DEFAULT_REGISTRY_OPEN_TIMEOUT} should
     * be sufficient for most configurations.
     *
     * <p>
     * Note that the setting of these registry timeouts is global and is not
     * KVS-specific like the other timeouts.
     *
     * @param timeout the read timeout
     * @param unit the {@code TimeUnit} of the timeout value.
     * @return this
     *
     * @throws IllegalArgumentException
     */
    public KVStoreConfig setRegistryReadTimeout(long timeout, TimeUnit unit)
            throws IllegalArgumentException {

        setRegistryReadTimeoutVoid(timeout, unit);
        return this;
    }

    /**
     * The void return setter for use by Bean editors.
     * @hidden
     */
    public void setRegistryReadTimeoutVoid(long timeout, TimeUnit unit)
        throws IllegalArgumentException {

        if (timeout <= 0) {
            throw new IllegalArgumentException
                ("Timeout may not be negative or zero");
        }
        registryReadTimeout = PropUtil.durationToMillis(timeout, unit);
    }

    /**
     * @hidden
     * Returns the read timeout associated with the sockets used to
     * make RMI registry requests.
     *
     * <p>If it is not overridden by calling {@link #setSocketReadTimeout}, the
     * default value is {@link #DEFAULT_REGISTRY_OPEN_TIMEOUT}.</p>
     *
     * @param unit the {@code TimeUnit} of the returned value. May not be null.
     *
     * @return The open timeout.
     */
    public long getRegistryReadTimeout(TimeUnit unit) {
        return PropUtil.millisToDuration(registryReadTimeout, unit);
    }

    /**
     * Returns the default timeout value (in ms) associated with chunk access
     * during operations on LOBs.
     *
     * @see KVLargeObject#getLOB
     * @see KVLargeObject#putLOB
     *
     * @since 2.0
     */
    public long getLOBTimeout(TimeUnit unit) {
        return PropUtil.millisToDuration(lobTimeout, unit);
    }

    /**
     * Configures default timeout value associated with chunk access during
     * operations on LOBs.
     *
     * @param timeout the open timeout.
     *
     * @param unit the {@code TimeUnit} of the timeout value.
     *
     * @return this
     *
     * @see KVLargeObject#getLOB
     * @see KVLargeObject#putLOB
     *
     * @since 2.0
     */
    public KVStoreConfig setLOBTimeout(long timeout, TimeUnit unit) {
        setLOBTimeoutVoid(timeout, unit);
        return this;
    }

    /**
     * The void return setter for use by Bean editors.
     * @hidden
     */
    public void setLOBTimeoutVoid(long timeout, TimeUnit unit)
        throws IllegalArgumentException {

        if (timeout <= 0) {
            throw new IllegalArgumentException
                ("Timeout may not be negative or zero");
        }
        lobTimeout = PropUtil.durationToMillis(timeout, unit);
    }

    /**
     * Returns the default suffix associated with LOB keys.
     *
     * @see KVLargeObject
     *
     * @since 2.0
     */
    public String getLOBSuffix() {
        return lobSuffix;
    }

    /**
     *
     * Configures the default suffix associated with LOB keys. The application
     * must ensure that the suffix is used consistently across all KVStore
     * handles. The suffix is used by the KVStore APIs to ensure that any
     * value-changing non-LOB APIs are not invoked on LOB objects and vice
     * versa. Failure to use the LOB suffix consistently can result in
     * <code>IllegalArgumentException</code> being thrown if a LOB object is
     * created with one LOB suffix and is subsequently accessed
     * when a different suffix is in effect. Write operations using
     * inconsistent LOB suffixes may result in LOB storage not being reclaimed.
     *
     * <p>
     * This method should only be used in the rare case that the default LOB
     * suffix value ".lob" is unsuitable.
     *
     * @param lobSuffix the LOB suffix string. It must be non-null and
     * have a length &GT 0.
     * @return this
     *
     * @see KVLargeObject
     *
     * @since 2.0
     */
    public KVStoreConfig setLOBSuffix(String lobSuffix) {
        setLOBSuffixVoid(lobSuffix);
        return this;
    }

    /**
     * The void return setter for use by Bean editors.
     * @hidden
     */
    public void setLOBSuffixVoid(String lobSuffix) {
        if ((lobSuffix == null) || (lobSuffix.length() == 0)) {
            throw new IllegalArgumentException
            ("The lobSuffix argument must be non-null " +
                "and have a length > 0 ");
        }
        this.lobSuffix = lobSuffix;
    }

    /**
     * Returns the number of trailing bytes of a partial LOB that must be
     * verified against the user supplied LOB stream when resuming a LOB
     * <code>put</code> operation.
     *
     * @see KVLargeObject
     *
     * @since 2.0
     */
    public long getLOBVerificationBytes() {
        return lobVerificationBytes;
    }

    /**
     * Configures the number of trailing bytes of a partial LOB that must be
     * verified against the user supplied LOB stream when resuming a
     * <code>putLOB</code> operation.
     *
     * @param lobVerificationBytes the number of bytes to be verified. A value
     * <=0 disables verification.
     *
     * @return this
     *
     * @see KVLargeObject
     *
     * @since 2.0
     */
    public KVStoreConfig setLOBVerificationBytes(long lobVerificationBytes) {
        setLOBVerificationBytesVoid(lobVerificationBytes);
        return this;
    }

    /**
     * The void return setter for use by Bean editors.
     * @hidden
     */
    public void setLOBVerificationBytesVoid(long lobVerificationBytes) {
        if (lobVerificationBytes < 0) {
            throw new IllegalArgumentException("lobVerificationBytes: " +
                                                lobVerificationBytes);
        }
        this.lobVerificationBytes = lobVerificationBytes;
    }

    /**
     * @hidden
     * Returns the number of contiguous chunks that can be stored in the same
     * partition for a given LOB.
     *
     * @see KVLargeObject
     *
     * @since 2.0
     */
    public int getLOBChunksPerPartition() {
        return lobConfig.getChunksPerPartition();
    }

    /**
     * @hidden
     *
     * Configures the number of contiguous chunks that can be stored in the
     * same partition for a given LOB.
     *
     * @param lobChunksPerPartition the number of partitions
     *
     * @return this
     *
     * @since 2.0
     */
    public KVStoreConfig setLOBChunksPerPartition(int lobChunksPerPartition) {
        setLOBChunksPerPartitionVoid(lobChunksPerPartition);
        return this;
    }

    /**
     * The void return setter for use by Bean editors.
     * @hidden
     */
    public void setLOBChunksPerPartitionVoid(int lobChunksPerPartition) {
        if (lobChunksPerPartition <= 0) {
            throw new IllegalArgumentException("lobChunksPerPartition: " +
                                               lobChunksPerPartition);
        }
        lobConfig.setChunksPerPartition(lobChunksPerPartition);
    }

    /**
     * @hidden
     * Returns the chunk size associated with the chunks used to store a LOB.
     *
     * @see KVLargeObject
     *
     * @since 2.0
     */
    public int getLOBChunkSize() {
        return lobConfig.getChunkSize();
    }

    /**
     * @hidden
     * Configures the chunk size associated with the chunks used to store a LOB.
     *
     * @see KVLargeObject
     *
     * @since 2.0
     */
    public KVStoreConfig setLOBChunkSize(int chunkSize) {
        setLOBChunkSizeVoid(chunkSize);
        return this;
    }

    /**
     * The void return setter for use by Bean editors.
     * @hidden
     */
    public void setLOBChunkSizeVoid(int chunkSize) {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("chunkSize: " + chunkSize);
        }
        lobConfig.setChunkSize(chunkSize);
    }

    /**
     * Returns the zones in which nodes must be located to be used for read
     * operations, or {@code null} if read operations can be performed on nodes
     * in any zone.
     *
     * @return the zones or {@code null}
     * @since 3.0
     */
    public String[] getReadZones() {
        return readZones == null ?
            null :
            Arrays.copyOf(readZones, readZones.length);
    }

    /**
     * Sets the zones in which nodes must be located to be used for read
     * operations.  If the argument is {@code null}, or this method has not
     * been called, then read operations can be performed on nodes in any zone.
     *
     * <p>The specified zones must exist at the time that this configuration
     * object is used to create a store, or else {@link
     * KVStoreFactory#getStore} will throw an {@link IllegalArgumentException}.
     *
     * <p>Zones specified for read operations can include primary and secondary
     * zones.  If the master is not located in any of the specified zones,
     * either because the zones are all secondary zones or because the master
     * node is not currently in one of the specified primary zones, then read
     * operations with {@link Consistency#ABSOLUTE} will fail.
     *
     * @param zones the zones or {@code null}
     * @return this
     * @throws IllegalArgumentException if the array argument is not {@code
     * null} and is either empty or contains {@code null} or duplicate elements
     * @since 3.0
     */
    public KVStoreConfig setReadZones(final String... zones) {
        setReadZonesVoid(zones);
        return this;
    }

    /**
     * The void return setter for use by Bean editors.
     * @hidden
     */
    public void setReadZonesVoid(final String[] zones) {
        if (zones == null) {
            readZones = null;
        } else if (zones.length == 0) {
            throw new IllegalArgumentException(
                "The zones argument must not be empty");
        } else {
            final String[] copy = Arrays.copyOf(zones, zones.length);
            for (int i = 0; i < copy.length; i++) {
                final String zone = copy[i];
                if (zone == null) {
                    throw new IllegalArgumentException(
                        "The zones argument must not contain null elements");
                }
                for (int j = i + 1; j < copy.length; j++) {
                    if (zone.equals(copy[j])) {
                        throw new IllegalArgumentException(
                            "The zones argument must not contain" +
                            " duplicate elements; found multiple copies of '" +
                            zone + "'");
                    }
                }
            }
            readZones = copy;
        }
    }

    @Override
    public String toString() {
        return "<KVStoreConfig" +
               " storeName=" + storeName +
               " helperHosts=" + Arrays.toString(helperHosts) +
               " requestTimeout=" + requestTimeout +
               " consistency=" + consistency +
               " durability=" + durability +
               " lobSuffix=" + lobSuffix +
               " lobVerificationBytes=" + lobVerificationBytes +
               " lobTimeout=" + lobTimeout +
               ((readZones != null) ?
                " readZones=" + Arrays.toString(readZones) :
                "") +
               ">";
    }

    /**
     * Configures security properties for the client. The supported properties
     * include both authentication properties and transport properties. 
     * See {@link KVSecurityConstants} for the supported properties.
     * @return this
     */
    public KVStoreConfig setSecurityProperties(Properties securityProps) {

        setSecurityPropertiesVoid(securityProps);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setSecurityPropertiesVoid(Properties securityProps) {

        this.securityProps = mergeSecurityProps(securityProps,
                                                masterSecurityProps);
    }

    /**
     * Returns a copy of the current security properties. This reflects both the
     * properties explictly set through setSecurityProperties as well as any
     * properties obtained from a security property file. Changes to the
     * returned object have no effect on configuration settings.
     *
     * @return the current security properties
     */
    Properties getSecurityProperties() {
        return (Properties) securityProps.clone();
    }

    /**
     * Merge the masterSecurityProperties with the input properties.
     *
     * @return a Properties object, with the settings from masterSecurityProps
     * overriding values from newSecurityProps. The caller must ensure that the
     * returned value is not modified, as it could be the master security
     * properties object that was read at construction time.
     */
    private static Properties mergeSecurityProps(
        Properties newSecurityProps,
        Properties masterSecurityProps) {

        if (newSecurityProps == null)  {
            if (masterSecurityProps == null) {
                /* If both are null, return an empty property set */
                return new Properties();
            }

            /* The (internal) caller is warned to not modify this */
            return masterSecurityProps;
        }

        final Properties result = (Properties) newSecurityProps.clone();

        if (masterSecurityProps != null) {
            for (String propName : masterSecurityProps.stringPropertyNames()) {
                final String propVal =
                    masterSecurityProps.getProperty(propName);
                result.setProperty(propName, propVal);
            }
        }

        return result;
    }

    /**
     * Read security properties from a configured property file.
     */
    private static Properties readSecurityProps()
        throws IllegalArgumentException {

        final String securityFile =
            System.getProperty(KVSecurityConstants.SECURITY_FILE_PROPERTY);
        if (securityFile == null) {
            return null;
        }

        try {
            return KVStoreLogin.createSecurityProperties(securityFile);
        } catch (IllegalStateException ise) {
            throw new IllegalArgumentException(
                "An error was encountered while processing the security " +
                "property file " + securityFile,
                ise);
        }
    }
}
