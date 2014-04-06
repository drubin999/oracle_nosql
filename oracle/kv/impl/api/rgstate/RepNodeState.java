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

package oracle.kv.impl.api.rgstate;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.EnumMap;
import java.util.Map;

import oracle.kv.Consistency;
import oracle.kv.impl.api.RequestHandlerAPI;
import oracle.kv.impl.api.ops.NOP;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.utilint.VLSN;

/**
 * The operational state associated with the RN or KV client. This state is
 * dynamic and is used primarily to dispatch a request to a specific RN within
 * a replication group.
 * <p>
 * Note that the methods used to modify state are specified as
 * <code>update</code> rather that <code>set</code> methods. Due to the
 * distributed and concurrent nature of the processing it's entirely possible
 * that state update requests can be received out of order, so each update
 * method must be prepared to ignore obsolete update requests. The return value
 * from the <code>update</code> methods indicates whether an update was
 * actually made, or whether the request was obsolete and therefore ignored.
 */
public class RepNodeState {

    private final RepNodeId rnId;

    /** The ID of the zone containing the RN, or null if not known. */
    private volatile DatacenterId znId = null;

    /**
     *  The remote request handler (an RMI reference) for the RN.
     */
    private final ReqHandlerRef reqHandlerRef;

    /**
     * The last known haState associated with the RN. We simply want the latest
     * state so access to it is unsynchronized.
     */
    private volatile State repState = null;

    /**
     * The VLSN state associated with this RN
     */
    private final VLSNState vlsnState;

    /**
     * Map of metadata sequence numbers. The sequence numbers is the last known
     * value. No entry will present if there is no information for a given
     * metadata type.
     */
    private final Map<MetadataType, Integer> seqNums =
                    new EnumMap<MetadataType, Integer>(MetadataType.class);

    /**
     * The value denoting that the metadata sequence number at that node is
     * unknown.
     */
    private static final int UNKNOWN_SEQ_NUM = -1;

    /**
     * The size of the sample used for the trailing average. The sample needs
     * to be large enough so that it represents the different types of
     * operations, but not too large so that it can't react rapidly enough to
     * changes in the response times.
     */
    public static final int SAMPLE_SIZE = 8;

    /**
     * Accumulates the response time from read requests, so they can be load
     * balanced.
     */
    private final ResponseTimeAccumulator readAccumulator;

    /**
     * The accumulated response times across both read and write operations.
     */
    private final AtomicLong accumRespTimeMs = new AtomicLong(0);

    /**
     * The number of request actively being processed by this node.
     */
    private final AtomicInteger activeRequestCount = new AtomicInteger(0);

    /* The max active request count. Just a rough metric. */
    private volatile int maxActiveRequestCount = 0;

    /**
     * The total requests dispatched to this node. We don't use an atomic long
     * here, since it's just intended to be a rough metric and losing an
     * occasional increment is acceptable.
     */
    private volatile long totalRequestCount;

    /**
     * The total number of requests that resulted in exceptions.
     */
    private volatile int errorCount;

    /**
     * The approx interval over which the VLSN rate is updated. We may want
     * to have a different rate for a client hosted dispatcher versus a RN
     * hosted dispatcher to minimize the number of open tcp connections. RNS
     * will use these connections only when forwarding requests, which should
     * be a relatively uncommon event.
     */
    public static int RATE_INTERVAL_MS = 10000;

    RepNodeState(RepNodeId rnId) {
        this(rnId, RATE_INTERVAL_MS);
    }

    RepNodeState(RepNodeId rnId, int rateIntervalMs) {
        this.rnId = rnId;
        readAccumulator = new ResponseTimeAccumulator();
        reqHandlerRef = new ReqHandlerRef();
        vlsnState = new VLSNState(rateIntervalMs);

        /*
         * Start it out as being in the replica state, so an attempt is made to
         * contact it by sending it a request. At this point the state can be
         * adjusted appropriately.
         */
        repState = State.REPLICA;
    }

    /**
     * Returns the unique resourceId associated with the RepNode
     */
    public RepNodeId getRepNodeId() {
        return rnId;
    }

    /**
     * Returns the ID of the zone containing the RepNode, or {@code null} if
     * not known.
     *
     * @return the zone ID or {@code null}
     */
    public DatacenterId getZoneId() {
        return znId;
    }

    /**
     * Sets the ID of the zone containing the RepNode.
     *
     * @param znId the zone ID
     */
    public void setZoneId(final DatacenterId znId) {
        this.znId = znId;
    }

    /**
     * @throws InterruptedException
     * @see ReqHandlerRef#get
     */
    public RequestHandlerAPI getReqHandlerRef(RegistryUtils registryUtils,
                                              long timeoutMs)
        throws InterruptedException {

        return reqHandlerRef.get(registryUtils, timeoutMs);
    }

    /**
     * @see ReqHandlerRef#needsResolution()
     */
    @SuppressWarnings("javadoc")
    public boolean reqHandlerNeedsResolution() {
        return reqHandlerRef.needsResolution();
    }

    /**
     * @see ReqHandlerRef#needsRepair()
     */
    @SuppressWarnings("javadoc")
    public boolean reqHandlerNeedsRepair() {
        return reqHandlerRef.needsRepair();
    }

    /**
     * @see ReqHandlerRef#resolve
     */
    public RequestHandlerAPI resolveReqHandlerRef(RegistryUtils registryUtils,
                                                  long timeoutMs)
        throws InterruptedException {

        return reqHandlerRef.resolve(registryUtils, timeoutMs);
    }

    /**
     * @see ReqHandlerRef#resolve
     */
    public void resetReqHandlerRef()
        throws InterruptedException {

        reqHandlerRef.reset();
    }

    /**
     * @see ReqHandlerRef#noteException(Exception)
     */
    @SuppressWarnings("javadoc")
    public void noteReqHandlerException(Exception e)
        throws InterruptedException {

        reqHandlerRef.noteException(e);
    }

    /**
     * Returns the SerialVersion that should be used with the request handler.
     */
    public short getRequestHandlerSerialVersion() {
        return reqHandlerRef.getSerialVersion();
    }

    /**
     * Returns the current known replication state associated with the
     * node.
     */
    public ReplicatedEnvironment.State getRepState() {
        return repState;
    }

    /**
     * Updates the rep state.
     *
     * @param state the new rep state
     */
    void updateRepState(State state) {
        repState = state;
    }

    /**
     * Returns the current VLSN associated with the RN.
     * <p>
     * The VLSN can be used to determine how current a replica is with respect
     * to the Master. It may return a null VLSN if the RN has never been
     * contacted.
     */
    VLSN getVLSN() {
        return vlsnState.getVLSN();
    }

    boolean isObsoleteVLSNState() {
        return vlsnState.isObsolete();
    }

    /**
     * Updates the VLSN associated with the node.
     *
     * @param newVLSN the new VLSN
     */
    void updateVLSN(VLSN newVLSN) {
       vlsnState.updateVLSN(newVLSN);
    }

    /**
     * Returns the current topo seq number associated with the node. A negative
     * return value means that the sequence number is unknown.
     */
    int getTopoSeqNum() {
        return getSeqNum(MetadataType.TOPOLOGY);
    }

    /**
     * Updates the topo sequence number, moving it forward. If it's less than
     * the current sequence number it's ignored.
     *
     * @param topoSeqNum the new sequence number
     */
    void updateTopoSeqNum(@SuppressWarnings("hiding") int topoSeqNum) {
        updateSeqNum(MetadataType.TOPOLOGY, topoSeqNum);
    }

    /**
     * Resets the topo sequence number as specified.
     */
    void resetTopoSeqNum(int endSeqNum) {
        resetSeqNum(MetadataType.TOPOLOGY, endSeqNum);
    }

    /**
     * Returns the current seq number associated with the node for the specified
     * metadata. A negative return value means that the sequence number is
     * unknown.
     * 
     * @param type a metadata type
     * @return the current seq number
     */
    public int getSeqNum(MetadataType type) {
        final Integer seqNum = seqNums.get(type);
        return seqNum == null ? UNKNOWN_SEQ_NUM : seqNum;
    }

    /**
     * Updates the sequence number of the specified metadata, moving it forward.
     * If it's less than the current sequence number it's ignored.
     *
     * @param type a metadata type
     * @param newSeqNum the new sequence number
     */
    public void updateSeqNum(MetadataType type, int newSeqNum) {
        synchronized (seqNums) {
            int oldSeqNum = getSeqNum(type);
            if (oldSeqNum < newSeqNum) {
                seqNums.put(type, newSeqNum);
            }
        }
    }
    
    /**
     * Resets the sequence number of the specified metadata.
     *
     * @param type a metadata type
     * @param newSeqNum the new sequence number
     */
    public void resetSeqNum(MetadataType type, int newSeqNum) {
        synchronized (seqNums) {
            seqNums.put(type, newSeqNum);
        }
    }

    /**
     * Returns the error count.
     */
    public int getErrorCount() {
        return errorCount;
    }

    /**
     * Increments the error count associated with the RN.
     *
     * @returns the incremented error count
     */
    public int incErrorCount() {
        return ++errorCount;
    }

    /**
     * Returns the average trailing read response time associated with the RN
     * in milliseconds.
     * <p>
     * It's used primarily for load balancing purposes.
     */
    int getAvReadRespTimeMs() {
        return readAccumulator.getAverage();
    }

    /**
     * Accumulates the average response time by folding in this contribution
     * from a successful request to the accumulated response times. This method
     * is only invoked upon successful completion of a read request.
     * <p>
     * <code>lastAccessTime</code> is also updated each time this method is
     * invoked.
     *
     * @param forWrite determines if the time is being accumulated for a write
     * operation
     * @param responseTimeMs the response time associated with this successful
     * request.
     */
    public void accumRespTime(boolean forWrite, int responseTimeMs) {
        if (!forWrite) {
            readAccumulator.update(responseTimeMs);
        }
        accumRespTimeMs.getAndAdd(responseTimeMs);
    }

    /**
     * Returns the number of outstanding remote requests to the RN.
     * <p>
     * It's used to ensure that all the outgoing connections are not used
     * up because the node is slow in responding to requests.
     */
    int getActiveRequestCount() {
        return activeRequestCount.get();
    }

    /**
     * Returns the max active requests at this RN, since the count was last
     * reset.
     */
    public int getMaxActiveRequestCount() {
        return maxActiveRequestCount;
    }

    /**
     * Returns the total number of requests dispatched to this node.
     */
    public long getTotalRequestCount() {
        return totalRequestCount;
    }

    public long getAccumRespTimeMs() {
        return accumRespTimeMs.get();
    }

    /**
     * Resets the total request count.
     */
    public void resetStatsCounts() {
        totalRequestCount = 0;
        maxActiveRequestCount = 0;
        accumRespTimeMs.set(0);
        errorCount = 0;
    }

    /**
     * Invoked before each remote request is issued to maintain the outstanding
     * request count. It must be paired with a requestEnd in a finally block.
     *
     * @return the number of requests active at this node
     */
    public int requestStart() {
        totalRequestCount++;
        final int count = activeRequestCount.incrementAndGet();
        maxActiveRequestCount = Math.max(maxActiveRequestCount, count);
        return count;
    }

    /**
     * Invoked after each remote request completes. It, along with the
     * requestStart method, is used to maintain the outstanding request count.
     */
    public void requestEnd() {
        activeRequestCount.decrementAndGet();
    }

    /**
     * Returns a descriptive string for the rep node state
     */
    public String printString() {
        return String.format("node: %s " +
                             "state: %s " +
                             "errors: %,d" +
                             "av resp time %,d ms " +
                             "total requests: %,d",
                             getRepNodeId().toString(),
                             getRepState().toString(),
                             getErrorCount(),
                             getAvReadRespTimeMs(),
                             getTotalRequestCount());
    }

    /**
     * AttributeValue encapsulates a RN state value along with a sequence
     * number used to compare attribute values for recency.
     *
     * @param <V> the type associate with the attribute value.
     */
    @SuppressWarnings("unused")
    private static class AttributeValue<V> {
        final V value;
        final long sequence;

        private AttributeValue(V value, long sequence) {
            super();
            this.value = value;
            this.sequence = sequence;
        }

        /** Returns the value associated with the attribute. */
        @SuppressWarnings("unused")
        private V getValue() {
            return value;
        }

        /**
         * Returns a sequence that can be used to order two attribute
         * values in time. Given two values v1 and v2, if v2.getSequence()
         * > v1.getSequence() implies that v2 is the more recent value.
         */
        @SuppressWarnings("unused")
        private long getSequence() {
            return sequence;
        }
    }

    /**
     * Encapsulates the computation of average response times.
     */
    private static class ResponseTimeAccumulator {

        /*
         * The samples in ms.
         */
        final short[] samples;
        int sumMs = 0;
        int index = 0;

        private ResponseTimeAccumulator() {
            samples = new short[SAMPLE_SIZE];
            sumMs = 0;
        }

        private synchronized void update(int sampleMs) {
            if (sampleMs > Short.MAX_VALUE) {
                sampleMs = Short.MAX_VALUE;
            }

            index = (++index >= SAMPLE_SIZE) ? 0 : index;
            sumMs += (sampleMs - samples[index]);
            samples[index] = (short)sampleMs;
        }

        /*
         * Unsynchronized to minimize contention, a slightly stale value is
         * good enough.
         */
        private int getAverage() {
            return (sumMs / SAMPLE_SIZE);
        }
    }

    /**
     * Wraps a remote RequestHandlerAPI reference along with its accompanying
     * state. It also encapsulates the operations used to maintain the remote
     * reference.
     * <p>
     * The RemoteReference is typically resolved via the first call to
     * get(). If an exception is encountered at any time, either during the
     * initial resolve call, or subsequently when an attempt is made to invoke
     * a method through it, the exception is noted in exceptionSummary and
     * the remote reference is considered to be in error. Such erroneous remote
     * references are not considered as candidates for request dispatch. The
     * RequestHandlerUpdate thread will attempt to resolve all such references
     * and restore them on a periodic basis. This resolution is done out of the
     * request's thread of control to minimize the timeout latencies associated
     * with erroneous references.
     */
    private class ReqHandlerRef {

        /* used to coordinate updates to the handler. */
        private final Semaphore semaphore = new Semaphore(1,true);

        /**
         *  The remote request handler (an RMI reference) for the RN.
         */
        private RequestHandlerAPI requestHandler;

        /**
         * It's non-null if there is an error that resulted in the
         * requestHandler iv above being nulled. It's volatile because it's
         * read without acquiring the semaphore by needsRepair() as part of a
         * request dispatch.
         */
        private volatile ExceptionSummary exceptionSummary;

        private short serialVersion;

        private ReqHandlerRef() {
            requestHandler = null;
            /* TODO: Set to SerialVersion.UNKNOWN to catch errors. */
            serialVersion = SerialVersion.CURRENT;
            exceptionSummary = null;
        }

        /**
         * Resets the state of the reference back to its initial state.
         * @throws InterruptedException
         */
        private void reset() throws InterruptedException {
            semaphore.acquire();
            try {
                requestHandler = null;
                /* TODO: Set to SerialVersion.UNKNOWN to catch errors. */
                serialVersion = SerialVersion.CURRENT;
                exceptionSummary = null;
            } finally {
                semaphore.release();
            }
        }

        /**
         * Returns the serial version number used to serialize requests to this
         * request handler.
         */
        private short getSerialVersion() {
            return serialVersion;
        }

        /**
         * Returns true if the ref is not currently resolved. This may be
         * because the reference has an exception associated with it or simply
         * because it was never referenced before.
         */
        private boolean needsResolution() {
            return (requestHandler == null);
        }

        /**
         * Returns true if the ref has a pending exception associated with it
         * and the reference needs to be resolved before before it can be used.
         *
         * Note that it's read outside the semaphore and that's ok, we just
         * need an approximate answer.
         */
        private boolean needsRepair() {
            return (exceptionSummary != null);
        }

        /**
         * Resolves the remote reference based upon its Topology in
         * RegistryUtils.
         *
         * @param registryUtils used to resolve the remote reference
         *
         * @param timeoutMs the amount of time to be spent trying to resolve
         * the handler
         *
         * @return the handle or null if one could not be established due to
         * a timeout, a remote exception, etc.
         *
         * @throws InterruptedException
         */
        private RequestHandlerAPI resolve(RegistryUtils registryUtils,
                                          long timeoutMs)
            throws InterruptedException {

            {   /* Try getting it contention-free first. */
                RequestHandlerAPI refNonVloatile = requestHandler;
                if (refNonVloatile != null) {
                    return refNonVloatile;
                }
            }

            boolean acquired =
                semaphore.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS);

            if (!acquired) {
                return null;
            }

           try {
                if (requestHandler != null) {
                    return requestHandler;
                }

                try {
                    requestHandler = registryUtils.getRequestHandler(rnId);

                    /*
                     * There is a possibility that the requested RN is not yet
                     * in the topology. In this case requestHandler will be
                     * null. This can happen during some elasticity operations.
                     */
                    if (requestHandler == null) {
                        noteExceptionInternal(
                              new IllegalArgumentException(rnId +
                                                           " not in topology"));
                        return null;
                    }
                    serialVersion = requestHandler.getSerialVersion();
                    exceptionSummary = null;
                    return requestHandler;
                } catch (RemoteException e) {
                    noteExceptionInternal(e);
                } catch (NotBoundException e) {
                    /*
                     * The service has not yet appeared in the registry, it may
                     * be down, or is in the process of coming up.
                     */
                    noteExceptionInternal(e);
                }

                return null;
            } finally {
                semaphore.release();
            }
        }

        /**
         * Returns a remote reference to the RN that can be used to make remote
         * method calls. It will try resolve the reference if it has never been
         * resolved before.
         *
         * @param registryUtils is used to resolve "never been resolved before"
         * references
         * @param timeoutMs the amount of time to be spent trying to resolve
         * the handler
         *
         * @return a remote reference to the RN's request handler, or null.
         * @throws InterruptedException
         */
        private RequestHandlerAPI get(RegistryUtils registryUtils,
                                      long timeoutMs)
            throws InterruptedException {

            final RequestHandlerAPI refNonVolatile = requestHandler;

            if (refNonVolatile != null) {
                /* The fast path. NO synchronization. */
                return refNonVolatile;
            }

            /*
             * Never been resolved, resolve it.
             */
            if (exceptionSummary == null) {
                return resolve(registryUtils, timeoutMs);
            }
            return null;
        }

        /**
         * Makes note of the exception encountered during the execution of a
         * request. This RN is removed from consideration until the background
         * thread resolves it once again.
         * @throws InterruptedException
         */
        private void noteException(Exception e) throws InterruptedException {
            semaphore.acquire();

            try {
               noteExceptionInternal(e);
            } finally {
                semaphore.release();
            }
        }

        /**
         * Assumes that the caller has already set the semaphore
         */
        private void noteExceptionInternal(Exception e) {
            assert semaphore.availablePermits() == 0;

            requestHandler = null;
            if (exceptionSummary == null) {
                exceptionSummary = new ExceptionSummary();
            }
            exceptionSummary.noteException(e);
        }
    }

    /**
     * Tracks errors encountered when trying to create the reference.
     */
    private static class ExceptionSummary {

        /**
         * Track errors resulting from attempts to create a remote reference.
         * If the value is > 0 then exception must be non null and denotes the
         * last exception that resulted in the reference being unresolved.
         */
        private int errorCount = 0;
        private Exception exception = null;
        private long exceptionTimeMs = 0;

        private void noteException(Exception e) {
            exception = e;
            errorCount++;
            exceptionTimeMs = System.currentTimeMillis();
        }

        @SuppressWarnings("unused")
        private Exception getException() {
            return exception;
        }

        @SuppressWarnings("unused")
        private long getExceptionTimeMs() {
            return exceptionTimeMs;
        }

        @SuppressWarnings("unused")
        private int getErrorCount() {
            return errorCount;
        }
    }

    /**
     * Returns true if the version based consistency requirements can be
     * satisfied at time <code>timeMs</code> based upon the historical rate of
     * progress associated with the node.
     *
     * @param timeMs the time at which the consistency requirement is to be
     * satisfied
     *
     * @param consistency the version consistency requirement
     */
    boolean inConsistencyRange(long timeMs, Consistency.Version consistency) {
        assert consistency != null;

        final VLSN consistencyVLSN =
            new VLSN(consistency.getVersion().getVLSN());

        return vlsnState.vlsnAt(timeMs).compareTo(consistencyVLSN) >= 0;
    }

    /**
     * Returns true if the time based consistency requirements can be satisfied
     * at time <code>timeMs</code> based upon the historical rate of progress
     * associated with the node.
     *
     * @param timeMs the time at which the consistency requirement is to be
     * satisfied
     *
     * @param consistency the time consistency requirement
     */
    boolean inConsistencyRange(long timeMs,
                               Consistency.Time consistency,
                               RepNodeState master) {
        assert consistency != null;

        if (master == null) {
            /*
             * No master information, assume it is. The information will get
             * updated as a result of the request directed to it.
             */
            return true;
        }

        final long lagMs =
                consistency.getPermissibleLag(TimeUnit.MILLISECONDS);
        final VLSN consistencyVLSN = master.vlsnState.vlsnAt(timeMs - lagMs);
        return vlsnState.vlsnAt(timeMs).compareTo(consistencyVLSN) >= 0;
    }

    /**
     * VLSNState encapsulates the handling of all VLSN state associated with
     * this node.
     * <p>
     * The VLSN state is typically updated based upon status
     * information returned in a {@link Response}. It's read during a request
     * dispatch for requests that specify a consistency requirement to help
     * direct the request to a node that's in a good position to satisfy the
     * constraint.
     * <p>
     * The state is also updated by {@link RepNodeStateUpdateThread} on a
     * periodic basis through the use of {@link NOP} requests.
     */
    private static class VLSNState {

        /**
         *  The last known vlsn
         */
        private volatile VLSN vlsn;

        /**
         * The time at which the vlsn above was last updated.
         */
        private long lastUpdateMs;

        /**
         * The starting vlsn used to compute the next iteration of the
         * vlsnsPerSec.
         *
         * Invariant: intervalStart <= vlsn
         */
        private VLSN intervalStart;

        /**
         *  The time at which the intervalStart was updated.
         *
         *  Invariant: intervalStartMs <= lastUpdateMs
         */
        private long intervalStartMs;

        /* The vlsn rate: vlsns/sec */
        private long vlsnsPerSec;

        private final int rateIntervalMs;

        private VLSNState(int rateIntervalMs) {
            vlsn = VLSN.NULL_VLSN;
            intervalStart = VLSN.NULL_VLSN;
            intervalStartMs = 0l;
            vlsnsPerSec = 0;
            this.rateIntervalMs = rateIntervalMs;
        }

        /**
         * Returns the current VLSN associated with this RN
         */
        private VLSN getVLSN() {
           return vlsn;
        }

        /**
         * Returns true if the state information has not been updated over an
         * interval exceeding MAX_RATE_INTERVAL_MS
         */
        private synchronized boolean isObsolete() {
            return (lastUpdateMs + rateIntervalMs)  <
                    System.currentTimeMillis();
        }

        /**
         * Updates the VLSN and maintains the vlsn rate associated with it.
         *
         * @param newVLSN the new VLSN
         */
        private synchronized void updateVLSN(VLSN newVLSN) {

            if ((newVLSN == null) || newVLSN.isNull()) {
                return;
            }

            lastUpdateMs = System.currentTimeMillis();
            if  (newVLSN.compareTo(vlsn) > 0) {
                vlsn = newVLSN;
            }

            final long intervalMs = (lastUpdateMs - intervalStartMs);
            if (intervalMs <= rateIntervalMs) {
                return;
            }

            if (intervalStartMs == 0) {
                resetRate(lastUpdateMs);
                return;
            }

            final long vlsnDelta = (vlsn.getSequence() -
                                    intervalStart.getSequence());
            if (vlsnDelta < 0) {
                /* Possible hard recovery */
                resetRate(lastUpdateMs);
            } else {
                intervalStart = vlsn;
                intervalStartMs = lastUpdateMs;
                vlsnsPerSec = (vlsnDelta * 1000) / intervalMs;
            }
        }

        /**
         * Returns the VLSN that the RN will have progressed to at
         * <code>timeMs</code> based upon its current known state and its rate
         * of progress.
         *
         * @param timeMs the time in ms
         *
         * @return the predicted VLSN at timeMs or NULL_VLSN if the state has
         * not yet been initialized.
         */
        private synchronized VLSN vlsnAt(long timeMs) {
            if (vlsn.isNull()) {
                return VLSN.NULL_VLSN;
            }

            final long deltaMs = (timeMs - lastUpdateMs);
            /* deltaMs can be negative. */

            final long vlsnAt = vlsn.getSequence() +
                            ((deltaMs * vlsnsPerSec) / 1000);

            /*
             * Return the zero VLSN if the extrapolation yields a negative
             * value since vlsns cannot be negative.
             */
            return vlsnAt < 0 ? new VLSN(0) : new VLSN(vlsnAt);
        }

        private void resetRate(long now) {
            intervalStart = vlsn;
            intervalStartMs = now;
            vlsnsPerSec = 0;
        }
    }
}
