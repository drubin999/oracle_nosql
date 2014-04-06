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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.Set;

import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.FaultException;
import oracle.kv.impl.api.ops.InternalOperation;
import oracle.kv.impl.api.ops.NOP;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.fault.TTLFaultException;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.topo.change.TopologyChange;
import oracle.kv.impl.util.SerialVersion;

/**
 * A request is issued either between Rep Nodes in the KV Store or from a
 * client to a Rep Node.  The request carries an operation to be performed.
 */
public class Request implements Externalizable {

    private static final long serialVersionUID = 1;

    /**
     * Used during testing: A non-zero value specifies the current serial
     * version, which acts as the maximum value for a request's serial version.
     */
    private static volatile short testCurrentSerialVersion;

    /**
     * The serialization version of the request and all its sub-objects.
     */
    private short serialVersion;

    /**
     * The API operation to execute.
     */
    private InternalOperation op;

    /**
     * The partition ID is used to determine and validate the destination rep
     * group.
     */
    private PartitionId partitionId;

    /**
     * The replication group ID is used to determine and validate the
     * destination. If equal to RepGroupId.NULL_ID the partitionId is used.
     *
     * Introduced in SerialVersion.V4.
     */
    private RepGroupId repGroupId;

    /**
     * Whether the request performs write, versus read, operations.
     */
    private boolean write;

    /**
     * The durability of a write request, null for a read request.
     */
    private Durability durability;

    /**
     * The consistency of a read request, null for a write request.
     */
    private Consistency consistency;

    /**
     * The sequence number of the topology {@link Topology#getSequenceNumber}
     * used as the basis for routing the request.
     */
    private int topoSeqNumber;

    /**
     * Identifies the original request dispatcher making the request. It's not
     * updated through forwards of the request.
     */
    private ResourceId initialDispatcherId;

    /**
     * The "time to live" associated with the request. It's decremented each
     * time the message is forwarded. Messages may be forwarded across rep
     * groups (in case of obsolete partition information) and within a rep
     * group in the absence of current "master" information.
     */
    private int ttl;

    /**
     * The RNS through which the request was forwarded in an attempt to locate
     * a master. The number of such forwards cannot exceed the size of an RG.
     * Each element in the array contains the "group relative" nodeNum of the
     * RepNodeId. This array is used to ensure that there are no forwarding
     * loops within an RG.
     */
    private byte[] forwardingRNs = new byte[0];

    /**
     * The timeout associated with the request. It bounds the maximum time
     * taken to execute the request and does not include request transmission
     * times.
     */
    private int timeoutMs;

    /**
     * The AuthContext associated with the request
     */
    private AuthContext authCtx;

    /**
     * The IDs of the zones that can be used for reads, or null if not
     * restricted.  Introduced with {@link SerialVersion#V4}.
     */
    private int[] readZoneIds = null;

    /**
     * Creates a partition based request. The target of this request is
     * specified by the partition ID.
     * <p>
     * The topoSeqNumber param is used to determine the {@link TopologyChange
     * topology changes}, if any, that must be returned back in the response.
     * <p>
     * The remoteRequestHandlerId is used to determine the rep group state
     * changes, if any, that need to be returned back as part of the response.
     * <p>
     * @param op the operation to be performed
     * @param partitionId the target partition
     * @param topoSeqNumber identifies the topology used as the basis for the
     * request
     * @param dispatcherId identifies the invoking
     * {@link RequestDispatcher}
     * @param timeoutMs the maximum number of milliseconds that should be used
     * to execute the request
     * @param readZoneIds the IDs of the zones that can be used for reads, or
     * {@code null} for writes or unrestricted read requests
     */
    public Request(InternalOperation op,
                   PartitionId partitionId,
                   boolean write,
                   Durability durability,
                   Consistency consistency,
                   int ttl,
                   int topoSeqNumber,
                   ResourceId dispatcherId,
                   int timeoutMs,
                   int[] readZoneIds) {
        this(op, partitionId, RepGroupId.NULL_ID, write,
             durability, consistency, ttl, topoSeqNumber,
             dispatcherId, timeoutMs, readZoneIds);
    }

    /**
     * Creates a group based request. The target of this request is specified
     * by the replication group ID.
     * <p>
     * The topoSeqNumber param is used to determine the {@link TopologyChange
     * topology changes}, if any, that must be returned back in the response.
     * <p>
     * The remoteRequestHandlerId is used to determine the rep group state
     * changes, if any, that need to be returned back as part of the response.
     * <p>
     * @param op the operation to be performed
     * @param repGroupId the target rep group
     * @param topoSeqNumber identifies the topology used as the basis for the
     * request
     * @param dispatcherId identifies the invoking {@link RequestDispatcher}
     */
    public Request(InternalOperation op,
                   RepGroupId repGroupId,
                   boolean write,
                   Durability durability,
                   Consistency consistency,
                   int ttl,
                   int topoSeqNumber,
                   ResourceId dispatcherId,
                   int timeoutMs,
                   int[] readZoneIds) {
        this(op, PartitionId.NULL_ID, repGroupId, write,
             durability, consistency, ttl, topoSeqNumber,
             dispatcherId, timeoutMs, readZoneIds);
        assert !repGroupId.isNull();
    }

    private Request(InternalOperation op,
                    PartitionId partitionId,
                    RepGroupId repGroupId,
                    boolean write,
                    Durability durability,
                    Consistency consistency,
                    int ttl,
                    int topoSeqNumber,
                    ResourceId dispatcherId,
                    int timeoutMs,
                    int[] readZoneIds) {
        assert (op != null);
        assert (durability != null) != (consistency != null);
        assert write == (durability != null);
        assert !write || (readZoneIds == null)
            : "Read zones should only be specified for read requests";

        this.serialVersion = SerialVersion.UNKNOWN;
        this.op = op;
        this.partitionId = partitionId;
        this.repGroupId = repGroupId;
        this.write = write;
        this.durability = durability;
        this.consistency = consistency;
        this.ttl = ttl;
        this.topoSeqNumber = topoSeqNumber;
        this.initialDispatcherId = dispatcherId;
        this.timeoutMs = timeoutMs;
        this.readZoneIds = readZoneIds;
    }

    /**
     * Factory method for creating NOP requests.
     */
    public static Request createNOP(int topoSeqNumber,
                                    ResourceId dispatcherId,
                                    int timeoutMs) {

        return new Request(new NOP(),
                           PartitionId.NULL_ID,
                           false,
                           null,
                           Consistency.NONE_REQUIRED,
                           1,
                           topoSeqNumber,
                           dispatcherId,
                           timeoutMs,

                           /*
                            * NOP requests are not generated by users, so they
                            * ignore read zones restrictions.
                            *
                            * TODO: Consider restricting NOP requests from
                            * being sent to RNs outside of the specified read
                            * zones.
                            */
                           null);
    }

    /* for Externalizable */
    public Request() {
    }

    @Override
    public void readExternal(ObjectInput in)
        throws IOException {

        serialVersion = in.readShort();
        partitionId = new PartitionId(in.readInt());

        /*
         * A pre-V4 request will not have a group ID. In this case set it
         * to a null ID.
         */
        repGroupId =
              (serialVersion < SerialVersion.V4) ? RepGroupId.NULL_ID :
                                                   new RepGroupId(in.readInt());

        if (!repGroupId.isNull() && !partitionId.isNull()) {
            throw new IllegalStateException("Both partition ID and group ID " +
                                            "are non-null");
        }
        write = ((in.readByte() != 0) ? true : false);
        if (write) {
            durability = new Durability(in, serialVersion);
            consistency = null;
        } else {
            durability = null;
            consistency = Consistency.readFastExternal(in, serialVersion);
        }
        ttl = in.readInt();

        final byte asize = in.readByte();
        forwardingRNs = new byte[asize];
        for (int i = 0; i < asize; i++) {
            forwardingRNs[i] = in.readByte();
        }

        timeoutMs = in.readInt();
        topoSeqNumber = in.readInt();
        initialDispatcherId = ResourceId.readFastExternal(in, serialVersion);
        op = InternalOperation.readFastExternal(in, serialVersion);

        /*
         * For V4 and later, read the number of restricted read zones followed
         * by that number of zone IDs, with 0 meaning no restriction.  Also,
         * write the AuthContext object information.
         */
        if (serialVersion >= SerialVersion.V4) {
            final int len = in.readInt();
            if (len == 0) {
                readZoneIds = null;
            } else {
                readZoneIds = new int[len];
                for (int i = 0; i < len; i++) {
                    readZoneIds[i] = in.readInt();
                }
            }

            if (in.readByte() != 0) {
                authCtx = new AuthContext(in, serialVersion);
            } else {
                authCtx = null;
            }
        }
    }

    @Override
    public void writeExternal(ObjectOutput out)
    throws IOException {

        assert serialVersion != SerialVersion.UNKNOWN;

        /*
         * Verify that the server can handle this operation.
         */
        short requiredVersion = op.getOpCode().requiredVersion();
        if (requiredVersion > serialVersion) {
            throw new UnsupportedOperationException
                ("Attempting an operation that is not supported by " +
                 "the server version.  Server version is " + serialVersion +
                 ", required version is " + requiredVersion +
                 ", operation is " + op);
        }

        out.writeShort(serialVersion);
        out.writeInt(partitionId.getPartitionId());

        /* The group ID was introduced in V4 */
        if (serialVersion >= SerialVersion.V4) {
            out.writeInt(repGroupId.getGroupId());
        } else {
            if (!repGroupId.isNull()) {
                throw new IllegalStateException("Attempting to write a newer " +
                                                "request with unsupported " +
                                                " serial version: " +
                                                serialVersion);
            }
        }
        out.writeByte(write ? 1 : 0);
        if (write) {
            durability.writeFastExternal(out, serialVersion);
        } else {
            consistency.writeFastExternal(out, serialVersion);
        }
        out.writeInt(ttl);

        out.writeByte(forwardingRNs.length);
        for (byte forwardingRN : forwardingRNs) {
            out.writeByte(forwardingRN);
        }

        out.writeInt(timeoutMs);
        out.writeInt(topoSeqNumber);
        initialDispatcherId.writeFastExternal(out, serialVersion);
        op.writeFastExternal(out, serialVersion);

        if (serialVersion >= SerialVersion.V4) {

            /*
             * For V4 and later, write the number of restricted read zones
             * followed by that number of zone IDs, with 0 meaning no
             * restriction
             */
            if (readZoneIds == null) {
                out.writeInt(0);

            } else {
                out.writeInt(readZoneIds.length);
                for (final int znId : readZoneIds) {
                    out.writeInt(znId);
                }
            }

            /*
             * for V4 and later, write AuthContext information
             */
            if (authCtx == null) {
                out.writeByte(0);
            } else {
                out.writeByte(1);
                authCtx.writeFastExternal(out, serialVersion);
            }
        } else if (readZoneIds != null) {
            throw new OperationFaultException(
                "The store configuration specifies read zones, but read" +
                " zones are not supported by the target replication node");
        }
    }

    public InternalOperation getOperation() {
        return op;
    }

    /**
     * Indicates if the request must be performed on the master.
     *
     * @return true if the request must be run on the master, false otherwise
     */
    public boolean needsMaster() {
        return isWrite() || (getConsistency() == Consistency.ABSOLUTE);
    }

    /**
     * Indicates if the request must be performed on a replica rather
     * than the master.
     *
     * @return true if the request must be run on a replica, false otherwise
     */
    public boolean needsReplica() {
        return !isWrite() &&
               (getConsistency() == Consistency.NONE_REQUIRED_NO_MASTER);
    }

    /**
     * Returns True if the request writes to the environment.
     */
    public boolean isWrite() {
        return write;
    }

    /**
     * Returns the durability associated with a write request, or null for a
     * read request.
     */
    public Durability getDurability() {
        return durability;
    }

    /**
     * Returns the consistency associated with a read request, or null for a
     * write request.
     */
    public Consistency getConsistency() {
        return consistency;
    }

    /**
     * Must be called before serializing the request by passing it to a remote
     * method.
     */
    public void setSerialVersion(short serialVersion) {

        /* Limit the serial version to testCurrentSerialVersion, if set */
        if ((testCurrentSerialVersion != 0) &&
            (serialVersion > testCurrentSerialVersion)) {
            serialVersion = testCurrentSerialVersion;
        }
        this.serialVersion = serialVersion;
    }

    /**
     * Set the current serial version to a different value, for testing.
     * Specifying {@code 0} reverts to the standard value.
     */
    public static void setTestSerialVersion(final short testSerialVersion) {
        testCurrentSerialVersion = testSerialVersion;
    }

    /**
     * Returns a value used to construct a matching response for this request,
     * so that the client receives a response serialized with the same version
     * as the request.
     */
    short getSerialVersion() {
        return serialVersion;
    }

    /**
     * Returns the partition ID associated with the request.
     */
    public PartitionId getPartitionId() {
        return partitionId;
    }

    public RepGroupId getRepGroupId() {
        return repGroupId;
    }

    public int getTopoSeqNumber() {
        return topoSeqNumber;
    }

    public void setTopoSeqNumber(int topoSeqNumber) {
        this.topoSeqNumber = topoSeqNumber;
    }

    public ResourceId getInitialDispatcherId() {
        return initialDispatcherId;
    }

    public int getTTL() {
        return ttl;
    }

    public void decTTL() throws FaultException {
        if (ttl-- == 0) {
            throw new TTLFaultException
                ("TTL exceeded for request: " + getOperation() +
                 " request dispatched by: " + getInitialDispatcherId());
        }
    }

    public int getTimeout() {
        return timeoutMs;
    }

    public void setTimeout(int timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    @Override
    public String toString() {
        return op.toString();
    }

    /**
     * Clears out the RNs in the forwarding chain. This typically done before
     * the request is forwarded to a node in a different group.
     */
    public void clearForwardingRNs() {
        forwardingRNs = new byte[0];
    }

    /**
     * Returns the set of RNIds that forwarded this request.
     */
    public Set<RepNodeId> getForwardingRNs(int rgid) {
        final HashSet<RepNodeId> forwardingRNIds = new HashSet<RepNodeId>();

        for (int nodeNum : forwardingRNs) {
            forwardingRNIds.add(new RepNodeId(rgid, nodeNum));
        }

        return forwardingRNIds;
    }

    /**
     * Updates the list of forwarding RNs, if the current dispatcher is an RN.
     */
    public void updateForwardingRNs(ResourceId currentDispatcherId,
                                    int groupId) {

        if (!currentDispatcherId.getType().isRepNode()) {
            return;
        }

        assert currentDispatcherId.getType().isRepNode();

        final RepNodeId repNodeId = (RepNodeId) currentDispatcherId;

        if (repNodeId.getGroupId() == groupId) {
            /* Add this RN to the list. */
            final byte[] updateList = new byte[forwardingRNs.length + 1];
            System.arraycopy(forwardingRNs, 0,
                             updateList, 0, forwardingRNs.length);
            assert repNodeId.getNodeNum() < 128;
            updateList[forwardingRNs.length] = (byte) repNodeId.getNodeNum();
            forwardingRNs = updateList;
        } else {
            /* Forwarding outside the group. */
            forwardingRNs = new byte[0];
        }
    }

    /**
     * Returns true if the request was initiated by the
     * <code>resourceId</code>.
     */
    public boolean isInitiatingDispatcher(ResourceId resourceId) {

        return initialDispatcherId.equals(resourceId);
    }

    /**
     * Set the security context in preparation for execution.
     */
    public void setAuthContext(AuthContext useAuthCtx) {
        this.authCtx = useAuthCtx;
    }

    /**
     * Get the security context 
     */
    public AuthContext getAuthContext() {
        return authCtx;
    }

    /**
     * Returns the IDs of the zones that can be used for read operations, or
     * {@code null} if not restricted.
     *
     * @return the zone IDs or {@code null}
     */
    public int[] getReadZoneIds() {
        return readZoneIds;
    }

    /**
     * Checks if an RN in a zone with the specified zone ID can be used for
     * this request.
     *
     * @param znId the zone ID or {@code null} if not known
     * @return whether an RN in the specified zone can be used
     */
    public boolean isPermittedZone(final DatacenterId znId) {
        if (write || (readZoneIds == null)) {
            return true;
        }
        if (znId == null) {
            return false;
        }
        final int znIdInt = znId.getDatacenterId();
        for (int elem : readZoneIds) {
            if (elem == znIdInt) {
                return true;
            }
        }
        return false;
    }
}
