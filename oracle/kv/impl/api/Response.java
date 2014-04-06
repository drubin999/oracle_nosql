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

import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;

import com.sleepycat.je.utilint.VLSN;

/**
 * The Response to a remote execution via the {@link RequestDispatcher}.
 * The response contains the request result and the following optional
 * components:
 * <ol>
 * <li> Topology changes. If the topology sequence number in the request is
 * less than the sequence number at the request target.
 * </li>
 * <li> Rep Group status update information. If the request changes has
 * group level status changes that it has not yet communicated back to the
 * request originator. </li>
 * </ol>
 */
public class Response implements Externalizable {

    private static final long serialVersionUID = 1L;

    /* The responder's rep node id. */
    private RepNodeId repNodeId;

    /* The current VLSN associated with the responder. */
    private VLSN vlsn;
    private Result result;
    private TopologyInfo topoInfo;
    private StatusChanges statusChanges;
    private short serialVersion;

    public Response(RepNodeId repNodeId,
                    VLSN vlsn,
                    Result result,
                    TopologyInfo topoInfo,
                    StatusChanges statusChanges,
                    short serialVersion) {

        this.repNodeId = repNodeId;
        this.vlsn = vlsn;
        this.result = result;
        this.topoInfo = topoInfo;
        this.statusChanges = statusChanges;
        this.serialVersion = serialVersion;
    }

    /* for Externalizable */
    public Response() {
    }

    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException {

        serialVersion = in.readShort();

        /*
         * The serialized form supports ResourceIds of different subclasses,
         * but here we only allow a RepNodeId.
         */
        repNodeId = (RepNodeId) ResourceId.readFastExternal(in, serialVersion);

        vlsn = new VLSN(in.readLong());

        result = Result.readFastExternal(in, serialVersion);

        /*
         * If there are no changes, statusChanges and topoChanges will be null.
         * Although we are using 'slow' serialization for these objects, when
         * they are null (the frequent case) this is very cheap.  We rely on
         * Topology.getChanges and RequestHandlerImpl.getStatusChanges to
         * return null (not an empty object) when there are no changes.
         */
        statusChanges = (StatusChanges) in.readObject();
        topoInfo = (TopologyInfo) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out)
        throws IOException {

        out.writeShort(serialVersion);
        repNodeId.writeFastExternal(out, serialVersion);
        out.writeLong(vlsn.getSequence());
        result.writeFastExternal(out, serialVersion);
        out.writeObject(statusChanges);
        out.writeObject(topoInfo);
    }

    /**
     * Must be called before returning a forwarded response, to reset the
     * version to the one specified by the client.
     */
    void setSerialVersion(short serialVersion) {
        this.serialVersion = serialVersion;
    }

    /**
     * Returns the effective serial version.
     */
    public short getSerialVersion() {
        return serialVersion;
    }

    /**
     * Returns the result associated with a request. This result can be
     * returned back to the invoking client.
     */
    public Result getResult() {
        return result;
    }

    /**
     * Returns information about the topology of the responding node or null if
     * the topology at the two nodes is the same.
     */
    public TopologyInfo getTopoInfo() {
        return topoInfo;
    }

    /**
     * Returns any status updates provided by the RN that actually responded
     * to the request. This information is used to update the RGST and the
     * partition map at RNs and KV clients.
     */
    public StatusChanges getStatusChanges() {
        return statusChanges;
    }

    public VLSN getVLSN() {
        return vlsn;
    }

    /**
     * Returns RN that responded to the request. The responding RN may be
     * different from the one to which the request was directed, if the
     * request was forwarded.
     *
     * @return the RepGroupId associated with the responding RN
     */
    public RepNodeId getRespondingRN() {
        return repNodeId;
    }

    /**
     * Sets the status changes that must be associated with the response.
     * @param statusChanges
     */
    public void setStatusChanges(StatusChanges statusChanges) {
        this.statusChanges = statusChanges;
    }

    /**
     * Sets the topology information that must be associated with the response.
     * @param topoInfo
     */
    public void setTopoInfo(TopologyInfo topoInfo) {
        this.topoInfo = topoInfo;
    }
}
