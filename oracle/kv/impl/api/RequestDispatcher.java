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

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Map;
import java.util.Set;

import oracle.kv.FaultException;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.rgstate.RepGroupStateTable;
import oracle.kv.impl.api.rgstate.RepNodeState;
import oracle.kv.impl.rep.RepNodeService;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.utilint.Latency;

/**
 * The RemoteRequestDispatcher is responsible for dispatching requests over the
 * network to a suitable RN, where they are handled by the
 * {@link RequestHandler}. The KV client contains an instance of the
 * RequestDispatcher, as does each RN.
 */
public interface RequestDispatcher {

    /**
     * Dispatches the request to a suitable RN based upon the contents of the
     * request where its either executed, or forwarded on to some other RN via
     * the RemoteRequestDispatcher at that RN if the targeted RN proved to be
     * unsuitable.
     * <p>
     * Note that all attempts to recover, e.g. work around some RMI or
     * networking issue, retry the request at different RN, etc. are performed
     * by the execute method. If all such attempts to recover fail, an
     * appropriate {@link FaultException} is thrown and typically propagated
     * all the way back to the KV client request invoker.
     *
     * @param request the request that must be processed remotely at some RN
     * @param excludeRNs the set of RNs to exclude from consideration for the
     * dispatch
     * @param loginMgr a null-allowable login manager for the request
     * @return the Response associated with the Request
     *
     * @throws FaultException if the requested execution failed
     */
    public Response execute(Request request,
                            Set<RepNodeId> excludeRNs,
                            LoginManager loginMgr)
        throws FaultException;

    /**
     * Overloading of the above when no RNs are being excluded.
     *
     * @see #execute(Request, Set, LoginManager)
     */
    public Response execute(Request request, LoginManager loginMgr)
        throws FaultException;

    /**
     * Executes an internal  NOP request at the target node
     *
     * @param targetRN the node when the NOP is to be executed
     * @param timeoutMs the timeout associated with the request.
     * @param loginMgr the null-allowable login manager which which to
     *        authenticate the call
     *
     * @return the response to the NOP request or null if no RMI handle to the
     * RN was currently available.
     *
     * @throws Exception if the request could not be satisfied
     */
    public Response executeNOP(RepNodeState targetRN,
                               int timeoutMs,
                               LoginManager loginMgr)
        throws Exception;

    /**
     * Returns the resource id associated with the remote request dispatcher.
     */
    public ResourceId getDispatcherId();

    /**
     * Returns the topology manager used as the basis for dispatching requests
     * and maintaining the Topology
     */
    public TopologyManager getTopologyManager();

    /**
     * Returns the {@link RepGroupStateTable associated} with the
     * RemoteRequestDispatcher.
     */
    public RepGroupStateTable getRepGroupStateTable();

    /**
     * Returns the Id of the partition associated with a given key.
     * Convenience method.
     */
    public PartitionId getPartitionId(byte[] keyBytes);

    /**
     * Shuts down the request dispatcher. The argument is non-null if it's
     * an abnormal shutdown.
     */
    public void shutdown(Throwable exception);

    /**
     * Returns the registry utils currently associated with this dispatcher.
     * Note that the instance could change with changes in Topology
     */
    public RegistryUtils getRegUtils();

    /**
     * Returns a snapshot of the operation latency stats associated with the
     * request dispatcher.
     *
     * @param clear if true clears the stats
     */
    public Map<OpCode, Latency> getLatencyStats(boolean clear);

    /**
     * The total number of requests that were retried by the request dispatcher.
     */
    public long getTotalRetryCount(boolean clear);

    /**
     * Returns the dispatchers exception handler. An exception caught by this
     * handler results in the process being restarted by the SNA.
     *
     * @see RepNodeService.ThreadExceptionHandler
     *
     * @return the dispatchers exception handler
     */
    @SuppressWarnings("javadoc")
    public UncaughtExceptionHandler getExceptionHandler();

    /**
     * Updates the current login manager, which is used for topology access,
     * NOP execution, and RequestHandler resolution.  It is not used for
     * general Request execution. A null value may be specified, which is fine
     * for a non-secure KVStore, but will prevent further Topology access in
     * a secure install until a non-null LoginManager is supplied.
     */
    public void setRegUtilsLoginManager(LoginManager loginMgr);

    /**
     * Returns the IDs of the zones that can be used for read operations, or
     * {@code null} if not restricted.
     *
     * @return the zone IDs or {@code null}
     */
    public int[] getReadZoneIds();
}
