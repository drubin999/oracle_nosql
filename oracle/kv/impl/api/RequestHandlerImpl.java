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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.Consistency;
import oracle.kv.ConsistencyException;
import oracle.kv.DurabilityException;
import oracle.kv.FaultException;
import oracle.kv.KVSecurityException;
import oracle.kv.KVStoreException;
import oracle.kv.RequestTimeoutException;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.ops.OperationHandler;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.api.rgstate.RepGroupState;
import oracle.kv.impl.api.rgstate.RepGroupStateTable;
import oracle.kv.impl.api.rgstate.RepNodeState;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.fault.RNUnavailableException;
import oracle.kv.impl.fault.SystemFaultException;
import oracle.kv.impl.rep.IncorrectRoutingException;
import oracle.kv.impl.rep.OperationsStatsTracker;
import oracle.kv.impl.rep.RepEnvHandleManager.StateChangeListenerFactory;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.RepNodeService;
import oracle.kv.impl.rep.migration.MigrationStreamHandle;
import oracle.kv.impl.security.AccessChecker;
import oracle.kv.impl.security.ExecutionContext;
import oracle.kv.impl.security.KVStoreRolePrincipal;
import oracle.kv.impl.security.OperationContext;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.Partition;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConsistencyTranslator;
import oracle.kv.impl.util.DurabilityTranslator;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RMISocketPolicy;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryPair;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.registry.RegistryUtils.InterfaceType;
import oracle.kv.impl.util.registry.VersionedRemoteImpl;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.OperationFailureException;
import com.sleepycat.je.ReplicaConsistencyPolicy;
import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.DatabasePreemptedException;
import com.sleepycat.je.rep.InsufficientAcksException;
import com.sleepycat.je.rep.InsufficientReplicasException;
import com.sleepycat.je.rep.LockPreemptedException;
import com.sleepycat.je.rep.MasterReplicaTransitionException;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicaConsistencyException;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.RollbackException;
import com.sleepycat.je.rep.RollbackProhibitedException;
import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.StateChangeListener;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.utilint.VLSN;
import com.sleepycat.utilint.StatsTracker;

/**
 * @see RequestHandler
 */
@SuppressWarnings("deprecation")
public class RequestHandlerImpl
    extends VersionedRemoteImpl implements RequestHandler {

    /**
     * Amount of time to wait after a lock conflict. Since we order all
     * multi-key access in the KV Store by key, the only source of lock
     * conflicts should be on the Replica as {@link LockPreemptedException}s.
     */
    private static final int LOCK_CONFLICT_RETRY_NS = 100000000;

    /**
     * Amount of time to wait after an environment failure. The wait allows
     * some time for the environment to restart, partition databases to be
     * re-opened, etc.  This is also the amount of time used to wait when
     * getting the environment, so the effective wait for the environment is
     * twice this value.
     */
    private static final int ENV_RESTART_RETRY_NS = 100000000;

    /**
     * An empty, immmutable role list, used when an operation is requested that
     * does not require authentication.
     */
    private static final List<KVStoreRolePrincipal> emptyRoleList =
        Collections.emptyList();

    /**
     * An immmutable role list, used when an operation is requested that
     * requires authentication.
     */
    private static final List<KVStoreRolePrincipal> authenticatedRoleList =
        Collections.unmodifiableList(
            Arrays.asList( new KVStoreRolePrincipal[] {
                    KVStoreRolePrincipal.AUTHENTICATED }));


    /**
     * The parameters used to configure the request handler.
     */
    private RepNodeService.Params params;

    /**
     * The rep node that will be used to satisfy all operations.
     */
    private RepNode repNode;

    /**
     *  Identifies the rep node servicing local requests.
     */
    private RepNodeId repNodeId;

    /**
     * Tracks the number of requests that are currently active, that is,
     * are being handled.
     */
    private final AtomicInteger activeRequests = new AtomicInteger(0);

    /**
     * All user operations directed to the requestHandler are implemented by
     * the OperationHandler.
     */
    private OperationHandler operationHandler;

    /**
     * The requestDispatcher used to forward requests that cannot be handled
     * locally.
     */
    private final RequestDispatcher requestDispatcher;

    /**
     * Mediates access to the shared topology.
     */
    private final TopologyManager topoManager;

    /**
     *  The table used to track the rep state of all rep nodes in the KVS
     */
    private final RepGroupStateTable stateTable;

    /**
     *  The last state change event communicated via the Listener.
     */
    private volatile StateChangeEvent stateChangeEvent;

    /**
     * The set of requesters that have been informed of the latest state change
     * event. The map is not currently bounded. This may become an issue if the
     * KVS is used in contexts where clients come and go on a frequent basis.
     * May want to clear out this map on some periodic basis as a defensive
     * measure.
     */
    private final Map<ResourceId, StateChangeEvent> requesterMap;

    /**
     * opTracker encapsulates all repNode stat recording.
     */
    private OperationsStatsTracker opTracker;

    /**
     * The amount of time to wait for the active requests to quiesce, when
     * stopping the request handler.
     */
    private int requestQuiesceMs;

    /**
     * The poll period used when quiescing requests.
     */
    private static final int REQUEST_QUIESCE_POLL_MS = 100;

    private final ProcessFaultHandler faultHandler;

    /**
     * Test to be used to during request handling to introduce request-specific
     * behavior.
     */
    private TestHook<Request> requestExecute;

    /**
     * The access checking implementation.  This will be null if security is
     * not enabled.
     */
    private final AccessChecker accessChecker;

    /**
     * Test hook to be invoked immediately before initiating a request
     * transaction commit.
     */
    private TestHook<RepImpl> preCommitTestHook;

    private final LogMessageAbbrev logAbbrev;

    private Logger logger = null;

    public RequestHandlerImpl(RequestDispatcher requestDispatcher,
                              ProcessFaultHandler faultHandler,
                              AccessChecker accessChecker) {
        super();

        this.requestDispatcher = requestDispatcher;
        this.faultHandler = faultHandler;
        this.topoManager = requestDispatcher.getTopologyManager();
        this.accessChecker = accessChecker;
        stateTable = requestDispatcher.getRepGroupStateTable();
        requesterMap =
            new ConcurrentHashMap<ResourceId, StateChangeEvent>();
        logAbbrev = new LogMessageAbbrev();
    }

    @SuppressWarnings("hiding")
    public void initialize(RepNodeService.Params params,
                           RepNode repNode,
                           OperationsStatsTracker opStatsTracker) {
        this.params = params;

        /* Get the rep node that we'll use for handling our requests. */
        this.repNode = repNode;
        opTracker = opStatsTracker;
        repNodeId = repNode.getRepNodeId();
        operationHandler = new OperationHandler(repNode, params);
        requestQuiesceMs = params.getRepNodeParams().getRequestQuiesceMs();
        logger = LoggerUtils.getLogger(this.getClass(), params);
    }

    /**
     * Returns the RepNode associated with this request handler.
     */
    public RepNode getRepNode() {
       return repNode;
    }

    /**
     * Returns the request dispatcher used to forward requests.
     */
    public RequestDispatcher getRequestDispatcher() {
        return requestDispatcher;
    }

    public StateChangeListenerFactory getListenerFactory() {
        return new StateChangeListenerFactory() {

            @Override
            public StateChangeListener create(ReplicatedEnvironment repEnv) {
                return new Listener(repEnv);
            }
        };
    }

    public void setTestHook(TestHook<Request> hook) {
        requestExecute = hook;
    }

    public void setPreCommitTestHook(TestHook<RepImpl> hook) {
        preCommitTestHook = hook;
    }

    /**
     * Apply security checks and execute the request
     */
    @Override
    public Response execute(final Request request)
        throws FaultException, RemoteException {
        
        return faultHandler.execute(new ProcessFaultHandler.
                                    SimpleOperation<Response>() {
            @Override
            public Response execute() {
                final ExecutionContext execCtx = checkSecurity(request);
                if (execCtx == null) {
                    return executeRequest(request);
                }

                return ExecutionContext.runWithContext(
                    new ExecutionContext.SimpleOperation<Response>() {
                        @Override
                        public Response run() {
                            return executeRequest(request);
                        }},
                    execCtx);
            }
        });
    }

    /**
     * Verify that the request is annotated with an appropriate access
     * token if security is enabled.  Only basic access checking is performed
     * at this level.  Table/column level access checks are implemented
     * at a deeper level.
     *
     * @throw SessionAccessException if there is an internal security error
     * @throw KVSecurityException if the a security exception is
     * generated by the requesting client
     */
    private ExecutionContext checkSecurity(Request request)
        throws SessionAccessException, KVSecurityException {
        if (accessChecker == null) {
            return null;
        }

        final RequestContext reqCtx = new RequestContext(request);
        return ExecutionContext.create(
            accessChecker, request.getAuthContext(), reqCtx);
    }

    /**
     * Executes the operation associated with the request.
     * <p>
     * All recoverable JE operations are handled at this level and retried
     * after appropriate corrective action. The implementation body comments
     * describe the correct action.
     * <p>
     */
    private Response executeRequest(final Request request) {

        activeRequests.incrementAndGet();

        try {
            final OpCode opCode = request.getOperation().getOpCode();

            if (OpCode.NOP.equals(opCode)) {
                return executeNOPInternal(request);
            } else if (topoManager.getTopology() != null) {
                return executeInternal(request);
            } else {

                /*
                 * The Topology is null. The node is waiting for one
                 * of the members in the rep group to push topology
                 * to it. Have the dispatcher re-direct the request to
                 * another node, or keep retrying until the topology
                 * is available.
                 */
                final String message = "awaiting topology push";
                throw new RNUnavailableException(message);
            }
        }  catch (ThreadInterruptedException  tie) {
            /* This node may be going down. */
            final String message =
                "RN: " + repNodeId + " was interrupted.";
            logger.info(message);
            /* Have the request dispatcher retry at a different node.*/
            throw new RNUnavailableException(message);
        } finally {
            activeRequests.decrementAndGet();
        }
    }

    /**
     * Wraps the execution of the request in a transaction and handles
     * any exceptions that might result from the execution of the request.
     */
    private Response executeInternal(Request request) {

        assert TestHookExecute.doHookIfSet(requestExecute, request);

        Response response = forwardIfRequired(request);
        if (response != null) {
            return response;
        }

        /* Can be processed locally. */
        OperationFailureException exception = null;
        final TransactionConfig txnConfig = setupTxnConfig(request);
        final long limitNs = System.nanoTime() +
            MILLISECONDS.toNanos(request.getTimeout());

        do {
            long sleepNs = 0;
            Transaction txn = null;
            MigrationStreamHandle streamHandle = null;
            ReplicatedEnvironment repEnv = null;
            final StatsTracker<OpCode> tracker = opTracker.getStatsTracker();
            final long startNs = tracker.markStart();
            try {

                final long getEnvTimeoutNs =
                    Math.min(limitNs - startNs, ENV_RESTART_RETRY_NS);
                repEnv = repNode.getEnv(NANOSECONDS.toMillis(getEnvTimeoutNs));
                if (repEnv == null) {

                    if (startNs + getEnvTimeoutNs < limitNs) {

                        /*
                         * It took too long to get the environment, but there
                         * is still time remaining before the request times
                         * out.  Chances are that another RN will be able to
                         * service the request before this environment is
                         * ready, because environment restarts are easily
                         * longer (30 seconds) than typical request timeouts (5
                         * seconds).  Report this RN as unavailable and let the
                         * client try another one.  [#22661]
                         */
                        throw new RNUnavailableException(
                            "Environment for RN: " + repNodeId +
                            " was unavailable after waiting for " +
                            (getEnvTimeoutNs/1000) + "ms");
                    }

                    /*
                     * Request timed out -- sleepBeforeRetry will throw the
                     * right exception.
                     */
                    sleepBeforeRetry(request, null,
                                     ENV_RESTART_RETRY_NS, limitNs);
                    continue;
                }

                txn = repEnv.beginTransaction(null, txnConfig);
                streamHandle = MigrationStreamHandle.initialize
                    (repNode, request.getPartitionId(), txn);
                final Result result = request.getOperation().execute
                    (txn, request.getPartitionId(), operationHandler);
                final OpCode opCode = request.getOperation().getOpCode();
                if (txn.isValid()) {
                    streamHandle.prepare();
                    /* If testing SR21210, throw InsufficientAcksException. */
                    assert TestHookExecute.doHookIfSet
                        (preCommitTestHook,
                         RepInternal.getRepImpl(repEnv));
                    txn.commit();
                } else {
                    /*
                     * The transaction could have been invalidated in the
                     * an unsuccessful Execute.execute operation, or
                     * asynchronously due to a master->replica transition.
                     *
                     * Note that single operation (non Execute requests)
                     * never invalidate transactions explicitly, so they are
                     * always forwarded.
                     */
                    if (!opCode.equals(OpCode.EXECUTE) ||
                        result.getSuccess()) {
                        /* Async invalidation independent of the request. */
                      throw new ForwardException();
                    }
                    /*
                     * An execute operation failure, allow a response
                     * generation which contains the reason for the failure.
                     */
                }
                response = createResponse(repEnv, request, result);
                tracker.markFinish(opCode, startNs, result.getNumRecords());
                return response;
            } catch (InsufficientAcksException iae) {
                /* Propagate RequestTimeoutException back to the client */
                throw new RequestTimeoutException
                    (request.getTimeout(),
                     "Timed out due to InsufficientAcksException", iae, true);
            } catch (ReplicaConsistencyException rce) {
                /* Propagate it back to the client */
                throw new ConsistencyException
                    (rce, ConsistencyTranslator.translate(
                      rce.getConsistencyPolicy(),
                      request.getConsistency()));
            } catch (InsufficientReplicasException ire) {
                /* Propagate it back to the client */
                throw new DurabilityException
                    (ire,
                     DurabilityTranslator.translate(ire.getCommitPolicy()),
                     ire.getRequiredNodeCount(), ire.getAvailableReplicas());
            } catch (ReplicaWriteException rwe) {
                /* Misdirected message, forward to the master. */
                return forward(request, repNodeId.getGroupId());
            } catch (UnknownMasterException rwe) {
                /* Misdirected message, forward to the master. */
                return forward(request, repNodeId.getGroupId());
            } catch (ForwardException rwe) {
                /* Misdirected message, forward to the master. */
                return forward(request, repNodeId.getGroupId());
            } catch (LockConflictException lockConflict) {

                /*
                 * Retry the transaction until the timeout associated with the
                 * request is exceeded. Note that LockConflictException covers
                 * the HA LockPreemptedException.
                 */
                exception = lockConflict;
                sleepNs = LOCK_CONFLICT_RETRY_NS;
            } catch (@SuppressWarnings("deprecation") MasterReplicaTransitionException rre) {
                /*
                 * As of JE 5.0.92.KV3 (NoSQL R2.1.24),
                 * MasterReplicatTransitionException is deprecated and no
                 * longer thrown. However, keep this catch to maintain backward
                 * compatibility with older JE versions, for a while.
                 */

                /* Re-establish handles. */
                repNode.asyncEnvRestart(repEnv, rre);
                sleepNs = ENV_RESTART_RETRY_NS;
            } catch (RollbackException rre) {
                /* Re-establish handles. */
                repNode.asyncEnvRestart(repEnv, rre);
                sleepNs = ENV_RESTART_RETRY_NS;
            } catch (RollbackProhibitedException pe) {
                logAbbrev.log(Level.SEVERE,
                              "Rollback prohibited admin intervention required",
                              repEnv,
                              pe);

                /*
                 * Rollback prohibited, ensure that the process exits and that
                 * the SNA does no try to restart, since it will result in the
                 * same exception until some corrective action is taken.
                 */
                throw new SystemFaultException("rollback prohibited", pe);
            } catch (IncorrectRoutingException ire) {

                /*
                 * An IncorrectRoutingException can occur at the end of a
                 * partition migration, where the local topology has been
                 * updated with the partition's new location (here) but the
                 * parition DB has not yet been opened (see
                 * RepNode.updateLocalTopology).
                 */
                return handleException(request, ire);
            } catch (DatabasePreemptedException dpe) {

                /*
                 * A DatabasePreemptedException can occur when the partition
                 * DB has been removed due to partition migration activity
                 * during the request.
                 */
                return handleException(request, dpe);
            } catch (EnvironmentFailureException efe) {
                /*
                 * All subclasses of EFE that needed explicit handling were
                 * handled above. Throw out, so the process fault handler can
                 * restart the RN.
                 */
              throw efe;
            } catch (KVSecurityException kvse) {
                /*
                 * Security exceptions are returned to the client.
                 */
                throw kvse;
            } catch (RNUnavailableException rnue) {
                logger.info(rnue.getMessage());
                /* Propagate it back to the client. */
                throw rnue;
            } catch (RuntimeException re) {
                final Response resp  =
                    handleRuntimeException(repEnv, txn, request, re);

                if (resp != null) {
                    return resp;
                }
                sleepNs = ENV_RESTART_RETRY_NS;
            } finally {
                if (response == null) {
                    TxnUtil.abort(txn);
                    /* Clear the failed operation's activity */
                    tracker.markFinish(null, 0L);
                }
                if (streamHandle != null) {
                    streamHandle.done();
                }
            }

            sleepBeforeRetry(request, exception, sleepNs, limitNs);

        } while (true);
    }

    /**
     * The method makes provisions for special handling of RunTimeException.
     * The default action on encountering a RuntimeException is to exit the
     * process and have it restarted by the SNA, since we do not understand the
     * nature of the problem and restarting, while high overhead, is safe.
     * However there are some cases where we would like to avoid RN process
     * restarts and this method makes provisions for such specialized handling.
     *
     * RuntimeExceptions can arise because JE does not make provisions for
     * asynchronous close of the environment. This can result in NPEs or ISEs
     * from JE when trying to perform JE operations, using database handles
     * belonging to the closed environments.
     *
     * TODO: The scope of this handler is rather large and this check could be
     * narrowed down to enclose only the JE operations.
     *
     * @return a Response, if the request could be handled by forwarding it
     * or null for a retry at this same RN.
     *
     * @throws RNUnavailableException if the request can be safely retried
     * by the requestor at some other RN.
     */
    private Response handleRuntimeException(ReplicatedEnvironment repEnv,
                                            Transaction txn,
                                            Request request,
                                            RuntimeException re)
        throws RNUnavailableException {

        if ((repEnv == null) || repEnv.isValid()) {

            /*
             * If the environment is OK (or has not been established) and the
             * exception is an IllegalStateException, it may be a case that
             *
             * - the database has been closed due to partition migration.
             *
             * - the transaction commit threw an ISE because the node
             * transitioned from master to replica, and the transaction was
             * asynchronously aborted by the JE RepNode thread.
             *
             * If so, try forward the request.
             */
            if (re instanceof IllegalStateException) {
                final Response resp = forwardIfRequired(request);
                if (resp != null) {
                    logger.log(Level.INFO,
                               "Request forwarded due to ISE: {0}",
                               re.getMessage());
                    return resp;
                }
                /*
                 * Response could have been processed at this node but wasn't.
                 *
                 * The operation may have used an earlier environment handle,
                 * acquired via the database returned by PartitionManager,
                 * which resulted in the ISE. [#23114]
                 *
                 * Have the caller retry the operation at some other RN if we
                 * can ensure that the environment was not modified by the
                 * transaction.
                 */
                if ((txn != null) &&
                    (txn.getState() != Transaction.State.COMMITTED) &&
                    (txn.getState() !=
                     Transaction.State.POSSIBLY_COMMITTED)) {

                    final String msg = "ISE:" +  re.getMessage() +
                        " Retry at some different RN." ;

                    logger.info(msg);
                    throw new RNUnavailableException(msg);
                }
            }

            /* Unknown failure, propagate the RE */
            logger.log(Level.SEVERE, "unexpected exception", re);
            throw re;
        }

        logAbbrev.log(Level.INFO,
                      "Ignoring exception and retrying at this RN, " +
                      "environment has been closed or invalidated",
                      repEnv,
                      re);

        /* Retry at this RN. */
        return null;
    }

    /**
     * Implements the execution of lightweight NOPs which is done without
     * the need for a transaction.
     */
    private Response executeNOPInternal(final Request request)
        throws RequestTimeoutException {

        final ReplicatedEnvironment repEnv =
            repNode.getEnv(request.getTimeout());

        if (repEnv == null) {
            throw new RequestTimeoutException
                (request.getTimeout(),
                 "Timed out trying to obtain environment handle.",
                 null,
                 true /*isRemote*/);
        }
        final Result result = request.getOperation().execute(null, null, null);
        return createResponse(repEnv, request, result);
    }

    /**
     * Handles a request that has been incorrectly directed at this node or
     * there is some internal inconsistency. If this node's topology has more
     * up-to-date information, forward it, otherwise throws a
     * RNUnavailableException which will cause the client to retry.
     *
     * The specified RuntimeException must an instance of either a
     * DatabasePreemptedException or an IncorrectRoutingException.
     */
    private Response handleException(Request request, RuntimeException re) {
        assert (re instanceof DatabasePreemptedException) ||
               (re instanceof IncorrectRoutingException);

        /* If this is a non-partition request, then rethrow */
        /* TODO - Need to check on how these exceptions are handled for group
         * directed dispatching.
         */
        if (request.getPartitionId().isNull()) {
            throw re;
        }
        final Topology topology = topoManager.getLocalTopology();
        final Partition partition = topology.get(request.getPartitionId());

        /*
         * If the local topology is newer and the partition has moved, forward
         * the request
         */
        if ((topology.getSequenceNumber() > request.getTopoSeqNumber()) &&
            !partition.getRepGroupId().sameGroup(repNode.getRepNodeId())) {
            request.clearForwardingRNs();
            return forward(request, partition.getRepGroupId().getGroupId());
        }
        throw new RNUnavailableException
                             ("Partition database is missing for partition: " +
                              partition.getResourceId());
    }

    /**
     * Forward the request, if the RG does not own the key, if the request
     * needs a master and this node is not currently the master, or if
     * the request can only be serviced by a node that is neither the
     * master nor detached and this node is currently either the master
     * or detached.
     *
     * @param request the request that may need to be forwarded
     *
     * @return the response if the request was processed after forwarding it,
     * or null if the request can be processed at this node.
     *
     * @throws KVStoreException
     */
    private Response forwardIfRequired(Request request) {

        /*
         * If the request has a group ID, use that, otherwise, use the partition
         * iID. If that is null, the request is simply directed to this node,
         * e.g. a NOP request.
         */
        RepGroupId repGroupId = request.getRepGroupId();
        
        if (repGroupId.isNull()) {
            final PartitionId partitionId = request.getPartitionId();
            repGroupId = partitionId.isNull() ?
                      new RepGroupId(repNodeId.getGroupId()) :
                      topoManager.getLocalTopology().getRepGroupId(partitionId);
        }

        if (repGroupId.getGroupId() != repNodeId.getGroupId()) {
            /* Forward the request to the appropriate RG */
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("RN does not contain group: " +
                            repGroupId + ", forwarding request.");
            }
            request.clearForwardingRNs();
            return forward(request, repGroupId.getGroupId());
        }

        final RepGroupState rgState = stateTable.getGroupState(repGroupId);
        final RepNodeState master = rgState.getMaster();

        if (request.needsMaster()) {
            /* Check whether this node is the master. */
            if ((master != null) &&
                 repNodeId.equals(master.getRepNodeId())) {
                /* All's well, it can be processed here. */
                return null;
            }
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("RN is not master, forwarding request. " +
                            "Last known master is: " +
                            ((master != null) ?
                             master.getRepNodeId() :
                             "unknown"));
            }
            return forward(request, repNodeId.getGroupId());

        } else if (request.needsReplica()) {

            ReplicatedEnvironment.State rnState =
                rgState.get(repNodeId).getRepState();

            /* If the RepNode is the MASTER or DETACHED, forward the request;
             * otherwise, service the request.
             */
            if ((master != null) && repNodeId.equals(master.getRepNodeId())) {
                rnState = ReplicatedEnvironment.State.MASTER;
            } else if (rnState.isReplica() || rnState.isUnknown()) {
                return null;
            }

            if (logger.isLoggable(Level.FINE)) {
                logger.fine("With requested consistency policy, RepNode " +
                            "cannot be MASTER or DETACHED, but RepNode [" +
                            repNodeId + "] is " + rnState + ". Forward the " +
                            "request.");
            }
            return forward(request, repNodeId.getGroupId());
        }
        return null;
    }

    /**
     * Returns topology information to be returned as part of the response. If
     * the Topology numbers match up there's nothing to be done.
     *
     * If the handler has a new Topology return the changes needed to bring the
     * requester up to date wrt the handler.
     *
     * If the handler has a Topology that's obsolete vis a vis the requester,
     * return the topology sequence number, so that the requester can push the
     * changes to it at some future time.
     *
     * @param reqTopoSeqNum the topology associated with the request
     *
     * @return null if the topologies match, or the information needed to
     * update either end.
     */
    private TopologyInfo getTopologyInfo(int reqTopoSeqNum) {

        final Topology topology = topoManager.getTopology();

        if (topology == null) {

            /*
             * Indicate that this node does not have topology so that the
             * request invoker can push Topology to it.
             */
            return TopologyInfo.EMPTY_TOPO_INFO;
        }

        final int topoSeqNum = topology.getSequenceNumber();

        if (topoSeqNum == reqTopoSeqNum) {
            /* Short circuit. */
            return null;
        }

        /* Topology mismatch, send changes, if this node has them. */
        return topology.getChangeInfo(reqTopoSeqNum + 1);
    }

    /**
     * Packages up the result of the operation into a Response, including all
     * additional status information about topology changes, etc.
     *
     * @param result the result of the operation
     *
     * @return the response
     */
    private Response createResponse(ReplicatedEnvironment repEnv,
                                    Request request,
                                    Result result) {

        final StatusChanges statusChanges =
            getStatusChanges(request.getInitialDispatcherId());

        VLSN currentVLSN = VLSN.NULL_VLSN;
        if (repEnv.isValid()) {
            final RepImpl repImpl = RepInternal.getRepImpl(repEnv);
            if (repImpl != null) {
                currentVLSN = repImpl.getVLSNIndex().getRange().getLast();
            }
        }

        return new Response(repNodeId,
                            currentVLSN,
                            result,
                            getTopologyInfo(request.getTopoSeqNumber()),
                            statusChanges,
                            request.getSerialVersion());
    }

    /**
     * Sleep before retrying the operation locally.
     *
     * @param request the request to be retried
     * @param exception the exception from the last preceding retry
     * @param sleepNs the amount of time to sleep before the retry
     * @param limitNs the limiting time beyond which the operation is timed out
     * @throws RequestTimeoutException if the operation is timed out
     */
    private void sleepBeforeRetry(Request request,
                                  OperationFailureException exception,
                                  long sleepNs,
                                  final long limitNs)
        throws RequestTimeoutException {

        if ((System.nanoTime() + sleepNs) >  limitNs) {

            final String message =  "Request handler: " + repNodeId +
             " Request: " + request.getOperation() + " timeout: " +
              request.getTimeout() + "ms. exceeded." +
              ((exception != null) ?
               (" Last retried exception: " + exception.getClass().getName() +
                " Message: " + exception.getMessage()) :
               "");

            throw new RequestTimeoutException
                (request.getTimeout(), message, exception, true /*isRemote*/);
        }

        if (sleepNs == 0) {
            return;
        }

        try {
            Thread.sleep(NANOSECONDS.toMillis(sleepNs));
        } catch (InterruptedException ie) {
            throw new IllegalStateException("unexpected interrupt", ie);
        }
    }

    /**
     * Forwards the request, modifying request and response attributes.
     * <p>
     * The request is modified to denote the topology sequence number at this
     * node. Note that the dispatcher id is retained and continues to be that
     * of the node originating the request.
     *
     * @param request the request to be forwarded
     *
     * @param repGroupId the group to which the request is being forwarded
     *
     * @return the modified response
     *
     * @throws KVStoreException
     */
    private Response forward(Request request, int repGroupId) {

        Set<RepNodeId> excludeRN = null;
        if (repNodeId.getGroupId() == repGroupId) {
            /* forwarding within the group */
            excludeRN = request.getForwardingRNs(repGroupId);
            excludeRN.add(repNodeId);
        }
        final short requestSerialVersion = request.getSerialVersion();
        final int topoSeqNumber = request.getTopoSeqNumber();
        request.setTopoSeqNumber
            (topoManager.getTopology().getSequenceNumber());
 
        final Response response =
            requestDispatcher.execute(request, excludeRN,
                                      (LoginManager) null);
        return updateForwardedResponse(requestSerialVersion, topoSeqNumber,
                                       response);

    }

    /**
     * Updates the response from a forwarded request with the changes needed
     * by the initiator of the request.
     *
     * @param requestSerialVersion the serial version of the original request.
     * @param reqTopoSeqNum of the topo seq number associated with the request
     * @param response the response to be updated
     *
     * @return the updated response
     */
    private Response updateForwardedResponse(short requestSerialVersion,
                                             int reqTopoSeqNum,
                                             Response response) {

        /*
         * Before returning the response to the client we must set its serial
         * version to match the version of the client's request, so that the
         * response is serialized with the version requested by the client.
         * This version may be different than the version used for forwarding.
         */
        response.setSerialVersion(requestSerialVersion);

        /*
         * Potential future optimization, use the request handler id to avoid
         * returning the changes multiple times.
         */
        response.setTopoInfo(getTopologyInfo(reqTopoSeqNum));
        return response;
    }

    /**
     * Create a transaction configuration. If the transaction is read only, it
     * uses Durability.READ_ONLY_TXN for the Transaction.
     */
    private TransactionConfig setupTxnConfig(Request request) {
        final TransactionConfig txnConfig = new TransactionConfig();

        if (request.isWrite()) {
            final com.sleepycat.je.Durability haDurability =
                DurabilityTranslator.translate(request.getDurability());
            txnConfig.setDurability(haDurability);
            return txnConfig;
        }

        /* A read transaction. */
        txnConfig.setDurability(com.sleepycat.je.Durability.READ_ONLY_TXN);

        final Consistency reqConsistency = request.getConsistency();

        /*
         * If the consistency requirement was absolute assume it was directed
         * at this node the master since it would have been forwarded otherwise
         * by the forwardIfRequired method. Substitute the innocuous HA
         * NO_CONSISTENCY in this case, since there is no HA equivalent for
         * ABSOLUTE.
         */
        final ReplicaConsistencyPolicy haConsistency =
            Consistency.ABSOLUTE.equals(reqConsistency) ?
                    com.sleepycat.je.rep.NoConsistencyRequiredPolicy.
                    NO_CONSISTENCY :
                    ConsistencyTranslator.translate(reqConsistency);
        txnConfig.setConsistencyPolicy(haConsistency);
        return txnConfig;
    }

    /**
     * Bind the request handler in the registry so that it can start servicing
     * requests.
     */
    public void startup()
        throws RemoteException {

        final StorageNodeParams snParams = params.getStorageNodeParams();
        final GlobalParams globalParams = params.getGlobalParams();
        final RepNodeParams repNodeParams = params.getRepNodeParams();
        final String kvStoreName = globalParams.getKVStoreName();
        final String csfName = ClientSocketFactory.
                factoryName(kvStoreName,
                            RepNodeId.getPrefix(),
                            InterfaceType.MAIN.interfaceName());
        RMISocketPolicy rmiPolicy =
            params.getSecurityParams().getRMISocketPolicy();
        SocketFactoryPair sfp =
            repNodeParams.getRHSFP(rmiPolicy,
                                   snParams.getServicePortRange(),
                                   csfName,
                                   kvStoreName);
        RegistryUtils.rebind(snParams.getHostname(),
                             snParams.getRegistryPort(),
                             kvStoreName,
                             repNode.getRepNodeId().getFullName(),
                             RegistryUtils.InterfaceType.MAIN,
                             this,
                             sfp.getClientFactory(),
                             sfp.getServerFactory());
    }

    /**
     * Unbind registry entry so that no new requests are accepted. The method
     * waits for requests to quiesce so as to minimize any exceptions on the
     * client side.
     *
     * If any exceptions are encountered, during the unbind, they are merely
     * logged and otherwise ignored, so that other components can continue
     * to be shut down.
     */
    public void stop() {

        final StorageNodeParams snParams = params.getStorageNodeParams();
        final GlobalParams globalParams = params.getGlobalParams();

        /* Stop accepting new requests. */
        try {
            RegistryUtils.unbind(snParams.getHostname(),
                                 snParams.getRegistryPort(),
                                 globalParams.getKVStoreName(),
                                 repNode.getRepNodeId().getFullName(),
                                 RegistryUtils.InterfaceType.MAIN,
                                 this);
        } catch (RemoteException e) {
            logger.log(Level.INFO,
                       "Ignoring exception while stopping request handler",
                       e);
            return;
        }

        /*
         * Wait for the requests to quiesce within the requestQuiesceMs
         * period.
         */
        new PollCondition(REQUEST_QUIESCE_POLL_MS, requestQuiesceMs) {

            @Override
            protected boolean condition() {
                return activeRequests.get() == 0;
            }

        }.await();

        /* Log requests that haven't quiesced. */
        final int activeRequestCount = activeRequests.get();
        if (activeRequestCount > 0) {
            logger.info("Requested quiesce period: " + requestQuiesceMs +
                        "ms was insufficient to quiesce all active " +
                        "requests for soft shutdown. " +
                        "Pending active requests: " + activeRequestCount);
        }

        requestDispatcher.shutdown(null);
    }

    /**
     * Returns the status changes that the requester identified by
     * <code>remoteRequestHandlerId</code> is not aware of.
     *
     * @param resourceId the id of the remote requester
     *
     * @return the StatusChanges if there are any to be sent back
     */
    public StatusChanges getStatusChanges(ResourceId resourceId) {

        if (stateChangeEvent == null) {

            /*
             * Nothing of interest to communicate back. This is unexpected, we
             * should not be processing request before the node is initialized.
             */
            return null;
        }
        if (requesterMap.get(resourceId) == stateChangeEvent) {

            /*
             * The current SCE is already know to the requester ignore it.
             */
            return null;
        }

        try {
            final State state = stateChangeEvent.getState();

            if (state.isMaster() || state.isReplica()) {
                final String masterName = stateChangeEvent.getMasterNodeName();
                return new StatusChanges(state,
                                         RepNodeId.parse(masterName),
                                         stateChangeEvent.getEventTime());
            }

            /* Not a master or a replica. */
            return new StatusChanges(state, null, 0);
        } finally {
            requesterMap.put(resourceId, stateChangeEvent);
        }
    }

    /**
     * For testing only.
     */
    LogMessageAbbrev getMessageAbbrev() {
        return logAbbrev;
    }

    /**
     * Listener for local state changes at this node.
     */
    private class Listener implements StateChangeListener {

        final ReplicatedEnvironment repEnv;

        public Listener(ReplicatedEnvironment repEnv) {
            this.repEnv = repEnv;
        }

        /**
         * Takes appropriate action based upon the state change. The actions
         * must be simple and fast since they are performed in JE's thread of
         * control.
         */
        @Override
        public void stateChange(StateChangeEvent sce)
            throws RuntimeException {

            stateChangeEvent = sce;
            final State state = sce.getState();
            logger.info("State change event: " + new Date(sce.getEventTime()) +
                        ", State: " + state +
                        ", Master: " +
                        ((state.isMaster() || state.isReplica()) ?
                         sce.getMasterNodeName() :
                         "none"));
            /* Ensure that state changes are sent out again to requesters */
            requesterMap.clear();
            stateTable.update(sce);
            if (repNode != null) {
                repNode.noteStateChange(repEnv, sce);
            }
        }
    }

    /**
     * Utility class to avoid a class of unnecessary stack traces. These stack
     * traces can cause important information to scroll out of the log window.
     *
     * The stack traces in this case are rooted in the same environment close
     * or invalidation. In such circumstances JE can throw an ISE, etc. in all
     * request threads as a request makes an attempt to access a JE resource
     * like a cursor or a database after the environment invalidation.
     */
    class LogMessageAbbrev {

        /**
         * Tracks the last exception to help determine whether it should be
         * printed along with a stack trace by the logAbbrev() method below.
         */
        private final AtomicReference<Environment> lastEnv =
            new AtomicReference<Environment>();

        /* stats for test verification. */
        volatile int abbrevCount = 0;
        volatile int fullCount = 0;

        /**
         * Log the message in an abbreviated form, without the stack trace, if
         * it's associated with the same invalid environment. Log it with the
         * full stack trace, if it's the first message associated with the
         * environment since its close or invalidation.
         *
         * It's the caller's responsibility to determine which messages can
         * benefit from such abbreviation and to ensure that the method is only
         * invoked when it detects the exception in the context of an
         * invalidated environment.
         *
         * @param logMessage the message to be logged
         * @param env the environment associated with the exception
         * @param exception the exception to be logged
         */
        private void log(Level level,
                         String logMessage,
                         Environment env,
                         RuntimeException exception) {
            if ((env != null) && (lastEnv.getAndSet(env) != env)) {
                /* New env, log it in full with a stack trace. */
                logger.log(level, logMessage, exception);
                fullCount++;
            } else {
                /*
                 * Runtime exception against the same environment, log just the
                 * exception message.
                 */
                final StackTraceElement[] traces = exception.getStackTrace();
                logger.log(level, logMessage +
                           " Exception:" + exception.getClass().getName() +
                           ", Message:" + exception.getMessage() +
                           ", Method:" +
                           (((traces == null) || (traces.length == 0)) ?
                            "unknown" : exception.getStackTrace()[0]));
                abbrevCount++;
            }
        }
    }

    /**
     * A utility exception used to indicate that the request must be forwarded
     * to some other node because the transaction used to implement the
     * request has been invalidated, typically because of a master->replica
     * transition.
     */
    @SuppressWarnings("serial")
    private class ForwardException extends Exception {}

    /**
     * Provides an implementation of OperationContext for access checking.
     */
    private class RequestContext implements OperationContext {
        private final Request request;

        private RequestContext(Request request) {
            this.request = request;
        }

        @Override
        public String describe() {
            return "API request: " + request.getOperation().toString();
        }

        @Override
        public List<KVStoreRolePrincipal> getRequiredRoles() {
            final OpCode opCode = request.getOperation().getOpCode();

            /* NOP does not require authentication */
            if (OpCode.NOP.equals(opCode)) {
                return emptyRoleList;
            }

            return authenticatedRoleList;
        }
    }
}
