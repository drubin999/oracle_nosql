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
package oracle.kv.impl.security;

import java.rmi.server.RemoteServer;
import java.rmi.server.ServerNotActiveException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.security.auth.Subject;

import oracle.kv.AuthenticationRequiredException;
import oracle.kv.UnauthorizedException;

/**
 * ExecutionContext contains the logic to manage the user identity and
 * operation description. It includes methods for recording the execution
 * for Thread-specific access at later points in the execution of an operation.
 */
public final class ExecutionContext {

    /**
     * Thread-local state tracking the per-thread ExecutionContext.
     * This allows deferred access checks to locate the relevant security
     * information.
     */
    private static final ThreadLocal<ExecutionContext> currentContext =
        new ThreadLocal<ExecutionContext>();

    /**
     * The AuthContext object provided in a remote call, which provides
     * identification of the caller.  This may be null.
     */
    private final AuthContext requestorCtx;

    /**
     * The identity and capabilities of the caller derived from the AuthContext
     * object provided in a remote call. This may be null.
     */
    private final Subject requestorSubj;

    /**
     * An object describing the operation and required authorization for the
     * operation.
     */
    private final OperationContext opCtx;

    /**
     * A string identitying the host from which the access originated.
     */
     private final String clientHost;

    /**
     * Construct an operation context object, which collects the
     * interesting bits of information about a thread of execution for
     * the purpose of security validation, etc.
     *
     * @param requestorCtx the identifying information provided by the caller.
     *   This may be null
     * @param requestorSubject the identity of the caller, derrived from the
     *   requestorCtx.  This may be null, and will be null if requestorCtx is
     *   null.
     * @param opCtx the operation description object
     */
    private  ExecutionContext(AuthContext requestorCtx,
                              Subject requestorSubject,
                              OperationContext opCtx) {
        this.requestorCtx = requestorCtx;
        this.requestorSubj = requestorSubject;
        this.opCtx = opCtx;
        this.clientHost = getRMIClientHost();
    }

    /**
     * Get the context object that identifies and authorizes the requestor
     * to perform an action.
     */
    public AuthContext requestorContext() {
        return requestorCtx;
    }

    /**
     * Get the Subject that describes the requestor and the associated
     * authorizations.
     */
    public Subject requestorSubject() {
        return requestorSubj;
    }

    /**
     * Get the host from which the request originated.
     */
    public String requestorHost() {
        return clientHost;
    }

    /**
     * Get the context object that describes the action being performed
     * by the user.
     */
    public OperationContext operationContext() {
        return opCtx;
    }

    /**
     * Create an execution context object and perform the initial
     * security check on it, based on the access requirements defined by
     * the accessCheck.
     *
     * @param accessCheck the access checker for the environment, which
     *   performs security checking operations.
     * @param requestorCtx the context object identifying the caller
     * @param opCtx the context object identifying the type of operation
     *   being performed, for the purpose of reporting.
     */
    public static ExecutionContext create(AccessChecker accessCheck,
                                          AuthContext requestorCtx,
                                          OperationContext opCtx)
        throws AuthenticationRequiredException, UnauthorizedException,
               SessionAccessException {

        final Subject reqSubj;
        if (accessCheck == null) {
            reqSubj = null;
        } else {
            reqSubj = accessCheck.identifyRequestor(requestorCtx);
        }

        final ExecutionContext execCtx =
            new ExecutionContext(requestorCtx, reqSubj, opCtx);

        if (accessCheck != null) {
            accessCheck.checkAccess(execCtx, opCtx);
        }

        return execCtx;
    }

    /**
     * Modify the current execution context to run code as the user that is
     * identified by execCtx.
     */
    public static <R, E extends Exception> R runWithContext(
        final Operation<R, E> operation, final ExecutionContext execCtx)
        throws E {

        final ExecutionContext priorCtx = currentContext.get();
        try {
            currentContext.set(execCtx);
            return operation.run();
        } finally {
            currentContext.set(priorCtx);
        }
    }

    /**
     * Modify the current execution context to run code as the user that is
     * identified by execCtx.
     */
    public static <R> R runWithContext(
        final SimpleOperation<R> operation, final ExecutionContext execCtx) {

        final ExecutionContext priorCtx = currentContext.get();
        try {
            currentContext.set(execCtx);
            return operation.run();
        } finally {
            currentContext.set(priorCtx);
        }
    }

    /**
     * Modify the current execution context to run code as the user that is
     * identified by execCtx.
     */
    public static <E extends Exception> void runWithContext(
        final Procedure<E> procedure, final ExecutionContext execCtx)
        throws E {

        final ExecutionContext priorCtx = currentContext.get();
        try {
            currentContext.set(execCtx);
            procedure.run();
        } finally {
            currentContext.set(priorCtx);
        }
    }

    /**
     * Modify the current execution context to run code as the user that is
     * identified by execCtx.
     */
    public static void runWithContext(
        final SimpleProcedure procedure, final ExecutionContext execCtx) {

        final ExecutionContext priorCtx = currentContext.get();
        try {
            currentContext.set(execCtx);
            procedure.run();
        } finally {
            currentContext.set(priorCtx);
        }
    }

    /**
     * Find out the current execution context.
     */
    public static ExecutionContext getCurrent() {
        return currentContext.get();
    }

    /**
     * Find out the current execution user Subject.
     */
    public static Subject getCurrentUserSubject() {
        final ExecutionContext execCtx = getCurrent();

        if (execCtx == null) {
            return null;
        }

        return execCtx.requestorSubject();
    }

    /**
     * Find out the current execution user client host.
     */
    public static String getCurrentUserHost() {
        final ExecutionContext execCtx = getCurrent();

        if (execCtx == null) {
            return null;
        }

        return execCtx.requestorHost();
    }

    /**
     * Find the user principal associated with the current execution user
     * Subject.
     */
    public static KVStoreUserPrincipal getCurrentUserPrincipal() {
        return getSubjectUserPrincipal(getCurrentUserSubject());
    }

    /**
     * Find the user principal associated with the specified Subject.
     */
    public static KVStoreUserPrincipal getSubjectUserPrincipal(Subject subj) {
        if (subj == null) {
            return null;
        }

        final Set<KVStoreUserPrincipal> userPrincs =
            subj.getPrincipals(KVStoreUserPrincipal.class);

        if (userPrincs.isEmpty()) {
            return null;
        }

        if (userPrincs.size() != 1) {
            throw new IllegalStateException(
                "Current user has multiple user principals");
        }

        return userPrincs.iterator().next();
    }

    /**
     * Find the role principals associated with the specified Subject.
     * @return a newly allocated array containing the associated role
     * principals.
     */
    public static KVStoreRolePrincipal[] getSubjectRolePrincipals(
        Subject subj) {

        if (subj == null) {
            return null;
        }

        final Set<KVStoreRolePrincipal> rolePrincs =
            subj.getPrincipals(KVStoreRolePrincipal.class);

        return rolePrincs.toArray(new KVStoreRolePrincipal[rolePrincs.size()]);
    }

    /**
     * Find the roles associated with the specified Subject.
     * @return a newly allocated array containing the associated roles.
     */
    public static KVStoreRole[] getSubjectRoles(Subject subj) {

        if (subj == null) {
            return null;
        }

        final Set<KVStoreRolePrincipal> rolePrincs =
            subj.getPrincipals(KVStoreRolePrincipal.class);

        final List<KVStoreRole> roles = new ArrayList<KVStoreRole>();
        for (KVStoreRolePrincipal princ : rolePrincs) {
            roles.add(princ.getRole());
        }
        return roles.toArray(new KVStoreRole[roles.size()]);
    }

    /**
     * Tests whether the ExecutionContext Subject has the specified role
     * assigned to it.
     * @param role a KVStoreRole to look for
     * @return true if the subject has the specified role
     */
    public boolean hasRole(KVStoreRole role) {
        return subjectHasRole(requestorSubj, role);
    }

    /**
     * Tests whether a particular Subject has the specified role assigned to it.
     * @param subj a Subject to inspect
     * @param role a KVStoreRole to look for
     * @return true if the subject has the specified role
     */
    public static boolean subjectHasRole(Subject subj, KVStoreRole role) {
        /* implemented inline to minimize overhead */
        for (Object princ : subj.getPrincipals()) {
            if (KVStoreRolePrincipal.class.isAssignableFrom(princ.getClass())) {
                final KVStoreRolePrincipal rolePrinc =
                    (KVStoreRolePrincipal) princ;
                if (rolePrinc.getRole().equals(role)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * The interface to be implemented by the operation to be run on behalf
     * of a user.
     *
     * @param <R> the Result type associated with the Operation
     * @param <E> the Exception type associated with the execution of the
     * operation.
     */
    public interface Operation<R, E extends Exception> {
        R run() throws E;
    }

    /**
     * A variant to simplify the handling of operations that do not throw
     * exceptions.
     *
     * @param <R> the Result type associated with the Operation
     */
    public interface SimpleOperation<R> {
        R run();
    }

    /**
     * A variant for procedural operations that does not return values.
     *
     * @param <E> the Exception type associated with the execution of the
     * operation.
     */
    public interface Procedure<E extends Exception> {
        void run() throws E;
    }

    /**
     * A variant for procedural operations that does not throw exceptions.
     */
    public interface SimpleProcedure {
        void run();
    }

    /**
     * Determine what host the current RMI requiest came from.  If there
     * is no active RMI requested, returns null.
     */
    private static String getRMIClientHost() {
        try {
            return RemoteServer.getClientHost();
        } catch (ServerNotActiveException snae) {
            /* We're not in the context of an RMI call */
            return null;
        }
    }
}
