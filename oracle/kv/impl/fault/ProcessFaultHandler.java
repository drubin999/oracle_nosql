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

package oracle.kv.impl.fault;

import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.FaultException;
import oracle.kv.KVSecurityException;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.test.TestStatus;

/**
 * The process level fault handler (ProcessFaultHandler) and its
 * application-specific subclasses intercept all RuntimeExceptions and perform
 * the following basic tasks:
 * <ol>
 * <li>
 * Log the fault.</li>
 * <li>
 * Update monitoring data. Note that we currently do not have a coordination
 * mechanism in place to ensure that the monitored data is pulled from the
 * process before it exits. TODO</li>
 * <li>
 * Initiates a shutdown of the process if the exception is not a
 * OperationFaultException.</li>
 * </ol>
 * <p>
 * If fault is detected in an RMI service thread, the exception is re-thrown.
 * There is a certain level of coordination that is required between the RMI
 * service call and the shutdown to ensure that the process does not exit
 * before the RMI call is completed. If the fault is detected in a web service
 * request, similar considerations apply. The error indication must be sent to
 * the caller before the process exits.
 * <p>
 * Shutting down the faulting request with a specific and explicit failure
 * indicator is desirable but not required behavior. It permits the request
 * initiator to log the cause of the fault at the client thus enabling better
 * fault analysis. In the absence of an explicit failure indicator, the broken
 * tcp connection would result in an IOException on the client instead causing
 * the request to be aborted in any case.
 * <p>
 * The faulting process exits with an appropriate exit code to indicate whether
 * the SNA should attempt to restart it. If the SNA itself is the process in
 * question, then init.d will attempt to restart it.
 */
public abstract class ProcessFaultHandler {

    /**
     * The default process exit code used for a fault that's not a subclass of
     * {@link SystemFaultException} or a {@link ProcessFaultException}.
     */
    private final ProcessExitCode defaultExitCode;

    protected Logger logger;

    public ProcessFaultHandler(Logger logger,
                               ProcessExitCode defaultExitCode) {
        this.logger = logger;
        this.defaultExitCode = defaultExitCode;
    }

    public ProcessExitCode getDefaultExitCode() {
        return defaultExitCode;
    }

    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    /**
     * Executes the operation inside a process level fault handler that forces
     * a shutdown if the operation throws a RuntimeException.
     * <p>
     * Note that any changes in the bodies of these methods must be
     * appropriately replicated in the next three methods that are variants of
     * this one.
     *
     * @param <R> The return type associated with the operation
     * @param <E> The type of exception thrown by the operation
     * @param operation the operation whose faults are to be handled
     * @return the return value when the operation is successful
     */
    public <R, E extends Exception> R execute(Operation<R, E> operation)
        throws E {

        try {
            return operation.execute();
        } catch (RuntimeException re) {
            rethrow(re);
        } catch (Error e) {
            rethrow(e);
        }
        assert false : " code not reachable ";
        return null;
    }

    /**
     * An overloading of the above method for simple operations that do not
     * throw exceptions.
     */
    public <R> R execute(SimpleOperation<R> operation) {

        try {
            return operation.execute();
        } catch (RuntimeException re) {
            rethrow(re);
        } catch (Error e) {
            rethrow(e);
        }

        assert false : " code not reachable ";
        return null;
    }

    /**
     * An overloading of the above method for Procedures.
     */
    public <E extends Exception> void execute(Procedure<E> proc)
        throws E {

        try {
            proc.execute();
        } catch (RuntimeException re) {
            rethrow(re);
        } catch (Error e) {
            rethrow(e);
        }
    }

    /**
     * An overloading of the above method for simple procedures that do not
     * throw exceptions.
     */
    public void execute(SimpleProcedure proc) {

        try {
            proc.execute();
        } catch (RuntimeException re) {
            rethrow(re);
        } catch (Error e) {
            rethrow(e);
        }
    }

    /**
     * Performs the processing required around an Error before re-throwing it.
     */
    public void rethrow(Error error) {

        try {
            logger.log(Level.SEVERE, "Process exiting with error", error);
            throw error;
        } finally {
            /*
             * Queue shutdown after the logging for the throw has completed.
             */
            queueShutdown(error, ProcessExitCode.RESTART);
        }
    }

    /**
     * Performs the processing required around a RuntimeException before
     * re-throwing it.
     */
    public void rethrow(final RuntimeException requestException)
        throws FaultException {

        ProcessExitCode exitCode;

        try {
            /* Throw to dispatch based upon the exception type. */
            throw requestException;
        } catch (ProcessFaultException rfe) {
            exitCode = rfe.getExitCode();
        } catch (SystemFaultException sfe) {
            exitCode = sfe.getExitCode();
        } catch (OperationFaultException nfe) {
            exitCode = null; /* Don't exit the process. */
        } catch (ClientAccessException cae) {
            exitCode = null; /* Don't exit the process. */
        } catch (KVSecurityException kvse) {
            exitCode = null; /* Don't exit the process. */
        } catch (SessionAccessException sae) {
            exitCode = null; /* Don't exit the process. */
        } catch (RuntimeException re) {
            exitCode = getExitCode(re, defaultExitCode);
        }

        try {
            if (exitCode != null) {
                logger.log(Level.SEVERE, "Process exiting", requestException);
            } else {
                /* Reduce logging output for errors that are not severe */
                if (logger.isLoggable(Level.FINE)) {
                    final String msg =
                        "Process fault handler handled exception: " +
                        requestException.getClass().getName() +
                        " Exception message: " + requestException.getMessage();
                    logger.fine(msg);
                }
            }
            throw getThrowException(requestException);
        } finally {
            /*
             * Queue shutdown after the logging for the throw has completed.
             */
            if (exitCode != null) {
                /* Must be the very last thing that's done. */
                queueShutdown(requestException, exitCode);
            }
        }
    }

    /**
     * Returns the exception that will be thrown out of the handler. Subclasses
     * of this class can override this method to wrap exceptions appropriately.
     *
     * @param requestException the runtime exception that was actually
     * encountered while processing a request.
     *
     * @return the exception to be thrown
     */
    protected RuntimeException
        getThrowException(RuntimeException requestException) {

        if (requestException instanceof ClientAccessException) {
            /*
             * This is a security exception generated by the client.
             * Unwrap it so that the client sees it in its orginal form.
             */
            throw (RuntimeException) requestException.getCause();
        }

        return requestException;
    }

    /**
     * Determines whether the process should exit due to the runtime exception
     * and the exit code it should use upon exit.
     *
     * @param requestException the runtime exception that was actually
     * encountered while processing a request. Note that it may be different
     * from the one that is actually thrown out of the handler; the latter is
     * determined by {@link #getThrowException(RuntimeException)}.
     *
     * @param exitCode the default exit code associated with the exception
     *
     * @return null if the process should not exit. A non null value if it
     * should.
     */
    protected ProcessExitCode getExitCode(RuntimeException requestException,
                                          ProcessExitCode exitCode) {
        return defaultExitCode;
    }

    /**
     * Initiates a process shutdown request. The method is simply a wrapper
     * around queueShutdownInternal which does the real work. The method
     * handles any exceptions encountered during shutdown by exiting promptly.
     *
     * OOMEs get special attention and try to avoid any allocation. Note that
     * OOMEs come from multiple sources depending on the type of memory (thread
     * (/etc/security/limits.conf nproc setting), heap, perm space, etc.) that
     * was exhausted. The java APIs aren't sufficient to distinguish amongst
     * these possibilities and navigate a safe path to communicate the real
     * cause of the problem. So in an OOME situation, we do a quick exit with
     * no logging of this problem to prevent cascading failures. A
     * distinguished process exit code ({@link ProcessExitCode#RESTART_OOME})
     * is used so that the SNA can log the OOME instead. Note that this
     * handling is mainly useful in non-heap OOME situations, since the process
     * typically becomes unresponsive (due to frequent GCs), long before an
     * OOME is thrown. Monitoring the free memory explicitly is a better way to
     * deal with heap exhaustion instead of waiting for an OOME.
     *
     * @param requestException the runtime exception that was actually
     * encountered while processing a request.
     *
     * @param exitCode the exitCode to be used by the process.
     */
    public final void queueShutdown(Throwable requestException,
                                    ProcessExitCode exitCode) {

        boolean immediateExit = true;
        if (requestException instanceof OutOfMemoryError) {
            exitCode = ProcessExitCode.RESTART_OOME;
        } else {
            /* Try for a clean process shutdown */
            try {
                queueShutdownInternal(requestException, exitCode);
                immediateExit = false;
            } catch (OutOfMemoryError ome) {
                exitCode = ProcessExitCode.RESTART_OOME;
            } catch (Throwable t) {
                logger.log(Level.SEVERE, "Process exiting", requestException);
                logger.log(Level.SEVERE, "Error during shutdown", t);
            }
        }

        if (immediateExit) {
            if (TestStatus.isActive()) {
                /*
                 * Throw  an exception in the test environment, don't exit the
                 * test.
                 */
                throw new IllegalStateException("exit", requestException);
            }
            /* Exit immediately */
            System.exit(exitCode.getValue());
        }
    }

    /**
     * Queues a shutdown in a process-specific way. To ensure correct
     * handling of exceptions, this method must only be invoked via
     * queueShutdown().
     *
     * @param requestException the runtime exception that was actually
     * encountered while processing a request.
     *
     * @param exitCode the exitCode to be used by the process
     */
    protected abstract void queueShutdownInternal(Throwable requestException,
                                                  ProcessExitCode exitCode);

    /*
     * The proliferation of differently named interfaces below is due to the
     * lack of overloading on Generic parameters in Java.
     */

    /**
     * The interface to be implemented by the operation whose process level
     * faults are to be handled.
     *
     * @param <R> the Result type associated with the Operation
     * @param <E> the Exception type associated with the execution of the
     * operation.
     */
    public interface Operation<R, E extends Exception> {
        R execute() throws E;
    }

    /**
     * A variant to simplify the handling of operations that do not throw
     * exceptions.
     *
     * @param <R> the Result type associated with the Operation
     */
    public interface SimpleOperation<R> {
        R execute();
    }

    /**
     * A variant for procedural operations that does not return values.
     *
     * @param <E> the Exception type associated with the execution of the
     * operation.
     */
    public interface Procedure<E extends Exception> {
        void execute() throws E;
    }

    /**
     * A variant for procedural operations that does not throw exceptions.
     */
    public interface SimpleProcedure {
        void execute();
    }
}
