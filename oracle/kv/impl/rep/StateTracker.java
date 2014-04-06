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

package oracle.kv.impl.rep;

import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.utilint.StoppableThread;

/**
 * Base class for threads which manage rep node state changes.
 */
public abstract class StateTracker extends StoppableThread {

    /**
     * The queue used to hold RN state transitions. Note that only the latest
     * transition is of interest. Consequently, as an optimization, earlier
     * entries can be discarded whenever that's convenient.
     *
     * The queue must at least be of length 2 so that it can hold the last
     * state transition and the EOQ marker
     */
    private final BlockingQueue<StateChangeEvent> stateTransitions =
        new ArrayBlockingQueue<StateChangeEvent>(2);

    /**
     * The End Of Queue marker associated with the
     * <code>stateTransitions</code> queue.
     */
    static private StateChangeEvent EOQ_DETACHED_STATE_MARKER =
        new StateChangeEvent(ReplicatedEnvironment.State.DETACHED,
                             NameIdPair.NULL);

    /**
     * The RN associated with the state changes.
     */
    protected final RepNode rn;

    protected final AtomicBoolean shutdown = new AtomicBoolean(false);

    /* The amount of time to wait for a soft shutdown of this thread. */
    static private final int SOFT_SHUTDOWN_MS = 10000;

    protected final Logger logger;

    /**
     * Creates the Manager. Note that the RN must be able to contact the SN so
     * that it can establish an RMI handle to the SNA. The RN is managed by the
     * SNA, and it's on the same machine as the SNA, so not being able to
     * contact it would imply there was some serious configuration issue
     *
     * @param the name of the thread
     * @param rn the RN whose state is to be tracked
     * @param logger the logger to be used
     */
    protected StateTracker(String name, RepNode rn, Logger logger) {
        super(null, rn.getExceptionHandler(), name);

        this.rn = rn;
        this.logger = logger;
    }

    /**
     * Returns true if there are no more state change events in the queue.
     * 
     * @return true if there are no more state change events in the queue
     */
    protected boolean isEmpty() {
        return stateTransitions.isEmpty();
    }
    
    /**
     * Invoked by the RN whenever the HA listener notifies it of an HA state
     * change. Since the listener thread must not be held up, the method merely
     * queues up the event for async processing by the Manager's thread.
     *
     * Note that this method is invoked sequentially given the semantics of the
     * HA state listener.
     *
     * @param stateChangeEvent the state change event
     */
    public void noteStateChange(StateChangeEvent stateChangeEvent) {

        /*
         * Must not miss state change events. Only the latest state matters.
         */
        while (true) {
            /* retry until the element is in the queue. */
            try {
                stateTransitions.add(stateChangeEvent);
                logger.log(Level.INFO, "{0} queue added:{1}",  // TODO - FINE
                          new Object[]{getName(), stateChangeEvent.getState()});
                return;
            } catch (IllegalStateException e) {

                /*
                 * No space in queue, remove an element and try again, only the
                 * latest state change matters.
                 */
                try {
                    final StateChangeEvent rem = stateTransitions.remove();
                    if (rem == EOQ_DETACHED_STATE_MARKER) {
                        
                        /*
                         * Events that come after EOQ can be ignored. Stick the
                         * EOQ back in the queue.
                         */
                        stateChangeEvent = EOQ_DETACHED_STATE_MARKER;
                        continue;
                    }
                    logger.log(Level.INFO, "{0} entry removed:{1}", // TODO - FINE
                               new Object[]{getName(), rem});
                } catch (NoSuchElementException nsee) {
                    /* Consumed by the run() thread */
                }
            }
        }
    }

    /**
     * The loop which processes state change requests.
     */
    @Override
    public void run() {
        boolean interrupted = false;

        try {
            runInternal();
        } catch (ThreadInterruptedException tie) {
            /* Expected as part of a hard shutdown. */
            interrupted = true;
        } catch (InterruptedException e) {
            /* Expected as part of a hard shutdown. */
            interrupted = true;
        } finally {
            logger.log(Level.INFO, "{0} thread exited rn: {1}{2}",
                       new Object[]{getName(), rn.getRepNodeId(),
                                    interrupted ? " Interrupted. " : ""});
        }
    }

    private void runInternal()
        throws InterruptedException {

        logger.log(Level.INFO, "{0} thread start rn: {1}",
                   new Object[]{getName(), rn.getRepNodeId()});

        while (!shutdown.get()) {
            final StateChangeEvent sce = stateTransitions.take();

            if (!isEmpty()) {

                /*
                 * Skip this state change event, if a new state transition
                 * supersedes this one.
                 */
                continue;
            }
            doNotify(sce);
            
            if (sce == EOQ_DETACHED_STATE_MARKER) {
                break;
            }
        }
    }
    
    protected abstract void doNotify(StateChangeEvent sce)
        throws InterruptedException;
    
    @Override
    public int initiateSoftShutdown() {
        assert shutdown.get();

        final long limitMs = System.currentTimeMillis() + SOFT_SHUTDOWN_MS;

        /* Place a special EOQ marker */
        noteStateChange(EOQ_DETACHED_STATE_MARKER);

        /* Return the amount of time left to wait, or -1 if none */
        final long waitMs = limitMs - System.currentTimeMillis();
        return (waitMs <= 0) ? -1 : (int)waitMs;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    /**
     * Stops this tracker thread and waits for the thread to exit.
     */
    public void shutdown() {
        if (!shutdown.compareAndSet(false, true)) {
            return;
        }
        shutdownThread(logger);
    }
}
