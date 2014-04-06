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

package oracle.kv.impl.monitor.views;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import oracle.kv.impl.admin.AdminServiceParams;
import oracle.kv.impl.measurement.ServiceStatusChange;
import oracle.kv.impl.monitor.Tracker;
import oracle.kv.impl.monitor.ViewListener;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * Tracks service status to provide a monitoring health indicator. Note that
 * multiple threads may be simultaneously inserting new changes.
 */
public class ServiceStatusTracker
    extends Tracker<ServiceChange> implements ViewListener<ServiceStatusChange> {

    static final int PRUNE_FREQUENCY = 40; /* Run the pruner modulo this. */

    /* Keep status information for each resource. */
    private final Map<ResourceId, ServiceChange> serviceHealth;
    /* Also keep track of the last logged status, to avoid repeated messages. */
    private final Map<ResourceId, ServiceChange> lastLoggedEvent;
    /* Keep a queue of status events for delivery to listeners. */
    private final List<EventHolder<ServiceChange>> queue;

    private final Logger logger;
    private int newInfoCounter = 0;

    /* For unit test usage only. */
    public ServiceStatusTracker() {
        this(null);
    }

    public ServiceStatusTracker(AdminServiceParams params) {
        /*
         * Use a concurrent hash map so we can read the service health safely
         * while it's being updated. However, updates need to be synchronized
         * so that an earlier service change from a laggard thread won't wipe
         * out a later change.
         */
        serviceHealth =
            new ConcurrentHashMap<ResourceId, ServiceChange>();
        lastLoggedEvent =
            new ConcurrentHashMap<ResourceId, ServiceChange>();

        queue = new ArrayList<EventHolder<ServiceChange>>();
        if (params == null) {
            /* Should only be for unit test usage */
            logger =
                LoggerUtils.getLogger(this.getClass(), "ServiceStatusTracker");
        } else {
            logger =
                LoggerUtils.getLogger(this.getClass(), params);
        }
    }

    private void prune() {
        long interesting = getEarliestInterestingTimeStamp();

        while (!queue.isEmpty()) {
            EventHolder<ServiceChange> se = queue.get(0);
            if (se.getSyntheticTimestamp() > interesting) {
                /* Stop if we've reached the earliest interesting timestamp. */
                break;
            }

            queue.remove(0);
        }
    }

    /**
     * Register a new change. Update the health map only if this change is
     * newer than a previous change, and does not obscure past information.
     */
    @Override
    public void newInfo(ResourceId rId, ServiceStatusChange change) {

        ServiceChange event = new ServiceChange(rId, change);
        EventAction result;

        synchronized (this) {
            if (newInfoCounter++ % PRUNE_FREQUENCY == 0) {
                prune();
            }

            ResourceId targetId = event.getTarget();
            ServiceChange old = serviceHealth.get(targetId);
            ServiceChange oldLogged = lastLoggedEvent.get(targetId);

            result = shouldUpdate(old, oldLogged, event);
            if (result.shouldMap) {
                serviceHealth.put(targetId, event);
                long syntheticTimestamp =
                    getSyntheticTimestamp(event.getChangeTime());
                queue.add
                    (new EventHolder<ServiceChange>
                     (syntheticTimestamp, event,
                      true /* Status changes are always recordable. */));
            }

            /* Remember the last logged event */
            if (result.shouldLog) {
                lastLoggedEvent.put(targetId, event);
            }

        }

        if (result.shouldMap) {
            notifyListeners();
        }

        if (result.shouldLog) {
            logger.info("[" + rId + "] " + change);
        }
    }

    /**
     * Determine whether this new event should be logged, and if it should be
     * shown in the health map and queue. The log should display all unique
     * events, even if they come out of order. The health map and queue should
     * consider time, and only display timely, current events.
     */
    private EventAction shouldUpdate(ServiceChange previous,
                                     ServiceChange previousLogged,
                                     ServiceChange newEvent) {

        /* This is the first status for this service, so use the new event. */
        if (previous == null) {
            return EventAction.LOG_AND_MAP;
        }

        /*
         * This "new" event is actually older than the one that is already
         * tracked. Log it, but don't map it.
         */
        if (previous.getChangeTime() > newEvent.getChangeTime()) {
            return EventAction.LOG_DONT_MAP;
        }

        /*
         * This "new" event is an unreachable status, manufactured by
         * the monitor. If the old status was more conclusive, don't supercede
         * it with an UNREACHABLE status, because that would lose information.
         */
        if ((newEvent.getStatus() == ServiceStatus.UNREACHABLE) &&
            (previous.getStatus().isTerminalState())) {
            if (previousLogged.getStatus() == ServiceStatus.UNREACHABLE) {
                /* We already logged this state; don't report it again. */
                return EventAction.DONT_LOG_OR_MAP;
            }
            return EventAction.LOG_DONT_MAP;
        }

        /*
         * This new event is the same status as the old event, and isn't a
         * real change.
         */
        if (newEvent.getStatus() == previous.getStatus()) {
            return EventAction.DONT_LOG_OR_MAP;
        }

        return EventAction.LOG_AND_MAP;
    }

    /**
     * Get the current status for all resources.
     */
    public Map<ResourceId, ServiceChange> getStatus() {
        return new HashMap<ResourceId, ServiceChange>(serviceHealth);
    }

    /**
     * Get a list of events that have occurred since the given time.
     */
    @Override
    public synchronized
        RetrievedEvents<ServiceChange> retrieveNewEvents(long pointInTime) {

        List<EventHolder<ServiceChange>> values =
        	new ArrayList<EventHolder<ServiceChange>>();

        long syntheticStampOfLastRecord = pointInTime;
        for (EventHolder<ServiceChange> se : queue) {
            if (se.getSyntheticTimestamp() > pointInTime) {
                values.add(se);
                syntheticStampOfLastRecord = se.getSyntheticTimestamp();
            }
        }

        return new RetrievedEvents<ServiceChange>
        	(syntheticStampOfLastRecord, values);
    }

    private static class EventAction {
        final boolean shouldLog;
        final boolean shouldMap;

        static EventAction LOG_AND_MAP = new EventAction(true, true);
        static EventAction LOG_DONT_MAP = new EventAction(true, false);
        static EventAction DONT_LOG_OR_MAP = new EventAction(false, false);

        private EventAction(boolean shouldLog, boolean shouldMap) {
            this.shouldLog = shouldLog;
            this.shouldMap = shouldMap;
        }
    }
}
