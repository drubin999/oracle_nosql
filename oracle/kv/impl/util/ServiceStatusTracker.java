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

package oracle.kv.impl.util;

import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import oracle.kv.impl.measurement.ServiceStatusChange;
import oracle.kv.impl.monitor.AgentRepository;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;

/**
 * Tracks service level state changes, so that it can be conveniently
 * monitored and tested.
 */
public class ServiceStatusTracker {

    private ServiceStatusChange prev = null;
    private ServiceStatusChange current =
        new ServiceStatusChange(ServiceStatus.STARTING);

    /**
     * The listeners that are invoked on each update.
     */
    private final List<Listener> listeners;

    /**
     * The logger used to log service state changes.
     */
    private Logger logger;

    /**
     * Create a ServiceStatusTracker when the MonitorAgent repository isn't
     * yet available.
     */
    public ServiceStatusTracker(Logger logger) {
        this.logger = logger;
        listeners = new LinkedList<Listener>();
    }

    /**
     * Create a ServiceStatusTracker and attach a MonitorAgent listener.
     */
    public ServiceStatusTracker
        (Logger logger, AgentRepository agentRepository) {
        this(logger);
        addListener(new StatusMonitor(agentRepository));
    }

    /**
     * Returns the instantaneous service status
     */
    public ServiceStatus getServiceStatus() {
        return current.getStatus();
    }

    /**
     * Reset logger
     */
    public synchronized void setLogger(Logger logger) {
        this.logger = logger;
    }

    /**
     * Add a new listener
     */
    public synchronized void addListener(Listener listener) {
        listeners.add(listener);
    }

    /**
     * Add a listener that connects this status tracker to the monitor agent.
     */
    public synchronized void addListener(AgentRepository agentRepository) {
        listeners.add(new StatusMonitor(agentRepository));
    }

    /**
     * Updates the service status if it has changed. Upon a change it invokes
     * any listeners that may be registered to listen to such changes.
     */
    public synchronized void update(ServiceStatus newStatus) {
        if (current.getStatus().equals(newStatus)) {
            current.updateTime();
            return;
        }

        prev = current;
        current = new ServiceStatusChange(newStatus);

        logger.info("Service status changed from " + prev.getStatus() +
                    " to " + newStatus);

        for (Listener listener : listeners) {
            listener.update(prev, current);
        }

    }

    /**
     * A Listener used for monitoring and test purposes.
     */
    public interface Listener {
        void update(ServiceStatusChange prevStatus,
                    ServiceStatusChange newStatus);
    }

    /**
     * Sends status changes to the monitor agent.
     */
    private class StatusMonitor implements Listener {

        private final AgentRepository monitorAgentBuffer;

        StatusMonitor(AgentRepository monitorAgentBuffer) {
            this.monitorAgentBuffer = monitorAgentBuffer;
        }

        @Override
        public void update(ServiceStatusChange prevStatus,
                           ServiceStatusChange newStatus) {
            monitorAgentBuffer.add(newStatus);
        }
    }
}
