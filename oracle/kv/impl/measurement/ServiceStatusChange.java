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

package oracle.kv.impl.measurement;

import java.io.Serializable;

import oracle.kv.impl.monitor.Metrics;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.ConfigurableService;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.FormatUtils;

/**
 * A notification that a {@link ConfigurableService} has had a service status
 * change.
 */
public class ServiceStatusChange implements Measurement, Serializable {

    private static final long serialVersionUID = 1L;
    protected final ServiceStatus newStatus;
    protected long now;

    public ServiceStatusChange(ServiceStatus newStatus) {

        this.newStatus = newStatus;
        this.now = System.currentTimeMillis();
    }

    @Override
    public int getId() {
        return Metrics.SERVICE_STATUS.getId();
    }

    public ServiceStatus getStatus() {
        return newStatus;
    }

    /**
     * @return the timeStamp
     */
    public long getTimeStamp() {
        return now;
    }

    public void updateTime() {
        now = System.currentTimeMillis();
    }

    /**
     * For unit testing.
     */
    public void updateTime(long time) {
        now = time;
    }

    @Override
    public String toString() {
        return "Service status: " + newStatus + " " +
            FormatUtils.formatDateAndTime(now);
    }

    @Override
    public long getStart() {
        return now;
    }

    @Override
    public long getEnd() {
        return now;
    }

    /**
     * Return the resourceId of the service that experienced this status
     * change. In subclasses, this may not be the same as the service that 
     * reported the change.
     */
    public ResourceId getTarget(ResourceId reportingResource) {
        return reportingResource;
    }
}
