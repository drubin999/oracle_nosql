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

import java.io.Serializable;

import oracle.kv.impl.measurement.ServiceStatusChange;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;

/**
 * Expresses a change in status at a remote service.
 */
public class ServiceChange implements Serializable {

    private static final long serialVersionUID = 1L;

    /*
     * The service which reported the change. This may not be the service
     * that experienced the change.
     */
    private final ResourceId reportingId;

    /* The service which experienced the status change. */
    private final ResourceId originalId;

    /* The timestamp when the change first took effect. */
    private final long changeTime;

    private final ServiceStatus current;

    public ServiceChange(ResourceId reportingId, ServiceStatusChange change) {
        this.reportingId = reportingId;
        this.originalId = change.getTarget(reportingId);
        this.changeTime = change.getTimeStamp();
        this.current = change.getStatus();
    }

    public long getChangeTime() {
        return changeTime;
    }

    public ServiceStatus getStatus() {
        return current;
    }

    /**
     * @return the resource which experienced the change.
     */
    public ResourceId getTarget() {
        return originalId;
    }

    /**
     * @return the resource which reported the status change. This may
     * not be the resource which experienced the change.
     */
    public ResourceId getReporter() {
        return reportingId;
    }

    /**
     * For now, use the status severity. In the future, we could add
     * additional factors.
     */
    public int getSeverity() {
        return current.getSeverity();
    }

    /**
     * For now, use the status's needAlert. In the future, we could add
     * additional factors.
     */
    public boolean isNeedsAlert() {
        return current.needsAlert();
    }
}
