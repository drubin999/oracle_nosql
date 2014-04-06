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

import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.FormatUtils;

/**
 * Used by a third party service (usually the SNA) to report a status change
 * experienced by another service (usually a RepNode).  This is useful when the
 * RepNode is shutting down or handling errors and may not be up to provide its
 * own service status.
 */
public class ProxiedServiceStatusChange extends ServiceStatusChange {

    private static final long serialVersionUID = 1L;

    /* The resource that experienced the change. */
    private final ResourceId target;

    /**
     * @param target the resource that experienced the change.
     * @param newStatus the new service status.
     */
    public ProxiedServiceStatusChange(ResourceId target,
                                      ServiceStatus newStatus) {
        super(newStatus);
        this.target = target;
    }

    @Override
    public ResourceId getTarget(ResourceId reportingResource) {
        return target;
    }

    @Override
    public String toString() {
        return target + ": Service status: " + newStatus + " " +
            FormatUtils.formatDateAndTime(now);
    }
}
