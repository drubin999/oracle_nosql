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

package oracle.kv.impl.monitor;

import oracle.kv.impl.measurement.MeasurementType;

/**
 * Provides a catalog of KVStore monitoring measurement types.
 *
 * The id field of the MeasurementType is used as the identifier for
 * metrics transmitted on the network. Because of that, any change or
 * addition of id values constitutes a network protocol change. Existing id
 * values should never be changed for any metric that has been used in an
 * production system.
 *
 * The Metrics catalog will need not be transmitted, and is not Serializable.
 */
public class Metrics {

    /*
     * This defines the types of monitoring packets sent within the KVS.
     */
    public static MeasurementType LOG_MSG = new MeasurementType
        (1, "Info", "Logging output from kvstore components.");
    public static MeasurementType SERVICE_STATUS = new MeasurementType
        (2, "ServiceStatus", "Service status for kvstore components.");
    public static MeasurementType PLAN_STATE = new MeasurementType
        (3, "PlanState", "Plan execution state changes.");
    public static MeasurementType PRUNED = new MeasurementType
        (4, "PrunedMeasurements",
         "Record of measurements that have aged out of the monitoring " +
         "repository before being viewed");

    public static MeasurementType RNSTATS = new MeasurementType
        (10, "RepNodeStats", "Collection of stats for a single RepNode");
}
