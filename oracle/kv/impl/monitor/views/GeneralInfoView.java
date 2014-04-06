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

import java.util.Collections;
import java.util.Set;
import java.util.logging.Logger;

import oracle.kv.impl.admin.AdminServiceParams;
import oracle.kv.impl.measurement.Measurement;
import oracle.kv.impl.measurement.MeasurementType;
import oracle.kv.impl.monitor.Metrics;
import oracle.kv.impl.monitor.Monitor;
import oracle.kv.impl.monitor.View;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * This MonitorView accepts miscellaneous information from all services and
 * funnels it to a single Logger which saves the information in the
 * kvstore-wide logging file.
 */
public class GeneralInfoView implements View {

    private final Logger logger;

    public GeneralInfoView(AdminServiceParams params) {
        logger = LoggerUtils.getStorewideViewLogger(this.getClass(), params);
    }

    @Override
    public String getName() {
        return Monitor.INTERNAL_GENERAL_INFO_VIEW;
    }

    @Override
    public Set<MeasurementType> getTargetMetricTypes() {
        return Collections.singleton(Metrics.PRUNED);
    }

    @Override
    public void applyNewInfo(ResourceId resourceId, Measurement m) {
        logger.info("[" + resourceId + "] " + m);
    }

    @Override
    public void close() {
      /* Nothing to do */
    }
}
