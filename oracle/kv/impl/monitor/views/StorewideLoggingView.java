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
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import oracle.kv.impl.admin.AdminServiceParams;
import oracle.kv.impl.measurement.LoggerMessage;
import oracle.kv.impl.measurement.Measurement;
import oracle.kv.impl.measurement.MeasurementType;
import oracle.kv.impl.monitor.Metrics;
import oracle.kv.impl.monitor.Monitor;
import oracle.kv.impl.monitor.View;
import oracle.kv.impl.monitor.ViewListener;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * This MonitorView accepts logging information from all kvstore services and
 * funnels it to a single Logger which saves the information in the
 * kvstore-wide logging file.
 */
public class StorewideLoggingView implements View {

    private final Logger logger;
    private final Set<ViewListener<LogRecord>> listeners;
    private TestHook<LoggerMessage> testHook;
    
    /* Used for error and usage messages. */
    private final String logfileName;

    public StorewideLoggingView(AdminServiceParams params) {
        logger = LoggerUtils.getStorewideViewLogger(this.getClass(), params);
        logger.setLevel(Level.ALL);
        listeners = new HashSet<ViewListener<LogRecord>>();
        logfileName = LoggerUtils.getStorewideLogName
            (params.getStorageNodeParams().getRootDirPath(), 
             params.getGlobalParams().getKVStoreName());
    }

    @Override
    public String getName() {
        return Monitor.INTERNAL_STOREWIDE_LOGGING_VIEW;
    }

    @Override
    public Set<MeasurementType> getTargetMetricTypes() {
        return Collections.singleton(Metrics.LOG_MSG);
    }

    public synchronized void addListener(ViewListener<LogRecord> listener) {
        listeners.add(listener);
    }

    @Override
    public void applyNewInfo(ResourceId resourceId,  Measurement m) {

        /* This view only accesses LoggerMessages, so a cast should be okay. */
        LoggerMessage logMsg = (LoggerMessage) m;
        LogRecord record = logMsg.getLogRecord();
        if (!logger.isLoggable(record.getLevel())) {
            return;
        }

        /*
         * The resourceId is not part of the information transmitted from the
         * service, since that would be redundant. The monitor knows, by dint
         * of the connection, the source of the measurements. However, append it
         * here to make the source obvious on the consolidated storewide view.
         */
        record.setMessage("[" + resourceId + "] " + record.getMessage());
        logger.log(record);

        /* Distribute the log records to any listeners. */
        for (ViewListener<LogRecord> listener : listeners) {
            listener.newInfo(resourceId, record);
        }

        assert TestHookExecute.doHookIfSet(testHook, logMsg);
    }

    public void setTestHook(TestHook<LoggerMessage> hook) {
        testHook = hook;
    }

    @Override
    public void close() {
        logger.fine("Closing StorewideLoggingView");
        for (Handler handler : logger.getHandlers()) {
            logger.removeHandler(handler);
            /*
             * Don't close the handler, let LoggerUtils do that.  Removing the
             * handler will trigger an update on the handler if required.
             */
        }
    }

    /* For use in error and usage messages. */
    public String getStorewideLogName() {
        return logfileName;
    }
}
