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

package oracle.kv.impl.util.server;

import java.io.File;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import oracle.kv.impl.admin.AdminServiceParams;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.api.ClientId;
import oracle.kv.impl.monitor.AdminDirectHandler;
import oracle.kv.impl.monitor.AgentRepository;
import oracle.kv.impl.monitor.LogToMonitorHandler;
import oracle.kv.impl.monitor.MonitorAgentHandler;
import oracle.kv.impl.monitor.MonitorKeeper;
import oracle.kv.impl.rep.RepNodeService;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.CommonLoggerUtils;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.LogFormatter;
import oracle.kv.util.FileHandler;

/**
 * General utilities for creating and formatting java.util loggers and handlers.
 */
public class LoggerUtils {

    /*
     * A general Logger provided by this class is hooked up to three handlers:
     *
     *  1. ConsoleHandler, which will display to stdout on the local machine
     *  2. FileHandler, which will display to the <resourceId>.log file on the
     *     local machine.
     *     Note that special loggers are created to also connect to the .perf
     *     and .stat files.
     *  3. MonitorHandler, which will funnel output to the Monitor. If this
     *     service is remote and implements a MonitorAgent, the output is
     *     saved in the buffered repository implemented by the MonitorAgent.
     *     If the service is local to the Admin process, the handler publishes
     *     it directly to Monitoring.
     *
     * Shared Handlers:
     *
     * Each logging resource must share a FileHandler, because different
     * FileHandlers are seen as conflicting by java.util.logging, and will open
     * unique files. Likewise, each logging resource shares a MonitorHandler,
     * which funnels logging output into the monitor agent buffer, for later
     * pickup by the monitoring system.
     *
     * This mapping is managed with the FileHandler and MonitorHandler map,
     * which are keyed by kvstore name and resource id. Note that when the
     * caller wants to obtain a logger, this obliges the caller to have the
     * kvstore name in hand. FileHandlers also require additional parameters to
     * configure the count and limit of files. The MonitorAgentHandler must be
     * configured to bound the size of the recording repository.
     *
     * Making the kvstore name and required parameters available when logging
     * means that parameter classes are passed downward many levels. That
     * creates a minor dissonance in pattern. We've centralized the file
     * handler map in this static map, whereas we've passed the parameter class
     * downward. Arguably we could have also passed the handler maps, or some
     * handle to it, in much the same way as we pass the parameters.
     *
     * We consciously chose not to do so. The handler map shouldn't be
     * referenced by the parameter class, and we don't want to pass additional
     * parameters downward.
     */

    /**
     * Directs logging output to a file per service, on the local node. The
     * file handlers are kept in a single global map to make it easier to
     * clean up upon exit.
     */
    private static final
        ConcurrentHashMap<ServiceHandlerKey, FileHandler> FILE_HANDLER_MAP;
    static {
        FILE_HANDLER_MAP =
            new ConcurrentHashMap<ServiceHandlerKey, FileHandler>();
    }

    /**
     * Directs iostat style perf output to a file on the admin node.
     */
    private static final
        ConcurrentHashMap<ServiceHandlerKey, FileHandler> PERF_FILE_HANDLER_MAP;
    static {
        PERF_FILE_HANDLER_MAP =
            new ConcurrentHashMap<ServiceHandlerKey, FileHandler>();
    }

    /**
     * Directs rep/environment stat output to a file on the admin node.
     */
    private static final
        ConcurrentHashMap<ServiceHandlerKey, FileHandler> STAT_FILE_HANDLER_MAP;
    static {
        STAT_FILE_HANDLER_MAP =
            new ConcurrentHashMap<ServiceHandlerKey, FileHandler>();
    }

    /**
     * Directs logging output to a single monitor agent repository per service,
     * on the local node.
     */
    private static final
        ConcurrentHashMap<ServiceHandlerKey, LogToMonitorHandler>
        MONITOR_HANDLER_MAP;
    static {
        MONITOR_HANDLER_MAP =
            new ConcurrentHashMap<ServiceHandlerKey, LogToMonitorHandler>();
    }

    /**
     * A single bootstrap log file that is not associated with a store.  This
     * is created by the SNA and used for logging and debugging bootstrap
     * startup state.
     */
    private static Logger bootstrapLogger;

    /**
     * Return a String to be used in error messages and other usage situations
     * that describes where the storewide logging file is.
     */
    public static String getStorewideLogName(String rootDirPath,
                                             String kvStoreName) {
        File loggingDir = FileNames.getLoggingDir
            (new File(rootDirPath), kvStoreName);
        return loggingDir.getPath() +
            File.separator + kvStoreName + "_{0..N}." +
            FileNames.LOG_FILE_SUFFIX;
    }

    /**
     * Get the single bootstrap logger.  There may be one of these for the SNA
     * and one for the bootstrap admin, but not in the same process.
     */
    public static Logger getBootstrapLogger(String kvdir,
                                            String filename,
                                            String label) {

        if (bootstrapLogger == null) {
            bootstrapLogger = Logger.getLogger(filename);
            bootstrapLogger.setUseParentHandlers(false);

            String logFilePattern = makeFilePattern(kvdir, filename,
                                                    FileNames.LOG_FILE_SUFFIX);
            try {
                FileHandler newHandler =
                    new oracle.kv.util.FileHandler(logFilePattern,
                                                   1000000, /* limit */
                                                   20, /* count */
                                                   true /* append */);
                newHandler.setFormatter(new LogFormatter(label));
                bootstrapLogger.addHandler(newHandler);
                addConsoleHandler(bootstrapLogger, label);
            } catch (IOException e) {
                throw new IllegalStateException("Problem creating bootstrap " +
                                                "log file: " + logFilePattern);
            }
        }
        return bootstrapLogger;
    }

    /**
     * This flavor of logger logs only to the console, and is for use by the
     * client library, which does not have disk access nor monitoring.
     */
    public static Logger getLogger(Class<?> cl,
                                   ClientId clientId) {
        return LoggerUtils.getLogger(cl,
                                     clientId.toString(),
                                     clientId,
                                     null,  /* globalParams */
                                     null); /* storageNodeParams */
    }

    /**
     * For logging with no resource id, which could be from a pre-registration
     * StorageNodeAgent, or from tests. Monitoring goes only to console, and
     * does not go to a file nor to monitoring.
     * @param label descriptive name used to prefix logging output.
     */
    public static Logger getLogger(Class<?> cl, String label) {
        return LoggerUtils.getLogger(cl,
                                     label,
                                     null,  /* resourceId */
                                     null,  /* globalParams */
                                     null); /* storageNodeParams */
    }

    /**
     * Obtain a logger which sends output to the console, its local logging
     * file, and the Monitor.
     */
    public static Logger getLogger(Class<?> cl,
                                   AdminServiceParams params) {
        AdminId adminId = params.getAdminParams().getAdminId();
        return LoggerUtils.getLogger(cl,
                                     adminId.toString(),
                                     adminId,
                                     params.getGlobalParams(),
                                     params.getStorageNodeParams());
    }

    /**
     * Obtain a logger which sends output to the console, its local logging
     * file, and its local MonitorAgent.
     */
    public static Logger getLogger(Class<?> cl,
                                   RepNodeService.Params params) {

        RepNodeId repNodeId = params.getRepNodeParams().getRepNodeId();
        return LoggerUtils.getLogger(cl,
                                     repNodeId.toString(),
                                     repNodeId,
                                     params.getGlobalParams(),
                                     params.getStorageNodeParams());
    }

    /**
     * Obtain a logger which sends output to the console, its local logging
     * file, and its local MonitorAgent.
     */
    public static Logger getLogger(Class<?> cl,
                                   GlobalParams globalParams,
                                   StorageNodeParams storageNodeParams) {

        StorageNodeId storageNodeId = storageNodeParams.getStorageNodeId();
        return LoggerUtils.getLogger(cl,
                                     storageNodeId.toString(),
                                     storageNodeId,
                                     globalParams,
                                     storageNodeParams);
    }

    /**
     * Get a logger which will only log to the Console.
     */
    public static Logger getConsoleOnlyLogger(Class<?> cl,
                                              RepNodeId repNodeId) {
        return LoggerUtils.getLogger(cl, repNodeId.toString(), repNodeId,
                                     null, null);
    }

    /**
     * Get a logger which will only log to the  resource's logging file. It's
     * meant for temporary use, to log information at service shutdown.
     * Global and StorageNodeParams must not be null.
     */
    public static Logger getFileOnlyLogger
        (Class<?> cl,
         ResourceId resourceId,
         GlobalParams globalParams,
         StorageNodeParams storageNodeParams ) {

        Logger logger = Logger.getLogger(cl.getName() + ".TEMP_" + resourceId);
        logger.setUseParentHandlers(false);

        /* Check whether the logger already has existing handlers. */
        boolean hasFileHandler = false;

        /*
         * [#18277] Add null check of logger.getHandlers() because the Resin
         * app server's implementation of logging can return null instead of an
         * empty array.
         */
        Handler[] handlers = logger.getHandlers();
        if (handlers != null) {
            for (Handler h : handlers) {
                if (h instanceof oracle.kv.util.FileHandler) {
                    hasFileHandler = true;
                }
            }
        }

        if (!hasFileHandler) {
            addLogFileHandler(logger,
                              resourceId.toString(),
                              resourceId.toString(),
                              globalParams.getKVStoreName(),
                              new File(storageNodeParams.getRootDirPath()),
                              storageNodeParams.getLogFileLimit(),
                              storageNodeParams.getLogFileCount());
        }

        return logger;
    }

    /**
     * Get a logger which will only log to the resource's perf file.
     */
    public static Logger getPerfFileLogger(Class<?> cl,
                                           GlobalParams globalParams,
                                           StorageNodeParams snParams ) {

        String kvName = globalParams.getKVStoreName();
        Logger logger = Logger.getLogger(cl.getName() + ".PERF_" + kvName);
        logger.setUseParentHandlers(false);

        Handler[] handlers = logger.getHandlers();
        boolean hasFileHandler = false;
        if (handlers != null) {
            for (Handler h : handlers) {
                if (h instanceof oracle.kv.util.FileHandler) {
                    hasFileHandler = true;
                    break;
                }
            }
        }

        if (hasFileHandler) {
            return logger;
        }

        /* Send this logger's output to a storewide .perf file. */
        addFileHandler(PERF_FILE_HANDLER_MAP,
                       logger,
                       new Formatter() {
                           @Override
                           public String format(LogRecord record) {
                               return record.getMessage() + "\n";
                           }
                       },
                       kvName,
                       kvName,
                       new File(snParams.getRootDirPath()),
                       FileNames.PERF_FILE_SUFFIX,
                       snParams.getLogFileLimit(),
                       snParams.getLogFileCount());
        return logger;
    }

    /**
     * Get a logger which will only log to the resource's stats file.
     */
    public static Logger getStatFileLogger(Class<?> cl,
                                           GlobalParams globalParams,
                                           StorageNodeParams snParams ) {

        String kvName = globalParams.getKVStoreName();
        Logger logger = Logger.getLogger(cl.getName() + ".STAT_" + kvName);
        logger.setUseParentHandlers(false);

        Handler[] handlers = logger.getHandlers();
        boolean hasFileHandler = false;
        if (handlers != null) {
            for (Handler h : handlers) {
                if (h instanceof oracle.kv.util.FileHandler) {
                    hasFileHandler = true;
                    break;
                }
            }
        }

        if (hasFileHandler) {
            return logger;
        }

        /* Send this logger's output to a storewide .stat file. */
        addFileHandler(STAT_FILE_HANDLER_MAP,
                       logger,
                       new LogFormatter(kvName),
                       kvName,
                       kvName,
                       new File(snParams.getRootDirPath()),
                       FileNames.STAT_FILE_SUFFIX,
                       snParams.getLogFileLimit(),
                       snParams.getLogFileCount());

        return logger;
    }

    /**
     * Obtain a logger which sends output to the console, its local logging
     * file, and its Monitor handler.
     */
    public static Logger getLogger(Class<?> cl,
                                   String prefix,
                                   ResourceId resourceId,
                                   GlobalParams globalParams,
                                   StorageNodeParams storageNodeParams ) {

        Logger logger = Logger.getLogger(cl.getName() + "." + resourceId);
        logger.setUseParentHandlers(false);

        /* Check whether the logger already has existing handlers. */
        boolean hasConsoleHandler = false;
        boolean hasFileHandler = false;
        boolean hasAdminDirectHandler = false;

        /*
         * [#18277] Add null check of logger.getHandlers() because the Resin
         * app server's implementation of logging can return null instead of an
         * empty array.
         */
        Handler[] handlers = logger.getHandlers();
        if (handlers != null) {
            for (Handler h : handlers) {
                if (h instanceof oracle.kv.util.ConsoleHandler) {
                    hasConsoleHandler = true;
                } else if (h instanceof oracle.kv.util.FileHandler) {
                    hasFileHandler = true;
                } else if (h instanceof
                           oracle.kv.impl.monitor.LogToMonitorHandler) {
                    hasAdminDirectHandler = true;
                }
            }
        }

        if (!hasConsoleHandler) {
            addConsoleHandler(logger, prefix);
        }

        /*
         * Only loggers that belong to kvstore classes that know their kvstore
         * directories, and are components with resource ids, log into a file
         */
        if (globalParams != null) {
            if ((storageNodeParams != null) && (!hasFileHandler)) {
                addLogFileHandler(logger,
                                  prefix,
                                  resourceId.toString(),
                                  globalParams.getKVStoreName(),
                                  new File(storageNodeParams.getRootDirPath()),
                                  storageNodeParams.getLogFileLimit(),
                                  storageNodeParams.getLogFileCount());
            }

            /*
             * If this service has a monitorHandler registered, connect to that
             * handler. TODO: Do we need to check for a current monitor handler?
             */
            if (!hasAdminDirectHandler) {
                addMonitorHandler(logger,
                                  globalParams.getKVStoreName(),
                                  resourceId);
            }
        }

        return logger;
    }

    /**
     * This logger displays output for the store wide view provided by the
     * monitoring node. Logging input comes from MonitorAgents and data sent
     * directly to the Monitor, and the resulting output is displayed to the
     * console and to a store wide log file. Used only by the Monitor. TODO:
     * funnel to UI.
     */
    public static Logger
        getStorewideViewLogger(Class<?> cl,  AdminServiceParams adminParams) {

        String storeName = adminParams.getGlobalParams().getKVStoreName();
        Logger logger = Logger.getLogger(cl.getName() + "." + storeName);

        logger.setUseParentHandlers(false);

        /* Check whether the logger already has existing handlers. */
        boolean hasConsoleHandler = false;
        boolean hasFileHandler = false;

        /*
         * [#18277] Add null check of logger.getHandlers() because the Resin
         * app server's implementation of logging can return null instead of an
         * empty array.
         */
        Handler[] handlers = logger.getHandlers();
        if (handlers != null) {
            for (Handler h : handlers) {
                if (h instanceof oracle.kv.util.StoreConsoleHandler) {
                    hasConsoleHandler = true;
                } else if (h instanceof oracle.kv.util.FileHandler) {
                    hasFileHandler = true;
                }
            }
        }

        if (!hasConsoleHandler) {
            Handler handler = new oracle.kv.util.StoreConsoleHandler();
            handler.setFormatter(new LogFormatter());
            logger.addHandler(handler);
        }

        /**
         * Use log file count and limit from AdminParams, not StorageNodeParams
         */
        if (!hasFileHandler) {
            StorageNodeParams snp = adminParams.getStorageNodeParams();
            GlobalParams gp = adminParams.getGlobalParams();
            AdminParams ap = adminParams.getAdminParams();
            addLogFileHandler(logger,
                              null, // label
                              gp.getKVStoreName(),
                              gp.getKVStoreName(),
                              new File(snp.getRootDirPath()),
                              ap.getLogFileLimit(),
                              ap.getLogFileCount());
        }
        return logger;
    }

    /**
     * Each service that implements a MonitorAgent which collects logging
     * output should register a handler in its name, before any loggers come
     * up.
     * @param kvName
     * @param resourceId
     * @param agentRepository
     */
    public static void
        registerMonitorAgentBuffer(String kvName,
                                   ResourceId resourceId,
                                   AgentRepository agentRepository) {

        /*
         * Create a handler that is just a pass through to the monitor agent's
         * buffer.
         */
        MonitorAgentHandler handler = new MonitorAgentHandler(agentRepository);

        String resourceName = resourceId.toString();
        handler.setFormatter(new LogFormatter(resourceName));
        MONITOR_HANDLER_MAP.put(new ServiceHandlerKey(resourceName, kvName),
                                handler);

    }

    public static void registerMonitorAdminHandler(String kvName,
                                                   AdminId adminId,
                                                   MonitorKeeper admin) {
        LogToMonitorHandler handler = new AdminDirectHandler(admin);
        String resourceName = adminId.toString();
        handler.setFormatter(new LogFormatter(resourceName));
        MONITOR_HANDLER_MAP.put(new ServiceHandlerKey(resourceName, kvName),
                                handler);
    }

    /**
     * Attach a handler which directs output to the monitoring system. If this
     * is a remote service, the logging output goes to the agent repository. If
     * this is a service that is on the Admin process, the logging output goes
     * directly to the Monitor.
     */
    private static void addMonitorHandler(Logger logger,
                                          String kvName,
                                          ResourceId resourceId) {

        Handler handler =
            MONITOR_HANDLER_MAP.get(new ServiceHandlerKey
                                    (resourceId.toString(), kvName));
        if (handler != null) {
            logger.addHandler(handler);
        }
    }

    /**
     * Attach a handler which directs output to stdout.
     */
    private static void addConsoleHandler(Logger logger, String prefix) {

        Handler handler = new oracle.kv.util.ConsoleHandler();
        handler.setFormatter(new LogFormatter(prefix));
        logger.addHandler(handler);
    }

    /**
     * Add a handler that sends this logger's output to a .log file.
     */
    private static void addLogFileHandler(Logger logger,
                                          String label,
                                          String resourceName,
                                          String kvName,
                                          File rootDir,
                                          int fileLimit,
                                          int fileCount) {
        addFileHandler(FILE_HANDLER_MAP, logger, new LogFormatter(label),
                       resourceName, kvName, rootDir,
                       FileNames.LOG_FILE_SUFFIX, fileLimit, fileCount);
    }

    /**
     * Attach a handler which directs output to a logging file on the node.
     */
    private static void addFileHandler
        (ConcurrentMap<ServiceHandlerKey,FileHandler> map,
         Logger logger,
         Formatter formatter,
         String resourceName,
         String kvName,
         File rootDir,
         String suffix,
         int fileLimit,
         int fileCount) {

        /*
         * Avoid calling new FileHandler unnecessarily, because the FileHandler
         * constructor will actually create the log file. Check the map first
         * to see if a handler exists.
         */
        ServiceHandlerKey handlerKey =
            new ServiceHandlerKey(resourceName, kvName);
        Handler existing = map.get(handlerKey);

        /*
         * A FileHandler exists, just connect it to this logger unless the
         * parameters have changed, in which case close the existing one and
         * make a new one.
         */
        if (existing != null) {
            oracle.kv.util.FileHandler fh =
                (oracle.kv.util.FileHandler) existing;
            if (fh.getLimit() == fileLimit && fh.getCount() == fileCount) {
                logger.addHandler(existing);
                return;
            }
            existing.close();
            map.remove(handlerKey);
            existing = null;
        }

        /* A FileHandler does not exist yet, so create one. */
        FileHandler newHandler;
        try {

            FileNames.makeLoggingDir(rootDir, kvName);

            String logFilePattern = makeFilePattern
                (FileNames.getLoggingDir(rootDir, kvName).getPath(),
                 resourceName, suffix);

            newHandler =
                new oracle.kv.util.FileHandler(logFilePattern,
                                               fileLimit,
                                               fileCount,
                                               true /* append */);
            newHandler.setFormatter(formatter);
        } catch (SecurityException e) {
            throw new IllegalStateException
                ("Problem creating log file for logger " + resourceName, e);
        } catch (IOException e) {
            throw new IllegalStateException
                ("Problem creating log file for logger " + resourceName, e);
        }

        existing = map.putIfAbsent(handlerKey, newHandler);
        if (existing == null) {
            logger.addHandler(newHandler);
        } else {
            /*
             * Something else beat us to the unch and registered a
             * FileHandler, so we won't be using the one we created. Release
             * its files.
             */
            newHandler.close();
            logger.addHandler(existing);
        }
    }

    /**
     * Follow a consistent naming convention for all KV log files.
     */
    private static String makeFilePattern(String parent,
                                          String fileName,
                                          String suffix) {
        return parent + File.separator + fileName + "_%g." + suffix;
    }

    /**
     * Close all FileHandler and Monitor Handlers for the given kvstore.
     * FileHandlers created by any KVStore process need to be released when the
     * process is shutdown. This method is thread safe, because the close()
     * call is reentrant and the FileHandlerMap is a concurrent map.
     * @param kvName if null, all file handlers for all kvstores running in this
     * process are closed. If a value is supplied, only those handlers that
     * work on behalf of the given store are close.
     */
    public static void closeHandlers(String kvName) {
        for (Map.Entry<ServiceHandlerKey,FileHandler> entry :
                 FILE_HANDLER_MAP.entrySet()) {
            if (entry.getKey().belongs(kvName)) {
                entry.getValue().close();
                FILE_HANDLER_MAP.remove(entry.getKey());
            }
        }

        for (Map.Entry<ServiceHandlerKey,FileHandler> entry :
                 PERF_FILE_HANDLER_MAP.entrySet()) {
            if (entry.getKey().belongs(kvName)) {
                entry.getValue().close();
                PERF_FILE_HANDLER_MAP.remove(entry.getKey());
            }
        }

        for (Map.Entry<ServiceHandlerKey,FileHandler> entry :
                 STAT_FILE_HANDLER_MAP.entrySet()) {
            if (entry.getKey().belongs(kvName)) {
                entry.getValue().close();
                STAT_FILE_HANDLER_MAP.remove(entry.getKey());
            }
        }

        for (Map.Entry<ServiceHandlerKey,LogToMonitorHandler> entry :
                 MONITOR_HANDLER_MAP.entrySet()) {
            if (entry.getKey().belongs(kvName)) {
                entry.getValue().close();
                MONITOR_HANDLER_MAP.remove(entry.getKey());
            }
        }
    }

    /**
     * Close all FileHandlers in the process.
     */
    public static void closeAllHandlers() {
        closeHandlers(null);
        if (bootstrapLogger != null) {
            Handler[] handlers = bootstrapLogger.getHandlers();
            if (handlers != null) {
                for (Handler h : handlers) {
                    h.close();
                }
            }
        }
    }

    /**
     * Get the value of a specified Logger property.
     */
    public static String getLoggerProperty(String property) {
        return CommonLoggerUtils.getLoggerProperty(property);
    }

    /**
     * Utility method to return a String version of a stack trace
     */
    public static String getStackTrace(Throwable t) {
        return CommonLoggerUtils.getStackTrace(t);
    }

    /**
     * File and MonitorHandlers are unique to each service within a kvstore
     * instance.
     */
    private static class ServiceHandlerKey {

        private final String resourceName;
        private final String kvName;

        ServiceHandlerKey(String resourceName, String kvName) {
            this.resourceName = resourceName;
            this.kvName = kvName;
        }

        /**
         * Return true if kvStoreName is null, or if it matches this key's
         * store.
         */
        boolean belongs(String kvStoreName) {
            if (kvStoreName == null) {
                return true;
            }

            return kvName.equals(kvStoreName);
        }

        /**
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result
                + ((kvName == null) ? 0 : kvName.hashCode());
            result = prime * result
                + ((resourceName == null) ? 0 : resourceName.hashCode());
            return result;
        }

        /**
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj == null) {
                return false;
            }

            if (getClass() != obj.getClass()) {
                return false;
            }

            ServiceHandlerKey other = (ServiceHandlerKey) obj;

            if (kvName == null) {
                if (other.kvName != null) {
                    return false;
                }
            } else if (!kvName.equals(other.kvName)) {
                return false;
            }

            if (resourceName == null) {
                if (other.resourceName != null) {
                    return false;
                }
            } else if (!resourceName.equals(other.resourceName)) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return kvName + "/" + resourceName;
        }
    }

    /**
     * Remove the given log handler from any associated loggers.
     */
    public static void removeHandler(Handler handler) {
        LogManager lm = LogManager.getLogManager();

        Enumeration<String> loggerNames = lm.getLoggerNames();

        while (loggerNames.hasMoreElements()) {
            String name = loggerNames.nextElement();
            Logger logger = lm.getLogger(name);
            if (logger != null) {
                Handler[] handlers = logger.getHandlers();
                if (handlers != null) {
                    for (Handler h : handlers) {
                        if (h == handler) {
                            logger.removeHandler(h);
                        }
                    }
                }
            }
        }
    }
}
