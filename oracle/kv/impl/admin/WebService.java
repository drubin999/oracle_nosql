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

package oracle.kv.impl.admin;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.webapp.WebAppContext;

/**
 * WebService controls the embedded servlet container that hosts the KVAdminUI
 * web application.
 */
public class WebService {

    CommandServiceAPI aservice;
    Server jetty;
    Logger logger;
    WebAppContext wac = null;

    public WebService(CommandServiceAPI aservice, Logger logger) {
        this.aservice = aservice;
        this.logger = logger;
        Log.setLog(new KVLogFacade(logger));
    }

    public void start(int httpPort) throws Exception {
        jetty = new Server(httpPort);
        wac = new WebAppContext();
        wac.setContextPath("/");
        wac.setWar
            (getClass().getClassLoader().getResource
             ("war/KVAdminUI").toExternalForm());
        wac.setAttribute("AdminService", aservice);
        wac.setAttribute("AdminLogger", logger);
        jetty.setHandler(wac);
        jetty.start();
    }

    public void stop() throws Exception {
        if (jetty != null) {
            jetty.stop();
        }
        logger.info("Stopped Jetty");
    }

    public void resetLog(Logger newLogger) {
        this.logger = newLogger;
        Log.setLog(new KVLogFacade(newLogger));
        if (wac == null) {
            logger.warning("Can't set logger for null WebAppContext");
        } else {
            wac.setAttribute("AdminLogger", newLogger);
        }
    }

    /**
     * An implementation of Jetty's logging facade that plays nicely with
     * our use of java.util.logging.
     */
    static class KVLogFacade implements org.eclipse.jetty.util.log.Logger {

        private final Logger julLogger;
        private final boolean isIgnored;

        public KVLogFacade(Logger logger) {
            julLogger = logger;
            isIgnored = Boolean.parseBoolean
                (System.getProperty
                 ("org.eclipse.jetty.util.log.IGNORED", "false"));
        }

        @Override
        public String getName() {
            return julLogger.getName();
        }

        @Override
        public void warn(String msg, Object... args) {
            julLogger.log(Level.WARNING, format(msg, args));
        }

        @Override
        public void warn(Throwable thrown) {
            warn("", thrown);
        }

        @Override
        public void warn(String msg, Throwable thrown) {
            julLogger.log(Level.WARNING, msg, thrown);
        }

        @Override
        public void info(String msg, Object... args) {
            julLogger.log(Level.INFO, format(msg, args));
        }

        @Override
        public void info(Throwable thrown) {
            info("", thrown);
        }

        @Override
        public void info(String msg, Throwable thrown) {
            julLogger.log(Level.INFO, msg, thrown);
        }

        @Override
        public boolean isDebugEnabled() {
            return julLogger.isLoggable(Level.FINE);
        }

        @Override
        public void setDebugEnabled(boolean enabled) {
            julLogger.setLevel(Level.FINE);
        }

        @Override
        public void debug(String msg, Object... args) {
            julLogger.log(Level.FINE, format(msg, args));
        }

        @Override
        public void debug(Throwable thrown) {
            debug("", thrown);
        }

        @Override
        public void debug(String msg, Throwable thrown) {
            julLogger.log(Level.FINE, msg, thrown);
        }

        @Override
        public org.eclipse.jetty.util.log.Logger getLogger(String name) {
            return this;
        }

        @Override
        public void ignore(Throwable ignored) {
            if (isIgnored) {
                warn(Log.IGNORED, ignored);
            }
        }

        private String format(String msg, Object... args) {
            if (msg == null) {
            		msg = "";
            }
            String braces = "{}";
            StringBuilder builder = new StringBuilder();
            int start = 0;
            for (Object arg : args) {
            	int bracesIndex = msg.indexOf(braces, start);
            	if (bracesIndex < 0) {
            		builder.append(msg.substring(start));
            		builder.append(" ");
            		builder.append(arg);
            		start = msg.length();
            	} else {
            		builder.append(msg.substring(start, bracesIndex));
            		builder.append(String.valueOf(arg));
            		start = bracesIndex + braces.length();
            	}
            }
            builder.append(msg.substring(start));
            return builder.toString();
        }
    }
}
