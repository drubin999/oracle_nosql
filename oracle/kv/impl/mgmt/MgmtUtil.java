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

package oracle.kv.impl.mgmt;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;

public class MgmtUtil {

    /* Command options, for "java -jar" usage. */
    public static final String MGMT_FLAG = "-mgmt";
    public static final String MGMT_NOOP_ARG = "none";
    public static final String MGMT_SNMP_ARG = "snmp";
    public static final String MGMT_JMX_ARG = "jmx";
    public static final String MGMT_POLL_PORT_FLAG = "-pollport";
    public static final String MGMT_TRAP_HOST_FLAG = "-traphost";
    public static final String MGMT_TRAP_PORT_FLAG = "-trapport";

    public static final String MGMT_NOOP_IMPL_CLASS =
        "oracle.kv.impl.mgmt.NoOpAgent";
    public static final String MGMT_SNMP_IMPL_CLASS =
        "oracle.kv.impl.mgmt.snmp.SnmpAgent";
    public static final String MGMT_JMX_IMPL_CLASS =
        "oracle.kv.impl.mgmt.jmx.JmxAgent";

    private static final Map<String, String> mgmtArg2ClassNameMap;
    static {
        mgmtArg2ClassNameMap = new HashMap<String, String>();
        mgmtArg2ClassNameMap.put(MGMT_NOOP_ARG, MGMT_NOOP_IMPL_CLASS);
        mgmtArg2ClassNameMap.put(MGMT_SNMP_ARG, MGMT_SNMP_IMPL_CLASS);
        mgmtArg2ClassNameMap.put(MGMT_JMX_ARG, MGMT_JMX_IMPL_CLASS);
    }

    public static String getMgmtUsage(boolean implOnly) {

        final StringBuilder usage = new StringBuilder("[");
        usage.append(MGMT_FLAG);
        usage.append(" {");
        String delimiter = "";
        for (String k : mgmtArg2ClassNameMap.keySet()) {
            usage.append(delimiter);
            usage.append(k);
            delimiter = "|";
        }
        usage.append("}]");
        if (! implOnly) {
            usage.append(" [");
            usage.append(MGMT_POLL_PORT_FLAG);
            usage.append(" <snmp poll port>]\n\t");
            usage.append("[");
            usage.append(MGMT_TRAP_HOST_FLAG);
            usage.append(" <snmp trap/notification hostname>]\n\t");
            usage.append("[");
            usage.append(MGMT_TRAP_PORT_FLAG);
            usage.append(" <snmp trap/notification port>]");
        } else {
            usage.append("]");
        }
        return usage.toString();
    }

    /**
     * Return the implementation class name for a given -mgmt flag argument.
     * Caller must check for a null return value.
     */
    public static String getImplClassName(String arg) {
        return mgmtArg2ClassNameMap.get(arg);
    }

    /**
     * Indicate whether the given implementation class name is one that we
     * approve of.
     */
    public static boolean verifyImplClassName(String arg) {
        return mgmtArg2ClassNameMap.values().contains(arg);
    }

    public static class ConfigParserHelper {
        private final CommandParser parser;
        final private boolean implOnly;

        private String mgmtImpl = null;
        private int  mgmtPollPort = 0;
        private String mgmtTrapHost = null;
        private int mgmtTrapPort = 0;

        public ConfigParserHelper(CommandParser parser, boolean implOnly) {
            this.parser = parser;
            this.implOnly = implOnly;
        }

        public boolean checkArg(String arg) {
            if (arg.equals(MgmtUtil.MGMT_FLAG)) {
                final String next = parser.nextArg(arg);
                mgmtImpl = MgmtUtil.getImplClassName(next);
                if (mgmtImpl == null) {
                    parser.usage
                        ("There is no mgmt implementation named " + next);
                }
                return true;
            }
            if (implOnly) {
                return false;
            }
            if (arg.equals(MgmtUtil.MGMT_POLL_PORT_FLAG)) {
                mgmtPollPort = parser.nextIntArg(arg);
                return true;
            }
            if (arg.equals(MGMT_TRAP_HOST_FLAG)) {
                mgmtTrapHost = parser.nextArg(arg);
                return true;
            }
            if (arg.equals(MGMT_TRAP_PORT_FLAG)) {
                mgmtTrapPort = parser.nextIntArg(arg);
                return true;
            }
            return false;
        }

        public void apply(BootstrapParams bp) {

            if (mgmtImpl != null) {
                /* Require a polling port for SNMP configuration. */
                if (mgmtImpl.equals(MGMT_SNMP_IMPL_CLASS) &&
                    (mgmtPollPort == 0)) {

                    parser.usage
                        ("When specifying " + MGMT_FLAG + " " + MGMT_SNMP_ARG +
                         ", you must also give " + MGMT_POLL_PORT_FLAG + ".");
                }

                bp.setMgmtClass(mgmtImpl);
            }
            if (mgmtPollPort != 0) {
                bp.setMgmtPollingPort(mgmtPollPort);
            }
            if (mgmtTrapHost != null) {
                bp.setMgmtTrapHost(mgmtTrapHost);
            }
            if (mgmtTrapPort != 0) {
                bp.setMgmtTrapPort(mgmtTrapPort);
            }
        }

        public String getSelectedImplClass() {
            return mgmtImpl;
        }
    }

    /*
     * Annoyingly, a MIB Textual Convention enumeration can't start with an
     * uppercase letter, nor can it contain an underscore, as our ServiceStatus
     * enum values do.  Provide here a mapping between MIB-friendly names and
     * ServiceStatus values.
     */
    private static Map<ServiceStatus, String> serviceStatusMap;
    static {
        serviceStatusMap = new HashMap<ServiceStatus, String>();
        serviceStatusMap.put(ServiceStatus.STARTING, "starting");
        serviceStatusMap.put(ServiceStatus.WAITING_FOR_DEPLOY,
                             "waitingForDeploy");
        serviceStatusMap.put(ServiceStatus.RUNNING, "running");
        serviceStatusMap.put(ServiceStatus.STOPPING, "stopping");
        serviceStatusMap.put(ServiceStatus.STOPPED, "stopped");
        serviceStatusMap.put(ServiceStatus.ERROR_RESTARTING, "errorRestarting");
        serviceStatusMap.put(ServiceStatus.ERROR_NO_RESTART, "errorNoRestart");
        serviceStatusMap.put(ServiceStatus.UNREACHABLE, "unreachable");
    }

    /**
     * Given a ConfigurableService.ServiceStatus, return the corresponding
     * ServiceStatusTC string, as defined in the MIB.
     */
    public static String ssEnum2TextualConvention(ServiceStatus ss) {
        final String tc = serviceStatusMap.get(ss);
        if (tc == null) {
            /* This would indicate a coding error. */
            throw new IllegalStateException("Unexpected ServiceStatus" + ss);
        }
        return tc;
    }

    /**
     * Given a normal millisecond timestamp, return a Byte array representing
     * the OCTET STRING of the DateAndTime SNMPv2 textual convention defined in
     * RFC 1903.
     */
    public static Byte[] makeDateAndTime(long timestamp) {
        final Calendar c = Calendar.getInstance();
        c.setTimeInMillis(timestamp);

        final int year = c.get(Calendar.YEAR);
        final int month = c.get(Calendar.MONTH);
        final int day = c.get(Calendar.DATE);
        final int hour = c.get(Calendar.HOUR_OF_DAY);
        final int minute = c.get(Calendar.MINUTE);
        final int second = c.get(Calendar.SECOND);
        final int millisecond = c.get(Calendar.MILLISECOND);

        final Byte[] tc = {
            new Byte((byte)(year >> 8)),
            new Byte((byte)(year & 0xff)),
            new Byte((byte)(month + 1)),
            new Byte((byte)day),
            new Byte((byte)hour),
            new Byte((byte)minute),
            new Byte((byte)second),
            new Byte((byte)(millisecond/100)) /* Yes, in deciseconds. */
        };
        return tc;
    }
}
