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

package oracle.kv.impl.admin.client;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.util.PasswordReader;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterMap;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellException;
import oracle.kv.util.shell.ShellUsageException;

/*
 * Some useful utilities for command validation and other functions
 */
public class CommandUtils {
    protected final static String eol = System.getProperty("line.separator");

    static void ensureTopoExists(String topoName, CommandServiceAPI cs,
                                 ShellCommand command)
        throws ShellException, RemoteException {

        List<String> topos = cs.listTopologies();
        if (topos.indexOf(topoName) == -1) {
            throw new ShellUsageException
                ("Topology " + topoName + " does not exist. " +
                 "Use topology list to see existing candidates.",
                 command);
        }
    }

    private static Topology.Component<?> get(CommandServiceAPI cs,
                                             ResourceId rid)
        throws RemoteException {

        Topology t = cs.getTopology();
        return t.get(rid);
    }

    static void ensureDatacenterExists(DatacenterId dcid, CommandServiceAPI cs,
                                       ShellCommand command)
        throws ShellException, RemoteException {

        if (get(cs, dcid) == null)  {
            throw new ShellUsageException("Zone does not exist: " +
                                          dcid, command);
        }
    }

    static void ensureRepNodeExists(RepNodeId rnid, CommandServiceAPI cs,
                                    ShellCommand command)
        throws ShellException, RemoteException {

        if (get(cs, rnid) == null)  {
            throw new ShellUsageException("RepNode does not exist: " +
                                          rnid, command);
        }
    }

    static void ensureStorageNodeExists(StorageNodeId snid,
                                        CommandServiceAPI cs,
                                        ShellCommand command)
        throws ShellException, RemoteException {

        if (get(cs, snid) == null)  {
            throw new ShellUsageException("StorageNode does not exist: " +
                                          snid, command);
        }
    }

    static void ensurePlanExists(int planId, CommandServiceAPI cs,
                                 ShellCommand command)
        throws ShellException, RemoteException {

        if (cs.getPlanById(planId) == null)  {
            throw new ShellUsageException
                ("Plan does not exist: " + planId, command);
        }
    }

    static void validateRepFactor(DatacenterId dcid, int rf,
                                  CommandServiceAPI cs, ShellCommand command)
        throws ShellException, RemoteException {

        Topology t = cs.getTopology();
        Datacenter dc = t.get(dcid);
        if (dc == null)  {
            throw new ShellUsageException("Zone does not exist: " +
                                          dcid, command);
        }
        if (rf < dc.getRepFactor()) {
            throw new ShellUsageException
                ("Replication factor may not be made smaller.  Current " +
                 " replication" + Shell.eolt + "factor is " +
                 dc.getRepFactor(), command);
        }
        if (rf == dc.getRepFactor()) {
            throw new ShellUsageException
                ("No change in replication factor, the operation will not " +
                 "be performed.", command);
        }
    }

    static void validatePool(String poolName, CommandServiceAPI cs,
                             ShellCommand command)
        throws ShellException, RemoteException {

        List<String> poolNames = cs.getStorageNodePoolNames();
        if (poolNames.indexOf(poolName) == -1) {
            throw new ShellUsageException("Pool does not exist: " +
                                          poolName, command);
        }
    }

    static DatacenterId getDatacenterId(String name, CommandServiceAPI cs,
                                        ShellCommand command)
        throws ShellException, RemoteException {

        Topology t = cs.getTopology();
        DatacenterMap dcMap = t.getDatacenterMap();
        for (Datacenter dc : dcMap.getAll()) {
            if (name.equals(dc.getName())) {
                return dc.getResourceId();
            }
        }
        throw new ShellUsageException("Zone does not exist: " + name,
                                      command);
    }

    /** Return whether the command flag specifies a datacenter ID. */
    static boolean isDatacenterIdFlag(final String flag) {
        return "-zn".equals(flag) || "-dc".equals(flag);
    }

    /** Return whether the command flag specifies a datacenter name. */
    static boolean isDatacenterNameFlag(final String flag) {
        return "-znname".equals(flag) || "-dcname".equals(flag);
    }

    /** Return whether the zone ID flag or value is deprecated. */
    static boolean isDeprecatedDatacenterId(final String flag,
                                            final String value) {
        return "-dc".equals(flag) || value.startsWith("dc");
    }

    /** Return whether the zone name flag is deprecated. */
    static boolean isDeprecatedDatacenterName(final String flag) {
        return "-dcname".equals(flag);
    }

    /*
     * Functions shared by change-policy and plan change-parameters
     */
    static void assignParam(ParameterMap map, String name, String value,
                            ParameterState.Info info,
                            ParameterState.Scope scope,
                            boolean showHidden,
                            ShellCommand command)
        throws ShellException {

        ParameterState pstate = ParameterState.lookup(name);
        String errorMessage = null;
        if (pstate != null) {
            if (pstate.getReadOnly()) {
                errorMessage = "Parameter is read-only: " + name;
            }
            if (!showHidden && pstate.isHidden()) {
                errorMessage = "Parameter can only be set using -hidden " +
                    "flag: " + name;
            }
            if (scope != null && scope != pstate.getScope()) {
                errorMessage = "Parameter cannot be used as a store-wide " +
                    "parameter: " + name;
            }
            if (info != null && !pstate.appliesTo(info)) {
                errorMessage = "Parameter is not valid for the service: " +
                    name;
            }
            if (errorMessage == null) {
                /* This method will validate the value if necessary */
                try {
                    map.setParameter(name, value);
                } catch (IllegalArgumentException iae) {
                    throw new ShellUsageException
                        ("Illegal parameter value:" + Shell.eolt +
                         iae.getMessage(), command);
                }
                return;
            }
        } else {
            errorMessage = "Unknown parameter field: " + name;
        }
        throw new ShellUsageException(errorMessage, command);
    }

    static void parseParams(ParameterMap map, String[] args, int i,
                            ParameterState.Info info,
                            ParameterState.Scope scope,
                            boolean showHidden,
                            ShellCommand command)
        throws ShellException {

        for (; i < args.length; i++) {
            String param = args[i];
            if (param.startsWith("-")) {
                throw new ShellUsageException
                    ("No flags are permitted after the -params flag",
                     command);
            }

            String splitArgs[] = null;

            /*
             * name="value with embedded spaces" will turn up as 2 args:
             * 1. name=
             * 2. value with embedded spaces
             */
            if (param.endsWith("=")) {
                if (++i >= args.length) {
                    throw new ShellUsageException
                        ("Parameters require a value after =", command);
                }
                splitArgs = new String[] {param.split("=")[0], args[i]};
            } else {
                splitArgs = param.split("=", 2);
            }

            if (splitArgs.length != 2) {
                throw new ShellUsageException
                    ("Unable to parse parameter assignment: " + param,
                     command);
            }
            assignParam(map, splitArgs[0].trim(), splitArgs[1].trim(),
                        info, scope, showHidden, command);
        }
    }

    /*
     * Format and sort the ParameterMap.  This is similar to
     * ParameterMap.showContents(true) except that it deals with hidden
     * parameters.
     */
    static String formatParams(ParameterMap map, boolean showHidden,
                               ParameterState.Info info) {
        StringBuilder sb = new StringBuilder();
        TreeSet<String> set = new TreeSet<String>(map.keys());
        for (String s : set) {
            Parameter p = map.get(s);
            ParameterState pstate = ParameterState.lookup(p.getName());
            if ((pstate != null) && (info == null || pstate.appliesTo(info)) &&
                (showHidden || !pstate.isHidden())) {
                sb.append(p.getName() + "=" + p + eol);
            }
        }
        return sb.toString();
    }

    /*
     * Get new password from user input
     */
    static char[] getPasswordFromInput(final PasswordReader passReader,
                                       final ShellCommand command)
        throws ShellException {

        try {
            final char[] newPassword =
                    passReader.readPassword("Enter the new password: ");
            if (newPassword == null || newPassword.length == 0) {
                throw new ShellUsageException(
                    "Empty password is unacceptable.", command);
            }
            final char[] rePasswd =
                    passReader.readPassword("Re-enter the new password: ");
            if (!Arrays.equals(newPassword, rePasswd)) {
                throw new ShellUsageException(
                    "Sorry, passwords do not match.", command);
            }
            SecurityUtils.clearPassword(rePasswd);
            return newPassword;
        } catch (IOException ioe) {
            throw new ShellUsageException(
                "Could not read password from console: " + ioe, command);
        }
    }
}
