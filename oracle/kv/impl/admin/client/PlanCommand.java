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

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.api.table.TableBuilder;
import oracle.kv.impl.api.table.TableEvolver;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.metadata.KVStoreUser.UserDescription;
import oracle.kv.impl.security.util.PasswordReader;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.security.util.ShellPasswordReader;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellException;
import oracle.kv.util.shell.ShellUsageException;

/*
 * Subcommands of plan
 *
 * cancel
 * change-parameters
 * change-storagedir
 * change-user
 * create-user
 * deploy-admin
 * deploy-datacenter
 * deploy-sn
 * deploy-topology
 * deploy-zone
 * drop-user
 * execute
 * interrupt
 * migrate-sn
 * remove-admin
 * remove-datacenter
 * remove-sn
 * remove-zone
 * start-service
 * stop-service
 * wait
 */
class PlanCommand extends SharedCommandWithSubs {

    private static final
        List<? extends SubCommand> subs =
                       Arrays.asList(new AddIndexSub(),
                                     new AddTableSub(),
                                     new ChangeMountPointSub(),
                                     new ChangeParamsSub(),
                                     new ChangeUserSub(),
                                     new CreateUserSub(),
                                     new DeployAdminSub(),
                                     new DeployDCSub(),
                                     new DeploySNSub(),
                                     new DeployZoneSub(),
                                     new DropUserSub(),
                                     new ExecuteSub(),
                                     new EvolveTableSub(),
                                     new InterruptSub(),
                                     new CancelSub(),
                                     new MigrateSNSub(),
                                     new RemoveAdminSub(),
                                     new RemoveSNSub(),
                                     new RemoveDatacenterSub(),
                                     new RemoveIndexSub(),
                                     new RemoveTableSub(),
                                     new RemoveZoneSub(),
                                     new StartServiceSub(),
                                     new StopServiceSub(),
                                     new DeployTopologySub(),
                                     new PlanWaitSub(),
                                     new RepairTopologySub());

    PlanCommand() {
        super(subs,
              "plan",
              4,  /* prefix length */
              0); /* min args -- let subs control it */
    }

    @Override
    protected String getCommandOverview() {
        return "Encapsulates operations, or jobs that modify store state." +
            eol + "All subcommands with the exception of " +
            "interrupt and wait change" + eol + "persistent state. Plans " +
            "are asynchronous jobs so they return immediately" + eol +
            "unless -wait is used.  Plan status can be checked using " +
            "\"show plans\"." + eol + "Optional arguments for all plans " +
            "include:" +
            eolt + "-wait -- wait for the plan to complete before returning" +
            eolt + "-plan-name -- name for a plan.  These are not unique" +
            eolt + "-noexecute -- do not execute the plan.  If specified " +
            "the plan" + eolt + "              " +
            "can be run later using \"plan execute\"" +
            eolt + "-force -- used to force plan execution and plan retry";
    }

    /*
     * Base abstract class for PlanSubCommands.  This class extracts
     * the generic flags "-wait" and "-noexecute" from the command line.
     */
    abstract static class PlanSubCommand extends SharedSubCommand {

        protected boolean execute;
        protected boolean wait;
        protected boolean force;
        protected String planName;
        static final String genericFlags =
            eolt + "[-plan-name <name>] [-wait] [-noexecute] [-force]";
        static final String dcFlagsDeprecation =
            "The -dc and -dcname flags, and the dc<ID> ID format, are" +
            " deprecated" + eol +
            "and have been replaced by -zn, -znname, and zn<ID>." +
            eol + eol;

        protected PlanSubCommand(String name, int prefixMatchLength) {
            super(name, prefixMatchLength);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            wait = false;
            execute = true;
            force = false;
            planName = null;

            return exec(args, shell);
        }

        protected int checkGenericArg(String arg, String[] args, int i)
            throws ShellException {

            int rval = 0;
            if ("-plan-name".equals(arg)) {
                planName = Shell.nextArg(args, i, this);
                rval = 1;
            } else if ("-wait".equals(arg)) {
                wait = true;
            } else if ("-noexecute".equals(arg)) {
                execute = false;
            } else if ("-force".equals(arg)) {
                force = true;
            } else {
                throw new ShellUsageException("Invalid argument: " + arg,
                                              this);
            }
            return rval;
        }

        public abstract String exec(String[] args, Shell shell)
            throws ShellException;

        /*
         * Return the most recently created plan's id.
         */
        protected static int getLastPlanId(CommandServiceAPI cs)
            throws RemoteException {

            int range[] =
                cs.getPlanIdRange(0L, (new Date()).getTime(), 1);

            return range[0];
        }

        /*
         * Encapsulate plan execution and optional waiting in a single place
         */
        protected String executePlan(int planId, CommandServiceAPI cs,
                                     Shell shell)
        throws RemoteException {

            /*
             * Implicitly approve plan.  TODO: change server side to do this
             */
            cs.approvePlan(planId);
            if (execute) {
                cs.executePlan(planId, force);
                if (wait) {
                    shell.println("Executed plan " + planId +
                                  ", waiting for completion...");
                    final Plan.State state = cs.awaitPlan(planId, 0, null);
                    return state.getWaitMessage(planId);
                }
                return "Started plan " + planId + ". Use show plan -id " +
                    planId + " to check status." + eolt +
                    "To wait for completion, use plan wait -id " + planId;
            }
            return "Created plan without execution: " + planId;
        }
    }

    static class ChangeMountPointSub extends PlanSubCommand {

        ChangeMountPointSub() {
            super("change-storagedir", 9);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            StorageNodeId snid = null;
            String path = null;
            boolean add = true;
            boolean addOrRemove = false;
            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if ("-sn".equals(arg)) {
                    final String argString = Shell.nextArg(args, i++, this);
                    snid = parseSnid(argString);
                } else if ("-storagedir".equals(arg)) {
                    path = Shell.nextArg(args, i++, this);
                } else if ("-path".equals(arg)) {
                    /*
                     * [#21880] use -storagedir as the flag name, but support
                     * -path until next R3 for backward compatibility.
                     */
                    path = Shell.nextArg(args, i++, this);
                } else if ("-add".equals(arg)) {
                    add = true;
                    addOrRemove = true;
                } else if ("-remove".equals(arg)) {
                    add = false;
                    addOrRemove = true;
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }
            if (snid == null || !addOrRemove) {
                shell.requiredArg(null, this);
            }
            try {
                final Parameters p = cs.getParameters();
                final StorageNodeParams snp = p.get(snid);
                ParameterMap mountMap = null;
                try {
                    mountMap =
                        StorageNodeParams.changeMountMap(p, snp, add, path);
                } catch (IllegalCommandException e) {
                    throw new ShellUsageException(e.getMessage(), this);
                }
                final int planId =
                    cs.createChangeParamsPlan(planName, snid, mountMap);
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected boolean matches(String commandName) {
            /* Allow deprecated name until R3 [#21880] */
            return super.matches(commandName) ||
                   Shell.matches(commandName, "change-mountpoint",
                                 prefixMatchLength);
        }

        @Override
        protected String getCommandSyntax() {
            return "plan change-storagedir -sn <id> " + eolt +
                    "-storagedir <path to storage directory> -add|-remove " +
                    genericFlags;
        }

        @Override
        protected String getCommandDescription() {
            return "Adds or remove a storage directory on a Storage Node," +
                eolt + "for storing a Replication Node";
        }
    }

    static final class ChangeParamsSub extends PlanSubCommand {

        final String incompatibleAllError =
            "Invalid argument combination: Only one of the flags " +
            "-all-rns, -all-sns, -all-admins and -security may be used.";

        final String serviceAllError =
            "Invalid argument combination: -service flag cannot be used " +
            "with -all-rns, -all-admins or -security flags";

        final String serviceDcError =
            "Invalid argument combination: -service flag cannot be used " +
            "with -zn, -znname, -dc, or -dcname flag";

        final String commandSyntax =
            "plan change-parameters -security | -service <id> | " +
            "-all-rns [-zn <id> | -znname <name>] | " +
            "-all-admins [-zn <id> | -znname <name>] [-dry-run]" +
            genericFlags + " -params [name=value]*";

        final String commandDesc =
            "Changes parameters for either the specified service, or for" +
            eolt +
            "all service instances of the same type that are deployed to" +
            eolt +
            "the specified zone or all zones.  The -security" +
            eolt +
            "flag allows changing store-wide global security parameters," +
            eolt +
            "and should never be used with other flags. The -service" +
            eolt +
            "flag allows a single instance to be affected; and should" +
            eolt +
            "never be used with either the -zn or -znname flag.  One of" +
            eolt +
            "the -all-* flags can be combined with the -zn or -znname" +
            eolt +
            "flag to change all instances of the service type deployed" +
            eolt +
            "to the specified zone; leaving unchanged, any" +
            eolt +
            "instances of the specified type deployed to other" +
            eolt +
            "zones. If one of the -all-* flags is used without" +
            eolt +
            "also specifying the zone, then the desired parameter" +
            eolt +
            "change will be applied to all instances of the specified" +
            eolt +
            "type within the store, regardless of zone.  The" +
            eolt +
            "parameters to change are specified via the -params flag," +
            eolt +
            "and consist of name/value pairs separated by spaces; where" +
            eolt +
            "any parameter values with embedded spaces must be quoted" +
            eolt +
            "(for example, name=\"value with spaces\").  Finally, if the" +
            eolt +
            "-dry-run flag is specified, the new parameters are returned" +
            eolt +
            "without applying the specified change." +
            eol +
            eolt +
            "Use \"show parameters\" to see what parameters can be " +
            "modified";

        String serviceName;
        StorageNodeId snid;
        RepNodeId rnid;
        AdminId aid;
        boolean allAdmin, allRN, allSN, security;

        boolean dryRun;

        ChangeParamsSub() {
            super("change-parameters", 10);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            serviceName = null;
            snid = null;
            aid = null;
            rnid = null;
            allAdmin = allRN = allSN = dryRun = security = false;

            boolean showHidden = cmd.showHidden();
            int i;
            boolean foundParams = false;

            DatacenterId dcid = null;
            String dcName = null;
            boolean getDcId = false;
            boolean deprecatedDcFlag = false;

            for (i = 1; i < args.length; i++) {
                final String arg = args[i];
                if ("-service".equals(arg)) {
                    serviceName = Shell.nextArg(args, i++, this);
                } else if ("-all-rns".equals(arg)) {
                    allRN = true;
                } else if ("-all-admins".equals(arg)) {
                    allAdmin = true;
                } else if ("-all-sns".equals(arg)) {
                    allSN = true;
                } else if (CommandUtils.isDatacenterIdFlag(arg)) {
                    dcid = DatacenterId.parse(Shell.nextArg(args, i++, this));
                    if (CommandUtils.isDeprecatedDatacenterId(arg, args[i])) {
                        deprecatedDcFlag = true;
                    }
                } else if (CommandUtils.isDatacenterNameFlag(arg)) {
                    dcName =  Shell.nextArg(args, i++, this);
                    if (CommandUtils.isDeprecatedDatacenterName(arg)) {
                        deprecatedDcFlag = true;
                    }
                } else if ("-security".equals(arg)) {
                    security = true;
                } else if ("-hidden".equals(arg)) {
                    showHidden = true;
                } else if ("-dry-run".equals(arg)) {
                    dryRun = true;
                } else if ("-params".equals(arg)) {
                    ++i;
                    foundParams = true;
                    break;
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }

            /* Verify argument combinations */
            if (serviceName == null) {
                if (!(allAdmin || allRN || allSN || security)) {
                    shell.requiredArg(null, this);
                } else {
                    /* check whether multiple incompatible options are given. */
                    if (((allAdmin ? 1 : 0) +
                         (allRN ? 1 : 0) +
                         (allSN ? 1 : 0) +
                         (security ? 1 : 0)) > 1) {

                        throw new ShellUsageException
                            (incompatibleAllError, this);
                    }
                    if (dcName != null) {
                        getDcId = true;
                    }
                }
            } else {
                if (allAdmin || allRN || allSN || security) {
                    throw new ShellUsageException(serviceAllError, this);
                }
                if (dcid != null || dcName != null) {
                    throw new ShellUsageException(serviceDcError, this);
                }
            }

            if (!foundParams) {
                shell.requiredArg("-params", this);
            }

            if (args.length <= i) {
                return "No parameters were specified";
            }

            final String deprecatedDcFlagPrefix =
                deprecatedDcFlag ? dcFlagsDeprecation : "";

            try {
                int planId = 0;
                final ParameterMap map = createChangeMap(cs, args, i,
                                                         showHidden);

                if (dryRun) {
                    return CommandUtils.formatParams(map, showHidden, null);
                }

                if (getDcId) {
                    dcid = CommandUtils.getDatacenterId(dcName, cs, this);
                }

                if (rnid != null) {
                    shell.verboseOutput("Changing parameters for " + rnid);
                    planId = cs.createChangeParamsPlan(planName, rnid, map);
                } else if (allRN) {
                    shell.verboseOutput(
                        "Changing parameters for all RepNodes" +
                        (dcName != null ?
                         " deployed to the " + dcName + " zone" :
                        (dcid != null ?
                         " deployed to the zone with id = " + dcid :
                         "")));
                    planId = cs.createChangeAllParamsPlan(planName, dcid, map);
                } else if (snid != null) {
                    shell.verboseOutput("Changing parameters for " + snid);
                    planId = cs.createChangeParamsPlan(planName, snid, map);
                } else if (allAdmin) {
                    shell.verboseOutput(
                        "Changing parameters for all Admins" +
                        (dcName != null ?
                         " deployed to the " + dcName + " zone" :
                         (dcid != null ?
                          " deployed to the zone with id = " + dcid :
                          "")));
                    planId = cs.createChangeAllAdminsPlan(planName, dcid, map);
                } else if (security) {
                    shell.verboseOutput("Changing global security parameters");
                    planId =
                        cs.createChangeGlobalSecurityParamsPlan(planName, map);
                } else if (aid != null) {
                    shell.verboseOutput("Changing parameters for " + aid);
                    planId = cs.createChangeParamsPlan(planName, aid, map);
                } else if (allSN) {
                    return "Can't change all SN params at this time";
                }
                if (shell.getVerbose()) {
                    shell.verboseOutput
                        ("New parameters:" + eol +
                         CommandUtils.formatParams(map, showHidden, null));
                }
                return deprecatedDcFlagPrefix + executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return commandSyntax;
        }

        /* TODO: help differentiated by service type */
        @Override
        protected String getCommandDescription() {
            return commandDesc;
        }

        private ParameterMap getServiceMap(CommandServiceAPI cs)
            throws ShellException, RemoteException {

            ParameterMap map = null;
            final Parameters p = cs.getParameters();
            try {
                rnid = RepNodeId.parse(serviceName);
                final RepNodeParams rnp = p.get(rnid);
                if (rnp != null) {
                    map = rnp.getMap();
                } else {
                    throw new ShellUsageException
                        ("No such service: " + serviceName, this);
                }
            } catch (IllegalArgumentException ignored) {
                try {
                    snid = StorageNodeId.parse(serviceName);
                    final StorageNodeParams snp = p.get(snid);
                    if (snp != null) {
                        map = snp.getMap();
                    } else {
                        throw new ShellUsageException
                            ("No such service: " + serviceName, this);
                    }
                } catch (IllegalArgumentException ignored1) {
                    try {
                        aid = AdminId.parse(serviceName);
                        final AdminParams ap = p.get(aid);
                        if (ap != null) {
                            map = ap.getMap();
                        } else {
                            throw new ShellUsageException
                                ("No such service: " + serviceName, this);
                        }
                    } catch (IllegalArgumentException ignored2) {
                        throw new ShellUsageException
                            ("Invalid service name: " + serviceName, this);
                    }
                }
            }
            return map;
        }

        private ParameterMap createChangeMap(CommandServiceAPI cs,
                                             String[] args,
                                             int i,
                                             boolean showHidden)
            throws ShellException, RemoteException {

            ParameterMap map = null;
            ParameterState.Info info = null;
            ParameterState.Scope scope;
            if (serviceName != null) {
                map = getServiceMap(cs);
                scope = null; /* allow any scope */
                if (map.getType().equals(ParameterState.REPNODE_TYPE)) {
                    info = ParameterState.Info.REPNODE;
                } else if (map.getType().equals(ParameterState.SNA_TYPE)) {
                    info = ParameterState.Info.SNA;
                } else {
                    info = ParameterState.Info.ADMIN;
                }
            } else {
                scope = ParameterState.Scope.STORE;
                map = new ParameterMap();
                if (allRN) {
                    map.setType(ParameterState.REPNODE_TYPE);
                    info = ParameterState.Info.REPNODE;
                } else if (allSN) {
                    map.setType(ParameterState.SNA_TYPE);
                    info = ParameterState.Info.SNA;
                } else if (security) {
                    map.setType(ParameterState.GLOBAL_TYPE);
                    info = ParameterState.Info.GLOBAL;
                } else {
                    map.setType(ParameterState.ADMIN_TYPE);
                    info = ParameterState.Info.ADMIN;
                }
            }
            CommandUtils.parseParams(map, args, i, info, scope,
                                     showHidden, this);
            return map;
        }
    }

    static final class ChangeUserSub extends PlanSubCommand {

        static final String COMMAND_SYNTAX =
            "plan change-user -name <user name> [-disable | -enable]" +
            eolt + "[-set-password [-password <new password>] " +
            "[-retain-current-password]]" + eolt +
            "[-clear-retained-password]" + genericFlags;

        static final String COMMAND_DESC =
            "Change a user with the specified name in the store. " +
            "The" + eolt + "-retain-current-password argument option " +
            "causes the current password to" + eolt + "be remembered " +
            "during the -set-password operation as a valid alternate " +
            eolt + "password for configured retention time or until" +
            " cleared using -clear-retained-password." + eolt +
            "If a retained password has already been set for the user," +
            eolt + "setting retained password again will cause an " +
            "error to be reported.";

        static final String RETAIN_FLAG = "-retain-current-password";
        static final String CLEAR_FLAG = "-clear-retained-password";
        static final String DISABLE_FLAG = "-disable";
        static final String ENABLE_FLAG = "-enable";
        static final String SET_PASSWORD_FLAG = "-set-password";
        static final String PASSWORD_FLAG = "-password";
        static final String NAME_FLAG = "-name";

        ChangeUserSub() {
            super("change-user", 10);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            /* Flags for change options */
            String userName = null;
            String password = null;
            boolean retainPassword = false;
            boolean clearRetainedPassword = false;
            boolean changePassword = false;
            char[] newPlainPassword = null;

            /* Use boxed boolean to help identify non-changed user state */
            Boolean isEnabled = null;

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if (NAME_FLAG.equals(arg)) {
                    userName = Shell.nextArg(args, i++, this);
                } else if (PASSWORD_FLAG.equals(arg)) {
                    password = Shell.nextArg(args, i++, this);
                } else if (SET_PASSWORD_FLAG.equals(arg)) {
                    changePassword = true;
                } else if (DISABLE_FLAG.equals(arg)) {
                    isEnabled = false;
                } else if (ENABLE_FLAG.equals(arg)) {
                    isEnabled = true;
                } else if (RETAIN_FLAG.equals(arg)) {
                    retainPassword = true;
                } else if (CLEAR_FLAG.equals(arg)) {
                    clearRetainedPassword = true;
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }
            if (userName == null) {
                shell.requiredArg(NAME_FLAG, this);
            }
            if (password != null && !changePassword) {
                return "Option -password is only valid in" +
                       "conjunction with -set-password.";
            }
            if (retainPassword && !changePassword) {
                return "Option -retain-current-password is only valid in" +
                       "conjunction with -set-password.";
            }
            /* Nothing changes */
            if (isEnabled == null && !changePassword && !clearRetainedPassword) {
                return "Nothing changed for user " + userName;
            }

            /* Get new password from user input */
            if (changePassword) {
                if (password != null) {
                    if (password.isEmpty()) {
                        return "Password may not be empty";
                    }
                    newPlainPassword = password.toCharArray();
                } else {
                    final PasswordReader READER = new ShellPasswordReader();
                    newPlainPassword =
                        CommandUtils.getPasswordFromInput(READER, this);
                }
            }

            try {
                final int planId =
                    cs.createChangeUserPlan(planName, userName, isEnabled,
                                            newPlainPassword, retainPassword,
                                            clearRetainedPassword);
                SecurityUtils.clearPassword(newPlainPassword);
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return COMMAND_SYNTAX;
        }

        @Override
        protected String getCommandDescription() {
            return COMMAND_DESC;
        }
    }

    static final class CreateUserSub extends PlanSubCommand {

        static final String COMMAND_SYNTAX =
            "plan create-user -name <user name> [-admin] [-disable]" +
            eolt + "[-password <new password>]" + genericFlags;

        static final String COMMAND_DESC =
            "Create a user with the specified name in the store." +
            eolt + "The -admin argument indicates that the created " +
            "user has full " + eolt + "administrative privileges.";

        static final String DISABLE_FLAG = "-disable";
        static final String ADMIN_FLAG = "-admin";
        static final String NAME_FLAG = "-name";
        static final String PASSWORD_FLAG = "-password";

        CreateUserSub() {
            super("create-user", 10);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            String userName = null;
            String password = null;
            boolean isAdmin = false;
            boolean isEnabled = true;

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if (NAME_FLAG.equals(arg)) {
                    userName = Shell.nextArg(args, i++, this);
                } else if (PASSWORD_FLAG.equals(arg)) {
                    password = Shell.nextArg(args, i++, this);
                } else if (ADMIN_FLAG.equals(arg)) {
                    isAdmin = true;
                } else if (DISABLE_FLAG.equals(arg)) {
                    isEnabled = false;
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }
            if (userName == null ) {
                shell.requiredArg(NAME_FLAG, this);
            }

            /* Get password from user input. */
            char[] plainPasswd;
            if (password != null) {
                if (password.isEmpty()) {
                    return "Password may not be empty";
                }
                plainPasswd = password.toCharArray();
            } else {
                final PasswordReader READER = new ShellPasswordReader();
                plainPasswd =
                    CommandUtils.getPasswordFromInput(READER, this);
            }

            try {
                final int planId =
                    cs.createCreateUserPlan(planName, userName, isEnabled,
                                            isAdmin, plainPasswd);
                SecurityUtils.clearPassword(plainPasswd);
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return COMMAND_SYNTAX;
        }

        @Override
        protected String getCommandDescription() {
            return  COMMAND_DESC;
        }
    }

    static final class DeployAdminSub extends PlanSubCommand {

        static final String COMMAND_SYNTAX =
            "plan deploy-admin -sn <id> -port <http port>" + genericFlags;

        static final String COMMAND_DESC =
            "Deploys an Admin to the specified storage node.  Its " +
            "graphical" + eolt + "interface will listen on the " +
            "specified port, or is diabled if the port is set to 0.";

        DeployAdminSub() {
            super("deploy-admin", 10);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            StorageNodeId snid = null;
            int port = 0; /* 0 means default http port */
            boolean portFound = false;

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if ("-port".equals(arg)) {
                    final String argString = Shell.nextArg(args, i++, this);
                    port = parseInt(argString, "Invalid HTTP port");
                    portFound = true;
                } else if ("-sn".equals(arg)) {
                    final String argString = Shell.nextArg(args, i++, this);
                    snid = parseSnid(argString);
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }
            if (snid == null || !portFound) {
                shell.requiredArg(null, this);
            }
            try {
                final int planId =
                    cs.createDeployAdminPlan(planName, snid, port);
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return COMMAND_SYNTAX;
        }

        @Override
        protected String getCommandDescription() {
            return COMMAND_DESC;
        }
    }

    static final class DropUserSub extends PlanSubCommand {

        static final String COMMAND_SYNTAX =
            "plan drop-user -name <user name>" + genericFlags;

        static final String COMMAND_DESC =
            "Drop a user with the specified name in the store. A" +
            eolt + "logged-in user may not drop itself.";

        DropUserSub() {
            super("drop-user", 7);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            String userName = null;

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if ("-name".equals(arg)) {
                    userName = Shell.nextArg(args, i++, this);
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }

            if (userName == null) {
                shell.requiredArg("-name", this);
            }

            try {
                /* Check if the user exists. If not, show warnings. */
                final Map<String, UserDescription> userDescMap =
                    cs.getUsersDescription();
                if (userDescMap == null || userDescMap.get(userName) == null) {
                    cmd.println("User " + userName + " does not exist.");
                }

                /*
                 * For failure recovery, execute the plan even though the user
                 * to drop does not exist.
                 */
                final int planId =
                    cs.createDropUserPlan(planName, userName);
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return COMMAND_SYNTAX;
        }

        @Override
        protected String getCommandDescription() {
            return COMMAND_DESC;
        }
    }

    static final class RemoveAdminSub extends PlanSubCommand {

        final String adminDcError =
            "Invalid argument combination: -admin flag cannot be used " +
            "with -zn, -znname, -dc, or -dcname flag";

        final String dcIdNameError =
            "Invalid argument combination: must use only one of the -zn," +
            " -znname, -dc, or -dcname flags";

        final String commandSyntax =
            "plan remove-admin -admin <id> | -zn <id> | -znname <name> " +
            "[-force] " + genericFlags;

        final String commandDesc =
            "Removes the desired Admin instances; either the single" +
            eolt +
            "specified instance, or all instances deployed to the specified" +
            eolt +
            "zone. If the -admin flag is used and there are 3 or " +
            eolt +
            "fewer Admins running in the store, or if the -zn or -znname" +
            eolt +
            "flag is used and the removal of all Admins from the specified" +
            eolt +
            "zone would result in only one or two Admins in the store," +
            eolt +
            "then the desired Admins will be removed only if the -force" +
            eolt +
            "flag is also specified. Additionally, if the -admin flag is" +
            eolt +
            "used and there is only one Admin in the store, or if the -zn or" +
            eolt +
            "-znname flag is used and the removal of all Admins from the" +
            eolt +
            "specified zone would result in the removal of all Admins" +
            eolt +
            "from the store, then the desired Admins will not be removed.";

        final String noAdminError =
            "There is no Admin in the store with the specified id ";

        final String only1AdminError =
            "Only one Admin in the store, so cannot remove the sole Admin ";

        final String tooFewAdminError =
            "Removing the specified Admin will result in fewer than 3" +
            eolt +
            "Admins in the store; which is strongly discouraged because" +
            eolt +
            "the loss of one of the remaining Admins will cause quorum" +
            eolt +
            "to be lost. If you still wish to remove the desired Admin," +
            eolt +
            "specify the -force flag. ";

        final String noAdminDcError =
            "There are no Admins in the specified zone ";

        final String allAdminDcError =
            "The specified zone contains all the Admins in the store," +
            eolt +
            "and so cannot be removed from the specified zone ";

        final String tooFewAdminDcError =
            "Removing all Admins from the specified zone will result" +
            eolt +
            "in fewer than 3 Admins in the store; which is strongly" +
            eolt +
            "discouraged because the loss of one of the remaining Admins " +
            eolt +
            "will cause quorum to be lost. If you still wish to remove" +
            eolt +
            "all of the Admins from the desired zone, specify the" +
            eolt +
            "-force flag. ";

        RemoveAdminSub() {
            super("remove-admin", 10);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            AdminId aid = null;

            DatacenterId dcid = null;
            String dcName = null;
            boolean getDcId = false;
            boolean deprecatedDcFlag = false;

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if ("-admin".equals(arg)) {
                    final String argString = Shell.nextArg(args, i++, this);
                    aid = parseAdminid(argString);
                } else if (CommandUtils.isDatacenterIdFlag(arg)) {
                    dcid = DatacenterId.parse(Shell.nextArg(args, i++, this));
                    if (CommandUtils.isDeprecatedDatacenterId(arg, args[i])) {
                        deprecatedDcFlag = true;
                    }
                } else if (CommandUtils.isDatacenterNameFlag(arg)) {
                    dcName = Shell.nextArg(args, i++, this);
                    if (CommandUtils.isDeprecatedDatacenterName(arg)) {
                        deprecatedDcFlag = true;
                    }
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }

            /* Verify argument combinations */
            if (aid == null) {
                if (dcid == null) {
                    if (dcName == null) {
                        shell.requiredArg(null, this);
                    } else {
                        getDcId = true;
                    }
                } else {
                    if (dcName != null) {
                        throw new ShellUsageException(dcIdNameError, this);
                    }
                }
            } else {
                if (dcid != null || dcName != null) {
                    throw new ShellUsageException(adminDcError, this);
                }
            }

            final String deprecatedDcFlagPrefix =
                deprecatedDcFlag ? dcFlagsDeprecation : "";

            /*
             * If admin count is low, there are restrictions.
             */
            Parameters parameters = null;
            try {
                parameters = cs.getParameters();
            } catch (RemoteException re) {
                cmd.noAdmin(re);
                return "";
            }

            final int nAdmins = parameters.getAdminCount();

            if (aid != null) {

                if (parameters.get(aid) == null) {
                    return noAdminError + "[" + aid + "]";
                }

                if (nAdmins == 1) {
                    return only1AdminError + "[" + aid + "]";
                }

                if (nAdmins < 4 && !force) {
                    return tooFewAdminError + "There are only " + nAdmins +
                           " Admins in the store.";
                }

            } else {

                final Set<AdminId> adminIdSet;
                try {

                    if (getDcId) {
                        dcid = CommandUtils.getDatacenterId(dcName, cs, this);
                    }

                    adminIdSet =
                        parameters.getAdminIds(dcid, cs.getTopology());
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                    return "";
                }

                String dcErrStr = "";
                if (dcName != null) {
                    dcErrStr = dcName;
                } else if (dcid != null) {
                    dcErrStr = dcid.toString();
                }

                if (adminIdSet.size() == 0) {
                    return noAdminDcError + "[" + dcErrStr + "]";
                }

                if (adminIdSet.size() == nAdmins) {
                    return allAdminDcError + "[" + dcErrStr + "]";
                }

                if (nAdmins - adminIdSet.size() < 3 && !force) {
                    return tooFewAdminDcError + "There are " + nAdmins +
                        " Admins in the store and " + adminIdSet.size() +
                        " Admins in the specified zone " +
                        "[" + dcErrStr + "]";
                }
            }

            try {
                final int planId =
                    cs.createRemoveAdminPlan(planName, dcid, aid);
                return deprecatedDcFlagPrefix + executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return commandSyntax;
        }

        @Override
        protected String getCommandDescription() {
            return commandDesc;
        }
    }

    static final class DeployDCSub extends DeployZoneSub {

        static final String dcCommandDeprecation =
            "The command:" + eol + eolt +
            "plan deploy-datacenter" + eol + eol +
            "is deprecated and has been replaced by:" + eol + eolt +
            "plan deploy-zone" + eol + eol;

        DeployDCSub() {
            super("deploy-datacenter", 10);
        }

        /** Add deprecation message. */
        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            return dcCommandDeprecation + super.exec(args, shell);
        }

        /** Add deprecation message. */
        @Override
        public String getCommandDescription() {
            return super.getCommandDescription() + eol + eolt +
                "This command is deprecated and has been replaced by:" +
                eol + eolt +
                "plan deploy-zone";
        }
    }

    static class DeployZoneSub extends PlanSubCommand {

        DeployZoneSub() {
            super("deploy-zone", 10);
        }

        DeployZoneSub(final String name, final int prefixMatchLength) {
            super(name, prefixMatchLength);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            String dcName = null;
            int rf = 0;
            boolean rfSet = false;
            DatacenterType type = DatacenterType.PRIMARY;

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if ("-name".equals(arg)) {
                    dcName = Shell.nextArg(args, i++, this);
                } else if ("-rf".equals(arg)) {
                    final String argString = Shell.nextArg(args, i++, this);
                    rf = parseInt(argString, "Invalid replication factor");
                    rfSet = true;
                } else if ("-type".equals(arg)) {
                    final String typeValue = Shell.nextArg(args, i++, this);
                    type = parseDatacenterType(typeValue);
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }
            if (dcName == null || !rfSet) {
                shell.requiredArg(null, this);
            }
            try {
                final int planId = cs.createDeployDatacenterPlan(
                    planName, dcName, rf, type);
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "plan " + name + " -name <zone name>" +
                eolt + "-rf <replication factor>" +
                eolt + "[-type [primary | secondary]]" +
                genericFlags;
        }

        @Override
        protected String getCommandDescription() {
            return
                "Deploys the specified zone to the store," + eolt +
                "creating a primary zone if -type is not specified.";
        }
    }

    static final class DeploySNSub extends PlanSubCommand {

        DeploySNSub() {
            super("deploy-sn", 9);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            DatacenterId dcid = null;
            String host = null;
            String dcName = null;
            int port = 0;
            boolean deprecatedDcFlag = false;

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if ("-host".equals(arg)) {
                    host = Shell.nextArg(args, i++, this);
                } else if ("-port".equals(arg)) {
                    final String argString = Shell.nextArg(args, i++, this);
                    port = parseInt(argString, "Invalid port");
                } else if (CommandUtils.isDatacenterIdFlag(arg)) {
                    dcid = DatacenterId.parse(Shell.nextArg(args, i++, this));
                    if (CommandUtils.isDeprecatedDatacenterId(arg, args[i])) {
                        deprecatedDcFlag = true;
                    }
                } else if (CommandUtils.isDatacenterNameFlag(arg)) {
                    dcName = Shell.nextArg(args, i++, this);
                    if (CommandUtils.isDeprecatedDatacenterName(arg)) {
                        deprecatedDcFlag = true;
                    }
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }
            final String deprecatedDcFlagPrefix =
                deprecatedDcFlag ? dcFlagsDeprecation : "";
            if ((dcid == null && dcName == null) ||
                host == null || port == 0) {
                shell.requiredArg(null, this);
            }
            try {
                if (dcid == null) {
                    dcid = CommandUtils.getDatacenterId(dcName, cs, this);
                }
                final int planId = cs.createDeploySNPlan(planName, dcid,
                                                         host, port, null);
                return deprecatedDcFlagPrefix + executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "plan deploy-sn -zn <id> | -znname <name> " +
                "-host <host> -port <port>" + genericFlags;
        }

        @Override
        protected String getCommandDescription() {
            return
                "Deploys the storage node at the specified host and port " +
                 "into the" + eolt + "specified zone.";
        }
    }

    static final class DeployTopologySub extends PlanSubCommand {

        DeployTopologySub() {
            super("deploy-topology", 10);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            String topoName = null;
            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if ("-name".equals(arg)) {
                    topoName = Shell.nextArg(args, i++, this);
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }
            if (topoName == null) {
                shell.requiredArg("-name", this);
            }
            try {
                final int planId =
                    cs.createDeployTopologyPlan(planName, topoName);
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "plan deploy-topology -name <topology name>" +
                genericFlags;
        }

        @Override
        protected String getCommandDescription() {
            return
                "Deploys the specified topology to the store.  This " +
                "operation can" + eolt + "take a while, depending on " +
                "the size and state of the store.";
        }
    }

     static final class RepairTopologySub extends PlanSubCommand {

            RepairTopologySub() {
                super("repair-topology", 4);
            }

            @Override
            public String exec(String[] args, Shell shell)
                throws ShellException {

                Shell.checkHelp(args, this);
                final CommandShell cmd = (CommandShell) shell;
                final CommandServiceAPI cs = cmd.getAdmin();

                for (int i = 1; i < args.length; i++) {
                    final String arg = args[i];
                    i += checkGenericArg(arg, args, i);
                }

                try {
                    final int planId =
                        cs.createRepairPlan(planName);
                    return executePlan(planId, cs, shell);
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
                return "";
            }

        @Override
        protected String getCommandSyntax() {
            return "plan repair-topology " + genericFlags;
        }

        @Override
        protected String getCommandDescription() {
            return
                "Inspects the store's deployed, current topology for " +
                "inconsistencies" + eolt +
                "in location metadata that may have arisen from the " +
                "interruption" + eolt +
                "or cancellation of previous deploy-topology or migrate-sn " +
                "plans. Where " + eolt +
                "possible, inconsistencies are repaired. This operation can"
                + eolt + "take a while, depending on the size and state of " +
                "the store.";
        }
    }


    static final class ExecuteSub extends PlanSubCommand {

        ExecuteSub() {
            super("execute", 3);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            int planId = 0;

            try {
                for (int i = 1; i < args.length; i++) {
                    final String arg = args[i];
                    if ("-id".equals(arg)) {
                        final String argString =
                            Shell.nextArg(args, i++, this);
                        planId = parseInt(argString, "Invalid plan ID");
                    } else if ("-last".equals(arg)) {
                        planId = getLastPlanId(cs);
                    } else {
                        i += checkGenericArg(arg, args, i);
                    }
                }
                if (planId == 0) {
                    shell.requiredArg("-id|-last", this);
                }
                CommandUtils.ensurePlanExists(planId, cs, this);
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "plan execute -id <id> | -last [-wait] [-force]";
        }

        @Override
        protected String getCommandDescription() {
            return
                "Executes a created, but not yet executed plan.  The plan " +
                "must have" + eolt + "been previously created using the " +
                "-noexecute flag. Use -last to" + eolt + "reference the " +
                "most recently created plan.";
        }
    }

    static final class InterruptSub extends InterruptCancelSub {

        InterruptSub() {
            super("interrupt", 3, true);
        }

        @Override
        protected String getCommandSyntax() {
            return "plan interrupt -id <plan id> | -last";
        }

        @Override
        protected String getCommandDescription() {
            return
                "Interrupts a running plan. An interrupted plan can " +
                "only be re-executed" + eolt + "or canceled.  Use -last " +
                "to reference the most recently" + eolt + "created plan.";
        }
    }

    static final class CancelSub extends InterruptCancelSub {

        CancelSub() {
            super("cancel", 3, false);
        }

        @Override
        protected String getCommandSyntax() {
            return "plan cancel -id <plan id> | -last";
        }

        @Override
        protected String getCommandDescription() {
            return
                "Cancels a plan that is not running.  A running plan " +
                "must be" + eolt + "interrupted before it can be canceled. " +
                "Use -last to reference the most" + eolt + "recently " +
                "created plan.";
        }
    }

    /*
     * Put interrupt/cancel into same code.
     */
    abstract static class InterruptCancelSub extends PlanSubCommand {
        private final boolean isInterrupt;

        protected InterruptCancelSub(String name, int prefixMatchLength,
                                     boolean isInterrupt) {
            super(name, prefixMatchLength);
            this.isInterrupt = isInterrupt;
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            int planId = 0;

            try {
                for (int i = 1; i < args.length; i++) {
                    final String arg = args[i];
                    if ("-id".equals(arg)) {
                        final String argString =
                            Shell.nextArg(args, i++, this);
                        planId = parseInt(argString, "Invalid plan ID");
                    } else if ("-last".equals(arg)) {
                        planId = getLastPlanId(cs);
                    } else {
                        shell.unknownArgument(arg, this);
                    }
                }
                if (planId == 0) {
                    shell.requiredArg("-id|-last", this);
                }
                CommandUtils.ensurePlanExists(planId, cs, this);
                if (isInterrupt) {
                    cs.interruptPlan(planId);
                    return "Plan " + planId + " was interrupted";
                }
                cs.cancelPlan(planId);
                return "Plan " + planId + " was canceled";
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }
    }

    static final class MigrateSNSub extends PlanSubCommand {

        MigrateSNSub() {
            super("migrate-sn", 7);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            StorageNodeId fromSnid = null;
            StorageNodeId toSnid = null;
            int port = 0;

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if ("-from".equals(arg)) {
                    final String argString = Shell.nextArg(args, i++, this);
                    fromSnid = parseSnid(argString);
                } else if ("-to".equals(arg)) {
                    final String argString = Shell.nextArg(args, i++, this);
                    toSnid = parseSnid(argString);
                } else if ("-admin-port".equals(arg)) {
                    final String argString = Shell.nextArg(args, i++, this);
                    port = parseInt(argString, "Invalid admin port");
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }
            if (fromSnid == null || toSnid == null) {
                shell.requiredArg(null, this);
            }
            try {
                /* If the old SN hosted an admin the port arg is required */
                final Parameters p = cs.getParameters();
                for (AdminParams ap: p.getAdminParams()) {
                    if (ap.getStorageNodeId().equals(fromSnid)) {
                        if (port == 0) {
                            throw new ShellUsageException
                                ("Admin port is required because the " +
                                 "old storage node hosted an Admin service",
                                 this);
                        }
                    }
                }
                CommandUtils.ensureStorageNodeExists(fromSnid, cs, this);
                CommandUtils.ensureStorageNodeExists(toSnid, cs, this);
                final int planId =
                    cs.createMigrateSNPlan(planName, fromSnid, toSnid, port);
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "plan migrate-sn -from <id> -to <id> " +
                "[-admin-port <admin port>]" + genericFlags;
        }

        @Override
        protected String getCommandDescription() {
            return
                "Migrates the services from one storage node to another. " +
                "The old node" + eolt + "must not be running.  If the old " +
                "node hosted an admin service" + eolt +
                "the -admin-port argument is required.";
        }
    }

    static final class RemoveSNSub extends PlanSubCommand {

        RemoveSNSub() {
            super("remove-sn", 9);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            StorageNodeId snid = null;
            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if ("-sn".equals(arg)) {
                    final String argString = Shell.nextArg(args, i++, this);
                    snid = parseSnid(argString);
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }
            if (snid == null) {
                shell.requiredArg("-sn", this);
            }

            try {
                CommandUtils.ensureStorageNodeExists(snid, cs, this);
                final int planId = cs.createRemoveSNPlan(planName, snid);
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "plan remove-sn -sn <id>" + genericFlags;
        }

        @Override
        protected String getCommandDescription() {
            return
                "Removes the specified storage node from the topology.";
        }
    }

    static final class StartServiceSub extends StartStopServiceSub {

        StartServiceSub() {
            super("start-service", 4, true);
        }

        @Override
        protected String getCommandSyntax() {
            return "plan start-service -service <id> | -all-rns" +
                    genericFlags;
        }

        @Override
        protected String getCommandDescription() {
            return "Starts the specified service(s).";
        }
    }

    static final class StopServiceSub extends StartStopServiceSub {

        StopServiceSub() {
            super("stop-service", 4, false);
        }

        @Override
        protected String getCommandSyntax() {
            return "plan stop-service -service <id> | -all-rns" +
                    genericFlags;
        }

        @Override
        protected String getCommandDescription() {
            return "Stops the specified service(s).";
        }
    }

    /*
     * Start/stop specified services
     */
    abstract static class StartStopServiceSub extends PlanSubCommand {
        private Set<RepNodeId> rnids;
        private final boolean isStart;

        protected StartStopServiceSub(String name, int prefixMatchLength,
                                      boolean isStart) {
            super(name, prefixMatchLength);
            this.isStart = isStart;
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            final String cannotMixMsg =
                "Cannot mix -service and -all* arguments";
            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            rnids = null;
            String serviceName = null;
            boolean allRN = false;

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if ("-service".equals(arg)) {
                    serviceName = Shell.nextArg(args, i++, this);
                    if (allRN) {
                        throw new ShellUsageException(cannotMixMsg, this);
                    }
                    addService(serviceName);
                } else if ("-all-rns".equals(arg)) {
                    if (rnids != null) {
                        throw new ShellUsageException(cannotMixMsg, this);
                    }
                    allRN = true;
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }

            if (rnids == null && !allRN) {
                shell.requiredArg("-service|-all-rns", this);
            }

            try {
                int planId = 0;
                if (allRN) {
                    if (isStart) {
                        planId = cs.createStartAllRepNodesPlan(planName);
                    } else {
                        planId = cs.createStopAllRepNodesPlan(planName);
                    }
                } else {
                    validateRepNodes(cs, rnids);
                    if (isStart) {
                        planId = cs.createStartRepNodesPlan(planName, rnids);
                    } else {
                        planId = cs.createStopRepNodesPlan(planName, rnids);
                    }
                }
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        /* Currently only RNs are supported */
        private void addService(String serviceName)
            throws ShellException {

            final RepNodeId rnid = parseRnid(serviceName);
            if (rnids == null) {
                rnids = new HashSet<RepNodeId>();
            }
            rnids.add(rnid);
        }
    }

    static final class PlanWaitSub extends PlanSubCommand {

        PlanWaitSub() {
            super("wait", 3);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            int planId = 0;
            int timeoutSecs = 0;
            try {
                for (int i = 1; i < args.length; i++) {
                    final String arg = args[i];
                    if ("-id".equals(arg)) {
                        final String argString =
                            Shell.nextArg(args, i++, this);
                        planId = parseInt(argString, "Invalid plan ID");
                    } else if ("-seconds".equals(arg)) {
                        final String argString =
                            Shell.nextArg(args, i++, this);
                        timeoutSecs =
                            parseInt(argString, "Invalid timeout value");
                    } else if ("-last".equals(arg)) {
                        planId = getLastPlanId(cs);
                    } else {
                        shell.unknownArgument(arg, this);
                    }
                }
                if (planId == 0) {
                    shell.requiredArg("-id", this);
                }
                CommandUtils.ensurePlanExists(planId, cs, this);
                final Plan.State state =
                    cs.awaitPlan(planId, timeoutSecs, TimeUnit.SECONDS);
                return state.getWaitMessage(planId);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "plan wait -id <id> | -last [-seconds <timeout in seconds>]";
        }

        @Override
        protected String getCommandDescription() {
            return
                "Waits for the specified plan to complete.  If the " +
                "optional timeout" + eolt + "is specified, wait that long, " +
                "otherwise wait indefinitely.  Use -last" + eolt +
                "to reference the most recently created plan.";

        }
    }

    static final class RemoveDatacenterSub extends RemoveZoneSub {

        static final String dcCommandDeprecation =
            "The command:" + eol + eolt +
            "plan remove-datacenter" + eol + eol +
            "is deprecated and has been replaced by:" + eol + eolt +
            "plan remove-zone" + eol + eol;

        RemoveDatacenterSub() {
            super("remove-datacenter", 9);
        }

        /** Add deprecation message. */
        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            return dcCommandDeprecation + super.exec(args, shell);
        }

        /** Add deprecation message. */
        @Override
        public String getCommandDescription() {
            return super.getCommandDescription() + eol + eolt +
                "This command is deprecated and has been replaced by:" +
                eol + eolt +
                "plan remove-zone";
        }
    }

    static class RemoveZoneSub extends PlanSubCommand {
        static final String ID_FLAG = "-zn";
        static final String NAME_FLAG = "-znname";

        RemoveZoneSub() {
            super("remove-zone", 9);
        }

        RemoveZoneSub(final String name, final int prefixMatchLength) {
            super(name, prefixMatchLength);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            DatacenterId id = null;
            String nameFlag = null;
            boolean deprecatedDcFlag = false;
            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if (CommandUtils.isDatacenterIdFlag(arg)) {
                    final String argVal = Shell.nextArg(args, i++, this);
                    id = parseDatacenterId(argVal);
                    if (CommandUtils.isDeprecatedDatacenterId(arg, argVal)) {
                        deprecatedDcFlag = true;
                    }
                } else if (CommandUtils.isDatacenterNameFlag(arg)) {
                    nameFlag = Shell.nextArg(args, i++, this);
                    if (CommandUtils.isDeprecatedDatacenterName(arg)) {
                        deprecatedDcFlag = true;
                    }
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }
            if (id == null && nameFlag == null) {
                shell.requiredArg(ID_FLAG + " | " + NAME_FLAG, this);
            }
            final String deprecatedDcFlagPrefix =
                deprecatedDcFlag ? dcFlagsDeprecation : "";
            try {
                if (id != null) {
                    CommandUtils.ensureDatacenterExists(id, cs, this);
                } else {
                    id = CommandUtils.getDatacenterId(nameFlag, cs, this);
                }

                final int planId = cs.createRemoveDatacenterPlan(planName, id);
                return deprecatedDcFlagPrefix + executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        public String getCommandSyntax() {
            return "plan " + name + " " + ID_FLAG + " <id> | " +
                   NAME_FLAG + " <name>" + genericFlags;
        }

        @Override
        public String getCommandDescription() {
            return "Removes the specified zone from the store.";
        }
    }

    static final class AddTableSub extends PlanSubCommand {
        static final String TABLE_NAME_FLAG = "-name";

        AddTableSub() {
            super("add-table", 5);
        }

        @Override
        @SuppressWarnings("null")
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);

            CommandShell cmd = (CommandShell)shell;
            CommandServiceAPI cs = cmd.getAdmin();
            String tableName = null;
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if (TABLE_NAME_FLAG.equals(arg)) {
                    tableName = Shell.nextArg(args, i++, this);
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }

            if (tableName == null) {
                shell.requiredArg(TABLE_NAME_FLAG, this);
            }
            Object obj = shell.getVariable(tableName);
            if (obj == null || !(obj instanceof TableBuilder)) {
                shell.invalidArgument("table " + tableName +
                    " is not yet built, please run the \'table create\' " +
                    "command to build it first.", this);
            }

            try {
                TableBuilder tb = (TableBuilder)obj;
                final int planId =
                    cs.createAddTablePlan(planName,
                                          tb.getName(),
                                          ((tb.getParent()!=null)?
                                           tb.getParent().getFullName():null),
                                          tb.getFieldMap(),
                                          tb.getPrimaryKey(),
                                          tb.getShardKey(),
                                          tb.isR2compatible(),
                                          tb.getSchemaId(),
                                          tb.getDescription());
                shell.removeVariable(tableName);
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "plan add-table " + TABLE_NAME_FLAG + " <name> " +
                genericFlags;
        }

        @Override
        protected String getCommandDescription() {
            return "Add a new table to the store.  " + "The table name is a " +
                "dot-separated name" + eolt + "with the format " +
                "tableName[.childTableName]*.  Use the table create" + eolt +
                "command to create the named table.  " +
                "Use \"table list -create\" to see the" + eolt +
                "list of tables that can be added.";
        }
    }

    static final class EvolveTableSub extends PlanSubCommand {
        static final String TABLE_NAME_FLAG = "-name";

        EvolveTableSub() {
            super("evolve-table", 8);
        }

        @Override
        @SuppressWarnings("null")
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);

            CommandShell cmd = (CommandShell)shell;
            CommandServiceAPI cs = cmd.getAdmin();
            String tableName = null;
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if (TABLE_NAME_FLAG.equals(arg)) {
                    tableName = Shell.nextArg(args, i++, this);
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }
            if (tableName == null) {
                shell.requiredArg(TABLE_NAME_FLAG, this);
            }

            Object obj = shell.getVariable(tableName);
            if (obj == null ||
                !(obj instanceof TableEvolver)) {
                shell.invalidArgument("table " + tableName +
                    " is not yet built, please run command \'table evolve\'" +
                    " to build it first.", this);
            }

            TableEvolver te = (TableEvolver)obj;
            try {
                final int planId =
                    cs.createEvolveTablePlan(planName,
                                             te.getTable().getFullName(),
                                             te.getTableVersion(),
                                             te.getFieldMap());
                shell.removeVariable(tableName);
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "plan evolve-table " + TABLE_NAME_FLAG + " <name> " +
                   genericFlags;
        }

        @Override
        protected String getCommandDescription() {
            return "Evolve a table in the store.  The table name is a " +
                "dot-separated name" + eolt + "with the format " +
                "tableName[.childTableName]*.  The named table must " + eolt +
                "have been evolved using the \"table evolve\" " +
                "command. Use" + eolt + "\"table list -evolve\" " +
                "to see the list of tables that can be evolved.";
        }
    }

    static final class RemoveTableSub extends PlanSubCommand {
        static final String TABLE_NAME_FLAG = "-name";
        static final String KEEP_DATA_FLAG = "-keep-data";

        RemoveTableSub() {
            super("remove-table", 8);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);

            CommandShell cmd = (CommandShell)shell;
            CommandServiceAPI cs = cmd.getAdmin();
            String tableName = null;
            boolean removeData = true;
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if (TABLE_NAME_FLAG.equals(arg)) {
                    tableName = Shell.nextArg(args, i++, this);
                } else if (KEEP_DATA_FLAG.equals(arg)) {
                    removeData = false;
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }

            if (tableName == null) {
                shell.requiredArg(TABLE_NAME_FLAG, this);
            }

            try {
                final int planId =
                    cs.createRemoveTablePlan(
                        planName, tableName, removeData);
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "plan remove-table " + TABLE_NAME_FLAG + " <name> " +
                   "[" + KEEP_DATA_FLAG + "]" +
                   genericFlags;
        }

        @Override
        protected String getCommandDescription() {
            return "Remove a table from the store.  The table name is a " +
                "dot-separated name" + eolt + "with the format " +
                "tableName[.childTableName]*.  The named table must " +
                "exist " + eolt + "and must not have any child tables.  " +
                "Indexes on the table are automatically" + eolt +
                "removed.  By default data stored in this table is also " +
                "removed.  Table" + eolt + "data may be optionally saved by " +
                "specifying the -keep-data flag." + eolt + "Depending " +
                "on the indexes and amount of data stored in the table this" +
                eolt + "may be a long-running plan.";
        }
    }

    static final class AddIndexSub extends PlanSubCommand {
        static final String INDEX_NAME_FLAG = "-name";
        static final String TABLE_FLAG = "-table";
        static final String FIELD_FLAG = "-field";
        static final String DESC_FLAG = "-desc";

        AddIndexSub() {
            super("add-index", 5);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);

            CommandShell cmd = (CommandShell)shell;
            CommandServiceAPI cs = cmd.getAdmin();
            String indexName = null;
            String tableName = null;
            String desc = null;
            List<String> fields = new ArrayList<String>();
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if (INDEX_NAME_FLAG.equals(arg)) {
                    indexName = Shell.nextArg(args, i++, this);
                } else if (TABLE_FLAG.equals(arg)) {
                    tableName = Shell.nextArg(args, i++, this);
                } else if (FIELD_FLAG.equals(arg)) {
                    fields.add(Shell.nextArg(args, i++, this));
                } else if (DESC_FLAG.equals(arg)) {
                    desc = Shell.nextArg(args, i++, this);
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }

            if (indexName == null) {
                shell.requiredArg(INDEX_NAME_FLAG, this);
            }
            if (tableName == null) {
                shell.requiredArg(TABLE_FLAG, this);
            }
            if (fields.size() == 0) {
                shell.requiredArg(FIELD_FLAG, this);
            }

            try {
                final int planId =
                    cs.createAddIndexPlan(planName,
                                          indexName,
                                          tableName,
                                          fields.toArray(new String[0]),
                                          desc);
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "plan add-index " + INDEX_NAME_FLAG + " <name> " +
                TABLE_FLAG + " <name> " +
                "[" + FIELD_FLAG + " <name>]* " + eolt +
                "[" + DESC_FLAG + " <description>]" +
                genericFlags;
        }

        @Override
        protected String getCommandDescription() {
            return "Add an index to a table in the store.  " +
            "The table name is a" + eolt + "dot-separated name with the " +
            "format tableName[.childTableName]*.";
        }
    }

    static final class RemoveIndexSub extends PlanSubCommand {
        static final String INDEX_NAME_FLAG = "-name";
        static final String TABLE_FLAG = "-table";

        RemoveIndexSub() {
            super("remove-index", 8);
        }

        @Override
        public String exec(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);

            CommandShell cmd = (CommandShell)shell;
            CommandServiceAPI cs = cmd.getAdmin();
            String indexName = null;
            String tableName = null;
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if (INDEX_NAME_FLAG.equals(arg)) {
                    indexName = Shell.nextArg(args, i++, this);
                } else if (TABLE_FLAG.equals(arg)) {
                    tableName = Shell.nextArg(args, i++, this);
                } else {
                    i += checkGenericArg(arg, args, i);
                }
            }

            if (indexName == null) {
                shell.requiredArg(INDEX_NAME_FLAG, this);
            }
            if (tableName == null) {
                shell.requiredArg(TABLE_FLAG, this);
            }

            try {
                final int planId =
                    cs.createRemoveIndexPlan(planName, indexName, tableName);
                return executePlan(planId, cs, shell);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "plan remove-index " + INDEX_NAME_FLAG + " <name> " +
                    TABLE_FLAG + " <name> " +
                    genericFlags;
        }

        @Override
        protected String getCommandDescription() {
            return "Remove an index from a table.  The table name is a " +
                "dot-separated name" + eolt + "with the format " +
                "tableName[.childTableName]*.";
        }
    }
}
