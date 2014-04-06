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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.rmi.RemoteException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TimeZone;
import java.util.TreeMap;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.Snapshot;
import oracle.kv.impl.admin.criticalevent.CriticalEvent;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.StatusReport;
import oracle.kv.impl.api.avro.AvroDdl;
import oracle.kv.impl.api.avro.AvroSchemaMetadata;
import oracle.kv.impl.api.avro.AvroSchemaStatus;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.monitor.views.PerfEvent;
import oracle.kv.impl.monitor.views.ServiceChange;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.metadata.KVStoreUser.UserDescription;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.FormatUtils;
import oracle.kv.impl.util.TopologyPrinter;
import oracle.kv.impl.util.TopologyPrinter.Filter;
import oracle.kv.table.Index;
import oracle.kv.table.Table;
import oracle.kv.util.shell.CommandWithSubs;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellArgumentException;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellException;
import oracle.kv.util.shell.ShellUsageException;

/*
 * show and its subcommands
 */
class ShowCommand extends CommandWithSubs {
    private static final List<? extends SubCommand> subs =
                                       Arrays.asList(new ShowParameters(),
                                                     new ShowAdmins(),
                                                     new ShowEvents(),
                                                     new ShowFaults(),
                                                     new ShowIndexes(),
                                                     new ShowPerf(),
                                                     new ShowPlans(),
                                                     new ShowPools(),
                                                     new ShowSchemas(),
                                                     new ShowSnapshots(),
                                                     new ShowTables(),
                                                     new ShowTopology(),
                                                     new ShowDatacenters(),
                                                     new ShowUpgradeOrder(),
                                                     new ShowUsers(),
                                                     new ShowZones());
    ShowCommand() {
        super(subs, "show", 2, 2);
    }

    @Override
    protected String getCommandOverview() {
        return "Encapsulates commands that display the state of the store " +
               "and its components.";
    }

    /*
     * ShowParameters
     */
    private static final class ShowParameters extends SubCommand {

        private ShowParameters() {
            super("parameters", 4);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            /*
             * parameters -policy | -security | -service <name>
             */
            if (args.length < 2) {
                shell.badArgCount(this);
            }
            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();
            boolean showHidden = cmd.showHidden();
            String serviceName = null;
            boolean isPolicy = false;
            boolean isSecurity = false;

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if ("-policy".equals(arg)) {
                    isPolicy = true;
                } else if ("-service".equals(arg)) {
                    serviceName = Shell.nextArg(args, i++, this);
                } else if ("-hidden".equals(arg)) {
                    showHidden = true;
                } else if ("-security".equals(arg)) {
                    isSecurity = true;
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            if (isPolicy && isSecurity) {
                throw new ShellUsageException
                    ("-policy and -security cannot be used together", this);
            }
            if (isPolicy) {
                if (serviceName != null) {
                    throw new ShellUsageException
                        ("-policy cannot be combined with a service", this);
                }
                try {
                    final ParameterMap map = cs.getPolicyParameters();
                    return CommandUtils.formatParams
                        (map, showHidden, ParameterState.Info.POLICY);
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
            }
            if (isSecurity) {
                if (serviceName != null) {
                    throw new ShellUsageException
                        ("-security cannot be combined with a service", this);
                }
                try {
                    final ParameterMap map =
                        cs.getParameters().getGlobalParams().
                            getGlobalSecurityPolicies();
                    return CommandUtils.formatParams(map, showHidden, null);
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
            }
            if (serviceName == null) {
                shell.requiredArg("-service|-policy", this);
            }

            RepNodeId rnid = null;
            AdminId aid = null;
            StorageNodeId snid = null;
            try {
                rnid = RepNodeId.parse(serviceName);
            } catch (IllegalArgumentException ignored) {
                try {
                    snid = StorageNodeId.parse(serviceName);
                } catch (IllegalArgumentException ignored1) {
                    try {
                        aid = AdminId.parse(serviceName);
                    } catch (IllegalArgumentException ignored2) {
                        shell.invalidArgument(serviceName, this);
                    }
                }
            }

            Parameters p;
            try {
                p = cs.getParameters();
                if (rnid != null) {
                    final RepNodeParams rnp = p.get(rnid);
                    if (rnp == null) {
                        noSuchService(rnid);
                    } else {
                        return CommandUtils.formatParams
                            (rnp.getMap(), showHidden,
                             ParameterState.Info.REPNODE);
                    }
                } else if (snid != null) {
                    final StorageNodeParams snp = p.get(snid);
                    if (snp == null) {
                        noSuchService(snid);
                        return "";
                    }
                    String result =
                        CommandUtils.formatParams(snp.getMap(), showHidden,
                                                  ParameterState.Info.SNA);
                    final ParameterMap mountMap = snp.getMountMap();
                    if (mountMap != null && mountMap.size() > 0) {
                        result += "Storage directories:" + eol;
                        for (String s : mountMap.keys()) {
                            result += Shell.makeWhiteSpace(4) + s + eol;
                        }
                    }
                    return result;
                } else if (aid != null) {
                    final AdminParams ap = p.get(aid);
                    if (ap == null) {
                        noSuchService(aid);
                    } else {
                        return CommandUtils.formatParams
                            (ap.getMap(), showHidden,
                             ParameterState.Info.ADMIN);
                    }
                }
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }

            return "show parameters...";
        }

        @Override
        protected String getCommandSyntax() {
            return "show parameters -policy | -security | -service <name>";
        }

        @Override
        protected String getCommandDescription() {
            return
                "Displays service parameters and state for the specified " +
                "service." + eolt + "The service may be a RepNode, " +
                "StorageNode, or Admin service," + eolt + "as identified " +
                "by any valid string, for example" + eolt + "rg1-rn1, sn1, " +
                "admin2, etc.  Use the -policy flag to show global policy" +
                eolt + "parameters. Use the -security flag to show global " +
                "security parameters.";
        }

        private void noSuchService(ResourceId rid)
            throws ShellException {

            throw new ShellArgumentException
                ("No such service: " + rid);
        }
    }

    /*
     * ShowAdmins
     */
    private static final class ShowAdmins extends SubCommand {

        private ShowAdmins() {
            super("admins", 2);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            if (args.length > 2) {
                shell.badArgCount(this);
            }
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            final String currentAdminHost = cmd.getAdminHostname();
            final int currentAdminPort = cmd.getAdminPort();

            try {
                final List<ParameterMap> admins = cs.getAdmins();
                final Topology t = cs.getTopology();
                String res = "";
                for (ParameterMap map : admins) {
                    final AdminParams params = new AdminParams(map);
                    res += params.getAdminId().toString()  + ": Storage Node ";
                    res += params.getStorageNodeId() + ", HTTP port ";
                    res += params.getHttpPort();

                    final StorageNode sn = t.get(params.getStorageNodeId());
                    if (currentAdminHost.equals(sn.getHostname()) &&
                        currentAdminPort == sn.getRegistryPort()) {
                        res += " (master)";
                    }
                    res += eol;
                }
                return res;
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return cantGetHere;
        }

        @Override
        protected String getCommandSyntax() {
            return "show admins";
        }

        @Override
        protected String getCommandDescription() {
            return "Displays basic information about deployed Admin services.";
        }
    }

    /*
     * ShowSchemas
     */
    static final class ShowSchemas extends SubCommand {

        private ShowSchemas() {
            super("schemas", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            if (args.length > 3) {
                shell.badArgCount(this);
            }

            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            if (args.length == 3) {
                if ("-name".equals(args[1])) {
                    return showSingleSchema(args[2], cs, cmd);
                }
                shell.unknownArgument(args[1], this);
            }
            final boolean includeDisabled;
            if (args.length == 2) {
                if (!"-disabled".equals(args[1])) {
                    shell.unknownArgument(args[1], this);
                }
                includeDisabled = true;
            } else {
                includeDisabled = false;
            }

            try {

                final SortedMap<String, AvroDdl.SchemaSummary> map =
                    cs.getSchemaSummaries(includeDisabled);

                if (map.isEmpty()) {
                    return "";
                }

                final StringBuilder builder =
                    new StringBuilder(map.size() * 100);

                for (final AvroDdl.SchemaSummary summary : map.values()) {
                    if (builder.length() > 0) {
                        builder.append(eol);
                    }
                    builder.append(summary.getName());
                    formatAvroSchemaSummary(summary, builder);
                    AvroDdl.SchemaSummary prevVersion =
                        summary.getPreviousVersion();
                    while (prevVersion != null) {
                        formatAvroSchemaSummary(prevVersion, builder);
                        prevVersion = prevVersion.getPreviousVersion();
                    }
                }
                return builder.toString();
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return cantGetHere;
        }

        @Override
        protected String getCommandSyntax() {
            return "show schemas [-disabled] | [-name <name>]";
        }

        @Override
        protected String getCommandDescription() {
            return
                "Displays schema details of the named schema or a list of " +
                "schemas" + eolt + "registered with the store. The " +
                "-disabled flag enables listing of" + eolt + "disabled " +
                "schemas.";
        }

        private void formatAvroSchemaSummary(AvroDdl.SchemaSummary summary,
                                             StringBuilder builder) {
            builder.append(eol).append("  ID: ").append(summary.getId());
            final AvroSchemaMetadata metadata = summary.getMetadata();
            if (metadata.getStatus() == AvroSchemaStatus.DISABLED) {
                builder.append(" (disabled)");
            }
            builder.append("  Modified: ").append
                (FormatUtils.formatDateAndTime(metadata.getTimeModified()));
            builder.append(", From: ").append(metadata.getFromMachine());
            if (!metadata.getByUser().isEmpty()) {
                builder.append(", By: ").append(metadata.getByUser());
            }
        }
        /**
         * Returns a valid schema ID for the given schemaName[.ID] string, or
         * throws ShellUsageException if there is no such schema.  The .ID
         * is required if the schema is disabled.
         *
         * @param idRequired is true if the .ID portion is reqiured.
         *
         * This is static so that it can be called from the DdlCommand as well.
         */
        protected static int parseSchemaNameAndId(String nameAndId,
                                                  boolean idRequired,
                                                  CommandServiceAPI cs,
                                                  ShellCommand command)
            throws ShellException, RemoteException {

            /* Get trailing .ID value, if any. */
            final int offset = nameAndId.lastIndexOf(".");
            int id = 0;
            if (offset > 0 && offset < nameAndId.length() - 1) {
                final String idArg = nameAndId.substring(offset + 1);
                try {
                    id = Integer.parseInt(idArg);
                    if (id <= 0) {
                        throw new ShellUsageException
                            ("Illegal schema ID: " + id, command);
                    }
                } catch (NumberFormatException e) /* CHECKSTYLE:OFF */ {
                    /* The ID remains zero. */
                } /* CHECKSTYLE:ON */
            }

            /* Get a map of all schemas. */
            final SortedMap<String, AvroDdl.SchemaSummary> map =
                cs.getSchemaSummaries(true /*includeDisabled*/);

            /*
             * If there is no trailing .ID, use the first active schema with
             * the given name.
             */
            if (id == 0) {
                if (idRequired) {
                    throw new ShellUsageException
                        (nameAndId + " does not containing a trailing .ID " +
                         "value", command);

                }

                final String name = nameAndId;
                AvroDdl.SchemaSummary summary = map.get(name);
                while (summary != null) {
                    if (summary.getMetadata().getStatus() !=
                        AvroSchemaStatus.DISABLED) {
                        return summary.getId();
                    }
                    summary = summary.getPreviousVersion();
                }
                throw new ShellUsageException
                    ("Schema " + name + " does not exist or is disabled." +
                     eol + "Use <schemaName>.<ID> to refer to a disabled " +
                     "schema.", command);
            }

            /*
             * Return the ID if there is such a schema with the given name and
             * ID.
             */
            final String name = nameAndId.substring(0, offset);
            AvroDdl.SchemaSummary summary = map.get(name);
            while (summary != null) {
                if (id == summary.getId()) {
                    return id;
                }
                summary = summary.getPreviousVersion();
            }
            throw new ShellUsageException
                ("Schema " + name + " does not exist or does not have ID " +
                 id, command);
        }

        private String showSingleSchema(String nameAndId,
                                        CommandServiceAPI cs,
                                        CommandShell cmd)
            throws ShellException {

            try {
                final int id;
                id = parseSchemaNameAndId(nameAndId, false /*idRequired*/,
                                          cs, this);
                final AvroDdl.SchemaDetails details = cs.getSchemaDetails(id);
                return details.getText();
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return cantGetHere;
        }
    }

    /*
     * ShowTopology
     */
    static final class ShowTopology extends SubCommand {
        static final String rnFlag = "-rn";
        static final String snFlag = "-sn";
        static final String stFlag = "-store";
        static final String shFlag = "-shard";
        static final String statusFlag = "-status";
        static final String perfFlag = "-perf";
        static final String dcFlagsDeprecation =
            "The -dc flag is deprecated and has been replaced by -zn." +
            eol + eol;

        private ShowTopology() {
            super("topology", 4);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            EnumSet<TopologyPrinter.Filter> filter =
                EnumSet.noneOf(Filter.class);
            Shell.checkHelp(args, this);
            boolean hasComponents = false;
            boolean deprecatedDcFlag = false;
            if (args.length > 1) {
                for (int i = 1; i < args.length; i++) {
                    if (CommandUtils.isDatacenterIdFlag(args[i])) {
                        filter.add(Filter.DC);
                        hasComponents = true;
                        if ("-dc".equals(args[i])) {
                            deprecatedDcFlag = true;
                        }
                    } else if (args[i].equals(rnFlag)) {
                        filter.add(Filter.RN);
                        hasComponents = true;
                    } else if (args[i].equals(snFlag)) {
                        filter.add(Filter.SN);
                        hasComponents = true;
                    } else if (args[i].equals(stFlag)) {
                        filter.add(Filter.STORE);
                        hasComponents = true;
                    } else if (args[i].equals(shFlag)) {
                        filter.add(Filter.SHARD);
                        hasComponents = true;
                    } else if (args[i].equals(statusFlag)) {
                        filter.add(Filter.STATUS);
                    } else if (args[i].equals(perfFlag)) {
                        filter.add(Filter.PERF);
                    } else {
                        shell.unknownArgument(args[i], this);
                    }
                }
            } else {
                filter = TopologyPrinter.all;
                hasComponents = true;
            }

            if (!hasComponents) {
                filter.addAll(TopologyPrinter.components);
            }

            final String deprecatedDcFlagPrefix =
                !deprecatedDcFlag ? "" : dcFlagsDeprecation;

            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();
            try {
                final ByteArrayOutputStream outStream =
                    new ByteArrayOutputStream();
                final PrintStream out = new PrintStream(outStream);

                final Topology t = cs.getTopology();
                final Parameters p = cs.getParameters();
                Map<ResourceId, ServiceChange> statusMap = null;
                if (filter.contains(Filter.STATUS)) {
                    statusMap = cs.getStatusMap();
                }
                Map<ResourceId, PerfEvent> perfMap = null;
                if (filter.contains(Filter.PERF)) {
                    perfMap = cs.getPerfMap();
                }
                TopologyPrinter.printTopology(t, out, p, filter,
                                              statusMap,
                                              perfMap,
                                              shell.getVerbose());
                return deprecatedDcFlagPrefix + outStream;
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return cantGetHere;
        }

        @Override
        protected String getCommandSyntax() {
            return
                "show topology [-zn] [-rn] [-sn] [-store] [-status] [-perf]";
        }

        @Override
        protected String getCommandDescription() {
            return
                "Displays the current, deployed topology. " +
                "By default show the entire " + eolt +
                "topology. The optional flags restrict the " +
                "display to one or more of" + eolt + "Zones, " +
                "RepNodes, StorageNodes and Storename," + eolt + "or " +
                "specify service status or performance.";
        }
    }

    /*
     * ShowEvents
     */
    private static final class ShowEvents extends SubCommand {

        private ShowEvents() {
            super("events", 2);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();
            if (Shell.checkArg(args, "-id")) {
                return showSingleEvent(args, cs, cmd, shell);
            }
            try {
                Date fromTime = null;
                Date toTime = null;
                CriticalEvent.EventType et = CriticalEvent.EventType.ALL;

                for (int i = 1; i < args.length; i++) {
                    if ("-from".equals(args[i])) {
                        if (++i >= args.length) {
                            shell.badArgCount(this);
                        }
                        fromTime = parseTimestamp(args[i], this);
                        if (fromTime == null) {
                            return "Can't parse " + args[i] +
                                   " as a timestamp.";
                        }
                    } else if ("-to".equals(args[i])) {
                        if (++i >= args.length) {
                            shell.badArgCount(this);
                        }
                        toTime = parseTimestamp(args[i], this);
                    } else if ("-type".equals(args[i])) {
                        if (++i >= args.length) {
                            shell.badArgCount(this);
                        }
                        try {
                            final String etype = args[i].toUpperCase();
                            et = Enum.valueOf(CriticalEvent.EventType.class,
                                              etype);
                        } catch (IllegalArgumentException iae) {
                            throw new ShellUsageException
                                ("Can't parse " + args[i] +
                                 " as an EventType.", this);
                        }
                    } else {
                        shell.unknownArgument(args[i], this);
                    }
                }

                final long from = (fromTime == null ? 0L : fromTime.getTime());
                final long to = (toTime == null ? 0L : toTime.getTime());

                final List<CriticalEvent> events = cs.getEvents(from, to, et);
                String msg = "";
                for (CriticalEvent ev : events) {
                    msg += ev.toString() + eol;
                }
                return msg;
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return cantGetHere;
        }

        @Override
        protected String getCommandSyntax() {
            return "show events [-id <id>] | [-from <date>] " +
                "[-to <date>]" + eolt + "[-type <stat|log|perf>]";
        }

        @Override
        protected String getCommandDescription() {
            return
                "Displays event details or list of store events.  Status " +
                "events indicate" + eolt + "changes in service status.  " +
                "Log events correspond to records written " + eolt + "to " +
                "the store's log, except that only records logged at " +
                "\"SEVERE\" are " + eolt + "displayed; which should be " +
                "investigated immediately.  To view records " + eolt +
                "logged at \"WARNING\" or lower consult the store's log " +
                "file." + eolt + "Performance events are not usually " +
                "critical but may merit investigation." + eolt + eolt +
                getDateFormatsUsage();
        }

        private String showSingleEvent(String[] args,
                                       CommandServiceAPI cs,
                                       CommandShell cmd,
                                       Shell shell)
            throws ShellException {

            if (args.length != 3) {
                shell.badArgCount(this);
            }

            if (!"-id".equals(args[1])) {
                shell.unknownArgument(args[1], this);
            }
            final String eventId = args[2];
            try {
                final CriticalEvent event = cs.getOneEvent(eventId);
                if (event == null) {
                    return "No event matches the id " + eventId;
                }
                return event.getDetailString();
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return cantGetHere;
        }
    }

    /*
     * ShowPlans
     *
     * show plans with no arguments: list the ten most recent plans.
     *
     * -last: show details of the most recently created plan.
     *
     * -id <id>: show details of the plan with the given sequence number, or
     *   If -num <n> is also given, list <n> plans, starting with plan #<id>.
     *
     * -num <n>: set the number of plans to list. Defaults to 10.
     *   If unaccompanied: list the n most recently created plans.
     *
     * -from <date>: list plans starting with those created after <date>.
     *
     * -to <date>: list plans ending with those created before <date>.
     *   Combining -from with -to describes the range between the two <dates>.
     *   Otherwise -num <n> applies; its absence implies the default of 10.
     */
    private static final class ShowPlans extends SubCommand {

        private ShowPlans() {
            super("plans", 2);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            if ((args.length == 3 && Shell.checkArg(args, "-id")) ||
                 (args.length == 2 && Shell.checkArg(args, "-last"))) {
                return showSinglePlan(shell, args, cs, cmd);
            }

            int planId = 0;
            int howMany = 0;
            Date fromTime = null, toTime = null;

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if ("-id".equals(arg)) {
                    planId = Integer.parseInt(Shell.nextArg(args, i++, this));
                } else if ("-num".equals(arg)) {
                    howMany = Integer.parseInt(Shell.nextArg(args, i++, this));
                } else if ("-from".equals(arg)) {
                    fromTime =
                        parseTimestamp(Shell.nextArg(args, i++, this), this);
                } else if ("-to".equals(arg)) {
                    toTime =
                        parseTimestamp(Shell.nextArg(args, i++, this), this);
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            if (planId != 0 && !(fromTime == null && toTime == null)) {
                throw new ShellUsageException
                    ("-id cannot be used in combination with -from or -to",
                     this);
            }

            /* If no other range selector is given, default to most recent. */
            if (planId == 0 && fromTime == null && toTime == null) {
                toTime = new Date();
            }

            /* If no other range limit is given, default to 10. */
            if ((fromTime == null || toTime == null) && howMany == 0) {
                howMany = 10;
            }

            /*
             * If a time-based range is requested, we need to get plan ID range
             * information first.
             */
            if (! (fromTime == null && toTime == null)) {
                try {

                    int range[] =
                        cs.getPlanIdRange
                        (fromTime == null ? 0L : fromTime.getTime(),
                         toTime == null ? 0L : toTime.getTime(),
                         howMany);

                    planId = range[0];
                    howMany = range[1];
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
            }

            String res = "";
            while (howMany > 0) {
                try {
                    SortedMap<Integer, Plan> sortedPlans =
                        new TreeMap<Integer, Plan>
                        (cs.getPlanRange(planId, howMany));

                    /* If we got zero plans back, we're out of plans. */
                    if (sortedPlans.size() == 0) {
                        break;
                    }

                    for (Integer k : sortedPlans.keySet()) {
                        final Plan p = sortedPlans.get(k);
                        res += String.format("%6d %-24s %s" + eol,
                                             p.getId(),
                                             p.getName(),
                                             p.getState().toString());

                        howMany--;
                        planId = k.intValue() + 1;
                    }
                } catch (RemoteException re) {
                    cmd.noAdmin(re);
                }
            }
            return res;
        }

        @Override
        protected String getCommandSyntax() {
            return "show plans [-last] [-id <id>] [-from <date] [-to <date>]" +
                "[-num <howMany>]";
        }

        @Override
        protected String getCommandDescription() {
            return
                "Shows details of the specified plan or lists all plans " +
                "that have been" + eolt + "created along with their " +
                "corresponding plan IDs and status." + eolt +
                eolt +
                "With no argument: lists the ten most recent plans." + eolt +
                "-last: shows details of the most recent plan" + eolt +
                "-id <id>: shows details of the plan with the given id;" + eolt+
                "    if -num <n> is also given, list <n> plans," + eolt +
                "    starting with plan #<id>." + eolt +
                "-num <n>: sets the number of plans to list." + eolt +
                "    Defaults to 10." + eolt +
                "-from <date>: lists plans after <date>." + eolt +
                "-to <date>: lists plans before <date>." + eolt +
                "    Combining -from with -to describes the range" + eolt +
                "    between the two <dates>.  Otherwise -num applies." + eolt +
                eolt +
                getDateFormatsUsage();
        }

        /*
         * Show details of a single plan.  TODO: add flags for varying details:
         * -tasks -finished, etc.
         */
        private String showSinglePlan(Shell shell, String[] args,
                                      CommandServiceAPI cs, CommandShell cmd)
            throws ShellException {

            int planId = 0;
            final boolean verbose = shell.getVerbose();

            try {
                if ("-last".equals(args[1])) {
                    planId = PlanCommand.PlanSubCommand.getLastPlanId(cs);
                } else if ("-id".equals(args[1])) {
                    if (args.length != 3) {
                        shell.badArgCount(this);
                    }
                    try {
                        planId = Integer.parseInt(args[2]);
                    } catch (IllegalArgumentException ignored) {
                        throw new ShellUsageException
                            ("Invalid plan ID: " + args[2], this);
                    }
                } else {
                    shell.unknownArgument(args[1], this);
                }

                long options = StatusReport.SHOW_FINISHED_BIT;
                if (verbose) {
                    options |= StatusReport.VERBOSE_BIT;
                }
                return cs.getPlanStatus(planId, options);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return cantGetHere;
        }
    }

    /*
     * ShowPools
     */
    private static final class ShowPools extends SubCommand {

        private ShowPools() {
            super("pools", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            if (args.length > 2) {
                shell.badArgCount(this);
            }
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();
            try {
                final List<String> poolNames = cs.getStorageNodePoolNames();
                String res = "";
                for (String pn : poolNames) {
                    res += pn + ": ";
                    for (StorageNodeId snid : cs.getStorageNodePoolIds(pn)) {
                        res += snid.toString() + " ";
                    }
                    res += eol;
                }
                return res;
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return cantGetHere;
        }

        @Override
        protected String getCommandSyntax() {
            return "show pools";
        }

        @Override
        protected String getCommandDescription() {
            return "Lists the storage node pools";
        }
    }

    static final class ShowFaults extends SubCommand {

        private ShowFaults() {
            super("faults", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            final Shell.CommandHistory history = shell.getHistory();
            final int from = 0;
            final int to = history.getSize();

            if (args.length > 1) {
                final String arg = args[1];
                if ("-last".equals(arg)) {
                    return history.dumpLastFault();
                } else if ("-command".equals(arg)) {
                    final String faultString = Shell.nextArg(args, 1, this);
                    int fault;
                    try {
                        fault = Integer.parseInt(faultString);
                        if (fault < 0 || fault >= history.getSize()) {
                            return "Index out of range: " + fault + "" + eolt +
                                   getBriefHelp();
                        }
                        if (history.commandFaulted(fault)) {
                            return history.dumpCommand(fault, true);
                        }
                        return "Command " + fault + " did not fault";
                    } catch (IllegalArgumentException e) {
                        shell.invalidArgument(faultString, this);
                    }
                } else {
                    shell.unknownArgument(arg, this);
                }
            }
            return history.dumpFaultingCommands(from, to);
        }

        @Override
        protected String getCommandSyntax() {
            return "show faults [-last] [-command <command index>]";
        }

        @Override
        protected String getCommandDescription() {
            return
                "Displays faulting commands.  By default all available " +
                "faulting commands" + eolt + "are displayed.  Individual " +
                "fault details can be displayed using the" + eolt +
                "-last and -command flags.";
        }
    }

    /*
     * TODO: Add filter flags
     */
    private static final class ShowPerf extends SubCommand {

        private ShowPerf() {
            super("perf", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();
            try {
                final ByteArrayOutputStream outStream =
                    new ByteArrayOutputStream();
                final PrintStream out = new PrintStream(outStream);

                final Map<ResourceId, PerfEvent> perfMap = cs.getPerfMap();

                out.println(PerfEvent.HEADER);
                for (PerfEvent pe : perfMap.values()) {
                    out.println(pe.getColumnFormatted());
                }
                return outStream.toString();
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return cantGetHere;
        }

        @Override
        protected String getCommandSyntax() {
            return "show perf";
        }

        @Override
        protected String getCommandDescription() {
            return
                "Displays recent performance information for each " +
                "Replication Node.";
        }
    }

    private static final class ShowSnapshots extends SubCommand {

        private ShowSnapshots() {
            super("snapshots", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();
            StorageNodeId snid = null;
            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if ("-sn".equals(arg)) {
                    final String sn = Shell.nextArg(args, i++, this);
                    try {
                        snid = StorageNodeId.parse(sn);
                    } catch (IllegalArgumentException iae) {
                        shell.invalidArgument(sn, this);
                    }
                } else {
                    shell.unknownArgument(arg, this);
                }
            }
            try {
                final Snapshot snapshot =
                    new Snapshot(cs, cmd.getLoginManager(),
                                 shell.getVerbose(), shell.getOutput());
                String [] list = null;
                if (snid != null) {
                    list = snapshot.listSnapshots(snid);
                } else {
                    list = snapshot.listSnapshots();
                }
                String ret = "";
                for (String ss : list) {
                    ret += ss + eol;
                }
                return ret;
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return cantGetHere;
        }

        @Override
        protected String getCommandSyntax() {
            return "show snapshots [-sn <id>]";
        }

        @Override
        protected String getCommandDescription() {
            return
                "Lists snapshots on the specified Storage Node. If no " +
                "Storage Node" + eolt + "is specified one is chosen from " +
                "the store.";
        }
    }

    private static final class ShowUpgradeOrder extends SubCommand {

        private ShowUpgradeOrder() {
            super("upgrade-order", 3);
        }

        @Override
        protected String getCommandSyntax() {
            return "show upgrade-order";
        }

        @Override
        protected String getCommandDescription() {
            return
                "Lists the Storage Nodes which need to be upgraded in an " +
                "order that" + eolt + "prevents disruption to the store's " +
                "operation.";
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {
            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();

            try {
                /*
                 * Thus coommand gets the order for upgrading to the version
                 * of the CLI.
                 */
                return cs.getUpgradeOrder(KVVersion.CURRENT_VERSION,
                                          KVVersion.PREREQUISITE_VERSION);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return cantGetHere;
        }
    }

    /*
     * ShowDatacenters
     */
    static final class ShowDatacenters extends ShowZones {

        static final String dcCommandDeprecation =
            "The command:" + eol + eolt +
            "show datacenters" + eol + eol +
            "is deprecated and has been replaced by:" + eol + eolt +
            "show zones" + eol + eol;

        ShowDatacenters() {
            super("datacenters", 4);
        }

        /** Add deprecation message. */
        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            return dcCommandDeprecation + super.execute(args, shell);
        }

        /** Add deprecation message. */
        @Override
        public String getCommandDescription() {
            return super.getCommandDescription() + eol + eolt +
                "This command is deprecated and has been replaced by:"
                + eol + eolt +
                "show zones";
        }
    }

    static class ShowZones extends SubCommand {
        static final String ID_FLAG = "-zn";
        static final String NAME_FLAG = "-znname";
        static final String DESC_STR = "zone";
        static final String dcFlagsDeprecation =
            "The -dc and -dcname flags, and the dc<ID> ID format, are" +
            " deprecated" + eol +
            "and have been replaced by -zn, -znname, and zn<ID>." +
            eol + eol;

        private ShowZones() {
            super("zones", 4);
        }

        ShowZones(final String name, final int prefixLength) {
            super(name, prefixLength);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {
            DatacenterId id = null;
            String nameFlag = null;
            boolean deprecatedDcFlag = false;

            Shell.checkHelp(args, this);
            if (args.length > 1) {
                for (int i = 1; i < args.length; i++) {
                    if (CommandUtils.isDatacenterIdFlag(args[i])) {
                        id = DatacenterId.parse(
                            Shell.nextArg(args, i++, this));
                        if (CommandUtils.isDeprecatedDatacenterId(
                                args[i-1], args[i])) {
                            deprecatedDcFlag = true;
                        }
                    } else if (CommandUtils.isDatacenterNameFlag(args[i])) {
                        nameFlag = Shell.nextArg(args, i++, this);
                        if (CommandUtils.isDeprecatedDatacenterName(
                                args[i-1])) {
                            deprecatedDcFlag = true;
                        }
                    } else {
                        shell.unknownArgument(args[i], this);
                    }
                }
            }

            final String deprecatedDcFlagPrefix =
                !deprecatedDcFlag ? "" : dcFlagsDeprecation;

            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();
            try {
                final ByteArrayOutputStream outStream =
                    new ByteArrayOutputStream();
                final PrintStream out = new PrintStream(outStream);
                final Topology topo = cs.getTopology();
                final Parameters params = cs.getParameters();
                TopologyPrinter.printZoneInfo(id, nameFlag, topo, out, params);
                return deprecatedDcFlagPrefix + outStream;
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return cantGetHere;
        }

        @Override
        protected String getCommandSyntax() {
            return "show " + name +
                   " [" + ID_FLAG + " <id> | " + NAME_FLAG + " <name>]";
        }

        @Override
        protected String getCommandDescription() {
            return
                "Lists the names of all " + DESC_STR + "s, or display " +
                "information about a" + eolt +
                "specific " + DESC_STR + ". If no " + DESC_STR + " is " +
                "specified, list the names" + eolt +
                "of all " + DESC_STR + "s. If a specific " + DESC_STR +
                " is specified using" + eolt +
                "either the " + DESC_STR + "'s id (via the '" + ID_FLAG +
                "' flag), or the " + DESC_STR + "'s" + eolt +
                "name (via the '" + NAME_FLAG + "' flag), then list " +
                "information such as the" + eolt + "names of the storage " +
                "nodes deployed to that " + DESC_STR + ".";
        }
    }

    /*
     * ShowTables
     */
    private static final class ShowTables extends SubCommand {
        final static String CMD_TEXT = "tables";
        final static String TABLE_FLAG = "-name";
        final static String PARENT_FLAG = "-parent";
        final static String LEVEL_FLAG = "-level";

        private ShowTables() {
            super(CMD_TEXT, 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            String tableName = null;
            String parentName = null;
            Integer maxLevel = null;
            Shell.checkHelp(args, this);
            if (args.length > 1) {
                for (int i = 1; i < args.length; i++) {
                    if (TABLE_FLAG.equals(args[i])) {
                        tableName = Shell.nextArg(args, i++, this);
                    } else if (PARENT_FLAG.equals(args[i])) {
                        parentName = Shell.nextArg(args, i++, this);
                    } else if (LEVEL_FLAG.equals(args[i])) {
                        String sLevel = Shell.nextArg(args, i++, this);
                        try {
                            maxLevel = Integer.valueOf(sLevel);
                        } catch (NumberFormatException nfe) {
                            shell.invalidArgument(sLevel, this);
                        }
                    } else {
                        shell.unknownArgument(args[i], this);
                    }
                }
            }

            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();
            TableMetadata meta = null;
            try {
                meta = cs.getMetadata(TableMetadata.class,
                                      MetadataType.TABLE);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            if (meta == null) {
                return "No table found.";
            }

            /* Show the specified table's meta data. */
            if (tableName != null) {
                TableImpl table = meta.getTable(tableName);
                if (table == null) {
                    return "Table " + tableName + " does not exist.";
                }
                return table.toJsonString(true);
            }

            /* Show multiple tables's meta data. */
            Map<String, Table> tableMap = null;
            boolean verbose = shell.getVerbose();
            if (parentName != null) {
                TableImpl tbParent = meta.getTable(parentName);
                if (tbParent == null) {
                    return "Table " + parentName + " does not exist.";
                }
                tableMap = tbParent.getChildTables();
            } else {
                tableMap = meta.getTables();
            }
            if (tableMap == null || tableMap.size() == 0) {
                return "No table found.";
            }
            return getAllTablesInfo(tableMap, maxLevel, verbose);
        }

        private String getAllTablesInfo(Map<String, Table> tableMap,
                                        Integer maxLevel, boolean verbose) {
            if (!verbose) {
                return "Tables: " + eolt +
                    getTableAndChildrenName(tableMap, 0, maxLevel);
            }
            return getTableAndChildrenMetaInfo(tableMap, 0, maxLevel);
        }

        @SuppressWarnings("null")
        private String getTableAndChildrenName(Map<String, Table> tableMap,
                                               int curLevel, Integer maxLevel) {
            final String INDENT = "  ";
            String indent = "";
            StringBuilder sb = new StringBuilder();
            if (curLevel > 0) {
                for (int i = 0; i < curLevel; i++) {
                    indent += INDENT;
                }
            }
            for (Map.Entry<String, Table> entry: tableMap.entrySet()) {
                TableImpl table = (TableImpl)entry.getValue();
                sb.append(indent);
                sb.append(table.getFullName());
                String desc = table.getDescription();
                if (desc != null && desc.length() > 0) {
                    sb.append(" -- ");
                    sb.append(desc);
                }
                sb.append(Shell.eolt);
                if (maxLevel != null && curLevel == maxLevel) {
                    continue;
                }
                Map<String, Table> childTabs = table.getChildTables();
                if (childTabs != null) {
                    sb.append(getTableAndChildrenName(
                                  childTabs, curLevel + 1, maxLevel));
                }
            }
            return sb.toString();
        }

        @SuppressWarnings("null")
        private String getTableAndChildrenMetaInfo(Map<String, Table> tableMap,
                                               int curLevel, Integer maxLevel) {
            StringBuffer sb = new StringBuffer();
            for (Map.Entry<String, Table> entry: tableMap.entrySet()) {
                TableImpl table = (TableImpl)entry.getValue();
                sb.append(table.getFullName());
                sb.append(":");
                sb.append(Shell.eol);
                sb.append(table.toJsonString(true));
                sb.append(Shell.eol);
                if (maxLevel != null && curLevel == maxLevel) {
                    continue;
                }
                if (table.getChildTables() != null) {
                    sb.append(
                        getTableAndChildrenMetaInfo(
                            table.getChildTables(), curLevel++, maxLevel));
                }
            }
            return sb.toString();
        }

        @Override
        protected String getCommandSyntax() {
            return "show " + CMD_TEXT + " [" + TABLE_FLAG + " <name>] " +
                "[" + PARENT_FLAG + " <name>] [" + LEVEL_FLAG + " <level>]";
        }

        @Override
        protected String getCommandDescription() {
            return "Display table metadata.  By default the names of all " +
                "top-tables and" + eolt + "their child tables are listed.  " +
                "Top-level tables are those without" + eolt + "parents.  " +
                "The level of child tables can be limited by specifying the" +
                eolt + LEVEL_FLAG + " flag.  If a specific table is named " +
                "its detailed metadata is" + eolt + "displayed.  The table " +
                "name is a dot-separated name with the format" + eolt +
                "tableName[.childTableName]*.  Flag " + PARENT_FLAG +
                " is used to show all child" + eolt + "tables for the " +
                "given parent table.";
        }
    }

    /*
     * ShowIndexes
     */
    private static final class ShowIndexes extends SubCommand {
        final static String CMD_TEXT = "indexes";
        final static String INDEX_FLAG = "-name";
        final static String TABLE_FLAG = "-table";

        private ShowIndexes() {
            super(CMD_TEXT, 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            String tableName = null;
            String indexName = null;
            Shell.checkHelp(args, this);
            if (args.length > 1) {
                for (int i = 1; i < args.length; i++) {
                    if (TABLE_FLAG.equals(args[i])) {
                        tableName = Shell.nextArg(args, i++, this);
                    } else if (INDEX_FLAG.equals(args[i])) {
                        indexName = Shell.nextArg(args, i++, this);
                    } else if (TABLE_FLAG.equals(args[i])) {
                        tableName = Shell.nextArg(args, i++, this);
                    } else {
                        shell.unknownArgument(args[i], this);
                    }
                }
            }

            if (indexName != null && tableName == null) {
                shell.requiredArg(TABLE_FLAG, this);
            }

            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();
            TableMetadata meta = null;
            try {
                meta = cs.getMetadata(TableMetadata.class,
                                      MetadataType.TABLE);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            if (meta == null) {
                return "No table found.";
            }

            if (tableName != null) {
                TableImpl table = meta.getTable(tableName);
                if (table == null) {
                    return "Table " + tableName + " does not exist.";
                }
                if (indexName != null) {
                    Index index = table.getIndex(indexName);
                    if (index == null) {
                        return "Index " + indexName + " on table " +
                            tableName + " does not exist.";
                    }
                    return getIndexInfo(index);
                }
                String ret = getTableIndexesInfo(table);
                if (ret == null) {
                    return "No Index found.";
                }
                return ret;
            }

            Map<String, Table> tableMap = null;
            tableMap = meta.getTables();
            if (tableMap == null || tableMap.isEmpty()) {
                return "No table found.";
            }
            return getAllTablesIndexesInfo(tableMap);
        }

        private String getAllTablesIndexesInfo(Map<String, Table> tableMap) {
            StringBuilder sb = new StringBuilder();
            for (Entry<String, Table> entry: tableMap.entrySet()) {
                Table table = entry.getValue();
                String ret = getTableIndexesInfo(table);
                if (ret == null) {
                    continue;
                }
                if (sb.length() > 0) {
                    sb.append(eol);
                }
                sb.append(ret);
                sb.append(eol);
                if (table.getChildTables() != null) {
                    ret = getAllTablesIndexesInfo(table.getChildTables());
                    if (ret.length() > 0) {
                        sb.append(eol);
                        sb.append(ret);
                    }
                }
            }
            return sb.toString();
        }

        private String getTableIndexesInfo(Table table) {
            Map<String, Index> map = table.getIndexes();
            if (map == null || map.isEmpty()) {
                return null;
            }
            StringBuilder sb = new StringBuilder();
            sb.append("Indexes on table ");
            sb.append(table.getFullName());
            for (Entry<String, Index> entry: map.entrySet()) {
                sb.append(eol);
                sb.append(getIndexInfo(entry.getValue()));
            }
            return sb.toString();
        }

        private String getIndexInfo(Index index) {
            StringBuilder sb = new StringBuilder();
            sb.append(Shell.tab);
            sb.append(index.getName());
            sb.append(" (");
            boolean first = true;
            for (String s : index.getFields()) {
                if (first) {
                    sb.append(", ");
                    first = false;
                }
                sb.append(s);
            }
            sb.append(")");
            if (index.getDescription() != null) {
                sb.append(" -- ");
                sb.append(index.getDescription());
            }
            return sb.toString();
        }

        @Override
        protected String getCommandSyntax() {
            return "show " + CMD_TEXT + " [" + TABLE_FLAG + " <name>] " +
                    "[" + INDEX_FLAG + " <name>]";
        }

        @Override
        protected String getCommandDescription() {
            return "Display index metadata. By default the indexes metadata " +
                "of all tables" + eolt +
                "are listed.  If a specific table is named its indexes " +
                "metadata are" + eolt +
                "displayed, if a specific index of the table is named its " +
                "metadata is " + eolt +
                "displayed.";
        }
    }

    /*
     * ShowUsers
     *
     * Print the user information stored in the security metadata copy. If
     * no user name is specified, a brief information of all users will be
     * printed. Otherwise, only the specified user will be printed.
     *
     * While showing the information of all users, the format will be:
     * <br>"user: id=xxx name=xxx"<br>
     * For showing the details of a specified user, it will be:
     * <br>"user: id=xxx name=xxx state=xxx type=xxx retained-passwd=xxxx"
     */
    private static final class ShowUsers extends SubCommand {

        static final String NAME_FLAG = "-name";
        static final String DESC_STR = "user";

        private ShowUsers() {
            super("users", 4);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {
            String nameFlag = null;

            Shell.checkHelp(args, this);
            if (args.length > 1) {
                for (int i = 1; i < args.length; i++) {
                    if ("-name".equals(args[i])) {
                        nameFlag = Shell.nextArg(args, i++, this);
                        if (nameFlag == null || nameFlag.isEmpty()) {
                            throw new ShellUsageException(
                                "User name could not be empty.", this);
                        }
                    } else {
                        shell.unknownArgument(args[i], this);
                    }
                }
            }

            final CommandShell cmd = (CommandShell) shell;
            final CommandServiceAPI cs = cmd.getAdmin();
            try {
                final ByteArrayOutputStream outStream =
                    new ByteArrayOutputStream();
                final PrintStream out = new PrintStream(outStream);
                final Map<String, UserDescription> userDescMap =
                        cs.getUsersDescription();

                if (userDescMap == null || userDescMap.isEmpty()) {
                    return "No users.";
                }
                if (nameFlag != null) { /* Print details for a user */
                    final UserDescription desc = userDescMap.get(nameFlag);
                    return desc == null ?
                           "User with name of " + nameFlag + " not found." :
                           "user: " + desc.details();
                } 

                /* Print summary for all users */
                final Collection<UserDescription> usersDesc =
                       userDescMap.values();
                for (final UserDescription desc : usersDesc) {
                    out.println("user: " + desc.brief());
                }
                return outStream.toString();

            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return cantGetHere;
        }

        @Override
        protected String getCommandSyntax() {
            return "show " + "users" + " [-name <name>]";
        }

        @Override
        protected String getCommandDescription() {
            return
                "Lists the names of all " + DESC_STR + "s, or displays " +
                "information about a" + eolt +
                "specific " + DESC_STR + ". If no " + DESC_STR + " is " +
                "specified, lists the names" + eolt +
                "of all " + DESC_STR + "s. If a " + DESC_STR +
                " is specified using the " + NAME_FLAG + " flag," + eolt +
                "then lists detailed information about the " + DESC_STR + ".";
        }
    }

    /**
     * When specifying event timestamps, these formats are accepted.
     */
    private static String[] dateFormats = {
        "yyyy-MM-dd HH:mm:ss.SSS",
        "yyyy-MM-dd HH:mm:ss",
        "yyyy-MM-dd HH:mm",
        "yyyy-MM-dd",
        "MM-dd-yyyy HH:mm:ss.SSS",
        "MM-dd-yyyy HH:mm:ss",
        "MM-dd-yyyy HH:mm",
        "MM-dd-yyyy",
        "HH:mm:ss.SSS",
        "HH:mm:ss",
        "HH:mm"
    };

    private static String getDateFormatsUsage() {
        String usage =
            "<date> can be given in the following formats," + eolt +
            "which are interpreted in the UTC time zone." + eolt;

        for (String fs : dateFormats) {
            usage += eolt + "    " + fs;
        }

        return usage;
    }

    /**
     * Apply the above formats in sequence until one of them matches.
     */
    private static Date parseTimestamp(String s, ShellCommand command)
        throws ShellUsageException {

        TimeZone tz = TimeZone.getTimeZone("UTC");

        Date r = null;
        for (String fs : dateFormats) {
            final DateFormat f = new SimpleDateFormat(fs);
            f.setTimeZone(tz);
            f.setLenient(false);
            try {
                r = f.parse(s);
                break;
            } catch (ParseException pe) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
        }

        if (r == null) {
            throw new ShellUsageException
                ("Invalid date format : " + s, command);
        }

        /*
         * If the date parsed is in the distant past (i.e., in January 1970)
         * then the string lacked a year/month/day.  We'll be friendly and
         * interpret the time as being in the recent past, that is, today.
         */

        final Calendar rcal = Calendar.getInstance(tz);
        rcal.setTime(r);

        if (rcal.get(Calendar.YEAR) == 1970) {
            final Calendar nowCal = Calendar.getInstance();
            nowCal.setTime(new Date());

            rcal.set(nowCal.get(Calendar.YEAR),
                     nowCal.get(Calendar.MONTH),
                     nowCal.get(Calendar.DAY_OF_MONTH));

            /* If the resulting time is in the future, subtract one day. */

            if (rcal.after(nowCal)) {
                rcal.add(Calendar.DAY_OF_MONTH, -1);
            }
            r = rcal.getTime();
        }
        return r;
    }
}
