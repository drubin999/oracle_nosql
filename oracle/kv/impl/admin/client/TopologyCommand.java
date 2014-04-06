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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.topo.TopologyCandidate;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.TopologyPrinter;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellException;

/*
 * Subcommands of topology
 *   clone
 *   change-repfactor
 *   create
 *   delete
 *   list
 *   move-repnode
 *   preview
 *   rebalance
 *   redistribute
 *   validate
 *   view
 */
class TopologyCommand extends SharedCommandWithSubs {

    private static final
        List<? extends SubCommand> subs =
                       Arrays.asList(new TopologyChangeRFSub(),
                                     new TopologyCloneSub(),
                                     new TopologyCreateSub(),
                                     new TopologyDeleteSub(),
                                     new TopologyListSub(),
                                     new TopologyMoveRNSub(),
                                     new TopologyPreviewSub(),
                                     new TopologyRebalanceSub(),
                                     new TopologyRedistributeSub(),
                                     new TopologyValidateSub(),
                                     new TopologyViewSub()
                                     );

    TopologyCommand() {
        super(subs,
              "topology",
              4,  /* prefix length */
              0); /* min args -- let subs control it */
    }

    @Override
    protected String getCommandOverview() {
        return "Encapsulates commands that manipulate store topologies." + eol +
            "Examples are " +
            "redistribution/rebalancing of nodes or changing replication" +
            eol + "factor.  Topologies are created and modified using this " +
            "command.  They" + eol + "are then deployed by using the " +
            "\"plan deploy-topology\" command.";
    }

    static class TopologyChangeRFSub extends SharedSubCommand {

        final static String dcFlagsDeprecation =
            "The -dc and -dcname flags, and the dc<ID> ID format, are" +
            " deprecated" + eol +
            "and have been replaced by -zn, -znname, and zn<ID>." +
            eol + eol;

        TopologyChangeRFSub() {
            super("change-repfactor", 5);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            CommandShell cmd = (CommandShell)shell;
            CommandServiceAPI cs = cmd.getAdmin();
            String topoName = null;
            String poolName = null;
            DatacenterId dcid = null;
            String dcName = null;
            int rf = 0;
            boolean deprecatedDcFlag = false;
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if ("-name".equals(arg)) {
                    topoName = Shell.nextArg(args, i++, this);
                } else if ("-pool".equals(arg)) {
                    poolName = Shell.nextArg(args, i++, this);
                } else if (CommandUtils.isDatacenterIdFlag(arg)) {
                    dcid = parseDatacenterId(Shell.nextArg(args, i++, this));
                    if (CommandUtils.isDeprecatedDatacenterId(arg, args[i])) {
                        deprecatedDcFlag = true;
                    }
                } else if (CommandUtils.isDatacenterNameFlag(arg)) {
                    dcName = Shell.nextArg(args, i++, this);
                    if (CommandUtils.isDeprecatedDatacenterName(arg)) {
                        deprecatedDcFlag = true;
                    }
                } else if ("-rf".equals(arg)) {
                    String rfString = Shell.nextArg(args, i++, this);
                    try {
                        rf = Integer.parseInt(rfString);
                    } catch (IllegalArgumentException iae) {
                        return "Invalid replication factor: " + rfString;
                    }
                    /* this is more for typos than actual validation */
                    if (rf <= 0 || rf > 30) {
                        return "Replication factor out of valid range: " + rf;
                    }
                } else {
                    shell.unknownArgument(arg, this);
                }
            }
            if (topoName == null || poolName == null || rf == 0 ||
                (dcid == null && dcName == null)) {
                shell.requiredArg(null, this);
            }
            final String deprecatedDcFlagPrefix =
                !deprecatedDcFlag ? "" : dcFlagsDeprecation;
            try {
                if (dcid == null) {
                    dcid = CommandUtils.getDatacenterId(dcName, cs, this);
                }
                CommandUtils.validatePool(poolName, cs, this);
                CommandUtils.ensureTopoExists(topoName, cs, this);
                CommandUtils.validateRepFactor(dcid, rf, cs, this);
                return deprecatedDcFlagPrefix +
                    cs.changeRepFactor(topoName, poolName, dcid, rf);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "topology change-repfactor -name <name> -pool " +
                "<pool name>" + eolt + "-zn <id> | -znname <name> -rf " +
                "<replication factor>";
        }

        @Override
        protected String getCommandDescription() {
            return
                "Modifies the topology to change the replication factor of " +
                "the specified" + eolt + "zone to a new value.  The " +
                "replication factor may not be" + eolt + "decreased at " +
                "this time.";
        }
    }

    static class TopologyCloneSub extends SubCommand {

        TopologyCloneSub() {
            super("clone", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            CommandShell cmd = (CommandShell)shell;
            CommandServiceAPI cs = cmd.getAdmin();
            String topoName = null;
            String fromName = null;
            boolean isCurrent = false;
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if ("-name".equals(arg)) {
                    topoName = Shell.nextArg(args, i++, this);
                } else if ("-from".equals(arg)) {
                    fromName = Shell.nextArg(args, i++, this);
                } else if ("-current".equals(arg)) {
                    isCurrent = true;
                } else {
                    shell.unknownArgument(arg, this);
                }
            }
            if (topoName == null || (fromName == null && !isCurrent)) {
                shell.requiredArg(null, this);
            }

            try {
                if (isCurrent) {
                    return cs.copyCurrentTopology(topoName);
                }
                CommandUtils.ensureTopoExists(fromName, cs, this);
                return cs.copyTopology(fromName, topoName);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "topology clone -from <from topology> -name " +
                "<to topology> or "+
                eolt + "topology clone -current -name <toTopology>";
        }

        @Override
        protected String getCommandDescription() {
            return
                "Clones an existing topology so as to create a new " +
                "candidate topology " + eolt +
                "to be used for topology change operations.";
        }
    }

    static class TopologyCreateSub extends SubCommand {

        TopologyCreateSub() {
            super("create", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            CommandShell cmd = (CommandShell)shell;
            CommandServiceAPI cs = cmd.getAdmin();
            String topoName = null;
            String poolName = null;
            int numPartitions = 0;
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if ("-name".equals(arg)) {
                    topoName = Shell.nextArg(args, i++, this);
                } else if ("-pool".equals(arg)) {
                    poolName = Shell.nextArg(args, i++, this);
                } else if ("-partitions".equals(arg)) {
                    String partString = Shell.nextArg(args, i++, this);
                    numPartitions = Integer.parseInt(partString);
                } else {
                    shell.unknownArgument(arg, this);
                }
            }
            if (topoName == null || poolName == null || numPartitions == 0) {
                shell.requiredArg(null, this);
            }

            try {
                CommandUtils.validatePool(poolName, cs, this);
                return cs.createTopology(topoName, poolName, numPartitions);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "topology create -name <candidate name> -pool " +
                "<pool name>" + eolt + "-partitions <num>";
        }

        @Override
        protected String getCommandDescription() {
            return
                "Creates a new topology with the specified number of " +
                "partitions" + eolt + "using the specified storage pool.";
        }
    }

    static class TopologyDeleteSub extends SubCommand {

        TopologyDeleteSub() {
            super("delete", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            CommandShell cmd = (CommandShell)shell;
            CommandServiceAPI cs = cmd.getAdmin();
            String topoName = null;
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if ("-name".equals(arg)) {
                    topoName = Shell.nextArg(args, i++, this);
                } else {
                    shell.unknownArgument(arg, this);
                }
            }
            if (topoName == null) {
                shell.requiredArg("-name", this);
            }

            try {
                CommandUtils.ensureTopoExists(topoName, cs, this);
                return cs.deleteTopology(topoName);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "topology delete -name <name>";
        }

        @Override
        protected String getCommandDescription() {
            return "Deletes a topology.";
        }
    }

    static class TopologyListSub extends SubCommand {

        TopologyListSub() {
            super("list", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            CommandShell cmd = (CommandShell)shell;
            CommandServiceAPI cs = cmd.getAdmin();
            if (args.length > 1) {
                shell.badArgCount(this);
            }
            try {
                List<String> topos = cs.listTopologies();
                Collections.sort(topos);
                StringBuilder sb = new StringBuilder();
                for (String oneTopo : topos) {
                    sb.append(oneTopo).append(eol);
                }
                return sb.toString();
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "topology list";
        }

        @Override
        protected String getCommandDescription() {
            return "Lists existing topologies.";
        }
    }

    static class TopologyMoveRNSub extends SubCommand {

        TopologyMoveRNSub() {
            super("move-repnode", 4);
        }

        @Override
        protected boolean isHidden() {
            return true;
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            CommandShell cmd = (CommandShell)shell;
            CommandServiceAPI cs = cmd.getAdmin();
            String topoName = null;
            RepNodeId rnid = null;
            StorageNodeId snid = null;
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if ("-name".equals(arg)) {
                    topoName = Shell.nextArg(args, i++, this);
                } else if ("-rn".equals(arg)) {
                    String rnString = Shell.nextArg(args, i++, this);
                    try {
                        rnid = RepNodeId.parse(rnString);
                    } catch (IllegalArgumentException iae) {
                        return "Invalid RepNode id: " + rnString;
                    }
                } else if ("-sn".equals(arg)) {
                    String snString = Shell.nextArg(args, i++, this);
                    try {
                        snid = StorageNodeId.parse(snString);
                    } catch (IllegalArgumentException iae) {
                        return "Invalid StorageNode id: " + snString;
                    }
                } else {
                    shell.unknownArgument(arg, this);
                }
            }
            if (topoName == null || rnid == null) {
                shell.requiredArg(null, this);
            }

            try {
                CommandUtils.ensureTopoExists(topoName, cs, this);
                CommandUtils.ensureRepNodeExists(rnid, cs, this);
                if (snid != null) {
                    CommandUtils.ensureStorageNodeExists(snid, cs, this);
                }
                return cs.moveRN(topoName, rnid, snid);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "topology move-repnode -name <name> -rn <id>";
        }

        @Override
        protected String getCommandDescription() {
            return
                "Modifies the topology to move the specified RepNode to " +
                "an available" + eolt + "storage node chosen by the system.";
        }
    }

    static class TopologyPreviewSub extends SubCommand {

        TopologyPreviewSub() {
            super("preview", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            CommandShell cmd = (CommandShell)shell;
            CommandServiceAPI cs = cmd.getAdmin();
            String topoName = null;
            String startName = null;
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if ("-name".equals(arg)) {
                    topoName = Shell.nextArg(args, i++, this);
                } else if ("-start".equals(arg)) {
                    startName = Shell.nextArg(args, i++, this);
                } else {
                    shell.unknownArgument(arg, this);
                }
            }
            if (topoName == null) {
                shell.requiredArg("-name", this);
            }

            try {
                CommandUtils.ensureTopoExists(topoName, cs, this);
                if (startName != null) {
                    CommandUtils.ensureTopoExists(startName, cs, this);
                }
                return cs.preview(topoName, startName, shell.getVerbose());
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "topology preview -name <name> [-start <from topology>]";
        }

        @Override
        protected String getCommandDescription() {
            return
                "Describes the actions that would be taken to transition " +
                "from the " + eolt + "starting topology to the named, target " +
                "topology. If -start is not " + eolt + "specified "  +
                "the current topology is used. This command should be used " +
                eolt +  "before deploying a new topology.";
        }
    }

    static class TopologyRebalanceSub extends SharedSubCommand {

        static final String dcFlagsDeprecation =
            "The -dc and -dcname flags, and the dc<ID> ID format, are" +
            " deprecated" + eol +
            "and have been replaced by -zn, -znname, and zn<ID>." +
            eol + eol;

        TopologyRebalanceSub() {
            super("rebalance", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            CommandShell cmd = (CommandShell)shell;
            CommandServiceAPI cs = cmd.getAdmin();
            String topoName = null;
            String poolName = null;
            DatacenterId dcid = null;
            String dcName = null;
            boolean deprecatedDcFlag = false;
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if ("-name".equals(arg)) {
                    topoName = Shell.nextArg(args, i++, this);
                } else if ("-pool".equals(arg)) {
                    poolName = Shell.nextArg(args, i++, this);
                } else if (CommandUtils.isDatacenterIdFlag(arg)) {
                    dcid = parseDatacenterId(Shell.nextArg(args, i++, this));
                    if (CommandUtils.isDeprecatedDatacenterId(arg, args[i])) {
                        deprecatedDcFlag = true;
                    }
                } else if (CommandUtils.isDatacenterNameFlag(arg)) {
                    dcName =  Shell.nextArg(args, i++, this);
                    if (CommandUtils.isDeprecatedDatacenterName(arg)) {
                        deprecatedDcFlag = true;
                    }
                } else {
                    shell.unknownArgument(arg, this);
                }
            }
            if (topoName == null || poolName == null) {
                shell.requiredArg(null, this);
            }
            final String deprecatedDcFlagPrefix =
                !deprecatedDcFlag ? "" : dcFlagsDeprecation;
            try {
                CommandUtils.validatePool(poolName, cs, this);
                CommandUtils.ensureTopoExists(topoName, cs, this);
                if (dcName != null) {
                    dcid = CommandUtils.getDatacenterId(dcName, cs, this);
                }
                if (dcid != null) {
                    CommandUtils.ensureDatacenterExists(dcid, cs, this);
                }
                return deprecatedDcFlagPrefix +
                    cs.rebalanceTopology(topoName, poolName, dcid);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "topology rebalance -name <name> -pool " +
                "<pool name> [-zn <id> | -znname <name>]";
        }

        @Override
        protected String getCommandDescription() {
            return
                "Modifies the named topology to create a \"balanced\" " +
                "topology. If the" + eolt + "optional -zn flag is used " +
                "only storage nodes from the specified" + eolt +
                "zone will be used for the operation.";
        }
    }

    static class TopologyRedistributeSub extends SubCommand {

        TopologyRedistributeSub() {
            super("redistribute", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            CommandShell cmd = (CommandShell)shell;
            CommandServiceAPI cs = cmd.getAdmin();
            String topoName = null;
            String poolName = null;
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if ("-name".equals(arg)) {
                    topoName = Shell.nextArg(args, i++, this);
                } else if ("-pool".equals(arg)) {
                    poolName = Shell.nextArg(args, i++, this);
                } else {
                    shell.unknownArgument(arg, this);
                }
            }
            if (topoName == null || poolName == null) {
                shell.requiredArg(null, this);
            }

            try {
                CommandUtils.validatePool(poolName, cs, this);
                CommandUtils.ensureTopoExists(topoName, cs, this);
                return cs.redistributeTopology(topoName, poolName);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "topology redistribute -name <name> -pool " +
                "<pool name>";
        }

        @Override
        protected String getCommandDescription() {
            return
                "Modifies the named topology to redistribute resources " +
                "to more efficiently" + eolt + "use those available.";
        }
    }

    static class TopologyValidateSub extends SubCommand {

        TopologyValidateSub() {
            super("validate", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            CommandShell cmd = (CommandShell)shell;
            CommandServiceAPI cs = cmd.getAdmin();
            String topoName = null;
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if ("-name".equals(arg)) {
                    topoName = Shell.nextArg(args, i++, this);
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            try {
                if (topoName != null) {
                    CommandUtils.ensureTopoExists(topoName, cs, this);
                }
                return cs.validateTopology(topoName);
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "topology validate [-name <name>]";
        }

        @Override
        protected String getCommandDescription() {
            return
                "Validates the specified topology. If no name is given, " +
                "the current " + eolt +
                "topology is validated. Validation will generate " +
                "\"violations\" and " + eolt + "\"notes\". Violations are " +
                "issues that can cause problems and should be " + eolt +
                "investigated. Notes are informational and highlight " +
                "configuration " + eolt +
                "oddities that could be potential issues or could be expected.";

        }
    }

    static class TopologyViewSub extends SubCommand {

        TopologyViewSub() {
            super("view", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            CommandShell cmd = (CommandShell)shell;
            CommandServiceAPI cs = cmd.getAdmin();
            String topoName = null;
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if ("-name".equals(arg)) {
                    topoName = Shell.nextArg(args, i++, this);
                } else {
                    shell.unknownArgument(arg, this);
                }
            }
            if (topoName == null) {
                shell.requiredArg("-name", this);
            }

            try {
                CommandUtils.ensureTopoExists(topoName, cs, this);
                TopologyCandidate tc = cs.getTopologyCandidate (topoName);
                Parameters params = cs.getParameters();
                return TopologyPrinter.printTopology(tc, params,
                                                     shell.getVerbose());
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "topology view -name <name>";
        }

        @Override
        protected String getCommandDescription() {
            return
                "Displays details of the specified topology.";
        }
    }
}
