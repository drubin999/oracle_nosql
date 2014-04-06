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
import java.util.List;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.VerifyConfiguration;
import oracle.kv.impl.admin.VerifyResults;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.util.shell.CommandWithSubs;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellException;

class VerifyCommand extends CommandWithSubs {

    private static final
        List<? extends SubCommand> subs =
                       Arrays.asList(new VerifyConfig(),
                                     new VerifyUpgrade(),
                                     new VerifyPrerequisite()
                                     );

    VerifyCommand() {
        super(subs,
              "verify",
              4,  /* prefix length */
              0); /* min args -- let subs control it */
    }

    @Override
    protected String getCommandOverview() {
        return "The verify command encapsulates commands that check various " +
               "parameters of the store.";
    }

    /*
     * Override execute() to implement the legacy "verify [-silent]" command
     * (no subcommands). Remove this support in the next major release.
     */
    @Override
    public String execute(String[] args, Shell shell)
        throws ShellException {

        if ((args.length == 1) ||
            ((args.length == 2) && args[1].equals("-silent"))) {

            final String[] newArgs =
                    (args.length == 1) ?
                            new String[] {"verify", "configuration"} :
                            new String[] {"verify", "configuration", "-silent"};

            /*
             * Prefix a deprecation message to the output of the new command.
             */
            return "The command:" + eol + eolt +
                   "verify [-silent]" + eol + eol +
                   "is deprecated and has been replaced by: " +  eol + eolt +
                   "verify configuration [-silent]" + eol + eol +
                   super.execute(newArgs, shell);

        }
        return super.execute(args, shell);
    }

    private static class VerifyConfig extends SubCommand {

        private VerifyConfig() {
            super("configuration", 3);
        }

        @Override
        protected String getCommandSyntax() {
            return "verify configuration [-silent]";
        }

        @Override
        protected String getCommandDescription() {
            return "Verifies the store configuration by iterating over the " +
                   "components and checking" + eolt + "their state " +
                   "against that expected in the Admin database.  This call " +
                   "may" + eolt + "take a while on a large store.";
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            if (args.length > 2) {
                shell.badArgCount(this);
            }
            CommandShell cmd = (CommandShell) shell;
            CommandServiceAPI cs = cmd.getAdmin();
            try {
                boolean showProgress = true;
                if (args.length > 1) {
                    if (args[1].equals("-silent")) {
                        showProgress = false;
                    } else {
                        shell.unknownArgument(args[1],this);
                    }
                }
                Topology topo = cs.getTopology();
                String res = VerifyConfiguration.displayTopology(topo);
                res += eol + "See " + cs.getStorewideLogName() +
                       " for progress messages" + eol;

                VerifyResults results = cs.verifyConfiguration(showProgress,
                                                               true);
                String progressReport = results.getProgressReport();
                if (progressReport == null) {
                    return res + results.display();
                } 
                return res + results.getProgressReport() + eol + 
                       results.display();
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }
    }

    private static class VerifyUpgrade extends SubCommand {

        private VerifyUpgrade() {
            super("upgrade", 3);
        }

        @Override
        protected String getCommandSyntax() {
            return "verify upgrade [-silent] [-sn snX]*";
        }

        @Override
        protected String getCommandDescription() {
            return "Verifies the storage nodes (and their managed components) "+
                   "are at or above the current version." + eolt +
                   "This call may take a while on a large store.";
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {
            CommandShell cmd = (CommandShell)shell;
            CommandServiceAPI cs = cmd.getAdmin();
            try {
                boolean showProgress = true;
                List<StorageNodeId> snIds = null;

                for (int i = 1; i < args.length; i++) {
                    final String arg = args[i];

                    if (arg.equals("-silent")) {
                        showProgress = false;

                    } else if ("-sn".equals(arg)) {
                        if (snIds == null) {
                            snIds = new ArrayList<StorageNodeId>(1);
                        }
                        String sn = Shell.nextArg(args, i++, this);
                        try {
                            snIds.add(StorageNodeId.parse(sn));
                        } catch (IllegalArgumentException iae) {
                            shell.invalidArgument(sn, this);
                        }

                    } else {
                        shell.unknownArgument(arg, this);
                    }
                }

                final Topology topo = cs.getTopology();
                String res = VerifyConfiguration.displayTopology(topo);
                res += eol + "See " + cs.getStorewideLogName() +
                       " for progress messages" + eol;

                final VerifyResults results =
                        cs.verifyUpgrade(KVVersion.CURRENT_VERSION,
                                         snIds,
                                         showProgress, true);

                return res + results.getProgressReport() + eol +
                       results.display();
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }
    }

    private static class VerifyPrerequisite extends SubCommand {

        private VerifyPrerequisite() {
            super("prerequisite", 3);
        }

        @Override
        protected String getCommandSyntax() {
            return "verify prerequisite [-silent] [-sn snX]*";
        }

        @Override
        protected String getCommandDescription() {
            return "Verifies the storage nodes are at or above the " +
                   "prerequisite software version needed to upgrade to " +
                   "the current version." + eolt +
                   "This call may take a while on a large store.";
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {
            CommandShell cmd = (CommandShell)shell;
            CommandServiceAPI cs = cmd.getAdmin();
            try {
                boolean showProgress = true;
                List<StorageNodeId> snIds = null;


                for (int i = 1; i < args.length; i++) {
                    final String arg = args[i];

                    if (arg.equals("-silent")) {
                        showProgress = false;

                    } else if ("-sn".equals(arg)) {
                        if (snIds == null) {
                            snIds = new ArrayList<StorageNodeId>(1);
                        }
                        String sn = Shell.nextArg(args, i++, this);
                        try {
                            snIds.add(StorageNodeId.parse(sn));
                        } catch (IllegalArgumentException iae) {
                            shell.invalidArgument(sn, this);
                        }

                    } else {
                        shell.unknownArgument(arg, this);
                    }
                }

                final Topology topo = cs.getTopology();
                String res = VerifyConfiguration.displayTopology(topo);
                res += eol + "See " + cs.getStorewideLogName() +
                       " for progress messages" + eol;

                final VerifyResults results =
                        cs.verifyPrerequisite(KVVersion.CURRENT_VERSION,
                                              KVVersion.PREREQUISITE_VERSION,
                                              snIds,
                                              showProgress, true);

                return res + results.getProgressReport() + results.display();
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }
    }
}
