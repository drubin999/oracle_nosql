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
import java.util.List;

import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.Snapshot;
import oracle.kv.util.shell.CommandWithSubs;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellException;

/*
 * Subcommands of snapshot
 *   create
 *   remove
 */
class SnapshotCommand extends CommandWithSubs {
    private static final
        List<? extends SubCommand> subs =
                       Arrays.asList(new CreateSnapshotSub(),
                                     new RemoveSnapshotSub());

    SnapshotCommand() {
        super(subs, "snapshot", 3, 2);
    }

    @Override
    protected String getCommandOverview() {
        return "The snapshot command encapsulates commands that create and " +
            "delete snapshots," + eol + "which are used for backup and " +
            "restore.";
    }

    private static class CreateSnapshotSub extends SnapshotSub {

        private CreateSnapshotSub() {
            super("create", 3, true);
        }

        @Override
        protected String getCommandSyntax() {
            return "snapshot create -name <name>";
        }

        @Override
        protected String getCommandDescription() {
            return
                "Creates a new snapshot using the specified name as " +
                "the prefix.";
        }
    }

    private static class RemoveSnapshotSub extends SnapshotSub {

        private RemoveSnapshotSub() {
            super("remove", 3, false);
        }

        @Override
        protected String getCommandSyntax() {
            return "snapshot remove -name <name> | -all";
        }

        @Override
        protected String getCommandDescription() {
            return
                "Removes the named snapshot.  If -all is specified " +
                "remove all snapshots.";
        }
    }

    private abstract static class SnapshotSub extends SubCommand {
        final boolean isCreate;

        protected SnapshotSub(String name, int prefixMatchLength,
                              boolean isCreate) {
            super(name, prefixMatchLength);
            this.isCreate = isCreate;
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            CommandShell cmd = (CommandShell)shell;
            CommandServiceAPI cs = cmd.getAdmin();
            String snapName = null;
            boolean removeAll = false;

            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if ("-name".equals(arg)) {
                    snapName = Shell.nextArg(args, i++, this);
                } else if ("-all".equals(arg)) {
                    removeAll = true;
                } else {
                    shell.unknownArgument(arg, this);
                }
            }
            if (snapName == null && !removeAll) {
                shell.requiredArg("-name", this);
            }

            try {
                Snapshot snapshot =
                    new Snapshot(cs, cmd.getLoginManager(), shell.getVerbose(),
                                 shell.getOutput());
                String output = "";
                if (isCreate) {
                    if (removeAll) {
                        shell.invalidArgument("-all", this);
                    }
                    String newSnapName = snapshot.createSnapshot(snapName);
                    if (snapshot.succeeded()) {
                        int numSuccess = snapshot.getSuccesses().size();
                        output = "Created snapshot named " + newSnapName +
                            " on " + "all " + numSuccess + " nodes";
                    } else if (snapshot.getQuorumSucceeded()) {
                        output =
                            "Create snapshot succeeded but not on all nodes" +
                            eol;
                    }
                } else {
                    if (removeAll) {
                        if (snapName != null) {
                            shell.invalidArgument("-all", this);
                        }
                        snapshot.removeAllSnapshots();
                        if (snapshot.succeeded()) {
                            output = "Removed all snapshots";
                        }
                    } else {
                        snapshot.removeSnapshot(snapName);
                        if (snapshot.succeeded()) {
                            output = "Removed snapshot " + snapName;
                        }
                    }
                }
                return output;
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            } catch (Exception e) {
                shell.handleUnknownException("Snapshot " + snapName +
                                             " failed", e);
            }
            return "";
        }
    }
}
