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

import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.topo.Topology;
import oracle.kv.util.Ping;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellException;

/*
 * Add flags, trim down output
 */
class PingCommand extends ShellCommand {

    PingCommand() {
        super("ping", 3);
    }

    @Override
    public String execute(String[] args, Shell shell)
        throws ShellException {

        if (args.length != 1) {
            shell.badArgCount(this);
        }
        CommandShell cmd = (CommandShell) shell;
        CommandServiceAPI cs = cmd.getAdmin();
        try {
            Topology topo = cs.getTopology();
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(os);
            Ping.pingTopology(topo, ps);
            return os.toString();
        } catch (RemoteException re) {
            cmd.noAdmin(re);
        }
        return "";
    }

    @Override
    public String getCommandDescription() {
        return
            "\"Ping\"s the runtime components of a store.  Only those" +
            eolt + "components available from the Topology are " +
            "contacted." + eolt + "This leaves out Admin services.";
    }
}
