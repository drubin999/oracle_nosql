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

import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellException;

class PolicyCommand extends ShellCommand {

    PolicyCommand() {
        super("change-policy", 4);
    }

    @Override
    public String execute(String[] args, Shell shell)
        throws ShellException {

        if (args.length < 3) {
            shell.badArgCount(this);
        }
        CommandShell cmd = (CommandShell) shell;
        CommandServiceAPI cs = cmd.getAdmin();

        boolean showHidden = cmd.showHidden();
        boolean dryRun = false;
        int i;
        boolean foundParams = false;
        for (i = 1; i < args.length; i++) {
            String arg = args[i];
            if ("-hidden".equals(arg)) {
                showHidden = true;
            } else if ("-dry-run".equals(arg)) {
                dryRun = true;
            } else if ("-params".equals(arg)) {
                ++i;
                foundParams = true;
                break;
            } else {
                shell.unknownArgument(arg, this);
            }
        }

        if (!foundParams) {
            shell.requiredArg(null, this);
        }
        if (args.length <= i) {
            return "No parameters were specified";
        }
        try {
            ParameterMap map = cs.getPolicyParameters();
            CommandUtils.parseParams(map, args, i, ParameterState.Info.POLICY,
                                     null, showHidden, this);
            if (dryRun) {
                return CommandUtils.formatParams(map, showHidden, null);
            }
            if (shell.getVerbose()) {
                shell.verboseOutput
                    ("New policy parameters:" + eol +
                     CommandUtils.formatParams(map, showHidden, null));
            }
            cs.setPolicies(map);
        } catch (RemoteException re) {
            cmd.noAdmin(re);
        }
        return "";
    }

    @Override
    protected String getCommandSyntax() {
        return name + " [-dry-run] -params [name=value]*";
    }

    @Override
    protected String getCommandDescription() {
        return
            "Modifies store-wide policy parameters that apply to not yet " +
            "deployed" + eolt + "services. The parameters to change " +
            "follow the -params flag and are" + eolt + "separated by " +
            "spaces. Parameter values with embedded spaces must be" +
            eolt + "quoted.  For example name=\"value with spaces\". " +
            "If -dry-run is" + eolt + "specified the new parameters " +
            "are returned without changing them.";
    }
}
