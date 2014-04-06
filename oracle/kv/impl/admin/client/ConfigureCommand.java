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
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellArgumentException;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellException;

class ConfigureCommand extends ShellCommand {
    private static final int maxStoreNameLen = 255;

    ConfigureCommand() {
        super("configure", 4);
    }

    @Override
    public String execute(String[] args, Shell shell)
        throws ShellException {

        if (args.length < 3) {
            shell.badArgCount(this);
        }
        if (!"-name".equals(args[1])) {
            shell.requiredArg("-name", this);
        }
        String storeName = args[2];
        CommandShell cmd = (CommandShell) shell;
        CommandServiceAPI cs = cmd.getAdmin();
        try {
            validateStoreName(storeName);
            cs.configure(storeName);
        } catch (RemoteException re) {
            cmd.noAdmin(re);
        }
        return "Store configured: " + storeName;
    }

    @Override
    protected String getCommandSyntax() {
        return "configure -name <storename>";
    }

    @Override
    protected String getCommandDescription() {
        return
            "Configures a new store.  This call must be made before " +
            "any other" + eolt + "administration can be performed.";
    }

    private void validateStoreName(String store)
        throws ShellException {

        if (store.length() > maxStoreNameLen) {
            throw new ShellArgumentException
                ("Invalid store name.  It exceeds the maximum length of " +
                 maxStoreNameLen);
        }
        for (char c : store.toCharArray()) {
            if (!isValid(c)) {
                throw new ShellArgumentException
                        ("Invalid store name: " + store +
                         ".  It must consist of " +
                         "letters, digits, hyphen, underscore, period.");
            }
        }
    }

    private boolean isValid(char c) {
        if (Character.isLetterOrDigit(c) ||
            c == '-' ||
            c == '_' ||
            c == '.') {
            return true;
        }
        return false;
    }
}
