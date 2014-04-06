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

package oracle.kv.util.shell;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/*
 * A class that implements boilerplate for command classes that have
 * sub-commands, such as "show" and "plan."
 *
 * Such classes extend this and implement their own sub-commands as instances
 * of SubCommand.
 */

public abstract class CommandWithSubs extends ShellCommand {
    private final List<? extends SubCommand> subCommands;
    private final int minArgCount;

    protected CommandWithSubs(List<? extends SubCommand> subCommands,
                              String name,
                              int prefixLength,
                              int minArgCount) {
        super(name, prefixLength);
        this.subCommands = subCommands;
        this.minArgCount = minArgCount;
        Collections.sort(this.subCommands,
                         new Shell.CommandComparator());
    }

    /**
     * Gets the command overview. Subclasses provide a string which is
     * prefixed to the help string for the top level command.
     *
     * @return the command overview
     */
    protected abstract String getCommandOverview();

    @Override
    public String execute(String[] args, Shell shell)
        throws ShellException {

        if ((minArgCount > 0 && args.length < minArgCount) ||
            args.length == 1) {
            shell.badArgCount(this);
        }
        String commandName = args[1];
        SubCommand command = findCommand(commandName);

        if ((command == null) || (command.isHidden() && !shell.showHidden())) {
            Shell.checkHelp(args, this);
            throw new ShellArgumentException(
                "Could not find " + name + " subcommand: " + commandName +
                eol + getVerboseHelp());
        }
        return command.execute(Arrays.copyOfRange
                               (args, 1, args.length), shell);
    }

    /**
     * Returns general help string. If called without a sub-command, a
     * multi-line syntax string with appropriate spacing is returned with each
     * sub-command appearing on a separate line. Hidden sub-commands will be
     * included in the list only if the hidden mode is set. If a sub-command is
     * specified the verbose help for that sub-command is returned.
     *
     * The top level command should not need to override this method.
     *
     * @return a help string
     */
    @Override
    protected final String getHelp(String[] args, Shell shell) {
        if (args.length <= 1) {
            String msg = getCommandOverview();
            msg += eol + getBriefHelp(shell.showHidden());
            return msg;
        }
        String commandName = args[1];
        SubCommand command = findCommand(commandName);
        if ((command != null) && (!command.isHidden() || shell.showHidden())) {
            return command.getVerboseHelp();
        }
        return("Could not find " + name + " subcommand: " + commandName +
               eol + getVerboseHelp());
    }

    /**
     * Returns a multi-line syntax string with appropriate spacing is returned
     * with each sub-command appearing on a separate line. Hidden sub-commands
     * will not be included in the list.
     *
     * The top level command should not need to override this method.
     *
     * @return a help string
     */
    @Override
    protected final String getBriefHelp() {
        return getBriefHelp(false);
    }

    private String getBriefHelp(boolean showHidden) {
        
        final StringBuilder sb = new StringBuilder();
        sb.append("Usage: ").append(name).append(" ");
        final String ws = Shell.makeWhiteSpace(sb.length());
        boolean first = true;
        for (SubCommand command : subCommands) {

            if (!showHidden && command.isHidden()) {
                continue;
            }
            if (first) {
                sb.append(command.getCommandName());
                first = false;
            } else {
                sb.append(" |").append(eol);
                sb.append(ws).append(command.getCommandName());
            }
        }
        return sb.toString();
    }

    /*
     * The top level command should not need to override this method. Also,
     * this method is only called from super.getBriefHelp() which has been
     * overridden and so should never be invoked.
     */
    @Override
    protected final String getCommandSyntax() {
        throw new AssertionError();
    }

    /* The top level command should not need to override this method */
    @Override
    protected final String getCommandDescription() {
        return "";
    }

    private SubCommand findCommand(String commandName) {
        for (SubCommand command : subCommands) {
            if (command.matches(commandName)) {
                return command;
            }
        }
        return null;
    }

    /*
     * Base abstract class for subcommands
     */
    public static abstract class SubCommand extends ShellCommand {
        protected final static String cantGetHere = "Cannot get here";

        protected SubCommand(String name, int prefixLength) {
            super(name, prefixLength);
        }
    }
}
