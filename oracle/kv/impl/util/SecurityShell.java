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

package oracle.kv.impl.util;

import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import oracle.kv.impl.security.util.PasswordReader;
import oracle.kv.impl.security.util.ShellPasswordReader;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellException;

/**
 * To implement a new command:
 * 1.  Implement a class that extends ShellCommand.
 * 2.  Add it to the static list, commands, in this class.
 *
 * Commands that have subcommands should extend SubCommand.  See one of the
 * existing classes for example code (e.g. WalletCommand).
 */

public class SecurityShell extends Shell {

    private static final String WALLET_COMMAND_CLASS =
        "oracle.kv.impl.util.WalletCommand";

    public static final String COMMAND_NAME = "securityconfig";
    public static final String COMMAND_DESC =
        "runs the security configuration command line interface";
    public static final String COMMAND_ARGS =
        CommandParser.getHostUsage() + " " +
        CommandParser.getPortUsage() +
        " [single command and arguments]";

    private CommandParser parser;
    private boolean noprompt = false;
    private int nextCommandIdx = 0;
    private String[] commandToRun;
    private /* final */ PasswordReader passwordReader;

    static final String prompt = "security-> ";
    static final String usageHeader =
        "Oracle NoSQL Database Security Configuration Commands:" + eol;

    /*
     * The list of commands available
     */
    private final List<ShellCommand> commands;

    public SecurityShell(InputStream input, PrintStream output,
                         PasswordReader passwordReader) {
        super(input, output);
        this.passwordReader = passwordReader;
        commands = new ArrayList<ShellCommand>();

        final List<? extends ShellCommand> basicCommands =
            Arrays.asList(new Shell.ExitCommand(),
                          new Shell.HelpCommand(),
                          new PwdfileCommand(),
                          new SecurityConfigCommand());
        commands.addAll(basicCommands);

        /* Wallet is conditional on EE */
        final ShellCommand walletCommand = findWalletCommand();
        if (walletCommand != null) {
            commands.add(walletCommand);
        }

        Collections.sort(commands, new Shell.CommandComparator());
    }

    @Override
    public void init() {
        /* nothing to do at this time */
    }

    @Override
    public void shutdown() {
        /* nothing to do at this time */
    }

    @Override
    public List<? extends ShellCommand> getCommands() {
        return commands;
    }

    @Override
    public String getPrompt() {
        return noprompt ? null : prompt;
    }

    @Override
    public String getUsageHeader() {
        return usageHeader;
    }

    /*
     * If retry is true, return that, but be sure to reset the value
     */
    @Override
    public boolean doRetry() {
        /* nothing to do at this time */
        return false;
    }

    public void start() {
        init();
        if (commandToRun != null) {
            try {
                final String result = run(commandToRun[0], commandToRun);
                output.println(result);
            } catch (ShellException se) {
                handleShellException(commandToRun[0], se);
            } catch (Exception e) {
                handleUnknownException(commandToRun[0], e);
            }
        } else {
            loop();
        }
        shutdown();
    }

    /**
     * For testing.
     */
    void enableHidden() {
        if (!showHidden()) {
            toggleHidden();
        }
    }

    private final class ShellParser extends CommandParser {
        public static final String NOPROMPT_FLAG = "-noprompt";
        /* Hidden flags: -noprompt */

        private ShellParser(String[] args) {
            /*
             * The true argument tells CommandParser that this class will
             * handle all flags, not just those unrecognized.
             */
            super(args, true);
        }

        @Override
        protected void verifyArgs() {
            if ((commandToRun != null) &&
                (nextCommandIdx < commandToRun.length)) {
                usage("Flags may not follow commands");
            }
        }

        @Override
        public void usage(String errorMsg) {
            if (errorMsg != null) {
                System.err.println(errorMsg);
            }
            System.err.println(KVSTORE_USAGE_PREFIX + COMMAND_NAME + eolt +
                               COMMAND_ARGS);
            System.exit(1);
        }

        @Override
        protected boolean checkArg(String arg) {
            if (NOPROMPT_FLAG.equals(arg)) {
                noprompt = true;
                return true;
            }
            addToCommand(arg);
            return true;
        }

        /*
         * Add unrecognized args to the commandToRun array.
         */
        private void addToCommand(String arg) {
            if (commandToRun == null) {
                commandToRun = new String[getNRemainingArgs() + 1];
            }
            commandToRun[nextCommandIdx++] = arg;
        }
    }

    public void parseArgs(String[] args) {
        parser = new ShellParser(args);
        parser.parseArgs();
    }

    public static void main(String[] args) {
        final SecurityShell shell =
            new SecurityShell(System.in, System.out,
                              new ShellPasswordReader());
        shell.parseArgs(args);
        shell.start();
        if (shell.getExitCode() != EXIT_OK) {
            System.exit(shell.getExitCode());
        }
    }

    PasswordReader getPasswordReader() {
        return passwordReader;
    }

    private ShellCommand findWalletCommand() {
        final Class<?> walletCommandClass;
        try {
            walletCommandClass = Class.forName(WALLET_COMMAND_CLASS);
        } catch (ClassNotFoundException cnfe) {
            return null;
        }

        try {
            return (ShellCommand) walletCommandClass.newInstance();
        } catch (IllegalAccessException iae) {
            /* class or ctor() not accessible */
            return null;
        } catch (InstantiationException ie) {
            /* class is abstract or not true class type, etc. */
            return null;
        } catch (ExceptionInInitializerError eiie) {
            /* ctor threw an exception */
            return null;
        } catch (SecurityException se) {
            /* No permissions to do instantiation */
            return null;
        }
    }

}
