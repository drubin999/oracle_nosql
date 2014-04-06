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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URI;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.AuthenticationRequiredException;
import oracle.kv.Durability;
import oracle.kv.KVSecurityException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.KVVersion;
import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.security.login.AdminLoginManager;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.impl.security.util.KVStoreLogin.CredentialsProvider;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.shell.AggregateCommand;
import oracle.kv.shell.DeleteCommand;
import oracle.kv.shell.GetCommand;
import oracle.kv.shell.PutCommand;
import oracle.kv.util.shell.CommandWithSubs;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellArgumentException;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellException;

import com.sleepycat.je.rep.ReplicatedEnvironment.State;

/**
 * To implement a new command:
 * 1.  Implement a class that extends ShellCommand.
 * 2.  Add it to the static list, commands, in this class.
 *
 * Commands that have subcommands should extend SubCommand.  See one of the
 * existing classes for example code (e.g. PlanCommand).
 */
public class CommandShell extends Shell {

    public static final String COMMAND_NAME = "runcli";
    public static final String COMMAND_NAME_ALIAS = "runadmin";
    public static final String COMMAND_DESC =
        "runs the command line interface";
    public static final String COMMAND_ARGS =
        CommandParser.getHostUsage() + " " +
        CommandParser.getPortUsage() + " " +
        CommandParser.optional(CommandParser.getStoreUsage()) + eolt +
        CommandParser.optional(CommandParser.getAdminHostUsage() + " " +
        CommandParser.getAdminPortUsage()) + eolt +
        CommandParser.optional(CommandParser.getUserUsage()) + " " +
        CommandParser.optional(CommandParser.getSecurityUsage()) + eolt +
        CommandParser.optional(CommandParser.getAdminUserUsage()) + " " +
        CommandParser.optional(CommandParser.getAdminSecurityUsage()) + eolt +
        "[single command and arguments]";

    /*
     * Internal use only, used to indicate that the CLI is run
     * from KVStoreMain.
     */
    public static final String RUN_BY_KVSTORE_MAIN= "-kvstore-main";

    private ShellParser parser;
    private CommandServiceAPI cs;
    private KVStore store = null;
    private boolean noprompt = false;
    private boolean noconnect = false;
    private boolean retry = false;
    private String[] commandToRun;
    private int nextCommandIdx = 0;
    private String adminHostname = null;
    private int adminRegistryPort = 0;
    private String storeHostname = null;
    private int storePort = 0;
    private String kvstoreName = null;
    private String commandName = null;
    private String adminUser;
    private String adminSecurityFile;
    private String storeUser;
    private String storeSecurityFile;
    private LoginHelper loginHelper;

    static final String prompt = "kv-> ";
    static final String usageHeader =
        "Oracle NoSQL Database Administrative Commands:" + eol;
    static final String versionString = " (" +
        KVVersion.CURRENT_VERSION.getNumericVersionString() + ")";

    /*
     * The list of commands available
     */
    private static
        List<? extends ShellCommand> commands =
                       Arrays.asList(new AggregateCommand(),
                                     new ConfigureCommand(),
                                     new ConnectCommand(),
                                     new DdlCommand(),
                                     new DeleteCommand(),
                                     new Shell.ExitCommand(),
                                     new GetCommand(),
                                     new Shell.HelpCommand(),
                                     new HiddenCommand(),
                                     new HistoryCommand(),
                                     new Shell.LoadCommand(),
                                     new LogtailCommand(),
                                     new PingCommand(),
                                     new PlanCommand(),
                                     new PolicyCommand(),
                                     new PoolCommand(),
                                     new PutCommand(),
                                     new ShowCommand(),
                                     new SnapshotCommand(),
                                     new TableCommand(),
                                     new TimeCommand(),
                                     new TopologyCommand(),
                                     new VerboseCommand(),
                                     new VerifyCommand()
                                     );

    public CommandShell(InputStream input, PrintStream output) {
        super(input, output);
        Collections.sort(commands,
                         new Shell.CommandComparator());
        loginHelper = new LoginHelper();
    }

    @Override
    public void init() {
        if (!noconnect) {
            try {
                connect();
            } catch (ShellException se) {
                output.println(se.getMessage());
            }
        }
    }

    @Override
    public void shutdown() {
        closeStore();
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
        boolean oldVal = retry;
        retry = false;
        return oldVal;
    }

    @Override
    public void handleUnknownException(String line, Exception e) {

        if (e instanceof AdminFaultException) {
            AdminFaultException afe = (AdminFaultException) e;
            String faultClassName = afe.getFaultClassName();
            String msg = afe.getMessage();
            /* Don't treat IllegalCommandException as a "fault" */
            if (faultClassName.contains("IllegalCommandException")) {
                /* strip version info from message -- it's just usage */
                int endIndex = msg.indexOf(versionString);
                if (endIndex > 0) {
                    msg = msg.substring(0, endIndex);
                }
                e = null;
            }
            history.add(line, e);
            output.println(msg);
        } else if (e instanceof KVSecurityException) {
            super.handleKVSecurityException(line, (KVSecurityException) e);
        } else {
            super.handleUnknownException(line, e);
        }
    }

    /**
     * Handles uncaught {@link KVSecurityException}s during execution of admin
     * commands. Currently we will retry login and connect to admin for
     * {@link AuthenticationRequiredException}s. The
     * {@link KVSecurityException}s during execution of store commands have
     * been wrapped as {@link ShellException}s and handled elsewhere.
     *
     * @param line command line
     * @param kvse instance of KVSecurityException
     * @return true if re-connect to admin successfully, or a ShellException
     * calls for a retry, otherwise returns false.
     */
    @Override
    public boolean handleKVSecurityException(String line,
                                             KVSecurityException kvse) {
        if (kvse instanceof AuthenticationRequiredException) {
            try {
                /* Login and connect to the admin again. */
                connectAdmin(true /* force login */);

                /* Retry the command */
                return true;
            } catch (ShellException se) {
                return handleShellException(line, se);
            } catch (Exception e) {
                handleUnknownException(line, e);
                return false;
            }
        }
        return super.handleKVSecurityException(line, kvse);
    }

    public CommandServiceAPI getAdmin()
        throws ShellException {

        ensureConnection();
        return cs;
    }

    public KVStore getStore()
        throws ShellException {

        if (store == null) {
            throw new ShellArgumentException("You are not connected to a " +
                    "store, please run 'connect store' to open a store.");
        }
        return store;
    }

    String getAdminHostname() {
        return adminHostname;
    }

    int getAdminPort() {
        return adminRegistryPort;
    }

    public void connect()
        throws ShellException {

        if (kvstoreName != null) {
            loginHelper.updateStoreLogin(storeUser, storeSecurityFile);
            openStore();
        }
        loginHelper.updateAdminLogin(adminUser, adminSecurityFile);
        connectAdmin(false /* force login */);
    }

    public void openStore(String host,
                          int port,
                          String storeName,
                          String user,
                          String securityFile)
        throws ShellException {

        storeHostname = host;
        storePort = port;
        kvstoreName = storeName;

        try {
            loginHelper.updateStoreLogin(user, securityFile);
        } catch (IllegalStateException ise) {
            throw new ShellException(ise.getMessage());
        } catch (IllegalArgumentException iae) {
            output.println(iae.getMessage());
        }
        openStore();
    }

    public void start() {
        init();
        if (commandToRun != null) {
            try {
                String result = run(commandToRun[0], commandToRun);
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

    public LoginManager getLoginManager() {
        return loginHelper.getAdminLoginMgr();
    }

    /*
     * Centralized call for connection issues.  If the exception indicates that
     * the admin service cannot be contacted null out the handle and force a
     * reconnect on a retry.
     */
    protected void noAdmin(RemoteException e)
        throws ShellException{

        if (e != null) {
            if (cs != null) {
                Throwable t = e.getCause();
                if (t != null) {
                    if (t instanceof EOFException ||
                        t instanceof java.net.ConnectException ||
                        t instanceof java.rmi.ConnectException) {
                        cs = null;
                        retry = true;
                    }
                }
            }
        }

        throw new ShellException("Cannot contact admin", e);
    }

    private void ensureConnection()
        throws ShellException {

        ShellException ce = null;
        if (cs == null) {
            output.println("Lost connection to Admin service.");
            output.print("Reconnecting...");
            for (int i = 0; i < 10; i++) {
                try {
                    output.print(".");
                    connectAdmin(false /* force login */);
                    output.println("");
                    return;
                } catch (ShellException se) {
                    final Throwable t = se.getCause();
                    if (t != null &&
                        t instanceof AuthenticationFailureException) {
                        throw se;
                    }
                    ce = se;
                }
                try {
                    Thread.sleep(6000);
                } catch(Exception ignored) {
                }
            }
            if (ce != null) {
                throw ce;
            }
        }
    }

    public void connectAdmin(String host,
                             int newPort,
                             String user,
                             String securityFileName)
        throws ShellException {

        try {
            loginHelper.updateAdminLogin(user, securityFileName);
        } catch (IllegalStateException ise) {
            throw new ShellException(ise.getMessage());
        } catch (IllegalArgumentException iae) {
            output.println(iae.getMessage());
        }

        adminHostname = host;
        adminRegistryPort = newPort;
        connectAdmin(false /* force login */);
    }

    /*
     * Connect to the admin. Handle a redirect to a master if we are
     * connecting to a replica.
     */
    private void connectAdmin(boolean forceLogin)
        throws ShellException {

        Exception e = null;
        try {
            cs = loginHelper.getAuthenticatedAdmin(adminHostname,
                                                   adminRegistryPort,
                                                   forceLogin);
            State adminState = cs.getAdminState();
            if (adminState == null) {
                /* Unconfigured -- connect to allow configuration. */
                return;
            }

            switch (adminState) {
            case MASTER:
                break;
            case REPLICA:
                URI rmiaddr = cs.getMasterRmiAddress();
                output.println("Redirecting to master at " + rmiaddr);
                adminHostname = rmiaddr.getHost();
                adminRegistryPort = rmiaddr.getPort();
                connectAdmin(false /* force login */);
                break;
            case DETACHED:
            case UNKNOWN:
                output.println
                    ("The Admin instance is unable to service this request.");
                cs = null;
                break;
            }
            return;
        } catch (AuthenticationRequiredException are) {
            /*
             * Will fail if try connecting to a secured admin without login.
             * Retry connecting and force login.
             */
            println(String.format(
                "Admin %s:%d requires authentication.", adminHostname,
                adminRegistryPort));
            connectAdmin(true /* force login */);
            return;
        } catch (RemoteException re) {
            e = re;
        } catch (NotBoundException nbe) {
            e = nbe;
        }
        throw new ShellException("Cannot connect to admin at " +
                                 adminHostname + ":" + adminRegistryPort, e);
    }

    /* Open the store. */
    private void openStore()
        throws ShellException {

        final KVStoreConfig kvstoreConfig =
            new KVStoreConfig(kvstoreName, storeHostname + ":" + storePort);

        /*
         * Use a more aggressive default Durability to make sure that records
         * get into a database.
         */
        kvstoreConfig.setDurability(Durability.COMMIT_SYNC);
        try {
            store = loginHelper.getAuthenticatedStore(kvstoreConfig);
        } catch (ShellException se) {
            throw se;
        } catch (Exception e) {
            throw new ShellException("Cannot connect to " + kvstoreName +
                " at " + storeHostname + ":" + storePort, e);
        }
        println("Connected to " + kvstoreName);
    }

    private void closeStore() {
        if (store != null) {
            store.close();
            store = null;
        }
    }

    static class VerboseCommand extends ShellCommand {

        VerboseCommand() {
            super("verbose", 4);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            if (shell.toggleVerbose()) {
                return "Verbose mode is now on";
            }
            return "Verbose mode is now off";
        }

        @Override
        public String getCommandDescription() {
            return
                "Toggles the global verbosity setting.  This property can " +
                "also" + eolt + "be set per-command using the -verbose " +
                "flag.";
        }
    }

    static class HiddenCommand extends ShellCommand {

        HiddenCommand() {
            super("hidden", 3);
        }

        @Override
        protected boolean isHidden() {
            return true;
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            CommandShell cs = (CommandShell) shell;
            if (cs.toggleHidden()) {
                return "Hidden parameters are enabled";
            }
            return "Hidden parameters are disabled";
        }

        @Override
        public String getCommandDescription() {
            return "Toggles visibility and setting of parameters that are " +
                   "normally hidden." + eolt + "Use these parameters only if " +
                   "advised to do so by Oracle Support.";
        }
    }

    static class HistoryCommand extends ShellCommand {

        HistoryCommand() {
            super("history", 4);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.CommandHistory history = shell.getHistory();
            int from = 0;
            int to = history.getSize();
            String parseFrom = null;
            String parseTo = null;
            boolean isLast = false;

            if (args.length > 1) {
                for (int i = 1; i < args.length; i++) {
                    String arg = args[i];
                    if ("-last".equals(arg)) {
                        parseFrom = Shell.nextArg(args, i++, this);
                        isLast = true;
                    } else if ("-from".equals(arg)) {
                        parseFrom = Shell.nextArg(args, i++, this);
                    } else if ("-to".equals(arg)) {
                        parseTo = Shell.nextArg(args, i++, this);
                    } else {
                        return "Invalid argument: " + arg + eolt +
                            getBriefHelp();
                    }
                }
                try {
                    if (parseFrom != null) {
                        from = Integer.parseInt(parseFrom);
                    }
                    if (parseTo != null) {
                        to = Integer.parseInt(parseTo);
                    }
                    if (isLast) {
                        from = history.getSize() - from;
                    }
                } catch (IllegalArgumentException e) {
                    return "Invalid integer argument:" + eolt +
                        getBriefHelp();
                }
            }
            return history.dump(from, to);
        }

        @Override
        protected String getCommandSyntax() {
            return "history [-last <n>] [-from <n>] [-to <n>]";
        }

        @Override
        protected String getCommandDescription() {
            return
                "Displays command history.  By default all history is " +
                "displayed." + eolt + "Optional flags are used to choose " +
                "ranges for display";
        }
    }

    static class ConnectCommand extends CommandWithSubs {
        private static final String COMMAND = "connect";
        private static final String HOST_FLAG = "-host";
        private static final String HOST_FLAG_DESC = HOST_FLAG + " <hostname>";
        private static final String PORT_FLAG = "-port";

        private static final List<? extends SubCommand> subs =
            Arrays.asList(new ConnectAdminCommand(),
                          new ConnectStoreCommand());

        ConnectCommand() {
            super(subs, COMMAND, 4, 2);
        }

        @Override
        protected String getCommandOverview() {
            return "Encapsulates commands that connect to the specified " +
                "host and registry port" + eol + "to perform administrative " +
                "functions or connect to the specified store to" + eol +
                "perform data access functions.";
        }

        /* ConnectAdmin command */
        static class ConnectAdminCommand extends SubCommand {
            private static final String SUB_COMMAND = "admin";
            private static final String PORT_FLAG_DESC =
                PORT_FLAG + " <registry port>";

            static final String CONNECT_ADMIN_COMMAND_DESC =
                "Connects to the specified host and registry port to " +
                "perform" + eolt + "administrative functions.  An Admin " +
                "service must be active on the" + eolt + "target host.  " +
                "If the instance is secured, you may need to provide" + eolt +
                "login credentials.";

            static final String CONNECT_ADMIN_COMMAND_SYNTAX =
                COMMAND + " " + SUB_COMMAND + " " + HOST_FLAG_DESC + " " +
                PORT_FLAG_DESC + eolt +
                CommandParser.optional(CommandParser.getUserUsage()) + " " +
                CommandParser.optional(CommandParser.getSecurityUsage());

            ConnectAdminCommand() {
                super(SUB_COMMAND, 3);
            }

            @Override
            public String execute(String[] args, Shell shell)
                throws ShellException {

                Shell.checkHelp(args, this);
                String host = null;
                int port = 0;
                String user = null;
                String security = null;

                for (int i = 1; i < args.length; i++) {
                    String arg = args[i];
                    if (HOST_FLAG.equals(arg)) {
                        host = Shell.nextArg(args, i++, this);
                    } else if (PORT_FLAG.equals(arg)) {
                        try {
                            port = Integer.parseInt
                                (Shell.nextArg(args, i++, this));
                        } catch (IllegalArgumentException iae) {
                            shell.invalidArgument(arg, this);
                        }
                    } else if (CommandParser.USER_FLAG.equals(arg)) {
                        user = Shell.nextArg(args, i++, this);
                    } else if (CommandParser.SECURITY_FLAG.equals(arg)) {
                        security = Shell.nextArg(args, i++, this);
                    } else {
                        shell.invalidArgument(arg, this);
                    }
                }
                if (host == null || port == 0) {
                    shell.badArgCount(this);
                }
                final CommandShell cmd = (CommandShell) shell;
                cmd.connectAdmin(host, port, user, security);
                return "Connected.";
            }

            @Override
            protected String getCommandSyntax() {
                return CONNECT_ADMIN_COMMAND_SYNTAX;
            }

            @Override
            protected String getCommandDescription() {
                return CONNECT_ADMIN_COMMAND_DESC;
            }
        }

        /* ConnectStore command */
        static class ConnectStoreCommand extends SubCommand {
            private static final String SUB_COMMAND = "store";
            private static final String PORT_FLAG_DESC = PORT_FLAG + " <port>";
            private static final String STORE_FLAG = "-name";
            private static final String STORE_FLAG_DESC =
                STORE_FLAG + " <storename>";

            static final String CONNECT_STORE_COMMAND_DESC =
                "Connects to a KVStore to perform data access functions." +
                eolt + "If the instance is secured, you may need to provide " +
                "login credentials.";

            static final String CONNECT_STORE_COMMAND_SYNTAX =
                COMMAND + " " + SUB_COMMAND + " " +
                CommandParser.optional(HOST_FLAG_DESC) + " " +
                CommandParser.optional(PORT_FLAG_DESC) + " " +
                STORE_FLAG_DESC + eolt +
                CommandParser.optional(CommandParser.getUserUsage()) + " " +
                CommandParser.optional(CommandParser.getSecurityUsage());

            ConnectStoreCommand() {
                super("store", 4);
            }

            @Override
            public String execute(String[] args, Shell shell)
                throws ShellException {

                Shell.checkHelp(args, this);
                String hostname = ((CommandShell)shell).storeHostname;
                int port = ((CommandShell)shell).storePort;
                String storeName = null;
                String user = null;
                String security = null;

                for (int i = 1; i < args.length; i++) {
                    String arg = args[i];
                    if (HOST_FLAG.equals(arg)) {
                        hostname = Shell.nextArg(args, i++, this);
                    } else if (PORT_FLAG.equals(arg)) {
                        try {
                            port = Integer.parseInt
                                (Shell.nextArg(args, i++, this));
                        } catch (IllegalArgumentException iae) {
                            shell.invalidArgument(arg, this);
                        }
                    } else if (STORE_FLAG.equals(arg)) {
                        storeName = Shell.nextArg(args, i++, this);
                    } else if (CommandParser.USER_FLAG.equals(arg)) {
                        user = Shell.nextArg(args, i++, this);
                    } else if (CommandParser.SECURITY_FLAG.equals(arg)) {
                        security = Shell.nextArg(args, i++, this);
                    } else {
                        shell.unknownArgument(arg, this);
                    }
                }

                if (hostname == null || port == 0 || storeName == null) {
                    shell.badArgCount(this);
                }

                /* Close current store, open new one.*/
                final CommandShell cmd = (CommandShell) shell;
                cmd.closeStore();
                try {
                    cmd.openStore(hostname, port, storeName, user, security);
                } catch (ShellException se) {
                    throw new ShellException(
                        se.getMessage() + eol +
                        "Warning: You are no longer connected to a store.");
                }
                return "Connected to " + storeName + " at " +
                    hostname + ":" + port + ".";
            }

            @Override
            protected String getCommandSyntax() {
                return CONNECT_STORE_COMMAND_SYNTAX;
            }

            @Override
            protected String getCommandDescription() {
                return CONNECT_STORE_COMMAND_DESC;
            }
        }
    }

    /* Time command */
    static class TimeCommand extends ShellCommand {

        public TimeCommand() {
            super("time", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            if (args.length == 1) {
                shell.badArgCount(this);
            }

            String commandName = args[1];
            ShellCommand command = shell.findCommand(commandName);
            if (command != null) {
                String[] cmdArgs = Arrays.copyOfRange(args, 1, args.length);
                long startTime = System.currentTimeMillis();
                String retString = command.execute(cmdArgs, shell);
                retString += eol + "Time: " +
                             (System.currentTimeMillis() - startTime) + " ms.";
                return retString;
            }

            return ("Could not find command: " + commandName +
                    eol + shell.getUsage());
        }

        @Override
        protected String getCommandSyntax() {
            return "time command [sub-command]";
        }

        @Override
        protected String getCommandDescription() {
            return "Runs the specified command and prints " +
                   eolt + "the elapsed time of the execution.";
        }
    }

    private class ShellParser extends CommandParser {
        public static final String NOCONNECT_FLAG = "-noconnect";
        public static final String NOPROMPT_FLAG = "-noprompt";
        public static final String HIDDEN_FLAG = "-hidden";

        /* Hidden flags: -noprompt, -noconnect, -hidden. */

        private ShellParser(String[] args) {
            /*
             * The true argument tells CommandParser that this class will
             * handle all flags, not just those unrecognized.
             */
            super(args, true);
        }

        @Override
        protected void verifyArgs() {
            if (!noconnect) {
                if (storeHostname == null || storePort == 0) {
                    usage("Missing required argument");
                }
                if (adminHostname == null && adminRegistryPort == 0) {
                    adminHostname = storeHostname;
                    adminRegistryPort = storePort;
                }
                if (adminHostname == null || adminRegistryPort == 0) {
                    usage("Missing required argument");
                }

                /*
                 * If no separate user or login file is set for admin, admin
                 * will share the same user and login file with store.
                 */
                if (adminUserName == null && adminSecurityFilePath == null) {
                    adminUserName = userName;
                    adminSecurityFilePath = securityFile;
                }
            }
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
            String usage;
            if (commandName == null) {
                usage = KVCLI_USAGE_PREFIX + eolt + COMMAND_ARGS;
            } else {
                usage = KVSTORE_USAGE_PREFIX + commandName + " " + eolt +
                    COMMAND_ARGS;
            }
            System.err.println(usage);
            System.exit(1);
        }

        @Override
        protected boolean checkArg(String arg) {

            if (HOST_FLAG.equals(arg)) {
                if (storeHostname == null) {
                    storeHostname = nextArg(arg);
                    return true;
                }
            } else if (PORT_FLAG.equals(arg)) {
                if (storePort == 0) {
                    storePort = Integer.parseInt(nextArg(arg));
                    return true;
                }
            } else if (STORE_FLAG.equals(arg)) {
                if (kvstoreName == null) {
                    kvstoreName = nextArg(arg);
                    return true;
                }
            } else if (ADMIN_HOST_FLAG.equals(arg)) {
                if (adminHostname == null) {
                    adminHostname = nextArg(arg);
                    return true;
                }
            } else if (ADMIN_PORT_FLAG.equals(arg)) {
                if (adminRegistryPort == 0) {
                    adminRegistryPort = Integer.parseInt(nextArg(arg));
                    return true;
                }
            } else if (RUN_BY_KVSTORE_MAIN.equals(arg)) {
                if (commandName == null) {
                    commandName = nextArg(arg);
                    return true;
                }
            } else if (USER_FLAG.equals(arg)) {
                if (userName == null) {
                    userName = nextArg(arg);
                    return true;
                }
            } else if (SECURITY_FLAG.equals(arg)) {
                if (securityFile == null) {
                    securityFile = nextArg(arg);
                    return true;
                }
            /* Admin and store may need different users and login files */
            } else if (ADMIN_USER_FLAG.equals(arg)) {
                if (adminUserName == null) {
                    adminUserName = nextArg(arg);
                    return true;
                }
            } else if (ADMIN_SECURITY_FLAG.equals(arg)) {
                if (adminSecurityFilePath == null) {
                    adminSecurityFilePath = nextArg(arg);
                    return true;
                }
            } else {
                if (NOCONNECT_FLAG.equals(arg)) {
                    noconnect = true; /* undocumented option */
                    return true;
                }
                if (NOPROMPT_FLAG.equals(arg)) {
                    noprompt = true;
                    return true;
                }
                if (HIDDEN_FLAG.equals(arg)) {
                    showHidden = true;
                    /* Some commands take this flag as well */
                    if (commandToRun != null) {
                        addToCommand(arg);
                    }
                    return true;
                }
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

    public void parseArgs(String args[]) {
        parser = new ShellParser(args);
        parser.parseArgs();
        storeUser = parser.getUserName();
        storeSecurityFile = parser.getSecurityFile();
        adminUser = parser.getAdminUserName();
        adminSecurityFile = parser.getAdminSecurityFile();
    }

    public static void main(String[] args) {
        CommandShell shell = new CommandShell(System.in, System.out);
        shell.parseArgs(args);
        shell.start();
        if (shell.getExitCode() != EXIT_OK) {
            System.exit(shell.getExitCode());
        }
    }

    /**
     * A helper class for client login of Admin and store.
     */
    private class LoginHelper implements CredentialsProvider {
        private final KVStoreLogin storeLogin = new KVStoreLogin();
        private final KVStoreLogin adminLogin = new KVStoreLogin();
        private static final String loginConnErrMsg =
            "Cannot connect to Admin login service %s:%d";

        private LoginManager adminLoginMgr;
        private PasswordCredentials storeCreds;

        private boolean isSecuredAdmin;
        private boolean isSecuredStore;

        /* Uses to record the last AFE during loginAdmin() method */
        private AuthenticationFailureException lastAdminAfe;

        /**
         * Updates the login information of admin.
         *
         * @param newUser new user name
         * @param newLoginFile new login file path
         */
        private void updateAdminLogin(final String newUser,
                                      final String newLoginFile) {

            adminLogin.updateLoginInfo(newUser, newLoginFile);

            /* Login is needed if SSL transport is used */
            isSecuredAdmin = adminLogin.foundSSLTransport();
            adminLogin.prepareRegistryCSF();
            adminLoginMgr = null;
        }

        /**
         * Updates the login information of store.
         *
         * @param newUser new user name
         * @param newLoginFile new login file path
         */
        private void updateStoreLogin(final String newUser,
                                      final String newLoginFile) {
            storeLogin.updateLoginInfo(newUser, newLoginFile);
            isSecuredStore = storeLogin.foundSSLTransport();
            storeCreds = null;
        }

        /**
         * Returns the CommandServiceAPI. If admin is secured, authentication
         * will be conducted using login file or via user interaction.
         *
         * @param host hostname of admin
         * @param port registry port of admin
         * @param forceLogin force login before connection
         * @return commandServiceAPI of admin
         */
        private CommandServiceAPI
            getAuthenticatedAdmin(final String host,
                                  final int port,
                                  final boolean forceLogin)
            throws ShellException,
                   RemoteException,
                   NotBoundException {

            if (forceLogin || (isSecuredAdmin && (adminLoginMgr == null))) {
                loginAdmin();
            }
            return RegistryUtils.getAdmin(host, port, adminLoginMgr);
        }

        /**
         * Returns a kvstore handler. If the store is secured, authentication
         * will be conducted using login file or via user interaction.
         *
         * @param config the kvconfig specified by user
         * @return kvstore handler
         */
        private KVStore getAuthenticatedStore(final KVStoreConfig config)
            throws ShellException {

            config.setSecurityProperties(storeLogin.getSecurityProperties());

            try {
                if (isSecuredStore && storeCreds == null) {
                    storeCreds = storeLogin.makeShellLoginCredentials();
                }
                return KVStoreFactory.getStore(
                    config, storeCreds,
                    KVStoreLogin.makeReauthenticateHandler(this));
            } catch (AuthenticationFailureException afe) {
                storeCreds = null;
                throw new ShellException(
                    "Login failed: " + afe.getMessage(), afe);
            } catch (IOException ioe) {
                throw new ShellException("Failed to get login credentials: " +
                                         ioe.getMessage());
            }
        }

        private LoginManager getAdminLoginMgr() {
            return adminLoginMgr;
        }

        /*
         * Tries to login the admin.
         */
        private void loginAdmin()
            throws ShellException {

            final String adminLoginUser = adminLogin.getUserName();
            lastAdminAfe = null;

            /*
             * When store creds is available, if no admin user is
             * specified, or the admin user is identical with store user,
             * try to login admin using the store creds.
             */
            if (storeCreds != null) {
                if ((adminLoginUser == null) ||
                    adminLoginUser.equals(storeCreds.getUsername())) {
                    if (loginAdmin(storeCreds)) {
                        return;
                    }
                }
            }

            /* Try anonymous login if user name is not specified. */
            if (adminLoginUser == null) {
                if (loginAdmin(null /* anonymous loginCreds */)) {
                    return;
                }
                println("Could not login as anonymous: " +
                        lastAdminAfe.getMessage());
            }

            /* Login using explicit username and login file. */
            try {
                if (!loginAdmin(adminLogin.makeShellLoginCredentials())) {
                    throw new ShellException(
                        "Login failed: " + lastAdminAfe.getMessage(),
                        lastAdminAfe);
                }
            } catch (IOException ioe) {
                throw new ShellException("Failed to get login credentials: " +
                                         ioe.getMessage());
            }
        }

        /**
         * Tries to login to admin using specified credentials.
         *
         * @param loginCreds login credentials. If null, anonymous login will
         * be tried.
         * @return true if and only if login successfully
         * @throws ShellException if fail to connect to admin login service.
         */
        private boolean loginAdmin(final PasswordCredentials loginCreds)
            throws ShellException {

            final String userName =
                (loginCreds == null) ? null : loginCreds.getUsername();
            final AdminLoginManager alm =
                new AdminLoginManager(userName, true);

            try {
                if (alm.bootstrap(adminHostname, adminRegistryPort,
                                  loginCreds)) {
                    adminLoginMgr = alm;
                    println("Logged in admin as " +
                            (userName == null ? "anonymous" : userName));
                    return true;
                }
                throw new ShellException(
                    String.format(loginConnErrMsg, adminHostname,
                                  adminRegistryPort));
            } catch (AuthenticationFailureException afe) {
                lastAdminAfe = afe;
                adminLoginMgr = null;
                return false;
            }
        }

        @Override
        public LoginCredentials getCredentials() {
            return storeCreds;
        }
    }
}
