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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.client.CommandShell;
import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.mgmt.MgmtUtil;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.sna.StorageNodeAgentImpl;
import oracle.kv.impl.util.SecurityConfigCreator.GenericIOHelper;
import oracle.kv.impl.util.SecurityConfigCreator.ParsedConfig;
import oracle.kv.util.GenerateConfig;
import oracle.kv.util.Load;
import oracle.kv.util.Ping;
import oracle.kv.util.kvlite.KVLite;

/**
 * Used as main class in Jar manifest for kvstore.jar and kvstoretest.jar.
 * Implements certain minor commands here: help, version, makebootconfig.
 * Delegates all other commands to the main() method of other classes.
 *
 * Delegation of parameters is not strictly a pass through:
 * + The first param, the command, is always removed before delegating.
 * + The -shutdown flag is added to the stop command args, since a single class
 *   (StorageNodeAgent/StorageNodeAgentImpl) handles both start and stop.
 *
 * As long as the kvctl and kvlite scripts are supported, for backward
 * compatibility, the CLI of the delegate classes cannot be changed, because
 * these classes are called directly by the scripts.
 */
public class KVStoreMain {

    private static final String HELP_COMMAND_NAME = "help";
    private static final String HELP_COMMAND_DESC = "prints usage info";
    private static final String HELP_COMMANDS_COMMAND = "commands";
    private static final String VERSION_COMMAND_NAME = "version";
    private static final String VERSION_COMMAND_DESC = "prints version";
    private static final String MAKECONFIG_COMMAND_NAME = "makebootconfig";
    private static final String MAKECONFIG_COMMAND_DESC =
        "creates configuration files required in kvroot";
    private static final String HARANGE_FLAG = "-harange";
    private static final String HAHOST_FLAG = "-hahost";
    private static final String SECURITY_CONFIGURE = "configure";
    private static final String SECURITY_ENABLE = "enable";
    private static final String SECURITY_NONE = "none";

    private static final String MAKECONFIG_COMMAND_ARGS =
        CommandParser.getRootUsage() + " " +
        CommandParser.getHostUsage() + " " +
        HARANGE_FLAG + " <startPort,endPort>" + "\n\t" +
        MakeConfigParser.STORE_SECURITY_FLAG +
        " configure|enable|none" + "\n\t" +
        CommandParser.getPortUsage() + " " +
        CommandParser.optional(CommandParser.getAdminUsage()) +
        CommandParser.optional(MakeConfigParser.FORCE_ADMIN_FLAG) +
        CommandParser.optional(StorageNodeAgent.CONFIG_FLAG +
                               " <configFile>") + "\n\t" +
        CommandParser.optional(MakeConfigParser.MOUNT_FLAG +
                               " <directory path>") +
        CommandParser.optional(MakeConfigParser.CAPACITY_FLAG +
                               " <n_rep_nodes>") + "\n\t" +
        CommandParser.optional(MakeConfigParser.CPU_FLAG +
                               " <ncpus>") +
        CommandParser.optional(MakeConfigParser.MEMORY_FLAG +
                               " <memory_mb>") + "\n\t" +
        CommandParser.optional(MakeConfigParser.SERVICERANGE_FLAG +
                               " <startPort,endPort>") + "\n\t" +
        CommandParser.optional(HAHOST_FLAG +
                               " <haHostname>") + "\n\t" +
        SecurityConfigCommand.ConfigParserHelper.getConfigUsage() + "\n\t" +
        MgmtUtil.getMgmtUsage(false);

    private static final String FLAG_DESCRIPTIONS =
      "\n  -root <kvroot>" +
      "\n\t# the root directory for the store" +
      "\n  -host <hostname>" +
      "\n\t# the hostname to use" +
      "\n  -port <port>" +
      "\n\t# the registry port to use" +
      "\n  -store <storename>" +
      "\n\t# the target store (used by load)" +
      "\n  -source <snapshot>" +
      "\n\t# the snapshot source for load" +
      "\n  -status <path>" +
      "\n\t# the status file used by load" +
      "\n  -admin <adminport>" +
      "\n\t# the HTTP port for the admin to use" +
      "\n  -runadmin" +
      "\n\t# force to start a bootstrap admin" +
      "\n  -hahost <hostname>" +
      "\n\t# the hostname to be used by HA. It defaults to the -host value." +
      "\n  -harange <portstart,portend>" +
      "\n\t# the range of ports for replicated services to use. " +
      "E.g. \"5030,5040\"" +
      "\n  -servicerange <portstart,portend>" +
      "\n\t# the range of ports for use by RMI services. " +
      "\n\t# E.g. \"5050,5060\" or \"0\" for unconstrained use of ports" +
      "\n  -config <configfile>" +
      "\n\t# the configuration file in kvroot, defaults to \"config.xml\"" +
      "\n  -storagedir <directory name>" +
      "\n\t# directory to use for the Replication Nodes hosted by this SN, " +
      "\n\t# more than one set of -storagedir <directory> flags may be " +
             "specified." +
      "\n  -capacity <num_rep_nodes>" +
      "\n\t# the number of RepNodes this Storage Node can handle." +
      "\n  -num_cpus <num_cpus>" +
      "\n\t# the number of CPUs on the Storage Node." +
      "\n  -memory_mb <memory_in_mb>" +
      "\n\t# the amount of memory available to use." +
      "\n  -script <scriptfile>" +
      "\n\t# the admin script file to execute";

    /**
     * Abstract Command.  A Command is identified by its name, which is the
     * first arg to main().
     */
    private static abstract class Command {
        final String name;
        final String description;

        Command(String name, String description) {
            this.name = name;
            this.description = description;
        }

        abstract void run(String[] args)
            throws Exception;

        abstract String getUsageArgs();

        boolean isHelpCommand() {
            return false;
        }
    }

    /**
     * The order commands appear in the array is the order they appear in the
     * 'help' and 'help commands' output.
     */
    private static Command[] ALL_COMMANDS = {

        new Command(KVLite.COMMAND_NAME, KVLite.COMMAND_DESC) {

            @Override
            void run(String[] args)
                throws Exception {

                KVLite.main(makeArgs(args));
            }

            @Override
            String getUsageArgs() {
                return KVLite.COMMAND_ARGS;
            }
        },

        new Command(MAKECONFIG_COMMAND_NAME, MAKECONFIG_COMMAND_DESC) {

            @Override
            void run(String[] args) {
                makeBootConfig(makeArgs(args));
            }

            @Override
            String getUsageArgs() {
                return MAKECONFIG_COMMAND_ARGS;
            }
        },

        new Command(SecurityShell.COMMAND_NAME, SecurityShell.COMMAND_DESC) {

            @Override
            void run(String[] args) {
                SecurityShell.main(makeArgs(args));
            }

            @Override
            String getUsageArgs() {
                return SecurityShell.COMMAND_ARGS;
            }
        },

        new Command(StorageNodeAgent.START_COMMAND_NAME,
                    StorageNodeAgent.START_COMMAND_DESC) {

            @Override
            void run(String[] args) {
                StorageNodeAgentImpl.main(makeArgs(args));
            }

            @Override
            String getUsageArgs() {
                return StorageNodeAgent.COMMAND_ARGS;
            }
        },

        new Command(StorageNodeAgent.STOP_COMMAND_NAME,
                    StorageNodeAgent.STOP_COMMAND_DESC) {

            @Override
            void run(String[] args) {
                /* Add -shutdown. */
                StorageNodeAgentImpl.main
                    (makeArgs(args, StorageNodeAgent.SHUTDOWN_FLAG));
            }

            @Override
            String getUsageArgs() {
                return StorageNodeAgent.COMMAND_ARGS;
            }
        },

        new Command(StorageNodeAgent.RESTART_COMMAND_NAME,
                    StorageNodeAgent.RESTART_COMMAND_DESC) {

            @Override
            void run(String[] args) {
                /* Add -shutdown. */
                StorageNodeAgentImpl.main
                    (makeArgs(args, StorageNodeAgent.SHUTDOWN_FLAG));
                /* Start. */
                StorageNodeAgentImpl.main(makeArgs(args));
            }

            @Override
            String getUsageArgs() {
                return StorageNodeAgent.COMMAND_ARGS;
            }
        },

        new Command(CommandShell.COMMAND_NAME_ALIAS,
                    CommandShell.COMMAND_DESC){

            @Override
            void run(String[] args)
                throws Exception {

                final String[] prefixArgs =
                    new String[]{args[0],
                                 CommandShell.RUN_BY_KVSTORE_MAIN,
                                 CommandShell.COMMAND_NAME_ALIAS};

                if (args.length > 1) {
                    String[] remainArgs = new String[args.length - 1];
                    System.arraycopy(args, 1, remainArgs, 0, remainArgs.length);
                    CommandShell.main(makeArgs(prefixArgs, remainArgs));
                } else {
                    CommandShell.main(makeArgs(prefixArgs));
                }
            }

            @Override
            String getUsageArgs() {
                return CommandShell.COMMAND_ARGS;
            }
        },

        new Command(Load.COMMAND_NAME, Load.COMMAND_DESC) {

            @Override
            void run(String[] args)
                throws Exception {

                Load.main(makeArgs(args));
            }

            @Override
            String getUsageArgs() {
                return Load.COMMAND_ARGS;
            }
        },

        new Command(Ping.COMMAND_NAME, Ping.COMMAND_DESC) {

            @Override
            void run(String[] args)
                throws Exception {

                Ping.main(makeArgs(args));
            }

            @Override
            String getUsageArgs() {
                return Ping.COMMAND_ARGS;
            }
        },

        new Command(VERSION_COMMAND_NAME, VERSION_COMMAND_DESC) {

            @Override
            void run(String[] args) {
                KVVersion.main(makeArgs(args));
            }

            @Override
            String getUsageArgs() {
                return null;
            }

            @Override
            boolean isHelpCommand() {
                return true;
            }
        },

        new Command(GenerateConfig.COMMAND_NAME,
                    GenerateConfig.COMMAND_DESC) {

            @Override
            void run(String[] args) {
                GenerateConfig.main(makeArgs(args));
            }

            @Override
            String getUsageArgs() {
                return GenerateConfig.COMMAND_ARGS;
            }
        },

        new Command(HELP_COMMAND_NAME, HELP_COMMAND_DESC) {

            @Override
            void run(String[] args) {
                doHelpCommand(args);
            }

            @Override
            String getUsageArgs() {
                final StringBuilder builder = new StringBuilder();
                builder.append('[');
                builder.append(HELP_COMMANDS_COMMAND);
                for (final Command cmd : ALL_COMMANDS) {
                    builder.append(" |\n\t ");
                    builder.append(cmd.name);
                }
                builder.append(']');
                return builder.toString();
            }

            @Override
            boolean isHelpCommand() {
                return true;
            }
        },
    };

    /**
     * For transforming args when delegating this main() to the specific
     * command class main().  Delete first arg (the command) and add any
     * additional args specified.
     */
    private static String[] makeArgs(String[] origArgs, String... addArgs) {
        final int useOrigArgs = origArgs.length - 1;
        final String[] newArgs = new String[useOrigArgs + addArgs.length];
        System.arraycopy(origArgs, 1, newArgs, 0, useOrigArgs);
        System.arraycopy(addArgs, 0, newArgs, useOrigArgs, addArgs.length);
        return newArgs;
    }

    /**
     * Returns the Command with the given name, or null if name is not found.
     */
    private static Command findCommand(String name) {
        for (final Command cmd : ALL_COMMANDS) {
            if (cmd.name.equals(name)) {
                return cmd;
            }
        }
        return null;
    }

    /**
     * Delegates to Command object named by first arg.  If no args, delegates
     * to 'help' Command.
     */
    @SuppressWarnings("null")
    public static void main(String args[])
        throws Exception {

        final String cmdName =
            (args.length == 0) ? HELP_COMMAND_NAME : args[0];

        final Command cmd = findCommand(cmdName);
        if (cmd == null) {
            usage("Unknown command: " + cmdName);
        }

        /* Note that cmd will not be null, because usage will do an exit. */
        if (findVerbose(args) && !cmd.isHelpCommand()) {
            System.err.println("Enter command: " + cmdName);
            cmd.run(args);
            System.err.println("Leave command: " + cmdName);
        } else {
            cmd.run(args);
        }
    }

    /**
     * Returns whether -verbose appears in the arg array.  This is the only
     * arg parsing necessary in this class, prior to delegating the command.
     */
    private static boolean findVerbose(String[] args) {
        for (final String arg : args) {
            if (arg.equals(CommandParser.VERBOSE_FLAG)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Implements 'help', 'help commands' and 'help COMMAND'.
     */
    private static void doHelpCommand(String[] args) {

        /* Just 'help', also used for no args. */
        if (args.length <= 1) {
            usage(null);
        }

        /* 'help <something>' */
        final String cmdName = args[1];

        /* 'help commands' */
        if (HELP_COMMANDS_COMMAND.equals(cmdName)) {
            System.err.println("Commands are:");
            for (final Command cmd : ALL_COMMANDS) {
                System.err.println("  " + cmd.name + "\n\t# " +
                                   cmd.description);
            }
            System.err.println("\nFlags used by the commands are:" +
                               FLAG_DESCRIPTIONS);
            usageExit();
        }

        /* 'help <command>' */
        final Command cmd = findCommand(cmdName);
        if (cmd == null) {
            usage("Unknown 'help' topic: " + args[1]);
        }

        /* Note that cmd will not be null, because usage will do an exit. */
        @SuppressWarnings("null")
        final String usageArgs = cmd.getUsageArgs();
        System.err.println
            (CommandParser.KVSTORE_USAGE_PREFIX + cmdName + " " +
             (cmd.isHelpCommand() ? "" :
              CommandParser.optional(CommandParser.VERBOSE_FLAG)) +
             ((usageArgs == null) ?  "" : ("\n\t" + usageArgs)));
        System.err.println("# " + cmd.description);
        usageExit();
    }

    /**
     * Top-level usage command.
     */
    private static void usage(String errorMsg) {
        if (errorMsg != null) {
            System.err.println(errorMsg);
        }
        final StringBuilder builder = new StringBuilder();
        builder.append(CommandParser.KVSTORE_USAGE_PREFIX);
        builder.append("\n  <");
        builder.append(ALL_COMMANDS[0].name);
        for (int i = 1; i < ALL_COMMANDS.length; i += 1) {
            builder.append(" |\n   ");
            builder.append(ALL_COMMANDS[i].name);
        }
        builder.append("> [-verbose] [args]");
        builder.append("\nUse \"help <commandName>\" to get usage for a ");
        builder.append("specific command");
        builder.append("\nUse \"help commands\" to get detailed usage ");
        builder.append("information");
        builder.append("\nUse the -verbose flag to get debugging output");
        System.err.println(builder);
        usageExit();
    }

    /**
     * Does System.exit on behalf of all usage commands.
     */
    private static void usageExit() {
        System.exit(2);
    }

    /**
     * Implements 'makebootconfig' command.
     */
    private static void makeBootConfig(String[] args) {

        final MakeConfigParser cp = new MakeConfigParser(args);
        cp.parseArgs();
        final File rootDir = new File(cp.getRootDir());

        /* Write security policy file. */
        final File secFile = new File(rootDir, "security.policy");
        if (secFile.exists()) {
            System.err.println(secFile.toString() + " exists, not creating");
        } else {
            cp.verbose("Creating " + secFile);
            ConfigUtils.createSecurityPolicyFile(secFile);
        }

        /* Write bootstrap config file. */
        final File configFile = new File(rootDir, cp.configFile);
        if (configFile.exists()) {
            System.err.println(configFile.toString() + " exists, not creating");
        } else {
            cp.getSecurityConfig().populateDefaults();
            final File secDir = new File(cp.getRootDir(), cp.getSecurityDir());

            if (cp.getSecurityAction().equals(SECURITY_CONFIGURE)) {
                if (secDir.exists()) {
                    System.err.println(
                        secDir + " exists, not creating");
                } else {
                    SecurityConfigCreator scCreator =
                        new SecurityConfigCreator(
                            cp.getRootDir(),
                            cp.getSecurityConfig(),
                            new GenericIOHelper(System.out));
                    try {
                        scCreator.createConfig();
                    } catch (Exception e) {
                        System.err.println("Caught exception " + e);
                        return;
                    }
                }
            } else if (cp.getSecurityAction().equals(SECURITY_NONE)) {
                if (secDir.exists()) {
                    System.err.println(
                        secDir + " will be ignored because -store-security " +
                        "none was specified.");
                }
            } else if (cp.getSecurityAction().equals(SECURITY_ENABLE)) {
                if (!secDir.exists()) {
                    System.err.println(secDir + " does not exist. Be " +
                                       "sure to create it before starting " +
                                       "the server.");
                } else if (!isSecurityDir(secDir)) {
                    System.err.println(secDir+ " does not appear to be " +
                                       "a valid security configuration.  Be " +
                                       "sure to replace it with a security " +
                                       "configuration before starting the " +
                                       "server.");
                }
            }

            cp.verbose("Creating " + configFile);
            final BootstrapParams bp = cp.getBootstrapParams();
            ConfigUtils.createBootstrapConfig(bp, configFile);
        }
    }

    /**
     * Check to see whether the named directory exists and appears to contain
     * a security directory.
     */
    private static boolean isSecurityDir(File secDir) {
        if (!secDir.exists() || !secDir.isDirectory()) {
            return false;
        }

        final String[] checkNames = {
            FileNames.SECURITY_CONFIG_FILE,
            "store.keys",
            "store.trust" };
        for (String name : checkNames) {
            if (!new File(secDir, name).exists()) {
                return false;
            }
        }
        
        /*
         * There is more validation that could be done, but this looks at
         * least plausible as a security directory.
         */
        return true;
    }

    /**
     * Arg parser for 'makebootconfig' command.
     */
    private static class MakeConfigParser extends CommandParser {

        MakeConfigParser(String[] args) {
            super(args);
            mgmtParser = new MgmtUtil.ConfigParserHelper(this, false);
            securityParser =
                new SecurityConfigCommand.ConfigParserHelper(this);
        }

        /*
         * [#21880] explains that -storagedir is more descriptive and correct
         * than the previous flag, -mount. Ideally, we'd share this flag
         * constant with the CLI processor, but don't want to pull in
         * server side dependency to client code.
         */
        public static final String MOUNT_FLAG = "-storagedir";
        public static final String CAPACITY_FLAG = "-capacity";
        public static final String CPU_FLAG = "-num_cpus";
        public static final String MEMORY_FLAG = "-memory_mb";
        private static final String SERVICERANGE_FLAG = "-servicerange";
        private static final String STORE_SECURITY_FLAG = "-store-security";
        private static final String FORCE_ADMIN_FLAG = "-runadmin";

        String configFile = StorageNodeAgent.DEFAULT_CONFIG_FILE;
        String haPortRange = null;
        String haHostname = null;
        String servicePortRange = null;
        String securityAction = null;
        MgmtUtil.ConfigParserHelper mgmtParser;
        SecurityConfigCommand.ConfigParserHelper securityParser;
        List<String> mountPoints = new ArrayList<String>();
        int capacity = 0;
        int num_cpus = 0;
        int memory_mb = 0;
        boolean isRunAdmin = false;

        public List<String> getMountPoints() {
            return mountPoints;
        }

        public int getCapacity() {
            return capacity;
        }

        public int getNumCPUs() {
            return num_cpus;
        }

        public int getMemoryMB() {
            return memory_mb;
        }

        public String getSecurityAction() {
            return securityAction;
        }

        public String getSecurityDir() {
            return getSecurityConfig().getSecurityDir();
        }

        private ParsedConfig getSecurityConfig() {
            return securityParser.getConfig();
        }

        @Override
        public void verifyArgs() {
            if (getRootDir() == null) {
                missingArg(CommandParser.ROOT_FLAG);
            }
            if (getHostname() == null) {
                missingArg(CommandParser.HOST_FLAG);
            }
            if (getRegistryPort() == 0) {
                missingArg(CommandParser.PORT_FLAG);
            }
            if (haPortRange == null) {
                missingArg(HARANGE_FLAG);
            }
            if (securityAction == null) {
                missingArg(STORE_SECURITY_FLAG);
            }

        }

        @Override
        public boolean checkArg(String arg) {
            if (arg.equals(StorageNodeAgent.CONFIG_FLAG)) {
                configFile = nextArg(arg);
                return true;
            }
            if (arg.equals(SERVICERANGE_FLAG)) {
                final String nextArg = nextArg(arg);
                try {
                    PortRange.validateService(nextArg);
                } catch (RuntimeException e) {
                    usage(e.getMessage());
                }
                servicePortRange = nextArg;
                return true;
            }

            if (arg.equals(HARANGE_FLAG)) {
                final String nextArg = nextArg(arg);
                try {
                    PortRange.validateHA(nextArg);
                } catch (RuntimeException e) {
                    usage(e.getMessage());
                }
                haPortRange = nextArg;
                return true;
            }
            if (arg.equals(HAHOST_FLAG)) {
                haHostname = nextArg(arg);
                return true;
            }
            if (arg.equals(MOUNT_FLAG)) {
                final String nextArg = nextArg(arg);
                mountPoints.add(nextArg);
                return true;
            }
            if (arg.equals(CAPACITY_FLAG)) {
                final String nextArg = nextArg(arg);
                capacity = Integer.parseInt(nextArg);
                return true;
            }
            if (arg.equals(CPU_FLAG)) {
                final String nextArg = nextArg(arg);
                num_cpus = Integer.parseInt(nextArg);
                return true;
            }
            if (arg.equals(MEMORY_FLAG)) {
                final String nextArg = nextArg(arg);
                memory_mb = Integer.parseInt(nextArg);
                return true;
            }
            if (arg.equals(STORE_SECURITY_FLAG)) {
                final String nextArg = nextArg(arg);
                securityAction = nextArg;
                if (!(securityAction.equals(SECURITY_CONFIGURE) ||
                      securityAction.equals(SECURITY_ENABLE) ||
                      securityAction.equals(SECURITY_NONE))) {
                    usage("The value '" + securityAction +
                          "' is not valid for " + STORE_SECURITY_FLAG);
                }
                return true;
            }
            if (arg.equals(FORCE_ADMIN_FLAG)) {
                isRunAdmin = true;
                return true;
            }
            if (securityParser.checkArg(arg)) {
                return true;
            }
            return mgmtParser.checkArg(arg);
        }

        @Override
        public void usage(String errorMsg) {
            if (errorMsg != null) {
                System.err.println(errorMsg);
            }
            System.err.println(KVSTORE_USAGE_PREFIX +
                               MAKECONFIG_COMMAND_NAME + "\n\t" +
                               MAKECONFIG_COMMAND_ARGS);
            usageExit();
        }

        private BootstrapParams getBootstrapParams() {
            final boolean isSecure = !getSecurityAction().equals(SECURITY_NONE);
            final BootstrapParams bp = new BootstrapParams
                (null /*rootDir*/, getHostname(), haHostname,
                 haPortRange, servicePortRange,
                 null /*storeName*/, getRegistryPort(),
                 getAdminPort(), getCapacity(), getMountPoints(),
                 isSecure);

            if (getNumCPUs() != 0) {
                bp.setNumCPUs(getNumCPUs());
            }
            if (getMemoryMB() != 0) {
                bp.setMemoryMB(getMemoryMB());
            }
            if (isSecure) {
                bp.setSecurityDir(getSecurityDir());
            }
            bp.setForceBootstrapAdmin(isRunAdmin);

            mgmtParser.apply(bp);
            return bp;
        }
    }
}
