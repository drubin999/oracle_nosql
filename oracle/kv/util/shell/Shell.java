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

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import oracle.kv.KVSecurityException;

/**
 *
 * Simple framework for a command line shell.  See CommandShell.java for a
 * concrete implementation.
 */
public abstract class Shell {
    protected final InputStream input;
    protected final PrintStream output;
    private ShellInputReader inputReader = null;
    protected final CommandHistory history;
    private int exitCode;
    protected boolean showHidden = false;
    private VariablesMap shellVariables = null;
    protected Stack<ShellCommand> stCurrentCommands = null;
    protected boolean isSecured = false;

    public final static String tab = "\t";
    public final static String eol = System.getProperty("line.separator");
    public final static String eolt = eol + tab;

    /*
     * These are somewhat standard exit codes from sysexits.h
     */
    public final static int EXIT_OK = 0;
    public final static int EXIT_USAGE = 64; /* usage */
    public final static int EXIT_INPUTERR = 65; /* bad argument */
    public final static int EXIT_UNKNOWN = 1; /* unknown exception */
    public final static int EXIT_NOPERM = 77; /* permission denied */

    /*
     * This variable changes per-command which means that things must be
     * single-threaded, which they are at this time.  The command line
     * parsing consumes any "-verbose" flag and sets this variable for
     * access by commands.
     */
    private boolean verbose = false;

    /*
     * This variable is toggled by the "verbose" command
     */
    private boolean global_verbose = false;

    /*
     * This is used to terminate the interactive loop
     */
    private boolean terminate = false;

    /*
     * These must be implemented by specific shell classes
     */
    public abstract List<? extends ShellCommand> getCommands();

    public abstract String getPrompt();

    public abstract String getUsageHeader();

    public abstract void init();

    public abstract void shutdown();

    /*
     * Concrete implementation
     */
    public Shell(InputStream input, PrintStream output) {
        this.input = input;
        this.output = output;
        history = new CommandHistory();
        stCurrentCommands = new Stack<ShellCommand>();
        shellVariables = new VariablesMap();
    }

    public String getUsage() {
        String usage = getUsageHeader();
        for (ShellCommand cmd : getCommands()) {
            if (!showHidden && cmd.isHidden()) {
                continue;
            }

            String help = cmd.getCommandName();
            if (help != null) {
                usage += tab + help + eol;
            }
        }
        return usage;
    }

    public boolean showHidden() {
        return showHidden;
    }

    protected boolean toggleHidden() {
        showHidden = !showHidden;
        return showHidden;
    }

    public void prompt() {
        String prompt = getPrompt();
        if (prompt != null) {
            output.print(prompt);
        }
    }

    /* Push a current command to stack. */
    public void pushCurrentCommand(ShellCommand command) {
        stCurrentCommands.push(command);
    }

    /* Get the current command on top of stack. */
    public ShellCommand getCurrentCommand() {
        if (stCurrentCommands.size() > 0) {
            return stCurrentCommands.peek();
        }
        return null;
    }

    /* Pop the current command on the top of stack. */
    public void popCurrentCommand() {
        stCurrentCommands.pop();
    }

    /* Return the customized propmt string of the current command. */
    public String getCurrentCommandPropmt() {
        ShellCommand command = getCurrentCommand();
        if (command == null) {
            return null;
        }

        Iterator<ShellCommand> it = stCurrentCommands.iterator();
        StringBuilder sb = new StringBuilder();
        while (it.hasNext()) {
            ShellCommand cmd = it.next();
            if (cmd.getPrompt() != null) {
                if (sb.length() > 0) {
                    sb.append(".");
                }
                sb.append(cmd.getPrompt());
            }
        }
        if (sb.length() > 0) {
            sb.append("-> ");
        }
        return sb.toString();
    }

    /* Store a variable. */
    public void addVariable(String name, Object value) {
        shellVariables.add(name, value);
    }

    /* Get the value of a variable. */
    public Object getVariable(String name) {
        return shellVariables.get(name);
    }

    /* Get all variables. */
    public Set<Entry<String, Object>> getAllVariables() {
        return shellVariables.getAll();
    }

    /* Remove a variable. */
    public void removeVariable(String name) {
        shellVariables.remove(name);
    }

    /* Remove all variables. */
    public void removeAllVariables() {
        shellVariables.reset();
    }

    public boolean doRetry() {
        return false;
    }

    public boolean handleShellException(String line, ShellException se) {
        /* do one retry */
        if (doRetry()) {
            return true;
        }

        if (se instanceof ShellHelpException) {
            history.add(line, null);
            output.println(((ShellHelpException)se).getVerboseHelpMessage());
            exitCode = EXIT_USAGE;
            return false;
        } else if (se instanceof ShellUsageException) {
            history.add(line, null);
            ShellUsageException sue = (ShellUsageException) se;
            output.println(sue.getMessage() + eol +
                           sue.getVerboseHelpMessage());
            exitCode = EXIT_USAGE;
            return false;
        } else if (se instanceof ShellArgumentException) {
            history.add(line, null);
            output.println(se.getMessage());
            exitCode = EXIT_INPUTERR;
            return false;
        }

        history.add(line, se);
        output.println("Error handling command " + line + ": " +
                       se.getMessage());
        return false;
    }

    public void handleUnknownException(String line, Exception e) {
        history.add(line, e);
        exitCode = EXIT_UNKNOWN;
        output.println("Unknown Exception: " + e.getClass());
    }

    /**
     * General handler of KVSecurityException. The default behavior is to log
     * the command and output error messages.
     *
     * @param line command line
     * @param kvse instance of AuthenticationRequiredException
     * @return true only if a retry is intentional
     */
    public boolean
        handleKVSecurityException(String line,
                                  KVSecurityException kvse) {
        history.add(line, kvse);
        output.println("Error handling command " + line + ": " +
                       kvse.getMessage());
        exitCode = EXIT_NOPERM;
        return false;
    }

    public void verboseOutput(String msg) {
        if (verbose || global_verbose) {
            output.println(msg);
        }
    }

    public void setTerminate() {
        terminate = true;
    }

    public boolean getTerminate() {
        return terminate;
    }

    /*
     * The primary loop that reads lines and dispatches them to the appropriate
     * command.
     */
    public void loop() {
        try {
            /* initialize input reader */
            inputReader = new ShellInputReader(this.input, this.output);
            inputReader.setDefaultPrompt(getPrompt());
            String line = null;
            while (!terminate) {
                try {
                    line = inputReader.readLine(getCurrentCommandPropmt());
                    if (line == null) {
                        break;
                    }
                } catch (IOException ioe) {
                    output.println("Exception reading input: " + ioe);
                    continue;
                }
                execute(line);
            }
        } finally {
            shutdown();
        }
    }

    public void println(String msg) {
        output.println(msg);
    }

    /*
     * Encapsulates runLine in try/catch blocks for calls from external tools
     * that construct Shell directly.  This is also used by loop().  This
     * function trims leading/trailing white space from the line.
     */
    public void execute(String line) {
        line = line.trim();
        try {
            if (line.length() == 0) {
                return;
            }
            try {
                runLine(line);
            } catch (KVSecurityException kvse) {
                /* Returns true to give a chance to retry the command once. */
                if (handleKVSecurityException(line, kvse)) {
                    runLine(line);
                }
            }
        } catch (ShellException se) {
            /* returns true if a retry is in order */
            if (handleShellException(line, se)) {
                execute(line);
            }
        } catch (Exception e) {
            handleUnknownException(line, e);
        }
    }

    public ShellCommand findCommand(String commandName) {
        for (ShellCommand command : getCommands()) {
            if (command.matches(commandName)) {
                return command;
            }
        }
        return null;
    }

    /*
     * Extract the named flag.  The flag must exist in the args
     */
    public static String[] extractArg(String[] args, String arg) {
        String[] retArgs = new String[args.length - 1];
        int i = 0;
        for (String s : args) {
            if (! arg.equals(s)) {
                retArgs[i++] = s;
            }
        }
        return retArgs;
    }

    private String[] checkVerbose(String[] args) {
        String[] retArgs = args;
        if (checkArg(args, "-verbose")) {
            verbose = true;
            retArgs = extractArg(args, "-verbose");
        }
        return retArgs;
    }

    /*
     * Parse a single line.  Treat "#" as comments
     */
    public String[] parseLine(String line) {
        int tokenType;
        List<String> words = new ArrayList<String>();
        StreamTokenizer st = new StreamTokenizer(new StringReader(line));

        st.resetSyntax();
        st.whitespaceChars(0, ' ');
        st.wordChars('!', 255);
        st.quoteChar('"');
        st.quoteChar('\'');
        st.commentChar('#');

        while (true) {
            try {
                tokenType = st.nextToken();
                if (tokenType == StreamTokenizer.TT_WORD) {
                    words.add(st.sval);
                } else if (tokenType == '\'' || tokenType == '"') {
                    words.add(st.sval);
                } else if (tokenType == StreamTokenizer.TT_NUMBER) {
                    output.println("Unexpected numeric token!");
                } else {
                    break;
                }
            } catch (IOException e) {
                break;
            }
        }
        return words.toArray(new String[words.size()]);
    }

    public void runLine(String line)
        throws ShellException {

        exitCode = EXIT_OK;
        if (line.length() > 0 && !line.startsWith("#")) {
            String[] splitArgs = parseLine(line);
            String commandName = splitArgs[0];
            String result = run(commandName, splitArgs);
            if (result != null) {
                output.println(result);
            }
            history.add(line, null);
        }
    }

    public String run(String commandName, String[] args)
        throws ShellException {

        ShellCommand command = null;
        String[] cmdArgs = null;

        command = getCurrentCommand();
        if (command != null) {
            cmdArgs = new String[args.length + 1];
            cmdArgs[0] = command.getCommandName();
            System.arraycopy(args, 0, cmdArgs, 1, args.length);
        } else {
            command = findCommand(commandName);
            cmdArgs = args;
        }

        if (command != null) {
            verbose = false; /* reset */
            cmdArgs = checkVerbose(cmdArgs);
            return command.execute(cmdArgs, this);
        }

        throw new ShellArgumentException(
            "Could not find command: " + commandName + eol + getUsage());
    }

    public boolean getVerbose() {
        return (verbose || global_verbose);
    }

    public PrintStream getOutput() {
        return output;
    }

    public ShellInputReader getInput() {
        return inputReader;
    }

    public boolean toggleVerbose() {
        global_verbose = !global_verbose;
        return global_verbose;
    }

    public int getExitCode() {
        return exitCode;
    }

    public CommandHistory getHistory() {
        return history;
    }

    public static String nextArg(String[] args, int index, ShellCommand cmd)
        throws ShellException {

        if (++index < args.length) {
            return args[index];
        }
        throw new ShellUsageException
            ("Flag " + args[index-1] + " requires an argument", cmd);
    }

    public void invalidArgument(String arg, ShellCommand command)
        throws ShellException {

        String msg = "Invalid argument: " + arg + eolt +
            command.getBriefHelp();
        throw new ShellArgumentException(msg);
    }

    public void unknownArgument(String arg, ShellCommand command)
        throws ShellException {

        String msg = "Unknown argument: " + arg;
        throw new ShellUsageException(msg, command);
    }

    public void badArgCount(ShellCommand command)
        throws ShellException {

        String msg = "Incorrect number of arguments for command: " +
            command.getCommandName();
        throw new ShellUsageException(msg, command);
    }

    public void badArgUsage(String arg, String info, ShellCommand command)
        throws ShellException {

        String msg = "Invalid usage of the " + arg +
            " argument to the command: " + command.getCommandName();
        if (info != null && !info.isEmpty()) {
            msg = msg + " - " + info;
        }
        throw new ShellUsageException(msg, command);
    }

    public void requiredArg(String arg, ShellCommand command)
        throws ShellException {

        String msg = "Missing required argument" +
            ((arg != null) ? " (" + arg + ")" : "")
            + " for command: " +
            command.getCommandName();
        throw new ShellUsageException(msg, command);
    }

    public static String makeWhiteSpace(int indent) {
        String ret = "";
        for (int i = 0; i < indent; i++) {
            ret += " ";
        }
        return ret;
    }

    /*
     * Look for -help or ? or -? in a command line.  This method makes it easy
     * for commands to accept -help or related flags later in the command line
     * and interpret them as help requests.
     */
    public static void checkHelp(String[] args, ShellCommand command)
        throws ShellException {

        for (String s : args) {
            String sl = s.toLowerCase();
            if (sl.equals("-help") ||
                sl.equals("help") ||
                sl.equals("?") ||
                sl.equals("-?")) {
                throw new ShellHelpException(command);
            }
        }
    }

    /*
     * Return true if the named argument is in the command array.
     * The arg parameter is expected to be in lower case.
     */
    public static boolean checkArg(String[] args, String arg) {
        for (String s : args) {
            String sl = s.toLowerCase();
            if (sl.equals(arg)) {
                return true;
            }
        }
        return false;
    }

    /*
     * Return the item following the specified flag, e.g. the caller may be
     * looking for the argument to a -name flag.  Return null if it does not
     * exist.
     */
    public static String getArg(String[] args, String arg) {
        boolean returnNext = false;
        for (String s : args) {
            if (returnNext) {
                return s;
            }
            String sl = s.toLowerCase();
            if (sl.equals(arg)) {
                returnNext = true;
            }
        }
        return null;
    }

    public static boolean matches(String inputName,
                                  String commandName) {
        return matches(inputName, commandName, 0);
    }

    public static boolean matches(String inputName,
                                  String commandName,
                                  int prefixMatchLength) {

        if (inputName.length() < prefixMatchLength) {
            return false;
        }

        if (prefixMatchLength > 0) {
            String match = inputName.toLowerCase();
            return (commandName.toLowerCase().startsWith(match));
        }

        /* Use the entire string for comparison */
        return commandName.toLowerCase().equals(inputName.toLowerCase());
    }

    public static class LoadCommand extends ShellCommand {

        public LoadCommand() {
            super("load", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            String path = null;
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if ("-file".equals(arg)) {
                    path = Shell.nextArg(args, i++, this);
                } else {
                    shell.unknownArgument(arg, this);
                }
            }
            if (path == null) {
                shell.requiredArg("-file", this);
            }

            FileReader fr = null;
            BufferedReader br = null;
            String retString = "";
            try {
                fr = new FileReader(path);
                br = new BufferedReader(fr);
                String line;
                while ((line = br.readLine()) != null &&
                       !shell.getTerminate()) {
                    try {
                        shell.runLine(line);
                    } catch (ShellException se) {
                        /* stop execution if false is returned */
                        if (!shell.handleShellException(line, se)) {
                            retString = "Script error in line \"" + line +
                                        "\", ending execution";
                            break;
                        }
                    } catch (Exception e) {
                        /* Unknown exceptions will terminate the script */
                        shell.handleUnknownException(line, e);
                        break;
                    }
                }
            } catch (IOException ioe) {
                return "Failed to load file: " + path;
            } finally {
                if (fr != null) {
                    try {
                        fr.close();
                    } catch (IOException ignored) /* CHECKSTYLE:OFF */ {
                    } /* CHECKSTYLE:ON */
                }
            }
            return retString;
        }

        @Override
        protected String getCommandSyntax() {
            return "load -file <path to file>";
        }

        @Override
        protected String getCommandDescription() {
            return
                "Load the named file and interpret its contents as a script " +
                "of commands" + eolt + "to be executed.  If any command in " +
                "the script fails execution will end.";
        }
    }

    public static class HelpCommand extends ShellCommand {

        public HelpCommand() {
            super("help", 2);
        }

        @Override
        protected boolean matches(String commandName) {
            return ("?".equals(commandName) ||
                    super.matches(commandName));
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            if (args.length == 1) {
                return shell.getUsage();
            }

            /* per-command help */
            String commandName = args[1];
            ShellCommand command = shell.findCommand(commandName);
            if (command != null) {
                return(command.getHelp(Arrays.copyOfRange(args, 1, args.length),
                                       shell));
            }
            return("Could not find command: " + commandName +
                   eol + shell.getUsage());
        }

        @Override
        protected String getCommandSyntax() {
            return "help [command [sub-command]]";
        }

        @Override
        protected String getCommandDescription() {
            return
                "Print help messages.  With no arguments the top-level shell" +
                " commands" + eolt + "are listed.  With additional commands " +
                "and sub-commands, additional" + eolt + "detail is provided.";
        }
    }

    public static class ExitCommand extends ShellCommand {

        public ExitCommand() {
            super("exit", 2);
        }

        @Override
        protected boolean matches(String commandName) {
            return super.matches(commandName) ||
                   Shell.matches(commandName, "quit", 2);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            shell.setTerminate();
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "exit | quit";
        }

        @Override
        protected String getCommandDescription() {
            return "Exit the interactive command shell.";
        }
    }

    /*
     * Maintain command history.
     *
     * TODO: limit the size of the list -- requires a circular buffer.
     */
    public class CommandHistory {
        private final List<CommandHistoryElement> history1 =
            new ArrayList<CommandHistoryElement>(100);

        /**
         * Add a command to the history
         *
         * @param command the command to add
         * @param e Exception encountered on command if any, otherwise null
         */
        public void add(String command, Exception e) {
            history1.add(new CommandHistoryElement(command, e));
        }

        /**
         * Gets the specified element in the history
         *
         * @param which the offset in the history array
         * @return the specified command
         */
        public CommandHistoryElement get(int which) {
            if (history1.size() > which) {
                return history1.get(which);
            }
            output.println("No such command in history at offset " + which);
            return null;
        }

        public int getSize() {
            return history1.size();
        }

        /**
         * Dumps the current history
         */
        public String dump(int from, int to) {
            from = Math.min(from, history1.size());
            to = Math.min(to, history1.size() - 1);
            String hist = "";
            for (int i = from; i <= to; i++) {
                hist += dumpCommand(i, false /* withFault */);
            }
            return hist;
        }

        /*
         * Caller verifies range
         */
        public boolean commandFaulted(int command) {
            CommandHistoryElement cmd = history1.get(command);
            return (cmd.getException() != null);
        }

        /*
         * Caller verifies range
         */
        public String dumpCommand(int command, boolean withFault) {
            CommandHistoryElement cmd = history1.get(command);
            String res = "";
            res = cmd.getCommand();
            if (withFault && cmd.getException() != null) {
                ByteArrayOutputStream b = new ByteArrayOutputStream();
                cmd.getException().printStackTrace(new PrintWriter(b, true));
                res += eolt + b.toString();
            }
            return (command + " " + res + eol);
        }

        public String dumpFaultingCommands(int from, int to) {
            from = Math.min(from, history1.size());
            to = Math.min(to, history1.size() - 1);
            String hist = "";
            for (int i = from; i <= to; i++) {
                CommandHistoryElement cmd = history1.get(i);
                Exception e = cmd.getException();
                if (e != null) {
                    String res = "";
                    res = cmd.getCommand();
                    hist += (i + " " + res + ": " + e.getClass() + eol);
                }
            }
            return hist;
        }

        public Exception getLastException() {
            for (int i = history1.size() - 1; i >= 0; i--) {
                CommandHistoryElement cmd = history1.get(i);
                if (cmd.getException() != null) {
                    return cmd.getException();
                }
            }
            return null;
        }

        public String dumpLastFault() {
            for (int i = history1.size() - 1; i >= 0; i--) {
                CommandHistoryElement cmd = history1.get(i);
                if (cmd.getException() != null) {
                    return dumpCommand(i, true);
                }
            }
            return "";
        }
    }

    class CommandHistoryElement {
        String command;
        Exception exception;

        public CommandHistoryElement(String command, Exception exception) {
            this.command = command;
            this.exception = exception;
        }

        public String getCommand() {
            return command;
        }

        public Exception getException() {
            return exception;
        }
    }

    public static class CommandComparator implements Comparator<ShellCommand> {

        @Override
        public int compare(ShellCommand o1, ShellCommand o2) {
            return o1.getCommandName().compareTo(o2.getCommandName());
        }
    }

    /*
     * Maintain a HashMap to store variables.
     */
     public static class VariablesMap implements Cloneable {
        private final HashMap<String, Object> variablesMap =
            new HashMap<String, Object>();

        public void add(String name, Object value) {
            variablesMap.put(name, value);
        }

        public Object get(String name) {
            return variablesMap.get(name);
        }

        public Set<Entry<String, Object>> getAll() {
            return variablesMap.entrySet();
        }

        public void remove(String name) {
            if (variablesMap.containsKey(name)) {
                variablesMap.remove(name);
            }
        }

        public int size() {
            return variablesMap.size();
        }

        public void reset() {
            variablesMap.clear();
        }

        @Override
        public VariablesMap clone() {
            VariablesMap map = new VariablesMap();
            for (Map.Entry<String, Object> entry : variablesMap.entrySet()) {
                map.add(entry.getKey(), entry.getValue());
            }
            return map;
        }

        @Override
        public String toString() {
            String retString = "";
            for (Map.Entry<String, Object> entry: variablesMap.entrySet()) {
                retString += Shell.tab + entry.getKey() + ": " +
                             entry.getValue() + Shell.eol;
            }
            return retString;
        }
    }
}
