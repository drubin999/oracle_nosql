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
import java.io.Console;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class ShellInputReader {
    /**
     * Used APIs for Jline2.0
     **/
    private final static String JL2_READER_CLS = "jline.console.ConsoleReader";
    private final static String JL2_TERMINAL_CLS = "jline.Terminal";

    private final static int MID_JL2_CR_READLINE = 0x00;
    private final static int MID_JL2_CR_GETTERM = 0x01;

    private final static String[] METHODS_JL2_READER = {
        "readLine", "getTerminal"
    };
    private final static int MID_JL2_TERM_GETHEIGHT = 0x00;
    private final static String[] METHODS_JL2_TERM = {
        "getHeight"
    };
    private final static MethodDef[] JL2ReaderMethodsDef = {
        new MethodDef(METHODS_JL2_READER[MID_JL2_CR_READLINE],
                      new Class[]{String.class, Character.class}),
        new MethodDef(METHODS_JL2_READER[MID_JL2_CR_GETTERM],
                      null)
    };
    private final static MethodDef[] JL2TermMethodsDef = {
        new MethodDef(METHODS_JL2_TERM[MID_JL2_TERM_GETHEIGHT], null)
    };
    /* default value for terminal height */
    private final static int TERMINAL_HEIGHT_DEFAULT = 25;

    private Object jReaderObj = null;
    private BufferedReader inputReader = null;
    private PrintStream output = null;
    private Map<String, Method> jReaderMethods = null;
    private Map<String, Method> jTermMethods = null;
    private String prompt = "";

    public ShellInputReader(InputStream input, PrintStream output) {
        initInputReader(input, output);
        this.output = output;
    }

    private void initInputReader(InputStream input, PrintStream output1) {
        if (isJlineCompatiblePlatform()) {
            try {
                Class<?> jReader = Class.forName(JL2_READER_CLS);
                Class<?> jTerminal = Class.forName(JL2_TERMINAL_CLS);
                jReaderMethods = new HashMap<String, Method>();
                for (MethodDef mdef: JL2ReaderMethodsDef) {
                    Method mtd = jReader.getMethod(mdef.getName(),
                                                   mdef.getArgTypes());
                    jReaderMethods.put(mdef.getName(), mtd);
                }

                jTermMethods = new HashMap<String, Method>();
                for (MethodDef mdef: JL2TermMethodsDef) {
                    Method mtd = jTerminal.getMethod(mdef.getName(),
                                                     mdef.getArgTypes());
                    jTermMethods.put(mdef.getName(), mtd);
                }

                /* Initialize ConsoleReader instance. */
                Constructor<?> csr = jReader.getConstructor(InputStream.class,
                                                            OutputStream.class);
                jReaderObj = csr.newInstance(input, output1);
            } catch (Exception ignored)  /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
        }
        if (jReaderObj == null) {
            /* Use normal inputStreamReader if failed to load jline library */
            inputReader = new BufferedReader(new InputStreamReader(input));
        }
    }

    private boolean isJlineCompatiblePlatform() {
        String os = System.getProperty("os.name").toLowerCase();
        if (os.indexOf("windows") != -1) {
	    /*
	     * Disable jline on Windows because of a Cygwin problem:
             * https://github.com/jline/jline2/issues/62
	     * This will be fixed in a later patch.
             */
            return false;
        }
        return true;
    }

    public void setDefaultPrompt(String prompt) {
        this.prompt = prompt;
    }

    public String getDefaultPrompt() {
        return this.prompt;
    }

    public String readLine()
        throws IOException {

        return readLine(null);
    }

    public String readLine(String promptString)
        throws IOException {

        String prompt1 = (promptString != null)?promptString:this.prompt;
        if (jReaderObj != null) {
            String name = METHODS_JL2_READER[MID_JL2_CR_READLINE];
            return (String)invokeJReaderMethod(jReaderObj, name,
                                               new Object[]{prompt1, null});
        }
		output.print(prompt1);
		return inputReader.readLine();
    }

    public char[] readPassword(String promptString) throws IOException {
        String input = null;
        final String pwdPrompt = (promptString != null) ? promptString :
                                                          this.prompt;

        if (jReaderObj != null) {
            final String name = METHODS_JL2_READER[MID_JL2_CR_READLINE];
            input = (String)invokeJReaderMethod(
                jReaderObj, name, new Object[]{pwdPrompt,
                                               new Character((char) 0)});
            return input == null ? null : input.toCharArray();
        }

        final Console console = System.console();
        if (console != null) {
            return console.readPassword(pwdPrompt);
        }

        output.print(pwdPrompt);
        input = inputReader.readLine();
        return input == null ? null : input.toCharArray();
    }

    public int getTerminalHeight() {
        if (jReaderObj != null) {
            try {
                String name = METHODS_JL2_READER[MID_JL2_CR_GETTERM];
                Object jTermObj = invokeJReaderMethod(jReaderObj, name, null);
                name = METHODS_JL2_TERM[MID_JL2_TERM_GETHEIGHT];
                return (Integer)invokeJTermMethod(jTermObj, name, null);
            } catch (IOException ignored)  /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
        }
        return getTermHeightImpl();
    }

    private Object invokeJReaderMethod(Object jReaderObj1,
                                       String name,
                                       Object[] args)
        throws IOException {

        Method method = null;
        if (!jReaderMethods.containsKey(name)) {
            throw new IOException("Method " + name +
                " of Jline.ConsoleReader is not initialized.");
        }
        method = jReaderMethods.get(name);
        return invokeMethod(jReaderObj1, method, args);
    }

    private Object invokeJTermMethod(Object jTermObj,
                                     String name,
                                     Object[] args)
        throws IOException {

        Method method = null;
        if (!jTermMethods.containsKey(name)) {
            throw new IOException("Method " + name + " of " +
                                  JL2_TERMINAL_CLS + " is not initialized.");
        }
        method = jTermMethods.get(name);
        return invokeMethod(jTermObj, method, args);
    }

    private Object invokeMethod(Object obj, Method method, Object[] args)
        throws IOException {

        String name = method.getName();
        try {
            return method.invoke(obj, args);
        } catch (IllegalAccessException iae) {
            throw new IOException("Invoke method " + name +
                                  " of Jline.ConsoleReader failed", iae);
        } catch (IllegalArgumentException iae) {
            throw new IOException("Invoke method " + name +
                                  " of Jline.ConsoleReader failed", iae);
        } catch (InvocationTargetException ite) {
            throw new IOException("Invoke method " + name +
                                  " of Jline.ConsoleReader failed", ite);
        }
    }

    private int getTermHeightImpl() {
        String os = System.getProperty("os.name").toLowerCase();
        if (os.indexOf("windows") != -1) {
            return TERMINAL_HEIGHT_DEFAULT;
        }
		int height = getUnixTermHeight();
		if (height == -1) {
		    height = TERMINAL_HEIGHT_DEFAULT;
		}
		return height;
    }

    /*
     * stty -a
     *  speed 38400 baud; rows 48; columns 165; line = 0; ...
     */
    private int getUnixTermHeight() {
        String ttyProps = null;
        final String name = "rows";
        try {
            ttyProps = getTermSttyProps();
            if (ttyProps != null && ttyProps.length() > 0) {
                return getTermSttyPropValue(ttyProps, name);
            }
        } catch (IOException ignored)  /* CHECKSTYLE:OFF */ {
        } catch (InterruptedException ignored) {
        } /* CHECKSTYLE:ON */
        return -1;
    }

    private String getTermSttyProps()
        throws IOException, InterruptedException {

        String[] cmd = {"/bin/sh", "-c", "stty -a </dev/tty"};
        Process proc = Runtime.getRuntime().exec(cmd);

        String s = null;
        StringBuilder sb = new StringBuilder();
        BufferedReader stdInput =
            new BufferedReader(new InputStreamReader(proc.getInputStream()));
        while ((s = stdInput.readLine()) != null) {
            sb.append(s);
        }

        BufferedReader stdError =
            new BufferedReader(new InputStreamReader(proc.getErrorStream()));
        while ((s = stdError.readLine()) != null) {
            sb.append(s);
        }

        proc.waitFor();
        return sb.toString();
    }

    private int getTermSttyPropValue(String props, String name) {
        StringTokenizer tokenizer = new StringTokenizer(props, ";\n");
        while (tokenizer.hasMoreTokens()) {
            String str = tokenizer.nextToken().trim();
            if (str.startsWith(name)) {
                return Integer.parseInt(
                        str.substring(name.length() + 1, str.length()));
            } else if (str.endsWith(name)) {
                return Integer.parseInt(
                        str.substring(0, (str.length() - name.length() - 1)));
            }
        }
        return 0;
    }

    private static class MethodDef {
        private final String name;
        private final Class<?>[] argsTypes;

        MethodDef(String name, Class<?>[] types) {
            this.name = name;
            this.argsTypes = types;
        }

        public String getName() {
            return this.name;
        }

        public Class<?>[] getArgTypes() {
            return this.argsTypes;
        }
    }
}
