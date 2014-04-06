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

package oracle.kv.util;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintStream;

/**
 * This class is responsible for generating a Java enumeration source class
 * that can be referenced by throwers of NoSQLRuntimeException which need
 * standard error messages generated.  To affect changes in the generated Java
 * enumeration, follow the steps below;
 *
 * 1. Add a new message to the file
 *    kv/kvstore/resources/msgs/messages.properties.
 * 2. Run the ant target gen-messages this will produce the file
 *    kv/kvstore/src/oracle/kv/util/ErrorMessage.java.
 * 3. Pass the new enum that was generated (and any other message
 *    parameters) to the constructor of NoSQLRuntimeException.
 * 4. Check in your changes to messages.properties.
 */
public class MessageFileProcessor {
    public static final String MESSAGES_FILE_BASE_NAME = "messages";
    public static final String MESSAGES_FILE_SUFFIX = "properties";
    public static final String MESSAGES_FILE_NAME =
        MESSAGES_FILE_BASE_NAME + "." + MESSAGES_FILE_SUFFIX;

    private static final String ARG_CODELINE_BASEDIR = "-d";
    private static final String COMMENT_DELIMITER = "/";
    private static final String MESSAGE_LINE_DELIMETER = ",";
    private static final String LINE_SEP = "\n";
    private static final String FILE_SEP = System.getProperty("file.separator");

    private static final String GEN_CODE_PKG_NAME = "oracle.kv.util";
    private static final String GEN_CODE_CLASS_NAME = "ErrorMessage";
    private static final String ERR_CODE_ENUM_PREFIX = "NOSQL_";

    private static final String PATH_TO_MESSAGE_FILE = FILE_SEP +
        "resources" + FILE_SEP + "msgs" + FILE_SEP + MESSAGES_FILE_NAME;
    private static final String PATH_TO_GENERATED_JAVA_CLASS = FILE_SEP +
        "src" + FILE_SEP + "oracle" + FILE_SEP + "kv" + FILE_SEP +
        "util" + FILE_SEP + GEN_CODE_CLASS_NAME + ".java";

    private static final String WARNING_COMMENT =
        "/*-\n" +
        " *\n" +
        " *  This file is part of Oracle NoSQL Database\n" +
        " *  Copyright (C) 2011, 2014 Oracle and/or its affiliates.  All rights reserved.\n" +
        " *\n" +
        " *  Oracle NoSQL Database is free software: you can redistribute it and/or\n" +
        " *  modify it under the terms of the GNU Affero General Public License\n" +
        " *  as published by the Free Software Foundation, version 3.\n" +
        " *\n" +
        " *  Oracle NoSQL Database is distributed in the hope that it will be useful,\n" +
        " *  but WITHOUT ANY WARRANTY; without even the implied warranty of\n" +
        " *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU\n" +
        " *  Affero General Public License for more details.\n" +
        " *\n" +
        " *  You should have received a copy of the GNU Affero General Public\n" +
        " *  License in the LICENSE file along with Oracle NoSQL Database.  If not,\n" +
        " *  see <http://www.gnu.org/licenses/>.\n" +
        " *\n" +
        " *  An active Oracle commercial licensing agreement for this product\n" +
        " *  supercedes this license.\n" +
        " *\n" +
        " *  For more information please contact:\n" +
        " *\n" +
        " *  Vice President Legal, Development\n" +
        " *  Oracle America, Inc.\n" +
        " *  5OP-10\n" +
        " *  500 Oracle Parkway\n" +
        " *  Redwood Shores, CA 94065\n" +
        " *\n" +
        " *  or\n" +
        " *\n" +
        " *  berkeleydb-info_us@oracle.com\n" +
        " *\n" +
        " *  [This line intentionally left blank.]\n" +
        " *  [This line intentionally left blank.]\n" +
        " *  [This line intentionally left blank.]\n" +
        " *  [This line intentionally left blank.]\n" +
        " *  [This line intentionally left blank.]\n" +
        " *  [This line intentionally left blank.]\n" +
        " *  EOF\n" +
        " *\n" +
        " */\n\n" +
        "//---------------------------------------------------------------------------\n" +
        "//--------         DO NOT EDIT THIS FILE DIRECTLY                 -----------\n" +
        "//--------         See kv.util.MessageFileProcessor.java          -----------\n" +
        "//---------------------------------------------------------------------------\n";

    private static String codelineBaseDir = null;

    public static void main(String args[]) {
        int i = 0;
        while (i < args.length) {
            if (args[i].equals(ARG_CODELINE_BASEDIR)) {
                if (args.length <= i) {
                    usageAndExit();
                }
                codelineBaseDir = args[i + 1];
                i += 2;
            } else {
                usageAndExit();
            }
            i++;
        }

        if (codelineBaseDir == null) {
            usageAndExit();
        }

        try {
            genJavaEnumFromMsgFile(codelineBaseDir + PATH_TO_MESSAGE_FILE,
                                   codelineBaseDir +
                                   PATH_TO_GENERATED_JAVA_CLASS);
        } catch (IOException E) {
            System.out.println(E.toString());
            E.printStackTrace();
            System.exit(-1);
        }
    }

    /**
     * Generate Java source by parsing the messages file.
     *
     * @param msgFilePath A fully qualified path to the error messages file.
     *
     * @param generatedEnumSrcFilePath A fully qualified path to the Java
     * source file to be generated.  This file will be overwritten if it
     * already exists.
     *
     * @throws IOException on errors
     */
    private static void
        genJavaEnumFromMsgFile(final String msgFilePath,
                               final String generatedEnumSrcFilePath)
        throws IOException {

        /*
         * If source file has not changed since target was modified, do
         * nothing.
         */
        if (new File(generatedEnumSrcFilePath).lastModified() >
            new File(msgFilePath).lastModified()) {
            return;
        }

        /* Generate header of target Java source file. */
        final PrintStream ps = new PrintStream(generatedEnumSrcFilePath);
        ps.println(WARNING_COMMENT);
        ps.println("package " + GEN_CODE_PKG_NAME + ";" + LINE_SEP);
        ps.println("public enum " + GEN_CODE_CLASS_NAME + " {" + LINE_SEP);

        final LineNumberReader reader =
            new LineNumberReader(new FileReader(msgFilePath));

        String currLine = null;
        boolean firstLine = true;
        while ((currLine = reader.readLine()) != null)  {
            final String key = getKey(currLine.trim(), reader.getLineNumber());
            if (key == null)  {
                continue;
            }

            if (!firstLine) {
                ps.println(",");
            } else {
                firstLine = false;
            }
            /* Output message in a comment for easier development. */
            ps.println("\t// " +
                       getMessageForKey(key, currLine, reader.getLineNumber()));
            ps.print("\t" + ERR_CODE_ENUM_PREFIX + key);
        }

        ps.println(LINE_SEP + " }");
        ps.flush();
        ps.close();
        System.out.println("Successfully wrote " + generatedEnumSrcFilePath);
    }

    /**
     * Retrieve the key from the message value from a line in the message file.
     *
     * @param currLine A line of text from the message file
     *
     * @param lineNumber The line number corresponding to the supplied currLine
     * parameter (used for exception messages).
     *
     * @return A string that is the key value from the message file.  If a
     * comment on the current line is encountered then this method will return
     * null.
     */
    public static String getKey(final String currLine, final int lineNumber) {

        final String[] tokens =  parseLine(currLine, lineNumber);
        return tokens != null ? tokens[0] : null;
    }

    /**
     * Retrieves the message from the message file that corresponds to the key
     *
     * @param requestedKey  The key to look up
     * @param ln Reader corresponding to the message file to scan
     * @return  The message associated with the supplied key or NULL if no
     *          message for the key is found
     * @throws IOException on error
     */
    public static String getMessageForKey(final String requestedKey,
                                          final LineNumberReader ln)
        throws IOException {

        String currLine = null;
        while ((currLine = ln.readLine()) != null)  {
            if ((currLine.length() == 0) ||
                (currLine.startsWith(COMMENT_DELIMITER)))   {
                continue;
            }

            final String msg = getMessageForKey(requestedKey, currLine.trim(),
                                                ln.getLineNumber());
            if (msg != null) {
                return msg;
            }
        }

        return null;
    }

    /**
     * Retrieves the message from the current line in the messages file that
     * corresponds to the key
     *
     * @param requestedKey The key to look up
     *
     * @param currLine The contents of a line from the messages file
     *
     * @param lineNumber The line number corresponding to currLine (used for
     * exceptions).
     *
     * @return The message associated with the supplied key or NULL if no
     * message for the key is found
     */
    private static String getMessageForKey(final String requestedKey,
                                           final String currLine,
                                           final int lineNumber) {

        final String[] pair = parseLine(currLine, lineNumber);
        if (pair[0].equals(requestedKey)) {
            return pair[1];
        }

        return null;
    }

    /**
     * Returns the tokens that have been parsed from a line of text from the
     * message file
     * @param currLine A line of text from the messages file
     * @param lineNumber The line number in the file corresponding to the
     *                   supplied line of text.  Used for exception messages.
     * @return An array of tokens in parse order otherwise NULL if the line is
     *         actually a comment line
     * @throws Exception on error
     */
    private static String[] parseLine(final String currLine,
                                      final int lineNumber)
        throws RuntimeException {

        if ((currLine.length() == 0) ||
            (currLine.startsWith(COMMENT_DELIMITER)))   {
            return null;
        }

        /**
         * The standard Oracle message file has exactly three tokens on
         * each non-commented line integer1, integer2, string
         * Where
         *    integer1 is the message ID
         *    integer2 is reserved and always 0
         *    string is the actual error message itself
         */
        final String[] msgTokens = currLine.split(MESSAGE_LINE_DELIMETER, 3);
        if (msgTokens.length < 3 ||
            msgTokens[0] == null ||
            msgTokens[0].length() == 0 ||
            msgTokens[2] == null ||
            msgTokens[2].length() == 0)  {
            throw new RuntimeException
                ("Unexpected message format at line " + lineNumber);
        }

        try {
            Integer.parseInt(msgTokens[0]);
        } catch (NumberFormatException e) {
            throw new RuntimeException
                ("Expected to find an integer as the first token on line " +
                 lineNumber + " but found '" + msgTokens[0] + "'");
        }

        final String[] ret = new String[2];
        ret[0] = msgTokens[0];

        /* Remove double quotes. */
        final String message = msgTokens[2].trim();
        if (!message.startsWith("\"")) {
            throw new RuntimeException
                ("Expected message on line " + lineNumber +
                 " to start with \" but found '" +
                 message.substring(0, 1) + "' instead.");
        }

        if (!message.endsWith("\"")) {
            throw new RuntimeException
                ("Expected message on line " + lineNumber +
                 " to end with \" but found '" +
                 message.substring(message.length() - 1) +
                 "' instead.");
        }

        ret[1] = message.substring(1, message.length() - 1);
        return ret;
    }

    private static void usageAndExit() {
        System.out.println("Usage: ");
        System.out.println("\t" + MessageFileProcessor.class.getName() + " " +
                           ARG_CODELINE_BASEDIR + " path_to_root_of_codeline");
        System.exit(-1);
    }
}
