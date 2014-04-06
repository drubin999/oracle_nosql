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

package oracle.kv.shell;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import oracle.kv.impl.admin.client.CommandShell;
import oracle.kv.impl.api.table.JsonUtils;
import oracle.kv.KVStore;
import oracle.kv.Key;
import oracle.kv.Value;
import oracle.kv.avro.AvroCatalog;
import oracle.kv.avro.JsonAvroBinding;
import oracle.kv.avro.JsonRecord;
import oracle.kv.shell.CommandUtils.RunTableAPIOperation;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.util.shell.CommandWithSubs;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellException;

import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;

public class PutCommand extends CommandWithSubs {
    final static String VALUE_FLAG = "-value";
    final static String JSON_FLAG = "-json";
    final static String FILE_FLAG = "-file";
    final static String IFABSENT_FLAG = "-if-absent";
    final static String IFABSENT_FLAG_DESC = IFABSENT_FLAG;
    final static String IFPRESENT_FLAG = "-if-present";
    final static String IFPRESENT_FLAG_DESC = IFPRESENT_FLAG;

    private static final
        List<? extends SubCommand> subs =
                   Arrays.asList(new PutKVCommand(),
                                 new PutTableCommand());
    public PutCommand() {
        super(subs, "put", 3, 2);
    }

    @Override
    protected String getCommandOverview() {
        return "The put command encapsulates commands that put key/value" +
            eol + "to the store or put a row to table.";
    }

    static class PutKVCommand extends SubCommand {
        final static String KEY_FLAG = "-key";
        final static String KEY_FLAG_DESC = KEY_FLAG + " <key>";
        final static String HEX_FLAG = "-hex";
        final static String HEX_FLAG_DESC = HEX_FLAG;
        final static String VALUE_FLAG_DESC = VALUE_FLAG + " <valueString>";
        final static String JSON_SCHEMA_FLAG_DESC = JSON_FLAG + " <schemaName>";
        final static String FILE_FLAG_DESC = FILE_FLAG;

        PutKVCommand() {
            super("kv", 2);
        }

        @SuppressWarnings("null")
        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            Key key = null;
            Value value = null;
            byte[] valueBytes = null;
            String stringValue = null;
            String schemaName = null;
            boolean isPutIfAbsent = false;
            boolean isPutIfPresent = false;
            boolean isJson = false;
            boolean isFile = false;
            boolean isBinHex = false;
            KVStore store = ((CommandShell)shell).getStore();

            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if (KEY_FLAG.equals(arg)) {
                    String keyString = Shell.nextArg(args, i++, this);
                    try {
                        key = CommandUtils.createKeyFromURI(keyString);
                    } catch (IllegalArgumentException iae) {
                        shell.invalidArgument(iae.getMessage(), this);
                    }
                } else if (VALUE_FLAG.equals(arg)) {
                    stringValue = Shell.nextArg(args, i++, this);
                } else if (FILE_FLAG.equals(arg)) {
                    isFile = true;
                } else if (HEX_FLAG.equals(arg)) {
                    isBinHex = true;
                } else if (JSON_FLAG.equals(arg)) {
                    schemaName = Shell.nextArg(args, i++, this);
                    isJson = true;
                } else if (IFABSENT_FLAG.equals(arg)) {
                    isPutIfAbsent = true;
                } else if (IFPRESENT_FLAG.equals(arg)) {
                    isPutIfPresent = true;
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            if (key == null) {
                shell.requiredArg(KEY_FLAG, this);
            }
            if (stringValue == null) {
                shell.requiredArg(VALUE_FLAG, this);
            }

            /* By default, set isPutIfAbsent/ifPutIfPresent to true. */
            if (!isPutIfAbsent && !isPutIfPresent) {
                isPutIfAbsent = true;
                isPutIfPresent = true;
            }

            if (isFile) {
                valueBytes = CommandUtils.readFromFile(stringValue);
            } else {
                valueBytes = stringValue.getBytes();
            }

            if (isJson) {
                InputStream in = null;
                in = new ByteArrayInputStream(valueBytes);
                value = createJsonValue(schemaName, in, store);
            } else if (isBinHex) {
                byte[] decoded =
                    CommandUtils.decodeBase64(new String(valueBytes));
                value = Value.createValue(decoded);
            } else {
                /* create the actual value */
                value = Value.createValue(valueBytes);
            }

            String retString = null;
            boolean updated = false;
            try {
                if (isPutIfAbsent && isPutIfPresent) {
                    if (store.putIfAbsent(key, value) == null) {
                        store.putIfPresent(key, value);
                        updated = true;
                    }
                } else if (isPutIfAbsent) {
                    if (store.putIfAbsent(key, value) == null) {
                        retString = "A value was already present with the " +
                            "given key " + CommandUtils.createURI(key) + ".";
                    }
                } else {
                    updated = true;
                    if (store.putIfPresent(key, value) == null) {
                        retString = "No existing value was present with the " +
                            "given key " + CommandUtils.createURI(key) + ".";
                    }
                }
            } catch (Exception e) {
                throw new ShellException("Exception from NoSQL DB in put. " +
                                         e.getMessage(), e);
            }

            if (retString == null) {
                retString = "Operation successful, record " +
                            (updated?"updated.":"inserted.");
            } else {
                retString = "Operation failed, " + retString;
            }
            return retString;
        }

        @Override
        protected String getCommandSyntax() {
            return "put " + getCommandName() + " " + KEY_FLAG_DESC + " " +
                VALUE_FLAG_DESC + " [" + FILE_FLAG_DESC + "] " + "[" +
                HEX_FLAG_DESC + " | " + JSON_SCHEMA_FLAG_DESC + "]" + eolt +
                "[" + IFABSENT_FLAG_DESC + " | " + IFPRESENT_FLAG_DESC + "]";
        }

        @Override
        protected String getCommandDescription() {
            return "Puts the specified key, value pair into the store" +
                eolt +
                "-file indicates that the value parameter is a file that " +
                "contains the" + eolt + "actual value" +
                eolt +
                "-hex indates that the value is a BinHex encoded byte value" +
                " with Base64" +
                eolt +
                "-json indicates that the value is a JSON string." +
                eolt +
                "-json and -file can be used together." +
                eolt + "-if-absent indicates to put a key/value pair only if " +
                "no value for " + eolt + "the given key is present. " +
                eolt +
                "-if-present indicates to put a key/value pair only if " +
                "a value for " + eolt + "the given key is present. " ;
        }

        private Value createJsonValue(String schema,
                                      InputStream content,
                                      KVStore store)
            throws ShellException {

            try {
                AvroCatalog catalog = store.getAvroCatalog();
                catalog.refreshSchemaCache(null);
                Map<String, Schema> schemaMap = catalog.getCurrentSchemas();
                Schema sch = schemaMap.get(schema);
                if (sch == null) {
                    throw new ShellException("Schema does not exist in the " +
                                             "catalog: " + schema);
                }
                JsonNode obj = JsonUtils.getObjectMapper().readTree(content);
                JsonAvroBinding jsonBinding = catalog.getJsonBinding(sch);
                return jsonBinding.toValue(new JsonRecord(obj, sch));
            } catch (JsonProcessingException jpe) {
                throw new ShellException(eolt +
                                         "Could not create JSON from input: " +
                                         eolt + jpe.getMessage(), jpe);
            } catch (IOException ioe) {
                throw new ShellException(eolt +
                                         "Could not create JSON from input: " +
                                         eolt + ioe.getMessage(), ioe);
            } catch (IllegalArgumentException iae) {
                String errMsg = (iae.getCause() != null)?
                                iae.getCause().getMessage():iae.getMessage();
                throw new ShellException(eolt +
                                         "Could not create JSON from input: " +
                                         eolt + errMsg, iae);
            }
        }
    }

    /*
     * table put command
     * */
    static class PutTableCommand extends SubCommand {
        final static String TABLE_FLAG = "-name";
        final static String TABLE_FLAG_DESC = TABLE_FLAG + " <name>";
        final static String FIELD_FLAG = "-field";
        final static String FIELD_FLAG_DESC = FIELD_FLAG + " <name>";
        final static String VALUE_FLAG_DESC = VALUE_FLAG + " <value>";
        final static String NULL_VALUE_FLAG = "-null-value";
        final static String NULL_VALUE_FLAG_DESC = "-null-value";
        final static String JSON_FLAG_DESC = JSON_FLAG + " <string>";
        final static String FILE_FLAG_DESC = FILE_FLAG + " <file>";
        final static String UPDATE_FLAG = "-update";
        final static String UPDATE_FLAG_DESC = UPDATE_FLAG;

        private static final String currentPutParams = "currentPutParams";
        private TableCmdWithSubs cmdSubs = new TablePutSubs();

        PutTableCommand() {
            super("table", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            PutArgs putParams =
                (PutArgs)getVariable(currentPutParams);
            if (putParams == null) {
                Shell.checkHelp(args, this);
                String tableName = null;
                String jsonString = null;
                String fileName = null;
                Boolean ifAbsent = null;
                boolean isUpdate = false;
                for (int i = 1; i < args.length; i++) {
                    String arg = args[i];
                    if (TABLE_FLAG.equals(arg)) {
                        tableName = Shell.nextArg(args, i++, this);
                    } else if (IFABSENT_FLAG.equals(arg)) {
                        ifAbsent = true;
                    } else if (IFPRESENT_FLAG.equals(arg)) {
                        ifAbsent = false;
                    } else if (JSON_FLAG.equals(arg)) {
                        jsonString = Shell.nextArg(args, i++, this);
                    } else if (FILE_FLAG.equals(arg)) {
                        fileName = Shell.nextArg(args, i++, this);
                    } else if (UPDATE_FLAG.equals(arg)) {
                        isUpdate = true;
                    } else {
                        shell.unknownArgument(arg, this);
                    }
                }

                if (tableName == null) {
                    shell.requiredArg(TABLE_FLAG, this);
                }

                final TableAPI tableImpl =
                    ((CommandShell)shell).getStore().getTableAPI();
                Table table = CommandUtils.findTable(tableImpl, tableName);
                if (fileName != null) {
                    return putJsonFromFile(tableImpl, table,
                                           fileName, ifAbsent);
                }
                Row row = null;
                if (jsonString != null) {
                    /* Put a single row parsed from the inputed JSON string. */
                    try {
                        row = table.createRowFromJson(jsonString, false);
                    } catch (IllegalArgumentException iae) {
                        throw new ShellException(iae.getMessage());
                    }
                    return doPutRow(tableImpl, row, ifAbsent, false);
                }
                row = table.createRow();
                ShellCommand cmd = this.clone();
                cmd.addVariable(currentPutParams,
                                new PutArgs(row, ifAbsent, isUpdate));
                cmd.setPrompt(tableName);
                shell.pushCurrentCommand(cmd);
                return null;
            }
            return cmdSubs.execute(putParams, args, shell);
        }

        private String putJsonFromFile(TableAPI tableImpl, Table table,
                                       String fileName, Boolean ifAbsent)
            throws ShellException {

            BufferedReader br = null;
            try {
                br = new BufferedReader(new FileReader(fileName));
                Row row = null;
                String line;
                int numRows = 0;
                while((line = br.readLine()) != null) {
                    try {
                        row = table.createRowFromJson(line, false);
                    } catch (IllegalArgumentException iae) {
                        throw new ShellException(iae.getMessage());
                    }
                    ++numRows;
                    String error = doPutRow(tableImpl, row, ifAbsent, true);
                    if (error != null) {
                        return error;
                    }
                }
                return "Inserted " + numRows + " rows";
            } catch (FileNotFoundException fnf) {
                throw new ShellException("File not found: " + fileName, fnf);
            } catch (IOException ioe) {
                throw new ShellException("IO error reading file: " + fileName,
                                         ioe);
            } finally {
                if (br != null) {
                    try {
                        br.close();
                    } catch (IOException ignored) {
                    }
                }
            }
        }

        private static class TablePutSubs extends TableCmdWithSubs {
            private static final
                List<? extends TableCmdSubCommand> tablePutSubs =
                               Arrays.asList(new TableAddArrayValueSub(),
                                             new TableAddMapValueSub(),
                                             new TableAddRecordValueSub(),
                                             new TableAddValueSub(),
                                             new TableCancelSub(),
                                             new TableExitSub(),
                                             new TableShowSub());
            TablePutSubs() {
                super(tablePutSubs, "", 0, 2);
            }

            @Override
            protected String getCommandOverview() {
                return "Set field value.";
            }
        }

        @Override
        protected String getCommandSyntax() {
            return "put " + getCommandName() + " " +
                TABLE_FLAG_DESC + " [" +
                IFABSENT_FLAG_DESC + " | " + IFPRESENT_FLAG_DESC + "]" +
                eolt +
                "[" + JSON_FLAG_DESC + "] [" + FILE_FLAG_DESC + "] [" +
                UPDATE_FLAG_DESC + "]";
        }

        @Override
        protected String getCommandDescription() {
            return "Put a row into the named table.  The table name is a " +
                "dot-separated" + eolt +  "name with the format " +
                "tableName[.childTableName]*." + eolt +
                "-if-absent indicates to put a row only if the row does " +
                "not exist." + eolt +
                "-if-present indicates to put a row only if the row " +
                "already exists." + eolt +
                "-json indicates that the value is a JSON string." + eolt +
                "-file can be used to load JSON strings from a file."+ eolt +
                "-update can be used to partially update the existing record.";
        }

        private static class PutArgs {
            private String fieldName;
            private FieldValue fieldValue;
            private Boolean ifAbsent;
            private boolean isUpdate;

            PutArgs(String name, FieldValue value) {
                this(name, value, null, false);
            }

            PutArgs(FieldValue value, Boolean ifAbsent, boolean isUpdate) {
                this(null, value, ifAbsent, isUpdate);
            }

            PutArgs(String name, FieldValue value,
                    Boolean ifAbsent, boolean isUpdate) {
                this.fieldName = name;
                this.fieldValue = value;
                this.ifAbsent = ifAbsent;
                this.isUpdate = isUpdate;
            }

            FieldValue getFieldValue() {
                return this.fieldValue;
            }

            void setFieldValue(FieldValue value) {
                this.fieldValue = value;
            }

            Boolean isPutIfAbsent() {
                return this.ifAbsent;
            }

            String getFieldName() {
                return this.fieldName;
            }

            boolean isUpdate() {
                return this.isUpdate;
            }
        }

        static abstract class TableCmdWithSubs extends CommandWithSubs {
            private PutArgs putArgs = null;
            TableCmdWithSubs(List<? extends SubCommand> subCommands,
                             String name,
                             int prefixLength,
                             int minArgCount) {
                super(subCommands, name, prefixLength, minArgCount);
                initSubs(subCommands);
            }

            private void initSubs(List<? extends SubCommand> tblSubs) {
                for (SubCommand command: tblSubs) {
                    ((TableCmdSubCommand)command).setParentCommand(this);
                }
            }

            protected String execute(PutArgs opArgs,
                                     String[] args,
                                     Shell shell)
                throws ShellException {

                if (isHelpCommand(args[1], shell)) {
                    String[] argsEx = new String[args.length - 1];
                    argsEx[0] = args[0];
                    System.arraycopy(args, 2, argsEx, 1, args.length - 2);
                    return getHelp(argsEx, shell);
                }
                this.putArgs = opArgs;
                return execute(args, shell);
            }

            protected FieldValue getFieldValue() {
                return putArgs.getFieldValue();
            }

            protected void setFieldValue(FieldValue value) {
                putArgs.setFieldValue(value);
            }

            protected Boolean isPutIfAbsent() {
                return putArgs.isPutIfAbsent();
            }

            protected String getFieldName() {
                return putArgs.getFieldName();
            }

            protected boolean isUpdate() {
                return putArgs.isUpdate();
            }

            private boolean isHelpCommand(String commandName, Shell shell) {
                ShellCommand cmd = shell.findCommand(commandName);
                return (cmd instanceof Shell.HelpCommand);
            }
        }

        static abstract class TableCmdSubCommand extends SubCommand {
            CommandWithSubs parentCommand = null;

            protected TableCmdSubCommand(String name, int prefixMatchLength) {
                super(name, prefixMatchLength);
            }

            protected void setParentCommand(CommandWithSubs command) {
                parentCommand = command;
            }

            protected FieldValue getCurrentFieldValue() {
                return ((TableCmdWithSubs)parentCommand).getFieldValue();
            }

            protected void setCurrentFieldValue(FieldValue value) {
                ((TableCmdWithSubs)parentCommand).setFieldValue(value);
            }

            protected String getCurrentFieldName() {
                return ((TableCmdWithSubs)parentCommand).getFieldName();
            }

            protected Boolean isPutIfAbsent() {
                return ((TableCmdWithSubs)parentCommand).isPutIfAbsent();
            }

            protected boolean isPutUpdate() {
                return ((TableCmdWithSubs)parentCommand).isUpdate();
            }
        }

        static class TableAddValueSub extends TableCmdSubCommand {
            final static String FILE_BINARY_DESC = FILE_FLAG +
                " <file-with-binary-content>";
            final static String command = "add-value";
            protected TableAddValueSub() {
                super(command, 5);
            }

            @Override
            public String execute(String[] args, Shell shell)
                throws ShellException {

                Shell.checkHelp(args, this);
                String fieldName = null;
                String sValue = null;
                boolean nullValue = false;
                boolean isFile = false;
                for (int i = 1; i < args.length; i++) {
                    String arg = args[i];
                    if (FIELD_FLAG.equals(arg)) {
                        fieldName = Shell.nextArg(args, i++, this);
                    } else if (VALUE_FLAG.equals(arg)) {
                        sValue = Shell.nextArg(args, i++, this);
                    } else if (NULL_VALUE_FLAG.equals(arg)) {
                        nullValue = true;
                    } else if (FILE_FLAG.equals(arg)) {
                        sValue = Shell.nextArg(args, i++, this);
                        isFile = true;
                    } else {
                        shell.unknownArgument(arg, this);
                    }
                }

                FieldValue currentVal = getCurrentFieldValue();
                if (!currentVal.isArray() && fieldName == null) {
                    shell.requiredArg(FIELD_FLAG, this);
                }
                if (nullValue) {
                    putNull(currentVal, fieldName);
                    return null;
                }

                FieldValue fdVal = null;
                if (sValue == null) {
                    /* Get fieldValue object from current command variable. */
                    ShellCommand cmd = shell.getCurrentCommand();
                    fdVal = (FieldValue)cmd.getVariable(fieldName);
                    if (fdVal == null) {
                        shell.requiredArg(VALUE_FLAG, this);
                    } else {
                        cmd.removeVariable(fieldName);
                    }
                } else {
                    FieldDef def =
                        CommandUtils.getFieldDef(currentVal, fieldName);
                    /* -file can only be used for binary or fixed field. */
                    if (isFile && (!def.isBinary() && !def.isFixedBinary())) {
                        shell.invalidArgument(FILE_FLAG +
                            " can not be used for " + def.getType() + " field",
                            this);
                    }
                    /**
                     * add-value command could not be used for complex type
                     * field: array, map and record.
                     */
                    if (def.isArray() || def.isMap() || def.isRecord()) {
                        String sType = def.getType().toString().toLowerCase();
                        throw new ShellException("Can't use " + command +
                            " for " + sType + " field, please run add-" +
                            sType + "-value to add value.");
                    }
                    fdVal = CommandUtils.createFieldValue(def, sValue, isFile);
                }
                putValue(currentVal, fieldName, fdVal);

                if (currentVal.isRow() && isPutUpdate()) {
                    TableAPI tableImpl =
                        ((CommandShell)shell).getStore().getTableAPI();
                    Row newRow = updateIfExists(tableImpl, currentVal.asRow());
                    if (newRow != null) {
                        setCurrentFieldValue(newRow);
                    }
                }
                return null;
            }

            private void putNull(FieldValue currentVal, String fieldName)
                throws ShellException {

                if (currentVal instanceof RecordValue) {
                    try {
                        currentVal.asRecord().putNull(fieldName);
                    } catch (IllegalArgumentException iae) {
                        throw new ShellException(iae.getMessage());
                    }
                } else {
                    throw new ShellException(
                        "Can not set null to the field: " + fieldName);
                }
            }

            private void putValue(FieldValue currentVal,
                                  String field, FieldValue value)
                throws ShellException {

                try {
                    if (currentVal.isRecord()) {
                        currentVal.asRecord().put(field, value);
                    } else if (currentVal.isArray()) {
                        currentVal.asArray().add(value);
                    } else if (currentVal.isMap()) {
                        currentVal.asMap().put(field, value);
                    }
                } catch (IllegalArgumentException iae) {
                    throw new ShellException(iae.getMessage());
                }
            }

            private Row updateIfExists(TableAPI tableImpl, Row row) {
                /* No need to update if all fields are filled in. */
                if (row.size() == row.getTable().getFields().size()) {
                    return null;
                }

                /* Check if all primary key fields' value are provided. */
                List<String> pkFields = row.getTable().getPrimaryKey();
                for(String fname: pkFields) {
                    if (row.get(fname) == null) {
                        return null;
                    }
                }

                /* Check if row associated with the primary key exists. */
                PrimaryKey key = row.createPrimaryKey();
                Row retRow = tableImpl.get(key, null);
                if (retRow == null) {
                    return null;
                }

                /* Copy values from the current row. */
                retRow.copyFrom(row);
                return retRow;
            }

            @Override
            protected String getCommandDescription() {
                return "Set field value." + eolt +
                    "-file flag can be used to input binary value from " +
                    "a file" + eolt + "for BINARY or FIXED_BINARY field.";
            }

            @Override
            protected String getCommandSyntax() {
                return command + " " + FIELD_FLAG_DESC + " [" +
                    VALUE_FLAG_DESC + " | " + NULL_VALUE_FLAG + " | " +
                    eolt + FILE_BINARY_DESC + "]";
            }
        }

        abstract static class TableAddComplexValueSub
            extends TableCmdSubCommand {

            private static TableCmdWithSubs cmdSubs = new AddComplexValueSubs();
            private static final String VAR_NAME = "currentVariable";

            TableAddComplexValueSub(String name, int prefixMatchLength) {
                super(name, prefixMatchLength);
            }

            abstract FieldValue createValue(FieldDef fieldDef)
                throws ShellException;

            @Override
            public String execute(String[] args, Shell shell)
                throws ShellException {

                String varName = VAR_NAME;
                PutArgs putArgs = ((PutArgs)getVariable(varName));
                if (putArgs == null) {
                    Shell.checkHelp(args, this);
                    FieldValue currentVal = getCurrentFieldValue();
                    String fieldName = null;
                    for (int i = 1; i < args.length; i++) {
                        String arg = args[i];
                        if (FIELD_FLAG.equals(arg)) {
                            fieldName = Shell.nextArg(args, i++, this);
                        } else {
                            shell.unknownArgument(arg, this);
                        }
                    }

                    if (fieldName == null) {
                        shell.requiredArg(FIELD_FLAG, this);
                    }
                    FieldDef fieldDef =
                        CommandUtils.getFieldDef(currentVal, fieldName);
                    FieldValue retVal = createValue(fieldDef);
                    ShellCommand cmd = this.clone();
                    cmd.addVariable(varName, new PutArgs(fieldName, retVal));
                    cmd.setPrompt(fieldName);
                    shell.pushCurrentCommand(cmd);
                    return null;
                }
                return cmdSubs.execute(putArgs, args, shell);
            }

            private static class AddComplexValueSubs extends TableCmdWithSubs {
                private static
                    List<? extends TableCmdSubCommand> complexValueSubs =
                               Arrays.asList(new TableAddArrayValueSub(),
                                             new TableAddMapValueSub(),
                                             new TableAddRecordValueSub(),
                                             new TableAddValueSub(),
                                             new TableCancelSub(),
                                             new TableExitSub(),
                                             new TableShowSub());
                AddComplexValueSubs() {
                    super(complexValueSubs, "", 0, 2);
                }

                @Override
                public String getCommandOverview() {
                    return "Set field value.";
                }
            }

            @Override
            protected String getCommandSyntax() {
                return getCommandName() + " " + FIELD_FLAG_DESC;
            }
        }

        /* sub command: add-map-value */
        static class TableAddMapValueSub extends TableAddComplexValueSub {
            final static String command = "add-map-value";

            protected TableAddMapValueSub() {
                super(command, 5);
            }

            @Override
            FieldValue createValue(FieldDef fieldDef)
                throws ShellException {

                try {
                    return fieldDef.createMap();
                } catch (ClassCastException cce) {
                    throw new ShellException(cce.getMessage());
                }
            }

            @Override
            protected String getCommandDescription() {
                return "Set map field value.";
            }
        }

        /* sub command: add-array-value */
        static class TableAddArrayValueSub extends TableAddComplexValueSub {
            private final static String command = "add-array-value";

            TableAddArrayValueSub() {
                super(command, 5);
            }

            @Override
            FieldValue createValue(FieldDef fieldDef)
                throws ShellException {

                try {
                    return fieldDef.createArray();
                } catch (ClassCastException cce) {
                    throw new ShellException(cce.getMessage());
                }
            }

            @Override
            protected String getCommandDescription() {
                return "Set array field value.";
            }
        }

        /* sub command: add-record-value */
        static class TableAddRecordValueSub extends TableAddComplexValueSub {
            private final static String command = "add-record-value";

            TableAddRecordValueSub() {
                super(command, 5);
            }

            @Override
            FieldValue createValue(FieldDef fieldDef)
                throws ShellException {

                try {
                    return fieldDef.createRecord();
                } catch (ClassCastException cce) {
                    throw new ShellException(cce.getMessage());
                }
            }

            @Override
            protected String getCommandDescription() {
                return "Set record field value.";
            }
        }

        /* sub command: show */
        static class TableShowSub extends TableCmdSubCommand {
            final static String command = "show";
            protected TableShowSub() {
                super(command, 2);
            }

            @Override
            public String execute(String[] args, Shell shell)
                throws ShellException {

                Shell.checkHelp(args, this);
                if (args.length != 1) {
                    shell.badArgCount(this);
                }
                FieldValue value = getCurrentFieldValue();
                return value.toJsonString(true);
            }

            @Override
            protected String getCommandDescription() {
                return "Show field value.";
            }
        }

        /* sub command: cancel */
        static class TableCancelSub extends TableCmdSubCommand {
            final static String command = "cancel";
            protected TableCancelSub() {
                super(command, 4);
            }

            @Override
            public String execute(String[] args, Shell shell)
                throws ShellException {

                Shell.checkHelp(args, this);
                if (args.length != 1) {
                    shell.badArgCount(this);
                }
                shell.popCurrentCommand();
                return null;
            }

            @Override
            protected String getCommandDescription() {
                return "Cancel the current operation.";
            }

        }

        /* sub command: exit */
        static class TableExitSub extends TableCmdSubCommand {
            final static String command = "exit";
            protected TableExitSub() {
                super(command, 4);
            }

            @Override
            public String execute(String[] args, Shell shell)
                throws ShellException {

                Shell.checkHelp(args, this);
                if (args.length != 1) {
                    shell.badArgCount(this);
                }

                String retString = null;
                final TableAPI tableImpl =
                    ((CommandShell)shell).getStore().getTableAPI();
                final FieldValue fieldValue = getCurrentFieldValue();
                if (fieldValue.isRow()) {
                    retString = doPutRow(tableImpl, fieldValue.asRow(),
                                         isPutIfAbsent(), false);
                    shell.popCurrentCommand();
                } else {
                    String fieldName = getCurrentFieldName();
                    shell.popCurrentCommand();
                    shell.getCurrentCommand()
                        .addVariable(fieldName, fieldValue);
                    shell.runLine("add-value -field \"" + fieldName + "\"");
                }
                return retString;
            }

            @Override
            protected String getCommandDescription() {
                return "Exit the current operation.";
            }
        }

        private static String doPutRow(final TableAPI tableImpl,
                                       final Row row,
                                       final Boolean ifAbsent,
                                       final boolean returnErrorOnly)
            throws ShellException {

            final StringBuilder sb = new StringBuilder();
            new RunTableAPIOperation() {
                @Override
                void doOperation(){
                    boolean updated = false;
                    if (ifAbsent != null) {
                        if (ifAbsent) {
                            if (tableImpl.putIfAbsent(row, null, null)
                                    == null) {
                                sb.append("Operation failed, ");
                                sb.append("A record was already present ");
                                sb.append("with the given primary key: ");
                                sb.append(row.createPrimaryKey()
                                          .toJsonString(false));
                            }
                        } else {
                            if (tableImpl.putIfPresent(row, null, null)
                                    == null) {
                                sb.append("Operation failed, ");
                                sb.append("No existing record was present ");
                                sb.append("with the given primary key: ");
                                sb.append(row.createPrimaryKey()
                                          .toJsonString(false));
                            } else {
                                updated = true;
                            }
                        }
                    } else {
                        if (tableImpl.putIfAbsent(row, null, null) == null) {
                            tableImpl.putIfPresent(row, null, null);
                            updated = true;
                        }
                    }
                    if (sb.length() == 0 && !returnErrorOnly) {
                        sb.append("Operation successful, row ");
                        sb.append((updated?"updated.":"inserted."));
                    }
                }
            }.run();
            if (sb.length() == 0) {
                return null;
            }
            return sb.toString();
        }
    }
}
