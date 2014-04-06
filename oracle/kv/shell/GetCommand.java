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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import oracle.kv.Direction;
import oracle.kv.impl.admin.client.CommandShell;
import oracle.kv.impl.api.table.JsonUtils;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.KeyValueVersion;
import oracle.kv.StoreIteratorException;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.avro.AvroCatalog;
import oracle.kv.avro.JsonAvroBinding;
import oracle.kv.avro.JsonRecord;
import oracle.kv.avro.SchemaNotAllowedException;
import oracle.kv.impl.util.FileUtils;
import oracle.kv.shell.CommandUtils.RunTableAPIOperation;
import oracle.kv.table.Index;
import oracle.kv.table.IndexKey;
import oracle.kv.table.KeyPair;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableUtils;
import oracle.kv.util.shell.CommandWithSubs;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellException;
import oracle.kv.util.shell.ShellInputReader;

import org.apache.avro.Schema;
import org.codehaus.jackson.map.ObjectWriter;

public class GetCommand extends CommandWithSubs {
    final static String FILE_FLAG = "-file";
    final static String FILE_FLAG_DESC = FILE_FLAG + " <output>";
    final static String START_FLAG = "-start";
    final static String END_FLAG = "-end";
    final static String JSON_FLAG = "-json";
    final static String KEY_ONLY_FLAG = "-keyonly";
    final static String KEY_ONLY_FLAG_DESC = KEY_ONLY_FLAG;

    private static final
        List<? extends SubCommand> subs =
            Arrays.asList(new GetKVCommand(),
                          new GetTableCommand());

    public GetCommand() {
        super(subs, "get", 3, 1);
    }

    @Override
    protected String getCommandOverview() {
        return "The get command encapsulates commands that get key/value" +
            eol + "pairs from store or get rows from table.";
    }

    static class GetKVCommand extends SubCommand {
        final static String KEY_FLAG = "-key";
        final static String KEY_FLAG_DESC = KEY_FLAG + " <key>";
        final static String VALUE_ONLY_FLAG = "-valueonly";
        final static String VALUE_ONLY_FLAG_DESC = VALUE_ONLY_FLAG;
        final static String END_FLAG_DESC = END_FLAG + " <prefixString>";
        final static String START_FLAG_DESC = START_FLAG + " <prefixString>";
        final static String JSON_FLAG_DESC = JSON_FLAG;
        final static String MULTI_FLAG = "-all";
        final static String MULTI_FLAG_DESC = MULTI_FLAG;

        public GetKVCommand() {
            super("kv", 2);
            /*
             * If there's a move to Jackson 2.x use the line below
             * instead of the one above:
             * ObjectWriter writer = mapper.writer().withDefaultPrettyPrinter();
             */
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);

            boolean iterate = false;
            Key key = null;
            String keyString = null;
            boolean isJson = false;
            String outFile = null;
            String rangeStart = null;
            String rangeEnd = null;
            boolean keyOnly = false;
            boolean valueOnly = false;
            KVStore store = ((CommandShell)shell).getStore();

            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if (KEY_FLAG.equals(arg)) {
                    keyString = Shell.nextArg(args, i++, this);
                    try {
                        key = CommandUtils.createKeyFromURI(keyString);
                    } catch (IllegalArgumentException iae) {
                        shell.invalidArgument(iae.getMessage(), this);
                    }
                } else if (JSON_FLAG.equals(arg)) {
                    isJson = true;
                } else if (MULTI_FLAG.equals(arg)) {
                    iterate = true;
                } else if (KEY_ONLY_FLAG.equals(arg)) {
                    keyOnly = true;
                } else if (VALUE_ONLY_FLAG.equals(arg)) {
                    valueOnly = true;
                } else if (FILE_FLAG.equals(arg)) {
                    isJson = true;
                    outFile = Shell.nextArg(args, i++, this);
                } else if (START_FLAG.equals(arg)) {
                    rangeStart = Shell.nextArg(args, i++, this);
                } else if (END_FLAG.equals(arg)) {
                    rangeEnd = Shell.nextArg(args, i++, this);
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            String retString = "";
            if (!iterate) {
                if (key == null) {
                    shell.requiredArg(KEY_FLAG, this);
                }
                ValueVersion valueVersion = null;
                try {
                    valueVersion = store.get(key);
                    if (valueVersion == null) {
                        return "Key not found in store: " + keyString;
                    }
                } catch (Exception e) {
                    throw new ShellException(
                        "Exception from NoSQL DB in get:" +
                            eolt + e.getMessage(), e);
                }
                Value value = valueVersion.getValue();
                if (isJson || (value.getFormat() == Value.Format.AVRO)) {
                    retString = jsonRecord(store, value);
                } else {
                    retString = printableString(value.getValue());
                }
                if (outFile != null) {
                    try {
                        FileUtils.writeStringToFile(new File(outFile),
                                                    retString);
                        retString = "Wrote value to file " + outFile + ".";
                    } catch (IOException ioe) {
                        throw new ShellException("Could not write to file " +
                                                 outFile, ioe);
                    }
                }
                return retString;
            }

            /* Initialize KeyRange */
            KeyRange kr = null;
            if (rangeStart != null || rangeEnd != null) {
                try {
                    kr = new KeyRange(rangeStart, true, rangeEnd, true);
                } catch (IllegalArgumentException iae) {
                    shell.invalidArgument(iae.getMessage(), this);
                }
            }

            /* Initialize RecordOutput */
            Writer fwriter = null;
            ResultOutput output = null;
            if (outFile != null) {
                try {
                    File file = new File(outFile);
                    fwriter = new BufferedWriter(new FileWriter(file));
                } catch (IOException ioe) {
                    throw new ShellException(
                        "Could not open the output file " + outFile, ioe);
                }
                output = new ResultOutput(fwriter);
            } else {
                output = new ResultOutput(shell);
            }

            /* Perform iteration */
            try {
                if (keyOnly) {
                    retString = iterateKeys(store, key, kr, output);
                } else {
                    retString = iterateValues(store, key, kr,
                                              valueOnly, output);
                }
            } catch (ShellException se) {
                throw se;
            } finally {
                if (fwriter != null) {
                    try {
                        fwriter.flush();
                        fwriter.close();
                    } catch (IOException ioe) {
                        throw new ShellException(
                            "Could not flush to file " + outFile, ioe);
                    }
                    retString += eol + "Wrote value to file " + outFile + ".";
                }
            }
            return retString;
        }

        private String iterateKeys(KVStore store, Key key, KeyRange kr,
                                   ResultOutput output)
            throws ShellException {

            Iterator<Key> it = null;
            try {
                if (key != null && key.getMinorPath() != null &&
                    key.getMinorPath().size() > 0) {
                    /* There's a minor key path, use it to advantage */
                    it = store.multiGetKeysIterator(Direction.FORWARD,
                                                    100, key, kr, null);
                } else {
                    /* A generic store iteration */
                    it = store.storeKeysIterator(Direction.UNORDERED,
                                                 100, key, kr, null);
                    if (!it.hasNext() && key != null) {
                        /*
                         * a complete major path won't work with store iterator
                         * and we can't distinguish between a complete major
                         * path or not, so if store iterator fails entire,
                         * try the key as a complete major path.
                         */
                        it = store.multiGetKeysIterator(Direction.FORWARD,
                                                        100, key, kr, null);
                    }
                }
            } catch (Exception e) {
                throw new ShellException(
                    "Exception from NoSQL DB in creating key iterator:" +
                        eolt + e.getMessage(), e);
            }

            long totalNumKeys = 0;
            StringBuilder sb = new StringBuilder();
            while (it.hasNext()) {
                if (!output.writeRecord(CommandUtils.createURI(it.next()))) {
                    break;
                }
                totalNumKeys++;
            }
            output.flushWriting();
            sb.append(eol);
            sb.append(totalNumKeys);
            sb.append(((totalNumKeys > 1) ? " Keys" : " Key"));
            sb.append(" returned.");
            return sb.toString();
        }

        private String iterateValues(KVStore store, Key key, KeyRange kr,
                                     boolean valueOnly, ResultOutput output)
            throws ShellException {

            Iterator<KeyValueVersion> it = null;
            try {
                if (key != null && key.getMinorPath() != null &&
                    key.getMinorPath().size() > 0) {
                    /* There's a minor key path, use it to advantage */
                    it = store.multiGetIterator(Direction.FORWARD,
                                                100, key, kr, null);
                } else {
                    /* A generic store iteration */
                    it = store.storeIterator(Direction.UNORDERED,
                                             100, key, kr, null);
                    if (!it.hasNext() && key != null) {
                        /*
                         * a complete major path won't work with store iterator
                         * and we can't distinguish between a complete major
                         * path or not, so if store iterator fails entire,
                         * try the key as a complete major path.
                         */
                        it = store.multiGetIterator(Direction.FORWARD,
                                                    100, key, kr, null);
                    }
                }
            } catch (Exception e) {
                throw new ShellException(
                    "Exception from NoSQL DB in creating iterator:" +
                        eolt + e.getMessage(), e);
            }

            long totalNumRecords = 0;
            StringBuilder sb  = new StringBuilder();
            while (it.hasNext()) {
                /* Generate string for key/values. */
                KeyValueVersion kvv = it.next();
                Value value = kvv.getValue();
                if (value == null) {
                    continue;
                }
                String record = "";
                if (!valueOnly) {
                    record += CommandUtils.createURI(kvv.getKey()) + eol;
                }
                if (value.getFormat() == Value.Format.AVRO) {
                    record += jsonRecord(store, value);
                } else {
                    record += printableString(value.getValue());
                }
                if (!output.writeRecord(record)) {
                    break;
                }
                totalNumRecords++;
            }
            output.flushWriting();
            sb.append(eol);
            sb.append(totalNumRecords);
            sb.append(((totalNumRecords > 1) ?
                      " Records returned." : " Record returned."));
            return sb.toString();
        }

        @Override
        protected String getCommandSyntax() {
            return "get " + getCommandName() + " " + KEY_FLAG_DESC +
                " [" + JSON_FLAG_DESC + "] [" + FILE_FLAG_DESC + "] " +
                "[" + MULTI_FLAG_DESC + "] [" + KEY_ONLY_FLAG_DESC +"] " +
                eolt + "[" + VALUE_ONLY_FLAG_DESC + "] " +
                "[" + START_FLAG_DESC + "] [" + END_FLAG_DESC + "]";
        }

        @Override
        protected String getCommandDescription() {
            return "Performs a simple get operation on the key in the store." +
                eolt +
                "-key indicates the key (prefix) to use.  Optional with -all." +
                eolt +
                "-json should be specified if the record is JSON" +
                eolt +
                "-all is specified for iteration starting at the key, or with" +
                eolt + "an empty key to iterate the entire store." +
                eolt +
                "-start and -end flags can be used for restricting the range " +
                "used" + eolt + "for iteration." +
                eolt +
                "-keyonly works with -all and restricts information to keys." +
                eolt +
                "-valueonly works with -all and restricts information to " +
                "values." + eolt +
                "-file is used to specify an output file, which is truncated.";
        }

        private String jsonRecord(KVStore store, Value value)
            throws ShellException {

            AvroCatalog catalog = store.getAvroCatalog();
            catalog.refreshSchemaCache(null);
            Map<String, Schema> schemaMap = catalog.getCurrentSchemas();
            JsonAvroBinding binding =
                catalog.getJsonMultiBinding(schemaMap);
            try {
                ObjectWriter writer = JsonUtils.createWriter(true);
                JsonRecord jsonRec = binding.toObject(value);
                return writer.writeValueAsString(jsonRec.getJsonNode());
            } catch (SchemaNotAllowedException sna) {
                throw new ShellException(
                    "The schema associated with this record is not of the " +
                    "correct type", sna);
            } catch (IllegalArgumentException iae) {
                throw new ShellException("The record is not Avro format", iae);
            } catch (IOException ioe) {
                throw new ShellException("Error formatting the record", ioe);
            }
        }

        /* Encoded with base64 if it is not display-able */
        private String printableString(byte[] buf)
            throws ShellException {

            if (isAsciiPrintable(buf)) {
                return new String(buf);
            }
            return CommandUtils.encodeBase64(buf) + " [Base64]";
        }

        private boolean isAsciiPrintable(byte[] buf) {
            if (buf == null) {
                return true;
            }
            for (byte element : buf) {
                if ((element < 32) || (element > 126)) {
                    return false;
                }
            }
            return true;
        }
    }

    static class GetTableCommand extends SubCommand {
        final static String TABLE_FLAG = "-name";
        final static String TABLE_FLAG_DESC = TABLE_FLAG + " <name>";
        final static String FIELD_FLAG = "-field";
        final static String FIELD_FLAG_DESC = FIELD_FLAG + " <name>";
        final static String VALUE_FLAG = "-value";
        final static String VALUE_FLAG_DESC = VALUE_FLAG + " <value>";
        final static String NULL_VALUE_FLAG = "-null-value";
        final static String NULL_VALUE_FLAG_DESC = "-null-value";
        final static String ANCESTOR_FLAG = "-ancestor";
        final static String ANCESTOR_FLAG_DESC = ANCESTOR_FLAG + " <name>";
        final static String CHILD_FLAG = "-child";
        final static String CHILD_FLAG_DESC = CHILD_FLAG + " <name>";
        final static String INDEX_FLAG = "-index";
        final static String INDEX_FLAG_DESC = INDEX_FLAG + " <name>";
        final static String JSON_FLAG_DESC = JSON_FLAG + " <string>";
        final static String END_FLAG_DESC = END_FLAG + " <value>";
        final static String START_FLAG_DESC = START_FLAG + " <value>";
        final static String REPORT_SIZE_FLAG = "-report-size";
        final static String REPORT_SIZE_FLAG_DESC = REPORT_SIZE_FLAG;
        final static String PRETTY_FLAG = "-pretty";
        final static String PRETTY_FLAG_DESC = PRETTY_FLAG;

        public GetTableCommand() {
            super("table", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);

            String tableName = null;
            String indexName = null;
            String rgStart = null;
            String rgEnd = null;
            String jsonString = null;
            String outFile = null;
            String frFieldName = null;
            boolean pretty = false;
            boolean reportSize = false;
            boolean keyOnly = false;
            List<String> lstAncestor = new ArrayList<String>();
            List<String> lstChild = new ArrayList<String>();
            HashMap<String, String> mapVals = new HashMap<String, String>();
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if (TABLE_FLAG.equals(arg)) {
                    tableName = Shell.nextArg(args, i++, this);
                } else if (FIELD_FLAG.equals(arg)) {
                    String fname = Shell.nextArg(args, i++, this);
                    if (++i < args.length) {
                        arg = args[i];
                        if (VALUE_FLAG.equals(arg)) {
                            String fVal = Shell.nextArg(args, i++, this);
                            mapVals.put(fname, fVal);
                        } else {
                            while (i < args.length) {
                                arg = args[i];
                                if (START_FLAG.equals(arg)) {
                                    rgStart = Shell.nextArg(args, i++, this);
                                } else if (END_FLAG.equals(arg)) {
                                    rgEnd = Shell.nextArg(args, i++, this);
                                } else {
                                    break;
                                }
                                i++;
                            }
                            if (rgStart == null && rgEnd == null) {
                                shell.invalidArgument(arg + ", " +
                                    VALUE_FLAG + " or " +
                                    START_FLAG + " | " + END_FLAG +
                                    " is reqired", this);
                            }
                            frFieldName = fname;
                            i--;
                        }
                    } else {
                        shell.requiredArg(VALUE_FLAG + " is reqired", this);
                    }
                } else if (INDEX_FLAG.equals(arg)) {
                    indexName = Shell.nextArg(args, i++, this);
                } else if (ANCESTOR_FLAG.equals(arg)) {
                    lstAncestor.add(Shell.nextArg(args, i++, this));
                } else if (CHILD_FLAG.equals(arg)) {
                    lstChild.add(Shell.nextArg(args, i++, this));
                } else if (FILE_FLAG.equals(arg)) {
                    outFile = Shell.nextArg(args, i++, this);
                } else if (JSON_FLAG.equals(arg)) {
                    jsonString = Shell.nextArg(args, i++, this);
                } else if (PRETTY_FLAG.equals(arg)) {
                    pretty = true;
                } else if (REPORT_SIZE_FLAG.equals(arg)) {
                    reportSize = true;
                } else if (KEY_ONLY_FLAG.equals(arg)) {
                    keyOnly = true;
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            if (tableName == null) {
                shell.requiredArg(TABLE_FLAG, this);
            }

            String retString = null;
            final TableAPI tableImpl =
                ((CommandShell)shell).getStore().getTableAPI();
            final Table table =
                CommandUtils.findTable(tableImpl, tableName);

            /* Create key. */
            RecordValue key = null;
            if (jsonString == null) {
                if (indexName == null) {
                    key = table.createPrimaryKey();
                } else {
                    key = CommandUtils.findIndex(table, indexName)
                          .createIndexKey();
                }
                /* Set fieldValues to key. */
                for (Map.Entry<String, String> entry: mapVals.entrySet()) {
                    String fname = entry.getKey();
                    CommandUtils.putIndexKeyValues(key, table,
                                                   fname, entry.getValue());
                }
            } else {
                key = CommandUtils.createKeyFromJson(table, indexName,
                                                     jsonString);
            }

            /* Initialize MultiRowOptions. */
            MultiRowOptions mro = null;
            if (rgStart != null || rgEnd != null ||
                !lstAncestor.isEmpty() || !lstChild.isEmpty()) {
                mro = CommandUtils.createMultiRowOptions(tableImpl,
                        table, key, lstAncestor, lstChild,
                        frFieldName, rgStart, rgEnd);
            }

            /* Initialize output file. */
            ResultOutput output = null;
            Writer fwriter = null;
            if (outFile != null) {
                try {
                    File file = new File(outFile);
                    fwriter = new BufferedWriter(new FileWriter(file));
                } catch (IOException ioe) {
                    throw new ShellException(
                        "Could not open the output file " + outFile, ioe);
                }
                output = new ResultOutput(fwriter);
            } else {
                output = new ResultOutput(shell);
            }

            /* Get rows */
            try {
                retString = doGetOperation(tableImpl, key, mro,
                    keyOnly, output, pretty, reportSize);
            } catch (ShellException se) {
                throw se;
            } finally {
                if (fwriter != null) {
                    try {
                        fwriter.flush();
                        fwriter.close();
                    } catch (IOException ioe) {
                        throw new ShellException(
                            "Could not flush to file " + outFile, ioe);
                    }
                    if (retString != null && retString.length() > 0) {
                        retString += eol;
                    }
                    retString += "Wrote result to file " + outFile + ".";
                }
            }
            return (retString == null || retString.length() == 0) ?
                    null : retString;
        }

        /**
         * Class used to tally size information:
         *  type: PRIMARY_KEY, DATA, INDEX_KEY
         *  name: name of index if type is INDEX_KEY, null for other types.
         *  min: minimal value of sizes.
         *  max: maximal value of sizes.
         *  sum: sum of all sizes.
         *  count: num of sizes.
         */
        private static final class SizeInfo {
            static enum Type {PRIMARY_KEY, DATA, INDEX_KEY}
            private Type type;
            private String name;
            private int min;
            private int max;
            private double sum;
            private int count;

            SizeInfo(Type type) {
                this(type, null);
            }

            SizeInfo(Type type, String name) {
                this.name = name;
                this.type = type;
                min = 0;
                max = 0;
                sum = 0.0;
                count = 0;
            }

            void tally(int size) {
                if (min == 0 || size < min) {
                    min = size;
                }
                if (size > max) {
                    max = size;
                }
                sum += size;
                count++;
            }

            String getName() {
                return name;
            }

            Type getType() {
                return type;
            }

            int getMin() {
                return min;
            }

            int getMax() {
                return max;
            }

            double getAvg() {
                if (count == 0) {
                    return 0.0;
                }
                return sum/count;
            }

            int getCount() {
                return count;
            }
        }

        private String doGetOperation(final TableAPI tableImpl,
                                      final RecordValue key,
                                      final MultiRowOptions mro,
                                      final boolean keyOnly,
                                      final ResultOutput output,
                                      final boolean pretty,
                                      final boolean reportSize)
            throws ShellException {

            final StringBuilder sb = new StringBuilder();
            new RunTableAPIOperation() {
                @Override
                void doOperation() throws ShellException {

                    if (key.isPrimaryKey()) {
                        PrimaryKey pKey = key.asPrimaryKey();
                        if (mro == null &&
                            CommandUtils.matchFullPrimaryKey(pKey)) {
                            Row row = tableImpl.get(pKey, null);
                            if (row != null) {
                                output.writeRecord(formatReturnInfo(row));
                            } else {
                                sb.append("Key not found in store: ");
                                sb.append(key.toJsonString(false));
                            }
                        } else {
                            Iterator<?> itr = null;
                            try {
                                if (CommandUtils.matchFullMajorKey(pKey)) {
                                    if (keyOnly) {
                                        itr = tableImpl.multiGetKeys(pKey,
                                            mro, null).iterator();
                                    } else {
                                        itr = tableImpl.multiGet(pKey,
                                            mro, null).iterator();
                                    }
                                } else {
                                    if (keyOnly) {
                                        itr = tableImpl.tableKeysIterator(pKey,
                                            mro, null);
                                    } else {
                                        itr = tableImpl.tableIterator(pKey,
                                            mro, null);
                                    }
                                }
                                SizeInfo[] stInfo = null;
                                if (reportSize) {
                                    stInfo = initSizeInfos(pKey.getTable(),
                                        false);
                                }
                                doIteration(itr, stInfo);
                            } finally {
                                if (itr instanceof TableIterator) {
                                    ((TableIterator<?>)itr).close();
                                }
                            }
                        }
                    } else {
                        TableIterator<?> itr = null;
                        try {
                            if (keyOnly) {
                                itr = tableImpl.tableKeysIterator((IndexKey)key,
                                    mro, null);
                            } else {
                                itr = tableImpl.tableIterator((IndexKey)key,
                                    mro, null);
                            }
                            SizeInfo[] stInfo = null;
                            if (reportSize) {
                                stInfo = initSizeInfos(
                                    ((IndexKey)key).getIndex().getTable(),
                                    true);
                            }
                            doIteration(itr, stInfo);
                        } finally {
                            if (itr != null) {
                                itr.close();
                            }
                        }
                    }
                    output.flushWriting();
                }

                /* Format return information for a single row. */
                private String formatReturnInfo(Row row) {
                    if (reportSize) {
                        return getSizeTitle(SizeInfo.Type.PRIMARY_KEY) +
                            " size: " + getKeySize(row) + eol +
                            getSizeTitle(SizeInfo.Type.DATA) + " size: " +
                            (keyOnly ? "Not available" : getDataSize(row));
                    }
                    return (keyOnly) ?
                        row.createPrimaryKey().toJsonString(pretty) :
                        row.toJsonString(pretty);
                }

                private SizeInfo[] initSizeInfos(Table table,
                                                 boolean indexScan) {
                    SizeInfo[] sizeInfos = null;
                    if (keyOnly) {
                        if (indexScan) {
                            sizeInfos = new SizeInfo[2];
                            sizeInfos[0] =
                                new SizeInfo(SizeInfo.Type.PRIMARY_KEY);
                            sizeInfos[1] =
                                new SizeInfo(SizeInfo.Type.INDEX_KEY);
                        } else {
                            sizeInfos = new SizeInfo[1];
                            sizeInfos[0] =
                                new SizeInfo(SizeInfo.Type.PRIMARY_KEY);
                        }
                    } else {
                        int i = 0;
                        sizeInfos = new SizeInfo[2 + table.getIndexes().size()];
                        sizeInfos[i++] =
                            new SizeInfo(SizeInfo.Type.PRIMARY_KEY);
                        sizeInfos[i++] = new SizeInfo(SizeInfo.Type.DATA);
                        for (Entry<String, Index> entry :
                             table.getIndexes().entrySet()) {
                            sizeInfos[i++] =
                                new SizeInfo(SizeInfo.Type.INDEX_KEY,
                                    entry.getKey());
                        }
                    }
                    return sizeInfos;
                }

                private void doIteration(final Iterator<?> iterator,
                                         SizeInfo[] stInfo)
                    throws ShellException {

                    long nRec = 0;
                    while (iterator.hasNext()) {
                        try {
                            Object obj = iterator.next();
                            if (stInfo != null) {
                                tallySize(obj, stInfo);
                            } else {
                                if (!output.writeRecord(getJsonString(obj))) {
                                    break;
                                }
                                nRec++;
                            }
                        } catch (StoreIteratorException sie) {
                            Throwable t = sie.getCause();
                            if (t != null && t instanceof FaultException) {
                                throw (FaultException)t;
                            }
                            throw new ShellException(
                                t != null ? t.getMessage() : sie.getMessage());
                        }
                    }
                    if (stInfo != null) {
                        output.writeRecord(formatOutputSizesInfo(stInfo));
                    } else {
                        sb.append(nRec);
                        sb.append((nRec > 1) ?
                            " rows returned." : " row returned.");
                    }
                }

                private String getJsonString(Object obj) {
                    if (obj instanceof KeyPair) {
                        return ((KeyPair)obj).getPrimaryKey()
                            .toJsonString(pretty);
                    }
                    return ((Row)obj).toJsonString(pretty);
                }

                private void tallySize(Object obj, SizeInfo[] stInfo) {
                    for (SizeInfo info: stInfo) {
                        switch (info.getType()) {
                        case PRIMARY_KEY:
                            info.tally(getKeySize(obj));
                            break;
                        case DATA:
                            info.tally(getDataSize(obj));
                            break;
                        case INDEX_KEY:
                            info.tally(getIndexKeySize(obj, info.getName()));
                            break;
                        default:
                            break;
                        }
                    }
                }

                private int getKeySize(Object obj) {
                    if (obj instanceof KeyPair) {
                        return TableUtils.getKeySize(
                            ((KeyPair)obj).getPrimaryKey());
                    }
                    return TableUtils.getKeySize((Row)obj);
                }

                private int getDataSize(Object obj) {
                    if (!(obj instanceof Row)) {
                        return 0;
                    }
                    return TableUtils.getDataSize((Row)obj);
                }

                private int getIndexKeySize(Object obj, String indexName) {
                    if (obj instanceof PrimaryKey) {
                        return 0;
                    }

                    if (obj instanceof KeyPair) {
                        return TableUtils.getKeySize(
                            ((KeyPair)obj).getIndexKey());
                    }

                    if (indexName == null) {
                        return 0;
                    }
                    final Row row = (Row)obj;
                    final Index index = row.getTable().getIndex(indexName);
                    if (index == null) {
                        return 0;
                    }
                    return TableUtils.getKeySize(index.createIndexKey(row));
                }

                private String getSizeTitle(SizeInfo.Type type) {
                    switch (type) {
                    case PRIMARY_KEY:
                        return "Primary Key";
                    case DATA:
                        return "Data";
                    case INDEX_KEY:
                        return "Index Key";
                    default:
                        break;
                    }
                    return null;
                }

                private String formatOutputSizesInfo(SizeInfo[] stInfo) {
                    StringBuilder buf = new StringBuilder();
                    Formatter fmt = new Formatter(buf);
                    int nRec = stInfo[0].getCount();
                    fmt.format("Number of records: %d", nRec);
                    for (SizeInfo info: stInfo) {
                        if (info.getName() != null) {
                            fmt.format(eol + "%s sizes of %s:",
                                getSizeTitle(info.getType()), info.getName());
                        } else {
                            fmt.format(eol + "%s sizes:",
                                getSizeTitle(info.getType()));
                        }
                        if (nRec > 0) {
                            fmt.format(eolt + "Minimum size: %d",
                                info.getMin());
                            fmt.format(eolt + "Maximum size: %d",
                                info.getMax());
                            fmt.format(eolt + "Average size: %.1f",
                                info.getAvg());
                        } else {
                            fmt.format(" Not available");
                        }
                    }
                    if (stInfo.length == 1) {
                        fmt.format(eol + "%s sizes: Not available",
                            getSizeTitle(SizeInfo.Type.DATA));
                    }
                    fmt.close();
                    return buf.toString();
                }
            }.run();
            return sb.toString();
        }

        @Override
        protected String getCommandDescription() {
            return "Performs a get operation to retrieve row(s) " +
                "from a named table." + eolt +
                "The table name is a dot-separated name with the format" +
                eolt + "tableName[.childTableName]*." + eolt +
                FIELD_FLAG + " and " + VALUE_FLAG + " pairs are used to " +
                "specify the field values of" + eolt +
                "the primary key or index key if using index specified by " +
                INDEX_FLAG + "," + eolt +
                "or with an empty key to iterate the the entire table." + eolt +
                FIELD_FLAG + "," + START_FLAG + " and " + END_FLAG + " flags " +
                "can be used for restricting the sub" + eolt +
                "range for retrieving rows associated with the parent key." +
                eolt +
                ANCESTOR_FLAG + " and " + CHILD_FLAG + " flags can be " +
                "used to return results from" + eolt +
                "specific ancestor and/or descendant tables as well as " +
                "the target" + eolt + "table." + eolt +
                JSON_FLAG + " indicates that the key field values are in " +
                "JSON format." + eolt +
                FILE_FLAG + " is used to specify an output file, " +
                "which is truncated." + eolt +
                KEY_ONLY_FLAG + " is used to restrict information to " +
                "keys only." + eolt +
                PRETTY_FLAG + " is used for a nicely formatted JSON string " +
                "with indentation" + eolt + "and carriage returns." + eolt +
                REPORT_SIZE_FLAG + " is used to show key and data size " +
                "information for primary" + eolt + "keys, data values, and " +
                "index keys for matching records.  When" + eolt +
                "-report-size is specified no data is displayed.";
        }

        @Override
        protected String getCommandSyntax() {
            return "get " + getCommandName() + " " + TABLE_FLAG_DESC +
                " [" + INDEX_FLAG_DESC + "]" +
                eolt +
                "[" + FIELD_FLAG_DESC + " " + VALUE_FLAG_DESC + "]* " +
                eolt +
                "[" + FIELD_FLAG_DESC + " [" + START_FLAG_DESC + "] [" +
                END_FLAG_DESC + "]]" +
                eolt +
                "[" + ANCESTOR_FLAG_DESC + "]* [" + CHILD_FLAG_DESC + "]*" +
                eolt +
                "[" + JSON_FLAG_DESC + "] [" + FILE_FLAG_DESC + "] " +
                "[" + KEY_ONLY_FLAG + "]" +
                eolt +
                "[" + PRETTY_FLAG_DESC + "] [" + REPORT_SIZE_FLAG_DESC + "]" ;
        }
    }

    private static class ResultOutput {
        private final static int MAX_OUTFILE_BUFF_SIZE = 512 * 1024;

        private int pageHeight;
        private int pageLines;
        private int pageRecords;
        private int totalRecords;
        private StringBuilder output;
        private Shell shell = null;
        private Writer writer = null;

        ResultOutput(Writer writer) {
            this.totalRecords = 0;
            this.output = new StringBuilder();
            this.writer = writer;
        }

        ResultOutput(Shell shell) {
            this.shell = shell;
            this.pageLines = 0;
            this.totalRecords = 0;
            this.pageRecords = 0;
            this.pageHeight = getTermHeight();
            this.output = new StringBuilder();
        }

        public void flushWriting()
            throws ShellException {

            if (IsOutputFile()) {
                try {
                    writer.write(output.toString());
                } catch (IOException ioe) {
                    throw new ShellException(
                        "Can not write to the output file", ioe);
                }
                output.setLength(0);
            } else {
                if (output.length() > 0) {
                    shell.println(output.toString());
                }
            }
        }

        public boolean writeRecord(String record)
            throws ShellException {

            if (IsOutputFile()) {
                writeToFile(record);
                return true;
            }
            return writeToTerm(record);
        }

        private void writeToFile(String record)
            throws ShellException {

            if (output.length() >= MAX_OUTFILE_BUFF_SIZE) {
                try {
                    writer.write(output.toString());
                } catch (IOException ioe) {
                    throw new ShellException(
                        "Can not write to the output file", ioe);
                }
                output.setLength(0);
            }
            if (record != null) {
                output.append(record);
                output.append(eol);
            }
        }

        private boolean writeToTerm(String record)
            throws ShellException {

            if (pageHeight < 0) {
                if (output.length() >= MAX_OUTFILE_BUFF_SIZE) {
                    shell.println(output.toString());
                }
                output.append(record);
                output.append(eol);
                return true;
            }

            if (pageLines >= pageHeight) {
                output.append("--More--(");
                output.append((totalRecords - pageRecords + 1));
                output.append("~");
                output.append(totalRecords);
                output.append(")");
                shell.println(output.toString());
                output.setLength(0);
                pageLines = 0;
                pageRecords = 0;
                try {
                    String ret = shell.getInput().readLine("");
                    if (ret.toLowerCase().startsWith("q")) {
                        return false;
                    }
                } catch (IOException e) {
                    throw new ShellException("Exception reading input");
                }
            }
            pageRecords++;
            output.append(record);
            output.append(eol);
            pageLines += countLines(record);
            totalRecords++;
            return true;
        }

        private boolean IsOutputFile() {
            return (writer != null);
        }

        private int countLines(String str) {
            return str.split("\r\n|\r|\n").length;
        }

        private int getTermHeight() {
            ShellInputReader inputReader = shell.getInput();
            if (inputReader != null) {
                return inputReader.getTerminalHeight();
            }
            return -1;
        }
    }
}
