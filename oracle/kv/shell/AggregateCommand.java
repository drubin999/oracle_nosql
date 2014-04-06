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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import oracle.kv.Direction;
import oracle.kv.KVStore;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.KeyValueVersion;
import oracle.kv.Value;
import oracle.kv.avro.AvroCatalog;
import oracle.kv.avro.JsonAvroBinding;
import oracle.kv.avro.JsonRecord;
import oracle.kv.avro.SchemaNotAllowedException;
import oracle.kv.impl.admin.client.CommandShell;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.codehaus.jackson.JsonNode;

/*
 * Aggregate command, a simple data aggregation command to count, sum, or
 * average numeric fields that match the input key and are of the appropriate
 * type.  Records found must be Avro for sum and avg to function properly.
 * Sum and avg match on any Avro recording containing the named numeric fields.
 */
public class AggregateCommand extends ShellCommand {
    private static final Type[] SCHEMA_NUMERIC_TYPES = {
        Type.INT, Type.LONG, Type.FLOAT, Type.DOUBLE
    };

    public AggregateCommand() {
        super("aggregate", 3);
    }

    @Override
    public String execute(String[] args, Shell shell)
        throws ShellException {

        boolean isCount = false;
        List<String> sumFields = null;
        List<Integer> avgIdxFields = null;
        Key key = null;
        String rangeStart = null;
        String rangeEnd = null;
        KVStore store = ((CommandShell) shell).getStore();

        for (int i = 1; i < args.length; i++) {
            String arg = args[i];
            if ("-count".equals(arg)) {
                isCount = true;
            } else if ("-sum".equals(arg)) {
                String str = Shell.nextArg(args, i++, this);
                String[] fields = str.split(",");
                if (sumFields == null) {
                    sumFields = new ArrayList<String>();
                }
                sumFields.addAll(Arrays.asList(fields));
            } else if ("-avg".equals(arg)) {
                String str = Shell.nextArg(args, i++, this);
                String[] fields = str.split(",");
                if (sumFields == null) {
                    sumFields = new ArrayList<String>();
                }
                sumFields.addAll(Arrays.asList(fields));
                if (avgIdxFields == null) {
                    avgIdxFields = new ArrayList<Integer>();
                }
                int index = sumFields.size() - fields.length;
                while (index < sumFields.size()) {
                    avgIdxFields.add(Integer.valueOf(index++));
                }
            } else if ("-key".equals(arg)) {
                String keyString = Shell.nextArg(args, i++, this);
                try {
                    key = CommandUtils.createKeyFromURI(keyString);
                } catch (IllegalArgumentException iae) {
                    shell.invalidArgument(iae.getMessage(), this);
                }
            } else if ("-start".equals(arg)) {
                rangeStart = Shell.nextArg(args, i++, this);
            } else if ("-end".equals(arg)) {
                rangeEnd = Shell.nextArg(args, i++, this);
            } else {
                shell.unknownArgument(arg, this);
            }
        }

        if (!isCount && (sumFields == null) && (avgIdxFields == null)) {
            shell.requiredArg("-count or -sum or -avg", this);
        }

        KeyRange kr = null;
        if (rangeStart != null || rangeEnd != null) {
            try {
                kr = new KeyRange(rangeStart, true, rangeEnd, true);
            } catch (IllegalArgumentException iae) {
                shell.invalidArgument(iae.getMessage(), this);
            }
        }

        String returnValue = "";
        if ((sumFields == null) && (avgIdxFields == null)) {
            returnValue = countKeys(store, key, kr);
        } else {
            returnValue = execAgg(store, key, kr, isCount,
                                  sumFields, avgIdxFields);
        }
        return returnValue;
    }

    /*
     * Count the keys
     */
    private String countKeys(KVStore store, Key key, KeyRange kr)
        throws ShellException {

        Iterator<Key> it = null;
        try {
            if (key != null &&
                key.getMinorPath() != null &&
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
                     * A complete major path won't work with store iterator and
                     * we can't distinguish between a complete major path or
                     * not, so if store iterator fails entire, try the key as
                     * a complete major path.
                     */
                    it = store.multiGetKeysIterator(Direction.FORWARD,
                                                    100, key, kr, null);
                }
            }
        } catch (Exception e) {
            throw new ShellException("Exception from NoSQL DB in creating " +
                                  "key iterator:" + eolt + e.getMessage(), e);
        }

        long totalKeys = 0;
        while (it.hasNext()) {
            it.next();
            totalKeys++;
        }
        return "count: " + totalKeys;
    }

    /*
     * execAgg is the heart of this command, where execution occurs.  It does
     * one of:
     *  - count the total num of records
     *  - sum for the field with numeric type
     *  - avg = sum/count
     */
    private String execAgg(KVStore store,
                             Key key,
                             KeyRange kr,
                             boolean showCount,
                             List<String> sumFields,
                             List<Integer> avgIdxFields)
        throws ShellException {

        Iterator<KeyValueVersion> it = null;
        try {
            if (key != null && key.getMinorPath() != null &&
                key.getMinorPath().size() > 0) {
                it = store.multiGetIterator(Direction.FORWARD,
                                            100, key, kr, null);
            } else {
                it = store.storeIterator(Direction.UNORDERED,
                                         100, key, kr, null);
                if (!it.hasNext() && key != null) {
                    it = store.multiGetIterator(Direction.FORWARD,
                                                100, key, kr, null);
                }
            }
        } catch (Exception e) {
            throw new ShellException("Exception from NoSQL DB in creating " +
                                     "iterator:" + eolt + e.getMessage(), e);
        }

        long count = 0;
        Object[] sums = new Object[sumFields.size()];
        while (it.hasNext()) {
            Value value = it.next().getValue();
            if (value == null || (value.getFormat() != Value.Format.AVRO)) {
                continue;
            }
            Object[] fieldsValue = getFieldsValue(store, value, sumFields);
            sumFieldValues(sums, fieldsValue);
            count++;
        }
        String returnValue = genStatsSummary(count, sumFields, avgIdxFields,
                                             showCount, sums);
        return returnValue;
    }

    private Object[] getFieldsValue(KVStore store,
                                    Value value,
                                    List<String> fields)
        throws ShellException {

        AvroCatalog catalog = store.getAvroCatalog();
        catalog.refreshSchemaCache(null);
        Map<String, Schema> schemaMap = catalog.getCurrentSchemas();
        JsonAvroBinding binding = catalog.getJsonMultiBinding(schemaMap);
        Object[] fieldValues = new Object[fields.size()];
        try {
            JsonRecord jsonRec = binding.toObject(value);
            for (int i = 0; i < fields.size(); i++) {
                fieldValues[i] = getScalarValue(jsonRec, fields.get(i));
            }
            return fieldValues;
        } catch (SchemaNotAllowedException sna) {
            throw new ShellException("The schema associated with this record" +
                                     " is not of the correct type", sna);
        } catch (IllegalArgumentException iae) {
            throw new ShellException("The record is not Avro format", iae);
        }
    }

    private Object getScalarValue(JsonRecord jsonRec, String fieldName) {
        Type type = getScalarType(jsonRec.getSchema(), fieldName);
        if (type == null) {
            return null;
        }
        JsonNode jsonNode = jsonRec.getJsonNode().get(fieldName);
        if (jsonNode.isNull() || !jsonNode.isNumber()) {
            return null;
        }
        switch (type) {
        case INT:
        case LONG:
            return Long.valueOf(jsonNode.getLongValue());
        case FLOAT:
        case DOUBLE:
            return Double.valueOf(jsonNode.getDoubleValue());
        default:
            break;
        }
        return null;
    }

    /*
     * Check if the field is numeric type.
     */
    private Type getScalarType(Schema schema, String fieldName) {
        Field field = schema.getField(fieldName);
        if (field == null) {
            return null;
        }
        Type fieldType = field.schema().getType();
        for (Type type: SCHEMA_NUMERIC_TYPES){
            if (fieldType.equals(type)) {
                return fieldType;
            }
        }
        return null;
    }

    /*
     * Do addition: sum = sum + value
     *  - check if get overflow err for long + long,
     *      cast to double and add them if so.
     *  - deal with long + double case,
     *    Fields with same name existed in different schemas has different type.
     *
     */
    private void sumFieldValues(Object[] sums, Object[] values) {
        for (int i = 0; i < values.length; i++) {
            Object value = values[i];
            if (value == null) {
                continue;
            }
            if (sums[i] == null) {
                sums[i] = value;
            } else {
                Object sum = sums[i];
                if ((value instanceof Long) && (sum instanceof Long)) {
                    /* long + long */
                    try {
                        long v = ((Long)value).longValue();
                        long s = ((Long)sum).longValue();
                        long l_sum = longAddAndCheck(v, s);
                        sums[i] = Long.valueOf(l_sum);
                    } catch (ArithmeticException ae) {
                        double d_sum = ((Long)value).doubleValue() +
                                       ((Long)sum).doubleValue();
                        sums[i] = Double.valueOf(d_sum);
                    }
                } else {
                    double d_value = 0.0d;
                    double d_sum = 0.0d;
                    if (value instanceof Long) {
                        d_value = ((Long)value).doubleValue();
                        d_sum = ((Double)sum).doubleValue();
                    } else if (sum instanceof Long) {
                        d_sum = ((Long)sum).doubleValue();
                        d_value = ((Double)value).doubleValue();
                    } else {
                        d_value = ((Double)value).doubleValue();
                        d_sum = ((Double)sum).doubleValue();
                    }
                    d_sum += d_value;
                    sums[i] = Double.valueOf(d_sum);
                }
            }
        }
    }

    /**
     * Add two long integers, checking for overflow.
     */
    private static long longAddAndCheck(long a, long b) {
        long ret;
        if (a > b) {
            /* use symmetry to reduce boundary cases */
            ret = longAddAndCheck(b, a);
        } else {
            /* assert a <= b */
            if (a < 0) {
                if (b < 0) {
                    /* check for negative overflow */
                    if (Long.MIN_VALUE - b <= a) {
                        ret = a + b;
                    } else {
                        throw new ArithmeticException("longAdd:underflow");
                    }
                } else {
                    /* Opposite sign addition is always safe */
                    ret = a + b;
                }
            } else {
                /* check for positive overflow */
                if (a <= Long.MAX_VALUE - b) {
                    ret = a + b;
                } else {
                    throw new ArithmeticException("longAdd:overflow");
                }
            }
        }
        return ret;
    }

    /* Generate output result. */
    private String genStatsSummary(long count,
                                   List<String> sumFields,
                                   List<Integer> avgIdxFields,
                                   boolean showCount,
                                   Object[] sums) {
        String returnString = "";
        if (showCount) {
            returnString = "count: " + count;
        }
        int j = 0;
        for (int i = 0; i < sumFields.size(); i++) {
            Object sum = sums[i];
            String fieldName = sumFields.get(i);
            boolean isAvg = false;
            if (avgIdxFields != null && j < avgIdxFields.size()) {
                if (i == avgIdxFields.get(j).intValue()) {
                    isAvg = true;
                    j++;
                }
            }
            if (sum == null) {
                returnString += eol + (isAvg ? "avg" : "sum") +
                    "(" + fieldName + "): " + "field \"" + fieldName
                    + "\" does not exist or is not a numeric field.";
                continue;
            }
            if ((sum instanceof Double) &&
                ((Double)sum).isInfinite()) {
                returnString += eol + (isAvg ? "avg" : "sum") +
                    "(" + fieldName + "): " + "double overflow error.";
                continue;
            }
            if (isAvg) {
                double avg = 0.0;
                if (sum instanceof Long) {
                    avg = (double)((Long)sum).longValue()/(double)count;
                } else if (sum instanceof Double){
                    avg = ((Double)sum).doubleValue()/count;
                }
                returnString += eol + "avg(" + fieldName + "): " + avg;
            } else {
                returnString += eol + "sum(" + fieldName + "): " + sum;
            }
        }
        return returnString;
    }

    @Override
    protected String getCommandSyntax() {
        return "aggregate [-count] [-sum <field[,field,..]>] " +
            "[-avg <field[,field,..]>] " +  eolt +
            "[-key <key>] [-start <prefixString>] [-end <prefixString>]";
    }

    @Override
    protected String getCommandDescription() {
        return "Performs simple data aggregation operations on numeric fields" +
            eolt +
            "-count returns the count of matching records" +
            eolt +
            "-sum returns the sum of the values of matching fields." +
            eolt + "     All records with a schema with the named field are" +
            eolt + "     matched.  Unmatched records are ignored." +
            eolt +
            "-avg returns the average of the values of matching fields" +
            eolt + "     All records with a schema with the named field are" +
            eolt + "     matched.  Unmatched records are ignored." +
            eolt +
            "-key specifies the key (prefix) to use." +
            eolt +
            "-start and -end flags can be used for restricting the range " +
            "used" + eolt + "for iteration." ;
    }
}
