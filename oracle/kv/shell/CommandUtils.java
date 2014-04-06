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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import oracle.kv.DurabilityException;
import oracle.kv.FaultException;
import oracle.kv.Key;
import oracle.kv.RequestTimeoutException;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.api.table.JsonUtils;
import oracle.kv.table.ArrayDef;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldDef.Type;
import oracle.kv.table.FieldRange;
import oracle.kv.table.FieldValue;
import oracle.kv.table.Index;
import oracle.kv.table.IndexKey;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.util.shell.ShellException;

class CommandUtils {

    /**
     * Create a Key from a URI-formatted string
     */
    static Key createKeyFromURI(String uri) {
        return Key.fromString(uri);
    }

    /**
     * Create a URI from a Key
     */
    static String createURI(Key key) {
        return key.toString();
    }

    /**
     * Translate the specified Base64 string into a byte array.
     * @throws ShellException
     */
    static String encodeBase64(byte[] buf)
            throws ShellException {
        try {
            return JsonUtils.encodeBase64(buf);
        } catch (IllegalArgumentException iae) {
            throw new ShellException("Get Exception in Base64 encoding:" + iae);
        }
    }

    /**
     * Decode the specified Base64 string into a byte array.
     * @throws ShellException
     */
    static byte[] decodeBase64(String str)
        throws ShellException {
        try {
            return JsonUtils.decodeBase64(str);
        } catch (IllegalArgumentException iae) {
            throw new ShellException("Get Exception in Base64 decoding:" + iae);
        }
    }

    abstract static class RunTableAPIOperation {
        abstract void doOperation() throws ShellException;

        void run() throws ShellException {
            try {
                doOperation();
            } catch (IllegalArgumentException iae) {
                throw new ShellException(iae.getMessage());
            } catch (IllegalCommandException ice) {
                throw new ShellException(ice.getMessage());
            } catch (DurabilityException de) {
                throw new ShellException(de.getMessage());
            } catch (RequestTimeoutException rte) {
                throw new ShellException(rte.getMessage());
            } catch (FaultException fe) {
                throw new ShellException(fe.getMessage());
            }
        }
    }

    static Table findTable(TableAPI tableImpl,
                           String tableName)
        throws ShellException {

        Table table = tableImpl.getTable(tableName);
        if (table != null) {
            return table;
        }
        throw new ShellException("Table does not exist: " + tableName);
    }

    static Index findIndex(Table table, String indexName)
        throws ShellException {

        Index index = table.getIndex(indexName);
        if (index != null) {
            return index;
        }
        throw new ShellException("Index does not exist: " + indexName +
            " on table: " + table.getFullName());
    }

    static FieldDef findFieldDef(Table table, String fieldName)
        throws ShellException {

        FieldDef def = table.getField(fieldName);
        if (def != null) {
            return def;
        }
        throw new ShellException("Field does not exist: " + fieldName +
            " in table: " + table.getFullName());
    }

    static boolean validateIndexField(String fieldName, Index index)
        throws ShellException {

        for (String fname: index.getFields()) {
            if (fieldName.equals(fname)) {
                return true;
            }
        }
        throw new ShellException
            ("Invalid index field: " + fieldName +
             " for index: " + index.getName() +
             " on table: " + index.getTable().getFullName());
    }

    static void putIndexKeyValues(RecordValue key, Table table,
                                  String fieldName, String sValue)
        throws ShellException {

        FieldDef def = findFieldDef(table, fieldName);
        FieldValue fdValue = createFieldValue(def, sValue);
        try {
            if (key.isPrimaryKey()) {
                key.asPrimaryKey().put(fieldName, fdValue);
            } else {
                key.put(fieldName, fdValue);
            }
        } catch (IllegalArgumentException iae) {
            throw new ShellException(iae.getMessage(), iae);
        }
    }

    static RecordValue createKeyFromJson(Table table,
                                         String indexName,
                                         String jsonString)
        throws ShellException {

        try {
            RecordValue key = null;
            if (indexName == null) {
                key = table.createPrimaryKeyFromJson(jsonString, false);
            } else {
                key = findIndex(table, indexName).
                        createIndexKeyFromJson(jsonString, false);
            }
            return key;
        } catch (IllegalArgumentException iae) {
            throw new ShellException(iae.getMessage(), iae);
        }
    }

    static FieldValue createFieldValue(FieldDef def, String sValue,
                                       boolean isFile)
        throws ShellException {

        try {
            FieldValue fdVal = null;
            switch (def.getType()) {
            case INTEGER:
                try {
                    fdVal = def.createInteger(Integer.parseInt(sValue));
                } catch (NumberFormatException nfe) {
                    throw new ShellException(
                        "Invalid integer value: " + sValue);
                } catch (IllegalArgumentException iae) {
                     throw new ShellException(iae.getMessage());
                }
                break;
            case LONG:
                try {
                    fdVal = def.createLong(Long.parseLong(sValue));
                } catch (NumberFormatException nfe) {
                    throw new ShellException(
                        "Invalid long value: " + sValue);
                } catch (IllegalArgumentException iae) {
                    throw new ShellException(iae.getMessage());
                }
                break;
            case DOUBLE:
                try {
                    fdVal = def.createDouble(Double.parseDouble(sValue));
                } catch (NumberFormatException nfe) {
                    throw new ShellException(
                        "Invalid double value: " + sValue);
                } catch (IllegalArgumentException iae) {
                    throw new ShellException(iae.getMessage());
                }
                break;
            case FLOAT:
                try {
                    fdVal = def.createFloat(Float.parseFloat(sValue));
                } catch (NumberFormatException nfe) {
                    throw new ShellException(
                        "Invalid float value: " + sValue);
                } catch (IllegalArgumentException iae) {
                    throw new ShellException(iae.getMessage());
                }
                break;
            case STRING:
                try {
                    fdVal = def.createString(sValue);
                } catch (IllegalArgumentException iae) {
                    throw new ShellException(iae.getMessage());
                }
                break;
            case ENUM:
                try {
                    fdVal = def.createEnum(sValue);
                } catch (IllegalArgumentException iae) {
                    throw new ShellException(iae.getMessage());
                }
                break;
            case BOOLEAN:
                try {
                    fdVal = def.createBoolean(Boolean.parseBoolean(sValue));
                } catch (IllegalArgumentException iae) {
                    throw new ShellException(iae.getMessage());
                }
                break;
            case FIXED_BINARY:
            case BINARY:
                byte[] data = null;
                if (isFile) {
                    data = readFromFile(sValue);
                } else {
                    data = decodeBase64(sValue);
                }
                try {
                    if (def.getType() == Type.BINARY) {
                        fdVal = def.createBinary(data);
                    } else {
                        fdVal = def.createFixedBinary(data);
                    }
                } catch (IllegalArgumentException iae) {
                    throw new ShellException(iae.getMessage());
                }
                break;
            case ARRAY:
                try {
                    fdVal = def.createArray();
                    FieldDef elementDef = getFieldDef(fdVal, null);
                    FieldValue fv = createFieldValue(elementDef, sValue);
                    fdVal.asArray().add(fv);
                } catch (IllegalArgumentException iae) {
                    throw new ShellException(iae.getMessage());
                }
                break;
            default:
                throw new ShellException("Can't create a field value for " +
                    def.getType() + " field from a string: " + sValue);
            }
            return fdVal;
        } catch (IllegalArgumentException iae) {
            throw new ShellException(iae.getMessage());
        }
    }

    private static FieldValue createFieldValue(FieldDef def, String sValue)
        throws ShellException {

        return createFieldValue(def, sValue, false);
    }

    static MultiRowOptions createMultiRowOptions(TableAPI tableImpl,
                                                 Table table,
                                                 RecordValue key,
                                                 List<String> ancestors,
                                                 List<String> children,
                                                 String frFieldName,
                                                 String rgStart,
                                                 String rgEnd)
        throws ShellException {

        FieldRange fr = null;
        List<Table> lstAncestor = null;
        List<Table> lstChild = null;
        if (frFieldName != null) {
            fr = createFieldRange(table, key, frFieldName, rgStart, rgEnd);
        }

        if (!ancestors.isEmpty()) {
            lstAncestor = new ArrayList<Table>(ancestors.size());
            for (String tname: ancestors) {
                lstAncestor.add(findTable(tableImpl, tname));
            }
        }
        if (!children.isEmpty()) {
            lstChild = new ArrayList<Table>(children.size());
            for (String tname: children) {
                lstChild.add(findTable(tableImpl, tname));
            }
        }
        if (fr != null || lstAncestor != null || lstChild != null) {
            return new MultiRowOptions(fr, lstAncestor, lstChild);
        }
        return null;
    }

    static boolean matchFullMajorKey(PrimaryKey key) {
        Table table = key.getTable();
        for (String fieldName: table.getShardKey()) {
            if (key.get(fieldName) == null) {
                return false;
            }
        }
        return true;
    }

    static boolean matchFullPrimaryKey(PrimaryKey key) {
        Table table = key.getTable();
        for (String fieldName: table.getPrimaryKey()) {
            if (key.get(fieldName) == null) {
                return false;
            }
        }
        return true;
    }

    private static FieldRange createFieldRange(Table table, RecordValue key,
                                 String fieldName, String start, String end)
        throws ShellException{

        try {
           FieldRange range = null;
           if (key.isPrimaryKey()) {
               range = table.createFieldRange(fieldName);
           } else {
               range = ((IndexKey)key).getIndex().createFieldRange(fieldName);
           }
           if (start != null) {
               range.setStart(
                   createFieldValue(getRangeFieldDef(range), start), true);
           }
           if (end != null) {
               range.setEnd(
                   createFieldValue(getRangeFieldDef(range), end), true);
           }
           return range;
        } catch (IllegalArgumentException iae) {
           throw new ShellException(iae.getMessage(), iae);
        }
    }

    private static FieldDef getRangeFieldDef(FieldRange range) {
        if (range.getDefinition().isArray()) {
            return ((ArrayDef)range.getDefinition()).getElement();
        }
        return range.getDefinition();
    }

    static FieldDef getFieldDef(FieldValue fieldValue, String fieldName)
        throws ShellException {

        try {
            if (fieldValue.isRecord()) {
                FieldDef def = fieldValue.asRecord().getDefinition()
                    .getField(fieldName);
                if (def == null) {
                    throw new ShellException("No such field: " + fieldName);
                }
                return def;
            } else if (fieldValue.isArray()) {
                return fieldValue.asArray().getDefinition().getElement();
            } else if (fieldValue.isMap()) {
                return fieldValue.asMap().getDefinition().getElement();
            }
            return null;
        } catch (ClassCastException cce) {
            throw new ShellException(cce.getMessage(), cce);
        }
    }

    static byte[] readFromFile(String fileName)
        throws ShellException {

        File file = new File(fileName);
        int len = (int)file.length();
        if (len == 0) {
            throw new ShellException(
                "Input file not found or empty: " + fileName);
        }
        byte[] buffer = new byte[len];

        FileInputStream fis = null;
        try {
            fis = new FileInputStream(file);
            fis.read(buffer);
        } catch (IOException ioe) {
            throw new ShellException("Read file error: " + fileName, ioe);
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException ignored) {
                }
            }
        }
        return buffer;
    }
}
