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

package oracle.kv.impl.api.table;

import static oracle.kv.impl.api.table.JsonUtils.NAME;
import static oracle.kv.impl.api.table.JsonUtils.TYPE;
import static oracle.kv.impl.api.table.JsonUtils.ENUM_NAME;
import static oracle.kv.impl.api.table.JsonUtils.ENUM_VALS;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import oracle.kv.impl.util.SortableString;
import oracle.kv.table.EnumDef;
import oracle.kv.table.FieldDef;

import com.sleepycat.persist.model.Persistent;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

@Persistent(version=1)
class EnumDefImpl extends FieldDefImpl
    implements EnumDef {

    private static final long serialVersionUID = 1L;
    private final String[] values;
    private final String name;
    private transient int encodingLen;

    EnumDefImpl(final String name, final String[] values,
                final String description) {
        super(FieldDef.Type.ENUM, description);
        this.name = name;
        this.values = values;
        validate();
        init();
    }

    EnumDefImpl(final String name, String[] values) {
        this(name, values, null);
    }

    private EnumDefImpl(EnumDefImpl impl) {
        super(impl);
        this.name = impl.name;
        values = Arrays.copyOf(impl.values, impl.values.length);
        encodingLen = impl.encodingLen;
    }

    private void readObject(java.io.ObjectInputStream in)
        throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        init();
    }

    private void init() {
        encodingLen = SortableString.encodingLength(values.length);
        if (encodingLen < 2) {
            encodingLen = 2;
        }
    }

    @SuppressWarnings("unused")
    private EnumDefImpl() {
        name = null;
        values = null;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String[] getValues() {
        return values;
    }

    @Override
    public boolean isValidKeyField() {
        return true;
    }

    @Override
    public boolean isValidIndexField() {
        return true;
    }

    @Override
    public boolean isEnum() {
        return true;
    }

    @Override
    public EnumDef asEnum() {
        return this;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof EnumDefImpl) {
            EnumDefImpl otherDef = (EnumDefImpl) other;
            return Arrays.equals(values, otherDef.getValues());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return super.hashCode() + Arrays.hashCode(values);
    }

    @Override
    protected void toJson(ObjectNode node) {
        super.toJson(node);
        node.put(ENUM_NAME, name);
        ArrayNode enumVals = node.putArray(ENUM_VALS);
        for (String val : getValues()) {
            enumVals.add(val);
        }
    }

    @Override
    public EnumDefImpl clone() {
        return new EnumDefImpl(this);
    }

    @Override
    public EnumValueImpl createEnum(String value) {
        return new EnumValueImpl(this, value);
    }

    /**
     * {
     *  "name": "xxx",
     *  "type": {
     *    "name" : "xxx",
     *    "type" : "enum",
     *    "symbols" " : [ "a", "b", ... , "z" ]
     *  }
     * }
     */
    @Override
    public JsonNode mapTypeToAvro(ObjectNode node) {
        if (node == null) {
            node = JsonUtils.createObjectNode();
        }
        node.put(NAME, name);
        node.put(TYPE, "enum");
        ArrayNode enumVals = node.putArray("symbols");
        for (String val : getValues()) {
            enumVals.add(val);
        }
        return node;
    }

    private void validate() {
        if (values == null || values.length < 1) {
            throw new IllegalArgumentException
                ("Enumerations requires one or more values");
        }
        if (name == null) {
            throw new IllegalArgumentException
                ("Enumerations require a name");
        }

        /*
         * Check for duplicate values and validate the values of the
         * enumeration strings themselves.
         */
        HashSet<String> set = new HashSet<String>();
        for (String value: values) {
            validateStringValue(value);
            if (set.contains(value)) {
                throw new IllegalArgumentException
                    ("Duplicated enumeration value: " + value);
            }
            set.add(value);
        }
    }

    /**
     * Validates the value of the enumeration string.  The strings must
     * work for Avro schema, which means avoiding special characters.
     */
    private void  validateStringValue(String value) {
        if (value.matches(TableImpl.VALID_NAME_CHAR_REGEX)) {
            throw new IllegalArgumentException
                ("Enumeration string names may contain only " +
                 "alphanumeric values plus the characters \"_\"");
        }
    }

    boolean isValidIndex(int index) {
        return (index < values.length);
    }

    /**
     * Create the value represented by this index in the declaration
     */
    EnumValueImpl createEnum(int index) {
        if (!isValidIndex(index)) {
            throw new IllegalArgumentException
                ("Index is out of range for enumeration: " + index);
        }
        return new EnumValueImpl(this, values[index]);
    }

    int indexOf(String enumValue) {
        for (int i = 0; i < values.length; i++) {
            if (enumValue.equals(values[i])) {
                return i;
            }
        }
        throw new IllegalArgumentException
            ("Value is not valid for the enumeration: " + enumValue);
    }

    /**
     * TODO
     */
    int getEncodingLen() {
        return encodingLen;
    }

    /*
     * Simple value comparison function, used to avoid circular calls
     * between equals() and EnumValueImpl.equals().
     */
    boolean valuesEqual(EnumDefImpl other) {
        return Arrays.equals(values, other.getValues());
    }

    void validateValue(String value) {
        for (String val : values) {
            if (val.equals(value)) {
                return;
            }
        }
        throw new IllegalArgumentException
            ("Invalid enumeration value '" + value +
             "', must be in values: " + Arrays.toString(values));
    }

    @Override
    FieldValueImpl createValue(JsonNode node) {
        if (node == null || node.isNull()) {
            return NullValueImpl.getInstance();
        }
        if (!node.isTextual()) {
            throw new IllegalArgumentException
                ("Default value for type ENUM is not a string");
        }
        return createEnum(node.getValueAsText());
    }

    /*
     * NOTE: this code is left here for future support of limited schema
     * evolution to add enumeration values.
     *
     * Enumeration values can only be added, and only at the end of the array.
     * This function ensures that this is done correctly.
    void addValue(String newValue) {
        if (SortableString.encodingLength(values.length + 1) >
            encodingLen) {
            throw new IllegalArgumentException
                ("Cannot add another enumeration value, too large: " +
                 values.length + 1);
        }
        String[] newArray = new String[values.length + 1];
        int i = 0;
        while (i < values.length) {
            newArray[i] = values[i++];
        }
        newArray[i] = newValue;
        values = newArray;
    }
    */
}
