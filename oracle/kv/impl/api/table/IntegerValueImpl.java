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

import oracle.kv.impl.util.SortableString;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.IntegerValue;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.IntNode;

import com.sleepycat.persist.model.Persistent;

@Persistent(version=1)
class IntegerValueImpl extends FieldValueImpl implements IntegerValue {
    private static final long serialVersionUID = 1L;
    private final int value;

    IntegerValueImpl(int value) {
        this.value = value;
    }

    /**
     * This constructor creates IntegerValueImpl from the String format used for
     * sorted keys.
     */
    IntegerValueImpl(String keyValue) {
        this.value = SortableString.intFromSortable(keyValue);
    }

    /* DPL */
    @SuppressWarnings("unused")
    private IntegerValueImpl() {
        value = 0;
    }

    /**
     * Create a new IntegerValueImpl from the String format used for sorted keys.
     */
    public static IntegerValue createFromKeyValue(String value) {
        return new IntegerValueImpl(value);
    }

    @Override
    public int get() {
        return value;
    }

    @Override
    public FieldDef.Type getType() {
        return FieldDef.Type.INTEGER;
    }

    @Override
    public IntegerValueImpl clone() {
        return new IntegerValueImpl(value);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof IntegerValueImpl) {
            return value == ((IntegerValueImpl)other).get();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return ((Integer) value).hashCode();
    }

    @Override
    public int compareTo(FieldValue other) {
        if (other instanceof IntegerValueImpl) {
            /* java 7
            return Integer.compare(value, ((IntegerValueImpl)other).value);
            */
            return ((Integer)value).compareTo(((IntegerValueImpl)other).value);
        }
        throw new ClassCastException
            ("Object is not an IntegerValue");
    }

    @Override
    public String formatForKey(FieldDef field) {
        int len =
            (field != null ? ((IntegerDefImpl) field).getEncodingLength() : 0);
        return SortableString.toSortable(value, len);
    }

    @Override
    public FieldValueImpl getNextValue() {
        if (value == Integer.MAX_VALUE) {
            return null;
        }
        return new IntegerValueImpl(value + 1);
    }

    @Override
    public FieldValueImpl getMinimumValue() {
        return new IntegerValueImpl(Integer.MIN_VALUE);
    }

    @Override
    public JsonNode toJsonNode() {
        return new IntNode(value);
    }

    @Override
    public IntegerValue asInteger() {
        return this;
    }

    @Override
    public boolean isInteger() {
        return true;
    }
}
