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
import oracle.kv.table.DoubleValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.DoubleNode;

import com.sleepycat.persist.model.Persistent;

@Persistent(version=1)
class DoubleValueImpl extends FieldValueImpl implements DoubleValue {
    private static final long serialVersionUID = 1L;
    private double value;

    /**
     */
    DoubleValueImpl(double value) {
        this.value = value;
    }

    /**
     * This constructor creates DoubleValueImpl from the String format used for
     * sorted keys.
     */
    DoubleValueImpl(String keyValue) {
        this.value = SortableString.doubleFromSortable(keyValue);
    }

    /* DPL */
    @SuppressWarnings("unused")
    private DoubleValueImpl() {
    }

    @Override
    public double get() {
        return value;
    }

    @Override
    public FieldDef.Type getType() {
        return FieldDef.Type.DOUBLE;
    }

    @Override
    public DoubleValueImpl clone() {
        return new DoubleValueImpl(value);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof DoubleValueImpl) {
            /* == doesn't work for the various Double constants */
            return Double.compare(value,((DoubleValueImpl)other).get()) == 0;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return ((Double) value).hashCode();
    }

    @Override
    public int compareTo(FieldValue other) {
        if (other instanceof DoubleValueImpl) {
            return Double.compare(value, ((DoubleValueImpl)other).value);
        }
        throw new ClassCastException
            ("Object is not an DoubleValue");
    }

    @Override
    public String formatForKey(FieldDef field) {
        return SortableString.toSortable(value);
    }

    @Override
    public FieldValueImpl getNextValue() {
        if (value == Double.MAX_VALUE) {
            return null;
        }
        return new DoubleValueImpl(Math.nextAfter(value,
                                                  Double.MAX_VALUE));
    }

    @Override
    public FieldValueImpl getMinimumValue() {
        return new DoubleValueImpl(Double.MIN_VALUE);
    }

    @Override
    public JsonNode toJsonNode() {
        return new DoubleNode(value);
    }

    @Override
    public DoubleValue asDouble() {
        return this;
    }

    @Override
    public boolean isDouble() {
        return true;
    }
}
