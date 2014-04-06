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

import static oracle.kv.impl.api.table.JsonUtils.MIN;
import static oracle.kv.impl.api.table.JsonUtils.MAX;

import oracle.kv.table.FloatDef;

import com.sleepycat.persist.model.Persistent;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

/**
 * FloatDefImpl implements the FloatDef interface.
 */
@Persistent(version=1)
class FloatDefImpl extends FieldDefImpl
    implements FloatDef {

    private static final long serialVersionUID = 1L;
    /*
     * These are not final to allow for schema evolution.
     */
    private final Float min;
    private final Float max;

    /**
     * Constructor requiring all fields.
     */
    FloatDefImpl(String description,
                 Float min, Float max) {
        super(Type.FLOAT, description);
        this.min = min;
        this.max = max;
        validate();
    }

    /**
     * This constructor defaults most fields.
     */
    FloatDefImpl() {
        super(Type.FLOAT);
        min = null;
        max = null;
    }

    private FloatDefImpl(FloatDefImpl impl) {
        super(impl);
        min = impl.min;
        max = impl.max;
    }

    @Override
    public Float getMin() {
        return min;
    }

    @Override
    public Float getMax() {
        return max;
    }

    @Override
    public boolean isFloat() {
        return true;
    }

    @Override
    public FloatDef asFloat() {
        return this;
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
    public boolean equals(Object other) {
        if (other instanceof FloatDefImpl) {
            FloatDefImpl otherDef = (FloatDefImpl) other;
            return (compare(getMin(), otherDef.getMin()) &&
                    compare(getMax(), otherDef.getMax()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        return super.hashCode() +
            (min != null ? min.hashCode() : 0) +
            (max != null ? max.hashCode() : 0);
    }

    @Override
    void toJson(ObjectNode node) {
        super.toJson(node);

        /*
         * Add min, max
         */
        if (min != null) {
            node.put(MIN, min);
        }
        if (max != null) {
            node.put(MAX, max);
        }
    }

    @Override
    public FloatDefImpl clone() {
        return new FloatDefImpl(this);
    }

    @Override
    public FloatValueImpl createFloat(float value) {
        validateRange(value);
        return new FloatValueImpl(value);
    }

    private void validate() {

        /* Make sure min <= max */
        if (min != null && max != null) {
            if (min > max) {
                throw new IllegalArgumentException
                    ("Invalid min or max value");
            }
        }
    }

    /**
     * Validates the value against the range if one exists.
     */
    private void validateRange(float val) {

        /* min/max are inclusive */
        if ((min != null && val < min) ||
            (max != null && val > max)) {
            StringBuilder sb = new StringBuilder();
            sb.append("Value, ");
            sb.append(val);
            sb.append(", is outside of the allowed range");
            throw new IllegalArgumentException(sb.toString());
        }
    }

    @Override
    FieldValueImpl createValue(JsonNode node) {
        if (node == null || node.isNull()) {
            return NullValueImpl.getInstance();
        }

        /*
         * Jackson does not have a FloatNode in the version being used, so
         * no further validation is possible.
         */
        if (!node.isNumber()) {
            throw new IllegalArgumentException
                ("Default value for type FLOAT is not a number");
        }
        return createFloat(Float.valueOf(node.getValueAsText()));
    }
}
