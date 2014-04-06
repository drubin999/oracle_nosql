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

import oracle.kv.table.BooleanValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;
import com.sleepycat.persist.model.Persistent;

@Persistent(version=1)
class BooleanValueImpl extends FieldValueImpl implements BooleanValue {
    private static final long serialVersionUID = 1L;
    private boolean value;

    BooleanValueImpl(boolean value) {
        this.value = value;
    }

    /**
     * Boolean.parseBoolean simply does a case-insensitive comparison to "true"
     * and assigns that value.  This means any other string results in false.
     */
    BooleanValueImpl(String value) {
        this.value = Boolean.parseBoolean(value);
    }

    /* DPL */
    @SuppressWarnings("unused")
    private BooleanValueImpl() {
    }

    public static BooleanValueImpl create(boolean value) {
        return new BooleanValueImpl(value);
    }

    @Override
    public boolean get() {
        return value;
    }

    @Override
    public FieldDef.Type getType() {
        return FieldDef.Type.BOOLEAN;
    }

    @Override
    public BooleanValueImpl clone() {
        return new BooleanValueImpl(value);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof BooleanValueImpl) {
            return value == ((BooleanValueImpl)other).get();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return ((Boolean) value).hashCode();
    }

    @Override
    public int compareTo(FieldValue other) {
        if (other instanceof BooleanValueImpl) {
            /* java 7
            return Boolean.compare(value, ((BooleanValueImpl)other).value);
            */
            return ((Boolean)value).compareTo(((BooleanValueImpl)other).value);
        }
        throw new ClassCastException
            ("Object is not an BooleanValue");
    }

    @Override
    public JsonNode toJsonNode() {
        return (value ? BooleanNode.TRUE : BooleanNode.FALSE);
    }

    @Override
    public BooleanValue asBoolean() {
        return this;
    }

    @Override
    public boolean isBoolean() {
        return true;
    }
}
