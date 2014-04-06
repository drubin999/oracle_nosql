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

import oracle.kv.table.BooleanDef;

import org.codehaus.jackson.JsonNode;

import com.sleepycat.persist.model.Persistent;

/**
 * BooleanDefImpl implements the BooleanDef interface.
 */
@Persistent(version=1)
class BooleanDefImpl extends FieldDefImpl
    implements BooleanDef {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor requiring all fields.
     */
    public BooleanDefImpl(final String description) {
        super(Type.BOOLEAN, description);
    }

    /**
     * This constructor defaults most fields.
     */
    public BooleanDefImpl() {
        super(Type.BOOLEAN);
    }

    private BooleanDefImpl(BooleanDefImpl impl) {
        super(impl);
    }

    @Override
    public boolean isBoolean() {
        return true;
    }

    @Override
    public BooleanDef asBoolean() {
        return this;
    }

    @Override
    public boolean equals(Object other) {
        return (other instanceof BooleanDefImpl);
    }

    @Override
    public BooleanDefImpl clone() {
        return new BooleanDefImpl(this);
    }

    @Override
    public BooleanValueImpl createBoolean(boolean value) {
        return new BooleanValueImpl(value);
    }

    @Override
    FieldValueImpl createValue(JsonNode node) {
        if (node == null || node.isNull()) {
            return NullValueImpl.getInstance();
        }
        if (!node.isBoolean()) {
            throw new IllegalArgumentException
                ("Default value for type BOOLEAN is not boolean");
        }
        return createBoolean(node.getBooleanValue());
    }
}
