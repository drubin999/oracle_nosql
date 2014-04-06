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

import java.io.IOException;

import oracle.kv.table.ArrayDef;

import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.node.ObjectNode;

/**
 * ArrayBuilder
 */
public class ArrayBuilder extends CollectionBuilder {

    ArrayBuilder(String description) {
        super(description);
    }

    ArrayBuilder() {
    }

    @Override
    public String getBuilderType() {
        return "Array";
    }

    @Override
    public ArrayDef build() {
        if (field == null) {
            throw new IllegalArgumentException
                ("Array has no field and cannot be built");
        }
        return new ArrayDefImpl((FieldDefImpl)field, description);
    }

    @SuppressWarnings("unused")
    @Override
    TableBuilderBase generateAvroSchemaFields(Schema schema,
                                              String name1,
                                              JsonNode defaultValue,
                                              String desc) {

        Schema elementSchema = schema.getElementType();
        super.generateAvroSchemaFields(elementSchema,
                                       elementSchema.getName(),
                                       null, /* no default */
                                       elementSchema.getDoc());
        return this;
    }

    /*
     * Create a JSON representation of the array field
     **/
    public String toJsonString(boolean pretty) {
        ObjectWriter writer = JsonUtils.createWriter(pretty);
        ObjectNode o = JsonUtils.createObjectNode();
        ArrayDefImpl tmp = new ArrayDefImpl((FieldDefImpl)field, description);
        tmp.toJson(o);
        try {
            return writer.writeValueAsString(o);
        } catch (IOException ioe) {
            return ioe.toString();
        }
    }
}
