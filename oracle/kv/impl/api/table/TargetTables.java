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
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.table.Table;

/**
 * Encapsulates target tables for a table operation that involves multiple
 * tables.  All table operations require a target table.  They optionally
 * affect child and/or ancestor tables as well.  Additionally, there is state
 * that indicates that for certain operations the target table itself should
 * not be returned.  This last piece of information is not yet used and is
 * preparing for a future enhancement.  At this time target tables may not be
 * excluded from results.
 *
 * Internally the target table and child tables requested are kept in the
 * same array, with the target being the first entry.  This simplifies the
 * server side which wants that array intact.
 */
public class TargetTables implements FastExternalizable {

    /*
     * Includes the target table id and optional child ids.
     */
    private final long[] targetAndChildIds;

    /*
     * Ancestor table ids, may be empty.
     */
    private final long[] ancestorTableIds;

    /*
     * Include the target table in results, or not.
     * For now (R3) this is always true.
     */
    private final boolean includeTarget;

    /**
     * Creates a TargetTables object on the client side
     *
     * @param targetTable the target of the operation, which is the table
     * from which the operation's key was created
     *
     * @param childTables a list of child tables to include in the operation,
     * or null
     *
     * @param ancestorTables a list of ancestor tables to include in the
     * operation, or null
     *
     * @throws IllegalArgumentException if there is no target table
     */
    public TargetTables(Table targetTable,
                        List<Table> childTables,
                        List<Table> ancestorTables) {
        if (targetTable == null) {
            throw new IllegalArgumentException
                ("Missing target table");
        }

        /* target table plus child tables */
        int arraySize = childTables != null ? childTables.size() + 1 : 1;

        targetAndChildIds = new long[arraySize];
        targetAndChildIds[0] = ((TableImpl) targetTable).getId();
        if (childTables != null) {
            int i = 1;
            for (Table table : childTables) {
                targetAndChildIds[i++] = ((TableImpl)table).getId();
            }
        }

        ancestorTableIds = makeIdArray(ancestorTables);

        /*
         * This is not currently negotiable.
         */
        includeTarget = true;
    }

    /**
     * Internal constructor used only by MultiDeleteTables when called from
     * a RepNode that is deleting table data on table removal.
     */
    public TargetTables(long tableId) {
        targetAndChildIds = new long[1];
        targetAndChildIds[0] = tableId;
        ancestorTableIds = new long[0];
        includeTarget = true;
    }

    /**
     * Creates a TargetTables instance on the server side from a messsage
     */
    public TargetTables(ObjectInput in,
                        @SuppressWarnings("unused") short serialVersion)
        throws IOException {

        short len = in.readShort();
        targetAndChildIds = new long[len];
        for (int i = 0; i < len; i++) {
            targetAndChildIds[i] = in.readLong();
        }
        len = in.readShort();
        ancestorTableIds = new long[len];
        for (int i = 0; i < len; i++) {
            ancestorTableIds[i] = in.readLong();
        }

        includeTarget = in.readBoolean();
    }

    /**
     * Serializes a TargetTables instance to be sent to the server
     */
    @Override
    public void writeFastExternal(ObjectOutput out, short serialVersion)
        throws IOException {

        out.writeShort(targetAndChildIds.length);
        for (long l : targetAndChildIds) {
            out.writeLong(l);
        }
        out.writeShort(ancestorTableIds.length);
        for (long l : ancestorTableIds) {
            out.writeLong(l);
        }
        out.writeBoolean(includeTarget);
    }

    /**
     * Returns the target table id.
     */
    public long getTargetTableId() {
        return targetAndChildIds[0];
    }

    /**
     * Returns the target table id in the array with child targets.
     */
    public long[] getTargetAndChildIds() {
        return targetAndChildIds;
    }

    /**
     * Returns the ancestor tables array. This is never null, but it may be
     * empty.
     */
    public long[] getAncestorTableIds() {
        return ancestorTableIds;
    }

    /**
     * Returns true if the target table is to be included in the results
     */
    public boolean getIncludeTarget() {
        return includeTarget;
    }

    /**
     * Returns true if there are ancestor tables.
     */
    public boolean hasAncestorTables() {
        return ancestorTableIds.length > 0;
    }

    /**
     * Returns true if there are child tables.
     */
    public boolean hasChildTables() {
        return targetAndChildIds.length > 1;
    }

    private long[] makeIdArray(List<Table> tables) {
        if (tables == null) {
            return new long[0];
        }
        final long[] ids = new long[tables.size()];
        int i = 0;
        for (Table table : tables) {
            ids[i++] = ((TableImpl)table).getId();
        }
        return ids;
    }
}
