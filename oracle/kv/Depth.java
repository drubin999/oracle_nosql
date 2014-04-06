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

package oracle.kv;

import java.util.EnumSet;

/**
 * Used with multiple-key and iterator operations to specify whether to select
 * (return or operate on) the key-value pair for the parent key, and the
 * key-value pairs for only immediate children or all descendants.
 */
public enum Depth {

    /**
     * Select only immediate children, do not select the parent.
     */
    CHILDREN_ONLY,

    /**
     * Select immediate children and the parent.
     */
    PARENT_AND_CHILDREN,

    /**
     * Select all descendants, do not select the parent.
     */
    DESCENDANTS_ONLY,

    /**
     * Select all descendants and the parent.
     */
    PARENT_AND_DESCENDANTS;

    private final static Depth[] DEPTHS_BY_ORDINAL;
    static {
        final EnumSet<Depth> set = EnumSet.allOf(Depth.class);
        DEPTHS_BY_ORDINAL = new Depth[set.size()];
        for (Depth op : set) {
            DEPTHS_BY_ORDINAL[op.ordinal()] = op;
        }
    }

    /**
     * For internal use only.
     * @hidden
     */
    public static Depth getDepth(int ordinal) {
        if (ordinal < 0 || ordinal >= DEPTHS_BY_ORDINAL.length) {
            throw new RuntimeException("unknown Depth: " + ordinal);
        }
        return DEPTHS_BY_ORDINAL[ordinal];
    }
}
