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

package oracle.kv.table;

import java.util.List;

/**
 * Defines parameters used in multi-row operations. A multi-row operation
 * selects rows from at least one table, called the target table, and optionally
 * selects rows from its ancestor and descendant tables.
 * <p>
 * The target table is defined by the {@link PrimaryKey} or {@link IndexKey}
 * passed as the first parameter of the multi-row method.  These include
 * <ul>
 * <li>{@link TableAPI#multiGet}
 * <li>{@link TableAPI#multiGetKeys}
 * <li>{@link TableAPI#tableIterator(PrimaryKey, MultiRowOptions, TableIteratorOptions)}
 * <li>{@link TableAPI#tableKeysIterator(PrimaryKey, MultiRowOptions, TableIteratorOptions)}
 * <li>{@link TableAPI#tableIterator(IndexKey, MultiRowOptions, TableIteratorOptions)}
 * <li>{@link TableAPI#tableKeysIterator(IndexKey, MultiRowOptions, TableIteratorOptions)}
 * <li>{@link TableAPI#multiDelete}
 * </ul>
 * <p>

 * By default only matching records from the target table are returned or
 * deleted.  {@code MultiRowOptions} can be used to specify whether the
 * operation should affect (return or delete) records from ancestor and/or
 * descendant tables for matching records.  In addition {@code MultiRowOptions}
 * can be used to specify sub-ranges within a table or index for all operations
 * it supports using {@link FieldRange}.
 * <p>
 * When results from multiple tables are returned they are always returned with
 * results from ancestor tables first even if the iteration is in reverse order
 * or unordered.  In this case results from multiple tables are mixed.  Because
 * an index iteration can result in multiple index entries matching the same
 * primary record it is possible to get duplicate return values for those
 * records as well as specified ancestor tables.  It is not valid to specify
 * child tables for index operations.  It is up to the caller to handle
 * duplicates and filter results from multiple tables.
 * <p>
 * The ability to return ancestor and child table rows and keys is useful and
 * can avoid additional calls from the client but it comes at a cost and should
 * be used only when necessary.  In the case of ancestor tables it means
 * verification or fetching of the ancestor row.  In the case of child tables
 * it means that the iteration cannot end until it has scanned all child table
 * records which may involve iteration over uninteresting records.
 *
 * @since 3.0
 */
public class MultiRowOptions {
    private FieldRange fieldRange;
    private List<Table> ancestors;
    private List<Table> children;

    /**
     * Full constructor requiring all members.
     */
    public MultiRowOptions(FieldRange fieldRange,
                           List<Table> ancestors,
                           List<Table> children) {
        this.fieldRange = fieldRange;
        this.ancestors = ancestors;
        this.children = children;
    }

    /**
     * A convenience constructor that takes only {@link FieldRange}.
     */
    public MultiRowOptions(FieldRange fieldRange) {
        this.fieldRange = fieldRange;
        this.ancestors = null;
        this.children = null;
    }

    /**
     * {@link FieldRange} restricts the selected rows to those matching a
     * range of field values for the first unspecified field in the target key.
     * <p>
     * The first unspecified key field is defined as the first key field
     * following the fields in a partial target key, or the very first key
     * field when a null target key is specified.
     * <p>
     * {@link FieldRange} may only be used when a partial or null target key is
     * specified.  If a complete target key is given, this member must be null.
     */
    public FieldRange getFieldRange() {
        return fieldRange;
    }

    /**
     * Specifies the parent (ancestor) tables for which rows are selected by
     * the operation.
     * <p>
     * Each table must be an ancestor table (parent, grandparent, etc.)
     * of the target table.
     * <p>
     * A row selected from a parent table will have that row's complete primary
     * key, which is a partial primary key for the row selected from the target
     * table.  At most one row from each parent table will be selected for each
     * selected target table row.
     * <p>
     * Rows from a parent table are always returned before rows from its
     * child tables.  This is the case for both forward and reverse iterations.
     * <p>
     * TODO Discuss: for an index method, fetching the parent table rows
     * requires additional operations per selected index key.  Redundant
     * fetches (when multiple selected target table rows have the same
     * parent) can be avoided by maintaining and checking a set of already
     * selected keys.
     */
    public List<Table> getIncludedParentTables() {
        return ancestors;
    }

    /**
     * Specifies the child (descendant) tables for which rows are selected by
     * the operation.
     * <p>
     * Each child table must be an descendant table (child, grandchild, great-
     * grandchild, etc.) of the target table.
     * <p>
     * The rows selected from each child table will have key field values
     * matching those in the key of a selected target table row.  Multiple
     * child table rows may be selected for each selected target table row.
     * <p>
     * Rows from a parent table are always returned before rows from its
     * child tables for forward iteration.  Reverse iterations will return
     * child rows first.
     * <p>
     * TODO Discuss: for an index method, fetching the child table rows
     * requires additional operations per selected index key.
     */
    public List<Table> getIncludedChildTables() {
        return children;
    }

    public MultiRowOptions setFieldRange(FieldRange newFieldRange) {
        this.fieldRange = newFieldRange;
        return this;
    }

    public MultiRowOptions setIncludedParentTables(List<Table> newAncestors) {
        this.ancestors = newAncestors;
        return this;
    }

    public MultiRowOptions setIncludedChildTables(List<Table> newChildren) {
        this.children = newChildren;
        return this;
    }
}
