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

import java.io.Serializable;
import java.util.Comparator;

/**
 * FieldComparator is a simple implementation of Comparator<String> that
 * is used for case-insensitive String comparisons.  This is used to
 * implement case-insensitive, but case-preserving names for fields, tables,
 * and indexes.
 *
 * IMPORTANT: technically this class should be declared @Persistent and
 * stored with JE instances of TreeMap that use it, but JE does not
 * currently store Comparator instances.  As a result the code that
 * uses previously-stored JE entities will not have the comparator set.
 * Fortunately that list is restricted to persistent plans and tasks
 * for creation and evolution of tables, and further, that code indirectly
 * uses FieldMap to encapsulate the relevant maps and that class *always*
 * deep-copies the source maps so the Comparator will always be set in
 * TableMetadata and related objects.
 */
class FieldComparator implements Comparator<String>, Serializable {
    static final FieldComparator instance = new FieldComparator();
    private static final long serialVersionUID = 1L;

    /**
     * Comparator<String>
     */
    @Override
    public int compare(String s1, String s2) {
        return s1.compareToIgnoreCase(s2);
    }
}

