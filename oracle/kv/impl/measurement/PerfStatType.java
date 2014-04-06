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

package oracle.kv.impl.measurement;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Description of statistics recorded at the RepNode and displayed via the
 * monitoring system. To keep statistics compact, and to guard against enum
 * evolution, this class itself is not transmitted across the wire. Instead,
 * the id value alone is used, and any change or addition to the ids is a
 * protocol change.
 */
public enum PerfStatType {

    /*
     * Status metrics for each type of operation.
     * When adding statistics, consider whether they should be added to
     * getDetailedStats, getSummaryStats, in order to be dumped to the
     * stats .csv files.
     */

        USER_SINGLE_OP_INT       (100, true,  "AllSingleKeyOperations"),
        USER_SINGLE_OP_CUM       (101, false, "AllSingleKeyOperations"),

        USER_MULTI_OP_INT (120, true, "AllMultiKeyOperations"),
        USER_MULTI_OP_CUM (121, false, "AllMultiKeyOperations"),

        /* gets */
        GET_INT           (200, true, "Gets"),
        GET_CUM           (201, false, "Gets"),

        PUT_INT            (301, true, "Puts"),
        PUT_IF_ABSENT_INT  (302, true, "PutIfAbsent"),
        PUT_IF_PRESENT_INT (303, true, "PutIfPresent"),
        PUT_IF_VERSION_INT (304, true, "PutIfVersion"),

        PUT_CUM            (321, false, "Puts"),
        PUT_IF_ABSENT_CUM  (322, false, "PutIfAbsent"),
        PUT_IF_PRESENT_CUM (323, false, "PutIfPresent"),
        PUT_IF_VERSION_CUM (324, false, "PutIfVersion"),

        DELETE_INT            (401, true,  "Deletes"),
        DELETE_IF_VERSION_INT (402, true, "DeleteIfVersion"),

        DELETE_CUM            (421, false, "Deletes"),
        DELETE_IF_VERSION_CUM (422, false, "DeleteIfVersion"),

        /* multiGet */
        MULTI_GET_INT               (500, true, "MultiGets"),
        MULTI_GET_KEYS_INT          (501, true, "MultiGetKeys"),
        MULTI_GET_ITERATOR_INT      (502, true, "MultiGetIterator"),
        MULTI_GET_KEYS_ITERATOR_INT (503, true, "MultiGetKeysIterator"),

        MULTI_GET_CUM               (520, false, "MultiGets"),
        MULTI_GET_KEYS_CUM          (521, false, "MultiGetKeys"),
        MULTI_GET_ITERATOR_CUM      (522, false, "MultiGetIterator"),
        MULTI_GET_KEYS_ITERATOR_CUM (523, false, "MultiGetKeysIterator"),

        /* storeIterator */
        STORE_ITERATOR_INT      (600, true, "StoreIterator"),
        STORE_KEYS_ITERATOR_INT (601, true, "StoreKeysIterator"),

        STORE_ITERATOR_CUM      (620, false, "StoreIterator"),
        STORE_KEYS_ITERATOR_CUM (621, false, "StoreKeysIterator"),

        /* multiDelete */
        MULTI_DELETE_INT (700, true, "MultiDeletes"),
        MULTI_DELETE_CUM (720, false, "MultiDeletes"),

        /* execute */
        EXECUTE_INT (800, true, "Executes"),
        EXECUTE_CUM (820, false, "Executes"),

        /* NOP */
        NOP_INT (900, true,  "NOPs"),
        NOP_CUM (920, false, "NOPs"),

        /* indexIterator */
        INDEX_ITERATOR_INT      (1000, true, "IndexIterator"),
        INDEX_KEYS_ITERATOR_INT (1001, true, "IndexKeysIterator"),

        INDEX_ITERATOR_CUM      (1020, false, "IndexIterator"),
        INDEX_KEYS_ITERATOR_CUM (1021, false, "IndexKeysIterator");


    /** Catalog of all perf stat types. */
    public static Map<Integer, PerfStatType> idMap =
        new HashMap<Integer, PerfStatType>();

    /** Set ancestor relationships. **/
    static {
        GET_INT.setParent(USER_SINGLE_OP_INT);
        GET_CUM.setParent(USER_SINGLE_OP_CUM);

        PUT_INT.setParent(USER_SINGLE_OP_INT);
        PUT_IF_ABSENT_INT.setParent(USER_SINGLE_OP_INT);
        PUT_IF_PRESENT_INT.setParent(USER_SINGLE_OP_INT);
        PUT_IF_VERSION_INT.setParent(USER_SINGLE_OP_INT);

        PUT_CUM.setParent(USER_SINGLE_OP_CUM);
        PUT_IF_ABSENT_CUM.setParent(USER_SINGLE_OP_CUM);
        PUT_IF_PRESENT_CUM.setParent(USER_SINGLE_OP_CUM);
        PUT_IF_VERSION_CUM.setParent(USER_SINGLE_OP_CUM);

        DELETE_INT.setParent(USER_SINGLE_OP_INT);
        DELETE_IF_VERSION_INT.setParent(USER_SINGLE_OP_INT);

        DELETE_CUM.setParent(USER_SINGLE_OP_CUM);
        DELETE_IF_VERSION_CUM.setParent(USER_SINGLE_OP_CUM);

        MULTI_GET_INT.setParent(USER_MULTI_OP_INT);
        MULTI_GET_KEYS_INT.setParent(USER_MULTI_OP_INT);
        MULTI_GET_ITERATOR_INT.setParent(USER_MULTI_OP_INT);
        MULTI_GET_KEYS_ITERATOR_INT.setParent(USER_MULTI_OP_INT);

        MULTI_GET_CUM.setParent(USER_MULTI_OP_CUM);
        MULTI_GET_KEYS_CUM.setParent(USER_MULTI_OP_CUM);
        MULTI_GET_ITERATOR_CUM.setParent(USER_MULTI_OP_CUM);
        MULTI_GET_KEYS_ITERATOR_CUM.setParent(USER_MULTI_OP_CUM);

        STORE_ITERATOR_INT.setParent(USER_MULTI_OP_INT);
        STORE_KEYS_ITERATOR_INT.setParent(USER_MULTI_OP_INT);

        STORE_ITERATOR_CUM.setParent(USER_MULTI_OP_CUM);
        STORE_KEYS_ITERATOR_CUM.setParent(USER_MULTI_OP_CUM);

        /* multiDelete */
        MULTI_DELETE_INT.setParent(USER_MULTI_OP_INT);
        MULTI_DELETE_CUM.setParent(USER_MULTI_OP_CUM);

        /* execute */
        EXECUTE_INT.setParent(USER_MULTI_OP_INT);
        EXECUTE_CUM.setParent(USER_MULTI_OP_CUM);

        INDEX_ITERATOR_INT.setParent(USER_MULTI_OP_INT);
        INDEX_KEYS_ITERATOR_INT.setParent(USER_MULTI_OP_INT);

        INDEX_ITERATOR_CUM.setParent(USER_MULTI_OP_CUM);
        INDEX_KEYS_ITERATOR_CUM.setParent(USER_MULTI_OP_CUM);

        for (PerfStatType t : EnumSet.allOf(PerfStatType.class)) {
            idMap.put(t.getId(), t);
        }
    }

    /** Retrieve the enum from the catalog */
    public static PerfStatType getType(int id) {
        return idMap.get(id);
    }

    /**
     * The list of stats that are displayed in the <resource>_details.csv file
     */
    public static PerfStatType[] getDetailedStats() {
        return new PerfStatType[] {
            NOP_INT,
            GET_INT,
            PUT_INT,
            PUT_IF_ABSENT_INT,
            PUT_IF_PRESENT_INT,
            PUT_IF_VERSION_INT,
            DELETE_INT,
            DELETE_IF_VERSION_INT,
            MULTI_GET_INT,
            MULTI_GET_KEYS_INT,
            MULTI_GET_ITERATOR_INT,
            MULTI_GET_KEYS_ITERATOR_INT,
            STORE_ITERATOR_INT,
            STORE_KEYS_ITERATOR_INT,
            MULTI_DELETE_INT,
            EXECUTE_INT,
            INDEX_ITERATOR_INT,
            INDEX_KEYS_ITERATOR_INT
        };
    }

    /**
     * The list of stats that are displayed in the <resource>_summary.csv file
     */
    public static PerfStatType[] getSummaryStats() {
        return new PerfStatType[] {USER_SINGLE_OP_INT, USER_MULTI_OP_INT};
    }

    private final int id;
    private final String name;
    private PerfStatType parent;

    private PerfStatType(int id, boolean isInterval, String name) {
        this.id = id;
        this.name = name + (isInterval ? "IntervalLatency" :
            "CumulativeLatency");
    }

    private void setParent(PerfStatType parent1) {
        this.parent = parent1;
    }

    public int getId() {
        return id;
    }

    public PerfStatType getParent() {
        return parent;
    }

    @Override
    public String toString() {
        return name;
    }
}
