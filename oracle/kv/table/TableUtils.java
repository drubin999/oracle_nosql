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

import oracle.kv.impl.api.table.IndexKeyImpl;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableImpl;

/**
 * TableUtils is a utility class that encapsulates a number of useful
 * functions that do not belong in any specific interface.
 *
 * @since 3.0
 */
public class TableUtils {

    /**
     * @hidden
     * Private constructor to satisfy CheckStyle and to prevent
     * instantiation of this utility class.
     */
    @SuppressWarnings("unused")
    private TableUtils() {
        throw new AssertionError("Instantiated utility class " +
                                 TableUtils.class);
    }

    /**
     * Returns the size of the serialized data for the row.  This includes
     * only the value portion of the record and not the key and is the length
     * of the byte array that will be put as a database record.
     *
     * @param row the Row to use for the operation
     *
     * @return the size of the data portion of the serialized row, in bytes
     *
     * @throws IllegalArgumentException if called with a {@link PrimaryKey}
     * which has not data size
     */
    public static int getDataSize(Row row) {
        return ((RowImpl) row).getDataSize();
    }

    /**
     * Returns the size of the serialized primary key for this row. It is the
     * length of the byte array that is used as the database key for this row.
     * If the key is not fully specified the value returned is that of the
     * partial key.
     *
     * @param row the Row to use for the operation
     *
     * @return the size of the key portion serialized row, in bytes
     */
    public static int getKeySize(Row row) {
        return ((RowImpl) row).getKeySize();
    }
    /**
     * Returns the size of the serialized index key. It is the length of the
     * byte array that is used as the database key for this row.  If the key is
     * not fully specified the value returned is that of the partial key.
     *
     * @param key the IndexKey to use for the operation
     *
     * @return the size of the key portion serialized row, in bytes
     */
    public static int getKeySize(IndexKey key) {
        return ((IndexKeyImpl) key).getKeySize();
    }

    /**
     * Get the table's ID.  The ID is a system generated identifier used to
     * create short keys for the table. It is not of general interest to
     * applications other than browers and introspective applications.
     *
     * @return the numeric id value
     */
    public static long getId(Table table) {
        return ((TableImpl) table).getId();
    }

    /**
     * Get the table's ID as a String.  This is the string that is used in keys
     * generated for this table.  It is not of general interest to applications
     * other than browers and introspective applications.  Because of the
     * serialization format this value may not be human readable.
     * <p>
     * If the table is created as compatible with an R2 Avro schema this
     * value is equal to the table name and is human readable.
     *
     * @return the string version of the id or the table name
     */
    public static String getIdString(Table table) {
        return ((TableImpl) table).getIdString();
    }
}
