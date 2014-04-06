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

import oracle.kv.Version;

/**
 * The Result associated with the execution of a TableOperation.
 *
 * @see TableOperationFactory
 * @see TableAPI#execute execute
 *
 * @since 3.0
 */
public interface TableOperationResult {

    /**
     * Whether the operation succeeded.  A put or delete operation may be
     * unsuccessful if the row or version was not matched.
     */
    boolean getSuccess();

    /**
     * For a put operation, the version of the new row.
     *
     * <p>Is null if any of the following conditions are true:</p>
     * <ul>
     * <li>The operation is not a put operation.
     * </li>
     * <li>The put operation did not succeed.
     * </li>
     * </ul>
     */
    Version getNewVersion();

    /**
     * For a put or delete operation, the version of the previous row
     * associated with the key.
     *
     * <p>Is null if any of the following conditions are true:</p>
     * <ul>
     * <li>The operation is not a put or delete operation.
     * </li>
     * <li>A previous value did not exist for the given row.
     * </li>
     * <li>The {@code prevReturn} {@link ReturnRow} parameter
     * specified that the version should not be returned.
     * </li>
     * <li>For a {@link TableOperationFactory#createPutIfVersion putIfVersion}
     * or {@link TableOperationFactory#createDeleteIfVersion deleteIfVersion}
     * operation, the {@code matchVersion} parameter matched the version of the
     * previous value.
     * </li>
     * </ul>
     */
    Version getPreviousVersion();

    /**
     * For a put or delete operation, the previous value associated with
     * the row.
     *
     * <p>Is null if any of the following conditions are true:</p>
     * <ul>
     * <li>The operation is not a put or delete operation.
     * </li>
     * <li>A previous value did not exist for the given row.
     * </li>
     * <li>The {@code prevReturn} {@link ReturnRow} parameter
     * specified that the row should not be returned.
     * </li>
     * <li>For a {@link TableOperationFactory#createPutIfVersion putIfVersion}
     * or {@link TableOperationFactory#createDeleteIfVersion deleteIfVersion}
     * operation, the {@code matchVersion} parameter matched the version of the
     * previous value.
     * </li>
     * </ul>
     */
    Row getPreviousRow();
}
