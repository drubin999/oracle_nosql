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

/**
 * Denotes an Operation in a sequence of operations passed to the {@link
 * KVStore#execute KVStore.execute} method.
 *
 * <p>Operation instances are created only by {@link OperationFactory} methods
 * and the Operation interface should not be implemented by the
 * application.</p>
 *
 * @see OperationFactory
 * @see KVStore#execute KVStore.execute
 */
public interface Operation {

    /**
     * The type of operation, as determined by the method used to create it.
     */
    public enum Type {

        /**
         * An operation created by {@link OperationFactory#createPut}.
         */
        PUT,

        /**
         * An operation created by {@link OperationFactory#createPutIfAbsent}.
         */
        PUT_IF_ABSENT,

        /**
         * An operation created by {@link OperationFactory#createPutIfPresent}.
         */
        PUT_IF_PRESENT,

        /**
         * An operation created by {@link OperationFactory#createPutIfVersion}.
         */
        PUT_IF_VERSION,

        /**
         * An operation created by {@link OperationFactory#createDelete}.
         */
        DELETE,

        /**
         * An operation created by {@link
         * OperationFactory#createDeleteIfVersion}.
         */
        DELETE_IF_VERSION,
    }

    /**
     * Returns the Key associated with the operation.
     */
    Key getKey();

    /**
     * Returns the operation Type.
     */
    Type getType();
    
    /**
     * Returns whether this operation should cause the {@link KVStore#execute
     * KVStore.execute} transaction to abort when the operation fails.
     */
    boolean getAbortIfUnsuccessful();
}
