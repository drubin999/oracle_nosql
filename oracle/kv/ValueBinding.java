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

import oracle.kv.avro.AvroBinding;
import oracle.kv.avro.AvroCatalog;

/**
 * Generic interface for translating between {@link Value}s (stored byte
 * arrays) and typed objects representing that value.  In other words, this
 * interface is used for serialization and deserialization of {@link Value}s.
 * <p>
 * A built-in {@link AvroBinding}, which is a {@code ValueBinding} subtype, may
 * be obtained from the {@link AvroCatalog}.  Or, the {@code ValueBinding}
 * interface may be implemented directly by the application to create custom
 * bindings, when the Avro data format is not used.
 * <p>
 * <em>WARNING:</em> We strongly recommend using an {@link AvroBinding}.  NoSQL
 * DB will leverage Avro in the future to provide additional features and
 * capabilities.
 *
 * @param <T> is the type of the deserialized object that is passed to {@link
 * #toValue toValue} and returned by {@link #toObject toObject}.  The specific
 * type depends on the particular binding that is used.
 *
 * @see AvroBinding
 * @see AvroCatalog
 *
 * @since 2.0
 */
public interface ValueBinding<T> {

    /**
     * After doing a read operation using a {@link KVStore} method, the user
     * calls {@code toObject} with the {@link Value} obtained from the read
     * operation.
     *
     * @param value the {@link Value} obtained from a {@link KVStore} read
     * operation method.
     *
     * @return the deserialized object.
     *
     * @throws RuntimeException if a parameter value is disallowed by the
     * binding; see {@link AvroBinding} for specific exceptions thrown when
     * using the Avro format.
     */
    public T toObject(Value value)
        throws RuntimeException;

    /**
     * Before doing a write operation, the user calls {@code toValue} passing
     * an object she wishes to store.  The resulting {@link Value} is then
     * passed to the write operation method in {@link KVStore}.
     *
     * @param object the object the user wishes to store, or at least
     * serialize.
     *
     * @return the serialized object.
     *
     * @throws RuntimeException if a parameter value is disallowed by the
     * binding; see {@link AvroBinding} for specific exceptions thrown when
     * using the Avro format.
     */
    public Value toValue(T object)
        throws RuntimeException;
}
