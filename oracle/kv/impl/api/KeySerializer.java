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

package oracle.kv.impl.api;

import oracle.kv.FaultException;
import oracle.kv.Key;
import oracle.kv.KeyRange;

/**
 * KeySerializer is responsible for serializing (toByteArray) keys before they
 * are included in a client API request, and deserializing (fromByteArray) keys
 * that are returned in a client API response.  It also restricts the key range
 * (restrictRange) when necessary.
 * <p>
 * The reason for the encapsulation of serialization in this class is to ensure
 * the keys in the internal keyspace (//) are never allowed to be accessed.
 * The internal keyspace (//) is restricted because it is used to hold internal
 * metadata.
 * <p>
 * Only clients that pass true for the allowInternalKeyspace parameter of the
 * internal KVStoreImpl copy constructor are allowed to access the internal
 * keyspace.  For normal clients that do not use this mechanism, the
 * PROHIBIT_INTERNAL_KEYSPACE KeySerializer is used.  When
 * allowInternalKeyspace is used, the ALLOW_INTERNAL_KEYSPACE KeySerializer is
 * used.
 * <p>
 * The server-internal keyspace (///) is a subspace of the internal keyspace
 * and may never be accessed by the client. Enforcement of the server-internal
 * keyspace access restriction is applied at the server side as a security
 * precaution.
 */
public class KeySerializer {

    /* Public for unit testing. */
    public static final String EXCEPTION_MSG =
        "First component of Key major path must not be empty";

    /** Private so only static instances can be used. */
    private KeySerializer() {
    }

    /**
     * Serializes the key.
     *
     * @throws IllegalArgumentException if the key is in the internal keyspace.
     * This exception is meant to be an indication of a programming error in
     * the client application.
     */
    public byte[] toByteArray(Key key) {
        return key.toByteArray();
    }

    /**
     * Deserializes the key.
     *
     * @throws FaultException if the key is in the internal keyspace.  This
     * should only occur if there is an internal bug that incorrectly returns a
     * key in the internal keyspace (via a client API response) to a client
     * that has not used allowInternalKeyspace.  This provides a double-check
     * to safeguard against potential bugs.
     */
    public Key fromByteArray(byte[] bytes) {
        return Key.fromByteArray(bytes);
    }

    /**
     * Returns a restricted range, if necessary to prevent access to the
     * internal keyspace, or the given subRange parameter if no restriction is
     * necessary.
     */
    public KeyRange restrictRange(@SuppressWarnings("unused") Key parentKey,
                                  KeyRange subRange) {
        return subRange;
    }

    /** Used for clients that have used allowInternalKeyspace. */
    public static final KeySerializer ALLOW_INTERNAL_KEYSPACE =
        new KeySerializer();

    /** Used for clients that have not used allowInternalKeyspace. */
    public static final KeySerializer PROHIBIT_INTERNAL_KEYSPACE =
        new KeySerializer() {

        /**
         * Prohibits keys in the internal keyspace.
         */
        @Override
        public byte[] toByteArray(Key key) {
            if (key.getMajorPath().get(0).isEmpty()) {
                throw new IllegalArgumentException
                    ("Invalid Key: " + key + ' ' + EXCEPTION_MSG);
            }
            return super.toByteArray(key);
        }

        /**
         * Prohibits keys in the internal keyspace.
         */
        @Override
        public Key fromByteArray(byte[] bytes) {
            final Key key = super.fromByteArray(bytes);
            if (key.getMajorPath().get(0).isEmpty()) {
                throw new FaultException
                    ("Internal error.  Invalid key returned: " + key +
                     ' ' + EXCEPTION_MSG, false /*isRemote*/);
            }
            return key;
        }

        /**
         * Returns a range that does not include keys in the internal keyspace.
         * The returned range is the intersection of the range
         *   ("", infinity)
         * where the exclusive begin key is the empty string, and the specified
         * subRange parameter.
         */
        @Override
        public KeyRange restrictRange(Key parentKey, KeyRange subRange) {

            /*
             * When parentKey is non-null we don't need to restrict the range,
             * because the parent key is guaranteed to represent a non-empty
             * prefix that is not in the internal keyspace.
             */
            if (parentKey != null) {
                return subRange;
            }
            
            /* Return intersection of subRange and ("", infinity). */
            if (subRange == null) {
                return new KeyRange("", false /*startInclusive*/,
                                    null /*end*/, false /*endInclusive*/);
            }
            if (subRange.getStart() == null ||
                (subRange.getStart().isEmpty() &&
                 subRange.getStartInclusive())) {
                return new KeyRange("", false /*startInclusive*/,
                                    subRange.getEnd(),
                                    subRange.getEndInclusive());
            }
            return subRange;
        }
    };
}
