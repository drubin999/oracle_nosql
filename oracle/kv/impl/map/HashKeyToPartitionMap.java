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

package oracle.kv.impl.map;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicBoolean;

import oracle.kv.Key;
import oracle.kv.impl.topo.PartitionId;

/**
 * A hash based implementation used to distribute keys across partitions.
 */
public class HashKeyToPartitionMap implements KeyToPartitionMap {

    private static final long serialVersionUID = 1L;

    final BigInteger nPartitions;

    transient DigestCache digestCache = new DigestCache();

    public HashKeyToPartitionMap(int nPartitions) {
        super();
        this.nPartitions = new BigInteger(Integer.toString(nPartitions));
    }

    @Override
    public int getNPartitions() {
        return nPartitions.intValue();
    }

    @Override
    public PartitionId getPartitionId(byte[] keyBytes) {
        MessageDigest md = null;
        try {
            if (digestCache == null) {
                digestCache = new DigestCache();
            }
            /* Clone one for use by this thread. */
            md = digestCache.get();

            /* Digest Key major path. */
            md.update(keyBytes, 0, Key.getMajorPathLength(keyBytes));

            final BigInteger index =
                new BigInteger(md.digest()).mod(nPartitions);
            return new PartitionId(index.intValue() + 1);
        } finally {
            digestCache.free(md);
        }
    }

    /**
     * Implements a single entry, wait-free cache for an instance of a digest,
     * since the cost of a MessageDigest.getInstance("MD5") call can be high
     * both in cpu terms, due to its dynamic nature and the security checks
     * that are performed and because the underlying Class.forName() calls seem
     * to either result in lock contention in native code, or are expensive,
     * as evidenced by the thread dumps.
     */
    static class DigestCache {
        /* True if the cached item is in use. */
        private final AtomicBoolean inUse;

        /* The single cached entry. */
        private final MessageDigest digest;

        /* The digest used to create clones. */
        private final MessageDigest protoDigest;

        public DigestCache() {
            try {
                protoDigest = MessageDigest.getInstance("MD5");
                digest = (MessageDigest) protoDigest.clone();
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("MD5 algorithm unavailable");
            }  catch (CloneNotSupportedException e) {
                throw new IllegalStateException("MD5 clone failed");
            }
            inUse = new AtomicBoolean(false);
        }

        /**
         * Returns the single cached digest, or clones one if the single cached
         * entry is busy.
         *
         * @return a MessageDigest
         */
        MessageDigest get() {
            if (inUse.compareAndSet(false, true)) {
                digest.reset();
                return digest;
            }
            try {
                return (MessageDigest) protoDigest.clone();
            } catch (CloneNotSupportedException e) {
                throw new IllegalStateException("MD5 clone failed");
            }
        }

        /**
         * Frees the message digest. If it is the single cached digest it
         * marks it as being available again. If not there is nothing to do
         * the GC will reclaim the clone.
         *
         * @param md the digest to be fred
         */
        void free(MessageDigest md) {
            if (md == digest) {
                if (!inUse.getAndSet(false)) {
                    throw new IllegalStateException
                        ("Expected digest to be in use");
                }
            }
        }
    }
}
