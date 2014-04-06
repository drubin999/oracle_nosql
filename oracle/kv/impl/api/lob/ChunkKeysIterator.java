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

package oracle.kv.impl.api.lob;

import java.util.Iterator;
import java.util.NoSuchElementException;

import oracle.kv.Key;

/**
 * Iterator to produce a sequence of chunk keys.
 */
class ChunkKeysIterator implements Iterator<Key>, Cloneable {

    final Key internalLobKey;
    final int chunksPerPartition;
    final long limitChunks;

    final ChunkKeyFactory keyFactory;

    /*
     * Iteration state. The linear chunk index. The value zero is special and
     * indicates a position before the first chunk.
     *
     * It has the range:  0 <= lcindex <= limitChunks
     */
    private long lcindex;

    /**
     * Position the iterator to yield the key associated with the chunk
     * containing <code>byteIndex</code> on the next call to the next()
     * method.
     *
     * @param internalLobKey the major key used to generate chunk keys
     *
     * @param byteIndex the byteIndex used to establish the initial chunk to
     * be returned by the iterator
     *
     * @param maxByteIndex determines the limitChunks value. The last returned
     * chunk will contain the byte associated with this index.
     *
     * @param chunkSize size used as the basis for determining chunk units
     *
     * @param chunksPerPartition the basis for determining chunkids
     *
     * @param keyFactory used to produce keys for this iterator
     */
    ChunkKeysIterator(Key internalLobKey,
                      long byteIndex,
                      long maxByteIndex,
                      int chunkSize,
                      int chunksPerPartition,
                      ChunkKeyFactory keyFactory) {
        super();

        if ((byteIndex < 0) || (chunkSize <= 0) || (chunksPerPartition <= 0)) {
            throw new IllegalStateException
                ("Byte index:" + byteIndex +
                 " chunk size:" + chunkSize +
                 " chunks per partition:" + chunksPerPartition);
        }

        if (byteIndex > maxByteIndex) {
            throw new IllegalStateException("Byte index:" + byteIndex +
                                            " > max byte index:" +
                                            maxByteIndex);
        }

        this.internalLobKey = internalLobKey;
        this.chunksPerPartition = chunksPerPartition;

        lcindex = (byteIndex / chunkSize);

        limitChunks = (maxByteIndex == 0 ) ? 0 :
            ((maxByteIndex - 1) / chunkSize) + 1;

        this.keyFactory = keyFactory;
    }

    @Override
    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Returns the super-chunk-relative chunk id associated with the current
     * position of the iterator. The returned chunk id has a value in the
     * range:
     *
     * 1 <= i <= chunksPerPartition
     */
    long getChunkId() {
        if (lcindex == 0) {
            throw new IllegalStateException("next( has not yet been invoked.");
        }

       final long chunkId = ((lcindex - 1) % chunksPerPartition) + 1;

       if (chunkId <= 0) {
           throw new IllegalStateException
               ("current chunk index:" + lcindex + " cid:" + chunkId);
       }

       return chunkId;
    }

    /**
     * Returns the super chunk id associated with the current position of the
     * iterator. The returns super chunk id has a value in the range:
     *
     * 1 <= i <= (limitChunks / chunksPerPartition) + 1
     */
    long getSuperChunkId() {
        if (lcindex == 0) {
            throw new IllegalStateException("next( has not yet been invoked.");
        }
       return ((lcindex  - 1) / chunksPerPartition) + 1;
    }

    /**
     * Reset the iterator to the chunk position associated with the iterator
     * argument.
     */
    void reset(ChunkKeysIterator i) {
        lcindex = i.lcindex;
    }

    /**
     * Skip the requested number of chunks if possible. Return the number of
     * chunks that were actually skipped. The next call to the iterator will
     * return the next chunk if there is one. If the number of chunks to be
     * skipped exceeds limitChunks, the iterator is repositioned at the
     * last chunk.
     *
     * @param skipChunks number of chunks to be skipped
     *
     * @return number of chunks actually skipped
     */
    long skip(long skipChunks) {

        if (skipChunks < 0) {
            throw new IllegalArgumentException("skip chunks: " + skipChunks);
        }

        if (skipChunks == 0) {
            return 0;
        }

        long savedLcindex = lcindex;

        lcindex += skipChunks;
        if (lcindex > limitChunks) {
            lcindex = limitChunks;
        }

        return (lcindex - savedLcindex);
    }

    /**
     * Returns the linear chunk index.
     *
     * @return the chunk index, an integer in the range:
     *         1 <= chunkId <= limitChunks
     * or zero if it is at start of the stream before any chunks have been
     * read
     */
    long currentChunkIndex() {
        return lcindex;
    }


    @Override
    public boolean hasNext() {
        return lcindex < limitChunks;
    }

    /**
     * Returns the next chunk key
     */
    @Override
    public Key next() {
        if (!hasNext()) {
            throw new NoSuchElementException
                ("current chunk index:" + lcindex + " max:" + limitChunks);
        }

        lcindex++;

        return keyFactory.create(internalLobKey,
                                 getSuperChunkId(), getChunkId());
    }

    /**
     * Sets the iterator to the preceding chunk, if there is one. That is, next
     * will return the same key it just returned.
     *
     * @return true if the iterator was not at the start and was backed up,
     * false otherwise.
     */
    public boolean backup() {
        if (lcindex > 0) {
            lcindex--;
            return true;
        }
        return false;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException
            ("Method not implemented: remove");
    }

    @Override
    public String toString() {
        return "<ChunkKeysIterator " +
            " lob version:" + keyFactory.getMetadataVersion() +
            " lcid:" + lcindex +
            " limit chunks:" + limitChunks +
            " scid: " + ((lcindex != 0) ? getSuperChunkId() : -1) +
            " chunkId: " + ((lcindex != 0) ? getChunkId() : -1)+ ">";
    }
}