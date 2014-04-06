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

package oracle.kv.impl.rep.migration;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.Response;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import oracle.kv.impl.topo.RepNodeId;

/**
 * The partition migration transfer protocol. There are three components in the
 * protocol, the initial transfer request sent by the target to the source node,
 * the transfer response, and the database operations, both sent from the source
 * to the target.
 */
class TransferProtocol {

    static final int VERSION = 1;

    /* -- Transfer request -- */

    /*
     * Transfer request size (not including the last key):
     *      4 int protocol version
     *      4 int partition ID
     *      4 int target group ID
     *      4 int target node number
     *      4 int last key size
     */
    private static final int REQUEST_SIZE = 4 + 4 + 4 + 4 + 4;

    /**
     * Object encapsulating a transfer request.
     */
    static class TransferRequest {

        final int partitionId;
        final RepNodeId targetRNId;
        final DatabaseEntry lastKey;

        private TransferRequest(int partitionId, RepNodeId targetRnId,
                                DatabaseEntry lastKey) {
            this.partitionId = partitionId;
            this.targetRNId = targetRnId;
            this.lastKey = lastKey;
        }

        /*
         * Writes a transfer request.
         */
        static void write(DataChannel channel, int partitionId,
                          RepNodeId targetRNId,
                          DatabaseEntry lastKey)
            throws IOException {

            final int keySize = (lastKey == null) ? 0 : lastKey.getSize();
            final ByteBuffer buffer =
                                ByteBuffer.allocate(REQUEST_SIZE + keySize);
            buffer.putInt(VERSION);
            buffer.putInt(partitionId);
            buffer.putInt(targetRNId.getGroupId());
            buffer.putInt(targetRNId.getNodeNum());
            buffer.putInt(keySize);

            if (lastKey != null) {
                buffer.put(lastKey.getData());
            }
            buffer.flip();
            channel.write(buffer);
        }

        /*
         * Reads a transfer request.
         */
        static TransferRequest read(DataChannel channel) throws IOException {
            ByteBuffer readBuffer =
                        ByteBuffer.allocate(TransferProtocol.REQUEST_SIZE);
            read(readBuffer, channel);

            final int version = readBuffer.getInt();

            if (version != TransferProtocol.VERSION) {
                final StringBuilder sb = new StringBuilder();
                sb.append("Protocol version mismatch, received ");
                sb.append(version);
                sb.append(" expected ");
                sb.append(TransferProtocol.VERSION);
                throw new IOException(sb.toString());
            }
            final int partitionId = readBuffer.getInt();
            final int targetGroupId = readBuffer.getInt();
            final int targetNodeNum = readBuffer.getInt();

            int lastKeySize = readBuffer.getInt();

            DatabaseEntry lastKey = null;
            if (lastKeySize > 0) {
                ByteBuffer lastKeyBuffer = ByteBuffer.allocate(lastKeySize);
                read(lastKeyBuffer, channel);
                lastKey = new DatabaseEntry(lastKeyBuffer.array());
            }
            return new TransferRequest(partitionId,
                                       new RepNodeId(targetGroupId,
                                                     targetNodeNum),
                                       lastKey);
        }

        private static void read(ByteBuffer bb, DataChannel channel)
            throws IOException {
            while (bb.remaining() > 0) {
                if (channel.read(bb) < 0) {
                    throw new IOException("Unexpected EOF");
                }
            }
            bb.flip();
        }

        /* -- Request response -- */

        /*
         * ACK Response
         *
         *  byte Response.OK
         */
        static void writeACKResponse(DataChannel channel) throws IOException {
            ByteBuffer buffer = ByteBuffer.allocate(1);
            buffer.put((byte)Response.OK.ordinal());
            buffer.flip();

            // TODO - Should this check be != buffer size?
            if (channel.write(buffer) == 0) {
                throw new IOException("Failed to write response. " +
                                      "Send buffer size: " +
                                      channel.getSocketChannel().socket().
                                      getSendBufferSize());
            }
        }

        /*
         * Busy Response
         *
         *  byte   Response.Busy
         *  int    numStreams - the number of migration streams the source
         *                      currently supports (may change, may be 0)
         *  int    reason message length
         *  byte[] reason message bytes
         */
        static void writeBusyResponse(DataChannel channel,
                                      int numStreams,
                                      String message) throws IOException {

            byte[] mb = message.getBytes();
            ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + 4 + mb.length);
            buffer.put((byte)Response.BUSY.ordinal());
            buffer.putInt(numStreams);
            buffer.putInt(mb.length);
            buffer.put(mb);
            buffer.flip();
            if (channel.write(buffer) == 0) {
                throw new IOException("Failed to write response. " +
                                      "Send buffer size: " +
                                      channel.getSocketChannel().socket().
                                      getSendBufferSize());
            }
        }

        /*
         * Error Response
         *
         *  byte   Response.Busy
         *  int    reason message length
         *  byte[] reason message bytes
         */
        static void writeErrorResponse(DataChannel channel,
                                       Response response,
                                       String message) throws IOException {

            byte[] mb = message.getBytes();
            ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + mb.length);
            buffer.put((byte)response.ordinal());
            buffer.putInt(mb.length);
            buffer.put(mb);
            buffer.flip();
            if (channel.write(buffer) == 0) {
                throw new IOException("Failed to write response. " +
                                      "Send buffer size: " +
                                      channel.getSocketChannel().socket().
                                      getSendBufferSize());
            }
        }

        static Response readResponse(DataInputStream stream)
            throws IOException {

            int ordinal = stream.read();
            if ((ordinal < 0) || (ordinal >= Response.values().length)) {
                throw new IOException("Error reading response= " + ordinal);
            }
            return Response.values()[ordinal];
        }

        static int readNumStreams(DataInputStream stream)
            throws IOException {
            return stream.readInt();
        }

        static String readReason(DataInputStream stream) {
            try {
                int size = stream.readInt();
                byte[] bytes = new byte[size];
                stream.readFully(bytes);
                return new String(bytes);
            } catch (IOException ioe) {
                return "";
            }
        }
    }

    /* -- DB OPs -- */

    /**
     * Operation messages. These are the messages sent from the source to the
     * target node during the partition data transfer.
     */
    static enum OP {

        /**
         * A DB read operation. A COPY is generated by the key-ordered reads
         * of the source DB.
         */
        COPY,

        /**
         * A client put operation.
         */
        PUT,

        /**
         * A client delete operation.
         */
        DELETE,

        /**
         * Indicates that client transaction is about to be committed. No
         * further PUT or DELETE messages should be sent for the transaction.
         */
        PREPARE,

        /**
         * The client transaction has been successfully committed.
         */
        COMMIT,

        /**
         * The client transaction has been aborted.
         */
        ABORT,

        /**
         * End of Data. The partition migration data transfer is complete and
         * no further messages will be sent from the source.
         */
        EOD ;

        /*
         * Gets the OP corresponding to the specified ordinal.
         */
        static OP get(int ordinal) {
            if ((ordinal >= 0) && (ordinal < values().length)) {
                return values()[ordinal];
            }
            return null;
        }
    }
}
