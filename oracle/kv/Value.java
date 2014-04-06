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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

import oracle.kv.impl.util.FastExternalizable;

/**
 * The Value in a Key/Value store.
 */
public class Value implements FastExternalizable {

    /**
     * Identifies the format of a value.
     *
     * @since 2.0
     */
    public enum Format {

        /*
         * Internally we use the first byte of the stored value to determine
         * the format (NONE or AVRO).
         *
         * + If the first stored byte is zero, the format is NONE.  In this
         *   case the byte array visible via the API (via getValue) does not
         *   contain this initial byte and its size is one less than the stored
         *   array size.
         *
         * + If the first stored byte is negative, it is the first byte of an
         *   Avro schema ID and the format is AVRO.  In this case the entire
         *   stored array is returned by getValue.
         *
         *   PackedInteger.writeSortedInt is used to format the schema ID, and
         *   this guarantees that the first byte of a positive number is
         *   negative.  Schema IDs are always positive, starting at one.
         *
         * + If the first stored byte is one, the format is TABLE, indicating
         *   that the record is part of a table and is serialized in a
         *   table-specific format.
         *
         * The stored byte array is always at least one byte.  For format NONE,
         * the user visible array may be empty in which case the stored array
         * has a single, zero byte.  For format AVRO, the user visible array
         * and the stored array are the same, but are always at least one byte
         * in length due to the presence of the schema ID.
         *
         * If an unexpected value is seen by the implementation an
         * IllegalStateException is thrown.  Additional positive values may be
         * used for new formats in the future.
         */

        /**
         * The byte array format is not known to the store; the format is known
         * only to the application.  Values of format {@code NONE} are created
         * with {@link #createValue}.  All values created using NoSQL DB
         * version 1.x have format {@code NONE}.
         */
        NONE,

        /**
         * The byte array format is Avro binary data along with an internal,
         * embedded schema ID.  Values of format {@code AVRO} are created and
         * decomposed using an {@link oracle.kv.avro.AvroBinding}.
         */
        AVRO,

        /**
         * The byte array format that is used by table rows.  Values with
         * this format are never created by applications but non-table
         * applications may accidentally see a table row.  These Values
         * cannot be deserialized by non-table applications.
         */
        TABLE;

        /**
         * For internal use only.
         * @hidden
         */
        public static Format fromFirstByte(int firstByte) {

            /* Zero means no format. */
            if (firstByte == 0) {
                return Format.NONE;
            }

            /* One means table format. */
            if (firstByte == 1) {
                return Format.TABLE;
            }

            /*
             * Avro schema IDs are positive, which means the first byte of the
             * package sorted integer is negative.
             */
            if (firstByte < 0) {
                return Format.AVRO;
            }

            /* Other values are not yet assigned. */
            throw new IllegalStateException
                ("Value has unknown format discriminator: " + firstByte);
        }
    }

    /**
     * An instance that represents an empty value for key-only records.
     */
    public static final Value EMPTY_VALUE = Value.createValue(new byte[0]);

    private final byte[] val;
    private final Format format;

    private Value(byte[] val, Format format) {
        this.val = val;
        this.format = format;
    }

    /**
     * For internal use only.
     * @hidden
     *
     * FastExternalizable constructor.
     * Used by the client when deserializing a response.
     */
    public Value(ObjectInput in,
                 @SuppressWarnings("unused") short serialVersion)
        throws IOException {

        final int len = in.readInt();
        if (len == 0) {
            throw new IllegalStateException
                ("Value is zero length, format discriminator is missing");
        }

        final int firstByte = in.readByte();
        format = Format.fromFirstByte(firstByte);

        /*
         * Both NONE and TABLE formats skip the first byte.
         */
        if (format == Format.NONE || format == Format.TABLE) {
            val = new byte[len - 1];
            in.readFully(val);
            return;
        }

        /*
         * AVRO includes the first byte because it is all or part of the
         * record's schema ID.
         */
        val = new byte[len];
        val[0] = (byte) firstByte;
        in.readFully(val, 1, len - 1);
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Deserialize into byte array.
     * Used by the service when deserializing a request.
     */
    public static byte[]
        readFastExternal(ObjectInput in,
                         @SuppressWarnings("unused") short serialVersion)
        throws IOException {

        final int len = in.readInt();
        if (len == 0) {
            throw new IllegalStateException
                ("Value is zero length, format discriminator is missing");
        }

        final byte[] bytes = new byte[len];
        in.readFully(bytes);
        return bytes;
    }

    /**
     * For internal use only.
     * @hidden
     *
     * FastExternalizable writer.
     * Used by the client when serializing a request.
     */
    @Override
    public void writeFastExternal(ObjectOutput out, short serialVersion)
        throws IOException {

        if (format == Format.NONE || format == Format.TABLE) {
            out.writeInt(val.length + 1);
            out.write((format == Format.NONE) ? 0 : 1);
            out.write(val);
            return;
        }

        out.writeInt(val.length);
        out.write(val);
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Serialize from byte array.
     * Used by the service when serializing a response.
     */
    public static void writeFastExternal(ObjectOutput out,
                                         @SuppressWarnings("unused")
                                         short serialVersion,
                                         byte[] bytes)
        throws IOException {

        if (bytes.length == 0) {
            throw new IllegalStateException
                ("Value is zero length, format discriminator is missing");
        }

        out.writeInt(bytes.length);
        out.write(bytes);
    }

    /**
     * Returns this Value as a serialized byte array, such that {@link
     * #fromByteArray} may be used to reconstitute the Value.
     * <p>
     * Note that this method does not always return an array equal to the
     * {@link #getValue} array.  The serialized representation of a Value may
     * contain an extra byte identifying the format.
     * </p>
     */
    public byte[] toByteArray() {

        if (format == Format.NONE || format == Format.TABLE) {
            final byte[] bytes = new byte[val.length + 1];
            System.arraycopy(val, 0, bytes, 1, val.length);
            return bytes;
        }

        return val;
    }

    /**
     * Deserializes the given bytes that were returned earlier by {@link
     * #toByteArray} and returns the resulting Value.
     * <p>
     * Note that an array equal to the {@link #getValue} array may not be
     * passed to this method.  To create a Value from only the value array,
     * call {@link #createValue} instead.
     * </p>
     */
    public static Value fromByteArray(byte[] bytes) {

        if (bytes.length == 0) {
            throw new IllegalStateException
                ("Value is zero length, format discriminator is missing");
        }

        final Format format = Format.fromFirstByte(bytes[0]);

        if (format == Format.NONE || format == Format.TABLE) {
            final byte[] val = new byte[bytes.length - 1];
            System.arraycopy(bytes, 1, val, 0, val.length);
            return new Value(val, format);
        }

        final byte[] val = bytes;
        return new Value(val, format);
    }

    /**
     * Creates a Value from a value byte array.
     *
     * The format of the returned value is {@link Format#NONE}.  This method
     * may not be used to create Avro values; for that, use an {@link
     * oracle.kv.avro.AvroBinding} instead.
     */
    public static Value createValue(byte[] val) {
        assert val != null;

        return new Value(val, Format.NONE);
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Creates a value with a given format.  Used by Avro bindings.
     */
    public static Value internalCreateValue(byte[] val, Format format) {
        return new Value(val, format);
    }

    /**
     * Returns the value byte array.
     */
    public byte[] getValue() {
        return val;
    }

    /**
     * Returns the value's format.
     *
     * @since 2.0
     */
    public Format getFormat() {
        return format;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Value)) {
            return false;
        }
        final Value o = (Value) other;
        if (format != o.format) {
            return false;
        }
        return Arrays.equals(val, o.val);
    }

    @Override
    public int hashCode() {
        return format.hashCode() + val.hashCode();
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer("<Value format:");
        sb.append(format);
        sb.append(" bytes:");
        for (int i = 0; i < 100 && i < val.length; i += 1) {
            sb.append(' ');
            sb.append(val[i]);
        }
        if (val.length > 100) {
            sb.append(" ...");
        }
        sb.append(">");
        return sb.toString();
    }
}
