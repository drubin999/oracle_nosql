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

package oracle.kv.impl.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Utility methods to facilitate serialization/deserialization
 */
public class SerializationUtil {

    /**
     * Returns the object de-serialized from the specified byte array.
     *
     * @param <T> the type of the deserialized object
     *
     * @param bytes the serialized bytes
     * @param oclass the class associated with the deserialized object
     *
     * @return the deserialized object
     */
    @SuppressWarnings("unchecked")
    public static <T> T getObject(byte[] bytes, Class<T> oclass) {

        if (bytes == null) {
            return null;
        }

        ObjectInputStream ois = null;
        try {
            ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
            return (T)ois.readObject();
        } catch (IOException ioe) {
            throw new IllegalStateException
                ("Exception deserializing object: " + oclass.getName(), ioe);
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalStateException
            ("Exception deserializing object: " + oclass.getName(), cnfe);
        } finally {
            if (ois != null) {
                try {
                    ois.close();
                } catch (IOException ioe) {
                    throw new IllegalStateException
                        ("Exception closing deserializing stream", ioe);
                }
            }
        }
    }

    /**
     * Returns a byte array containing the serialized form of this object.
     *
     * @param object the object to be serialized
     *
     * @return the serialized bytes
     */
    public static byte[] getBytes(Object object) {
        ObjectOutputStream oos = null;
        try {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(baos);
            oos.writeObject(object);
            oos.close();
            return baos.toByteArray();
        } catch (IOException ioe) {
            throw new IllegalStateException
                ("Exception serializing object: " +
                 object.getClass().getName(),
                 ioe);
        } finally {
            if (oos != null) {
                try {
                    oos.close();
                } catch (IOException ioe) {
                    throw new IllegalStateException
                        ("Exception closing serializing stream", ioe);
                }
            }
        }
    }
}
