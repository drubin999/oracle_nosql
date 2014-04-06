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

import java.io.Serializable;
import java.util.Arrays;

/**
 * Username/password credentials.  This class provides the standard mechanism
 * for an application to authenticate as a particular user when accessing a
 * KVStore instance.  The object contains sensitive information and should be
 * kept private.  When no longer needed the user should call clear() to erase
 * the internal password information.
 *
 * @since 3.0
 */
public class PasswordCredentials implements LoginCredentials, Serializable {
    private static final long serialVersionUID = 1L;
    private String username;
    private char[] password;

    /**
     * Creates a username/password credential set.   The password passed in is
     * copied internal to the object.  For maximum security, it is recommended
     * that you call the {@link #clear()} method when you are done with the
     * object to avoid have the password being present in the Java memory heap.
     *
     * @param username the name of the user
     * @param password the password of the user
     * @throws IllegalArgumentException if either username or password
     * have null values.
     */
    public PasswordCredentials(String username, char[] password)
        throws IllegalArgumentException {

        if (username == null) {
            throw new IllegalArgumentException(
                "The username argument must not be null");
        }
        if (password == null) {
            throw new IllegalArgumentException(
                "The password argument must not be null");
        }
        this.username = username;
        this.password = Arrays.copyOf(password, password.length);
    }

    /**
     * @see LoginCredentials#getUsername()
     */
    @Override
    public String getUsername() {
        return username;
    }

    /**
     * Gets the password. This returns a copy of the password. The caller should
     * clear the returned memory when the value is no longer needed.
     *
     * @return The password for the user.
     */
    public char[] getPassword() {
        return Arrays.copyOf(password, password.length);
    }

    /**
     * Wipes out the password storage to ensure it does not hang around in
     * in the Java VM memory space.
     */
    public void clear() {
        Arrays.fill(password, ' ');
    }
}
