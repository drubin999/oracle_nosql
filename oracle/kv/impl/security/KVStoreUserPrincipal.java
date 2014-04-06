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
package oracle.kv.impl.security;

import java.io.Serializable;
import java.security.Principal;

import javax.security.auth.Subject;

/**
 * Represents the identity of a KVStore user.
 */
public class KVStoreUserPrincipal implements Principal, Serializable {

    private static final long serialVersionUID = 1L;
    private final String username;
    private final String userId;

    public static final String INTERNAL_NAME = "_internal_";

    public static final KVStoreUserPrincipal INTERNAL =
        new KVStoreUserPrincipal(INTERNAL_NAME);

    /**
     * Constructs a user principal object for a "user" with no id.
     * The obvious case of this is the internal user.
     *
     * @param username the name of the user - must be non-null
     * @throws IllegalArgumentException if the username is null
     */
    public KVStoreUserPrincipal(String username) {
        this(username, null);
    }

    /**
     * Constructs a user principal object.
     *
     * @param username the name of the user - must be non-null
     * @param userId the id of the user - null allowable
     * @throws IllegalArgumentException if the username is null
     */
    public KVStoreUserPrincipal(String username, String userId) {
        if (username == null) {
            throw new IllegalArgumentException("username may not be null");
        }
        this.username = username;
        this.userId = userId;
    }

    /**
     * Get the userID of the user represented by this principal.
     * If the user is not a registered user (e.g. the internal KVStore
     * infrastructure user), this will be null.  If not null, if can be
     * used to lookup up a KVStoreUser.
     */
    public String getUserId() {
        return userId;
    }

    /**
     * Get the principal associated with the currently active user.
     * @return the active user principal
     */
    public static KVStoreUserPrincipal getCurrentUser() {

        return ExecutionContext.getCurrentUserPrincipal();
    }

    /**
     * Get the principal associated with the specified Subject.
     * @return the user principal
     */
    public static KVStoreUserPrincipal getSubjectUser(Subject subj) {

        return ExecutionContext.getSubjectUserPrincipal(subj);
    }

    /*
     * Principal interface methods
     */

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        return username.equals(((KVStoreUserPrincipal) other).username);
    }

    @Override
    public String getName() {
        return username;
    }

    @Override
    public int hashCode() {
        return username.hashCode();
    }

    @Override
    public String toString() {
        return "KVStoreUserPrincipal(" + username + "," + userId + ")";
    }

}
