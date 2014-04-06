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
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;

/**
 * Represents a "Role" associated with user.  This exists so that the roles
 * can be attached to subjects as a "Principal".
 */
public final class KVStoreRolePrincipal implements Principal, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The KVStore role assigned to an identity.
     */
    private final KVStoreRole role;

    /**
     * The role associated with the server itself.
     */
    public static final KVStoreRolePrincipal INTERNAL =
        new KVStoreRolePrincipal(KVStoreRole.INTERNAL);

    /**
     * The role associated with logged-in users (or internal).
     */
    public static final KVStoreRolePrincipal AUTHENTICATED =
        new KVStoreRolePrincipal(KVStoreRole.AUTHENTICATED);

    /**
     * The role associated with admin users.
     */
    public static final KVStoreRolePrincipal ADMIN =
        new KVStoreRolePrincipal(KVStoreRole.ADMIN);

    /**
     * A map to allow mapping from KVStoreRole to the corresponding
     * canonical KVStoreRolePrincipal.
     */
    private static final Map<KVStoreRole, KVStoreRolePrincipal> roleMap =
        new HashMap<KVStoreRole, KVStoreRolePrincipal>();

    static {
        roleMap.put(INTERNAL.getRole(), INTERNAL);
        roleMap.put(AUTHENTICATED.getRole(), AUTHENTICATED);
        roleMap.put(ADMIN.getRole(), ADMIN);
    }

    /**
     * Find the canonical KVStoreRolePrincipal instance corresponding to a
     * KVStoreRole.
     */
    public static KVStoreRolePrincipal get(KVStoreRole role) {
        return roleMap.get(role);
    }

    /**
     * Return an array of all KVStoreRoles associated with a Subject.
     * @param subj the subject to consider
     * @return a newly allocated array containing the assigned roles
     */
    public static KVStoreRole[] getSubjectRoles(Subject subj) {
        return ExecutionContext.getSubjectRoles(subj);
    }

    /*
     * Principal interface methods
     */

    /**
     * Check for equality.
     * @see Object#equals
     */
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        return role == ((KVStoreRolePrincipal) other).role;
    }

    /**
     * Get the principal name.
     * @see Principal#getName
     */
    @Override
    public String getName() {
        return role.toString();
    }

    /**
     * Get the hashCode value for the object.
     * @see Object#hashCode()
     */
    @Override
    public int hashCode() {
        return role.hashCode();
    }

    /**
     * Get a string representation of this object.
     * @see Object#toString()
     */
    @Override
    public String toString() {
        return "KVStoreRolePrincipal(" + role + ")";
    }

    /*
     * non-interface methods
     */

    private KVStoreRolePrincipal(KVStoreRole role) {
        this.role = role;
    }

    /**
     * Return the KVStoreRole object associated with this principal.
     */
    public KVStoreRole getRole() {
        return role;
    }
}
