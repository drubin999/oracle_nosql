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

package oracle.kv.impl.security.metadata;

import java.io.Serializable;
import java.security.Principal;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.security.auth.Subject;

import oracle.kv.impl.security.KVStoreRolePrincipal;
import oracle.kv.impl.security.KVStoreUserPrincipal;
import oracle.kv.impl.security.metadata.SecurityMetadata.SecurityElementType;

/**
 * KVStore user definition.
 */
public class KVStoreUser extends SecurityMetadata.SecurityElement {

    private static final long serialVersionUID = 1L;

    /**
     * User types, new types must be added to the end of this list.
     */
    public static enum UserType { LOCAL }

    private final String userName;

    private UserType userType = UserType.LOCAL;

    /* Used as the main password in authentication  */
    private PasswordHashDigest primaryPassword;

    /*
     * This password is mainly used during password updating procedure, and is
     * intended for letting the new and old password take effect simultaneously
     * in a specified period for authentication.
     */
    private PasswordHashDigest retainedPassword;

    /*
     * Whether the user is enabled. A user is active and is able to login the
     * system only when it is enabled.
     */
    private boolean enabled;

    /* Whether the user is an Admin */
    private boolean isAdmin;

    /*
     * TODO: Roles granted for the user, e.g.:
     * List<KVStoreRole> grantedRoles;
     */

    /**
     * Create an initial user instance with specified name, without password
     * and is not yet enabled.
     */
    public KVStoreUser(final String name) {
        this.userName = name;
    }

    /*
     * Copy ctor
     */
    private KVStoreUser(final KVStoreUser other) {
        super(other);
        userName = other.userName;
        userType = other.userType;
        enabled = other.enabled;
        isAdmin = other.isAdmin;

        primaryPassword = other.primaryPassword == null ?
                          null : other.primaryPassword.clone();
        retainedPassword = other.retainedPassword == null ?
                           null : other.retainedPassword.clone();
    }

    /**
     * Sets the type of user. The valid types are defined as in
     * {@link UserType}. Only the {@code UserType.LOCAL} is defined in current
     * version.
     *
     * @param type type of user
     * @return this
     */
    KVStoreUser setUserType(final UserType type) {
        this.userType = type;
        return this;
    }

    /**
     * Gets the type of the user.
     *
     * @return user type defined as in {@link UserType}
     */
    public UserType getUserType() {
        return userType;
    }

    /**
     * Gets the name of the user.
     *
     * @return user name
     */
    public String getName() {
        return userName;
    }

    /**
     * Save the encrypted password of the user. The password will be used as
     * the primary one in authentication.
     *
     * @param primaryPasswd the primary password
     * @return this
     */
    public KVStoreUser setPassword(final PasswordHashDigest primaryPasswd) {
        primaryPassword = primaryPasswd;
        return this;
    }

    /**
     * Retains the current primary password as a secondary password during the
     * password changing operation. This enables users to login using both new
     * and old passwords.
     *
     * @return this
     */
    public KVStoreUser retainPassword() {
        /* Retained password could not be overridden. */
        if (retainedPasswordValid()) {
            throw new IllegalStateException(
                "Could not override an existing retained password.");
        }
        retainedPassword = primaryPassword;
        retainedPassword.refreshCreateTime();
        return this;
    }

    /**
     * Gets the primary password of the user.
     *
     * @return a PasswordHashDigest object containing the primary password
     */
    public PasswordHashDigest getPassword() {
        return primaryPassword;
    }

    /**
     * Gets the retained secondary password of the user.
     *
     * @return a PasswordHashDigest object containing the secondary password
     */
    public PasswordHashDigest getRetainedPassword() {
        return retainedPassword;
    }

    /**
     * Clears the current retained secondary password.
     */
    public void clearRetainedPassword() {
        retainedPassword = null;
    }

    /**
     * Checks if the user is in enabled state.
     *
     * @return true if enabled, otherwise false.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Checks if the user is an administrator.
     *
     * @return true if is admin, otherwise false.
     */
    public boolean isAdmin() {
        return isAdmin;
    }

    /**
     * Checks if the retained password is valid. The retained password is valid
     * iff. it is not null and not expired.
     */
    public boolean retainedPasswordValid() {
        return (retainedPassword != null) && (!retainedPassword.isExpired());
    }

    /**
     * Marks the user as an Admin or not.
     *
     * @param flag whether to be an admin
     * @return this
     */
    public KVStoreUser setAdmin(final boolean flag) {
        this.isAdmin = flag;
        return this;
    }

    /**
     * Marks the user as enabled or not.
     *
     * @param flag whether to be enabled
     * @return this
     */
    public KVStoreUser setEnabled(final boolean flag) {
        this.enabled = flag;
        return this;
    }

    /**
     * Get both brief and detailed description of a user for showing.
     *
     * @return a pair of <brief, details> information
     */
    public UserDescription getDescription() {
        final boolean rPassActive = retainedPasswordValid();
        String retainInfo;

        if (rPassActive) {
            final String expireInfo = String.format(
                "%d minutes", TimeUnit.MILLISECONDS.toMinutes(
                                retainedPassword.getLifeTime()));
            retainInfo = String.format("active [expiration: %s]", expireInfo);
        } else {
            retainInfo = "inactive";
        }
        final String details =
                String.format("%s enabled=%b type=%s admin=%b retain-passwd=%s",
                              toString(), enabled, userType, isAdmin,
                              retainInfo);

        return new UserDescription(toString(), details);
    }

    /**
     * Verifies if the plain password matches with the password of the user.
     *
     * @param password the plain password
     * @return true iff. all the following conditions holds:
     * <li>the user is enabled, and</li>
     * <li>the primary password matches with the plain password, or the
     * retained password is valid and matches with the plain password. </li>
     */
    public boolean verifyPassword(final char[] password) {
        if (password == null || password.length == 0) {
            return false;
        }

        if (!isEnabled()) {
            return false;
        }
        return getPassword().verifyPassword(password) ||
            (retainedPasswordValid() &&
             getRetainedPassword().verifyPassword(password));
    }

    /**
     * Creates a Subject with the KVStoreRolePrincipals and
     * KVStoreUserPrincipals indicated by this entry.
     *
     * @return a newly created Subject
     */
    public Subject makeKVSubject() {
        final String userId = getElementId();
        final Set<Principal> userPrincipals = new HashSet<Principal>();
        userPrincipals.add(KVStoreRolePrincipal.AUTHENTICATED);
        if (isAdmin) {
            userPrincipals.add(KVStoreRolePrincipal.ADMIN);
        }
        userPrincipals.add(new KVStoreUserPrincipal(userName, userId));

        final Set<Object> publicCreds = new HashSet<Object>();
        final Set<Object> privateCreds = new HashSet<Object>();
        return new Subject(true /* readOnly */,
                           userPrincipals, publicCreds, privateCreds);
    }

    @Override
    public SecurityElementType getElementType() {
        return SecurityElementType.KVSTOREUSER;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        final int result =
            17 * prime + (userName == null ? 0 : userName.hashCode());
        return result;
    }

    /**
     * Two KVStoreUsers are identical iff. they have the same names and ids.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof KVStoreUser)) {
            return false;
        }
        if (!super.equals(obj)) {
            return false;
        }
        final KVStoreUser other = (KVStoreUser) obj;
        if (userName == null) {
            return (other.userName == null);
        }
        return userName.equals(other.userName);
    }

    @Override
    public String toString() {
        return String.format("id=%s name=%s", super.getElementId(), userName);
    }

    @Override
    public KVStoreUser clone() {
        return new KVStoreUser(this);
    }


    /**
     * A convenient class to store the description of a kvstore user for
     * showing. With this class we do not need to pass the full KVStoreUser
     * copy to client for showing, avoiding the security risk.
     */
    public static class UserDescription implements Serializable {

        private static final long serialVersionUID = 1L;

        private final String brief;
        private final String details;

        public UserDescription(String brief, String details) {
            this.brief = brief;
            this.details = details;
        }

        /**
         * Gets the brief description.
         *
         * @return briefs
         */
        public String brief() {
            return brief;
        }

        /**
         * Gets the detailed description.
         *
         * @return details
         */
        public String details() {
            return details;
        }
    }

}
