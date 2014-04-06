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

package oracle.kv.impl.admin.plan.task;

import java.util.Arrays;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.plan.SecurityMetadataPlan;
import oracle.kv.impl.security.metadata.KVStoreUser;
import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.security.util.SecurityUtils;

import com.sleepycat.persist.model.Persistent;

/**
 * Add a user
 */
@Persistent
public class AddUser extends UpdateMetadata<SecurityMetadata> {

    private static final long serialVersionUID = 1L;

    private String userName;
    private boolean isEnabled;
    private boolean isAdmin;
    private char[] plainPassword;

    public AddUser(SecurityMetadataPlan plan,
                   String userName,
                   boolean isEnabled,
                   boolean isAdmin,
                   char[] plainPassword) {
        super(plan);

        final SecurityMetadata secMd = plan.getMetadata();

        /* Ensure the first created user to be Admin */
        if ((secMd == null) || secMd.getAllUsers().isEmpty()) {
            if (!(isAdmin && isEnabled)) {
                throw new IllegalCommandException(
                    "The first user in the store must be -admin and enabled.");
            }
        }

        this.userName = userName;
        this.isAdmin = isAdmin;
        this.isEnabled = isEnabled;
        this.plainPassword = Arrays.copyOf(plainPassword, plainPassword.length);

        /* Prevent overwriting an existing user with different attributes */
        if (secMd != null) {
            final KVStoreUser preExistUser = secMd.getUser(userName);
            if (preExistUser != null) {
                checkPreExistingUser(preExistUser);
            }
        }
    }

    @SuppressWarnings("unused")
    private AddUser() {
    }

    /**
     * Checks whether a pre-existing user has same enabled, admin and password
     * attributes with the requested new one.
     *
     * @param user the pre-exist user
     * @throws IllegalCommandException  If any of the enabled state, the admin
     * role and the password between the pre-existing user and the new
     * requested user is different.
     */
    private void checkPreExistingUser(final KVStoreUser user) {
        if (user.isEnabled() != isEnabled) {
            throw new IllegalCommandException(
                "User with name " + userName +
                " already exists but has enabled state of " +
                user.isEnabled() +
                " rather than the requested enabled state of " + isEnabled);
        }
        if (user.isAdmin() != isAdmin) {
            throw new IllegalCommandException(
                "User with name " + userName +
                " already exists but has admin setting of " + user.isAdmin() +
                " rather than the requested admin setting of " + isAdmin);
        }
        if (!user.verifyPassword(plainPassword)) {
            throw new IllegalCommandException(
                "User with name " + userName +
                " already exists but has different password with the" +
                " requested one.");
        }
    }

    @Override
    protected SecurityMetadata updateMetadata() {
        SecurityMetadata md = plan.getMetadata();
        if (md == null) {
            final String storeName =
                    plan.getAdmin().getParams().getGlobalParams().
                    getKVStoreName();
            md = new SecurityMetadata(storeName);
        }

        if (md.getUser(userName) == null) {
            /* The user does not yet exist, so add the entry to the MD */
            final KVStoreUser newUser = new KVStoreUser(userName);
            newUser.setEnabled(isEnabled).setAdmin(isAdmin).
                setPassword(((SecurityMetadataPlan) plan).
                            makeDefaultHashDigest(plainPassword));
            md.addUser(newUser);
            plan.getAdmin().saveMetadata(md, plan);
        }

        /*
         * Wipe out the plain password setting to ensure it does not hang
         * around in in the Java VM memory space.
         */
        SecurityUtils.clearPassword(plainPassword);

        return md;
    }
}
