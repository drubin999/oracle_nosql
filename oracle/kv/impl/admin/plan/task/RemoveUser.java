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

import java.util.Collection;

import com.sleepycat.persist.model.Persistent;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.plan.SecurityMetadataPlan;
import oracle.kv.impl.security.metadata.KVStoreUser;
import oracle.kv.impl.security.metadata.SecurityMetadata;

/**
 * Remove a user
 */
@Persistent
public class RemoveUser extends UpdateMetadata<SecurityMetadata> {

    private static final long serialVersionUID = 1L;

    private String userName;

    public RemoveUser(SecurityMetadataPlan plan,
                      String userName) {
        super(plan);

        final SecurityMetadata secMd = plan.getMetadata();
        ensureNotDropLastAdmin(userName, secMd);

        this.userName = userName;
    }

    @SuppressWarnings("unused")
    private RemoveUser() {
    }

    /**
     * Ensures not to remove the last enabled Admin.
     */
    private static void ensureNotDropLastAdmin(final String userToDrop,
                                               final SecurityMetadata secMd) {
        if (secMd == null) {
            return;
        }

        final KVStoreUser existUser = secMd.getUser(userToDrop);
        if (existUser != null && existUser.isAdmin()) {
            final Collection<KVStoreUser> users = secMd.getAllUsers();

            for (final KVStoreUser user : users) {
                if (!user.getName().equals(userToDrop) &&
                    user.isAdmin() &&
                    user.isEnabled()) {
                    return;
                }
            }
            throw new IllegalCommandException(
                "Cannot drop the only enabled Admin of: " + userToDrop);
        }
    }

    @Override
    protected SecurityMetadata updateMetadata() {
        final SecurityMetadata secMd = plan.getMetadata();

        /* Guards against dropping last admin again */
        ensureNotDropLastAdmin(userName, secMd);

        if (secMd != null && secMd.getUser(userName) != null) {
            /* The user exists, so remove the entry from the MD */
            secMd.removeUser(secMd.getUser(userName).getElementId());
            plan.getAdmin().saveMetadata(secMd, plan);
        }

        return secMd;
    }
}
