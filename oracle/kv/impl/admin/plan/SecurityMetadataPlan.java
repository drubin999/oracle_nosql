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

package oracle.kv.impl.admin.plan;

import java.security.SecureRandom;
import java.util.concurrent.atomic.AtomicInteger;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.PlannerAdmin;
import oracle.kv.impl.admin.plan.task.AddUser;
import oracle.kv.impl.admin.plan.task.ChangeUser;
import oracle.kv.impl.admin.plan.task.RemoveUser;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.security.KVStoreUserPrincipal;
import oracle.kv.impl.security.PasswordHash;
import oracle.kv.impl.security.metadata.PasswordHashDigest;
import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.util.VersionUtil;

import com.sleepycat.persist.model.Persistent;

/**
 * Plan class representing all security metadata operations
 */
@Persistent
public class SecurityMetadataPlan extends MetadataPlan<SecurityMetadata> {

    private static final long serialVersionUID = 1L;

    private static final SecureRandom random = new SecureRandom();

    private static final KVVersion SECURITY_VERSION =
            KVVersion.R3_0; /* R3.0 Q1/2014 */

    public SecurityMetadataPlan(AtomicInteger idGen,
                                String planName,
                                Planner planner) {
        super(idGen, planName, planner);

        /* Ensure all nodes in the store support security feature */
        final PlannerAdmin admin = planner.getAdmin();
        final KVVersion storeVersion = admin.getStoreVersion();

        if (VersionUtil.compareMinorVersion(
                storeVersion, SECURITY_VERSION) < 0) {
            throw new IllegalCommandException(
                "Cannot perform security metadata related operations when" +
                " not all nodes in the store support security feature." +
                " The highest version supported by all nodes is " +
                storeVersion.getNumericVersionString() +
                ", but security metadata operations require version "
                + SECURITY_VERSION.getNumericVersionString() + " or later.");
        }
    }

    /* No-arg ctor for DPL */
    @SuppressWarnings("unused")
    private SecurityMetadataPlan() {
    }

    /*
     * Ensure operator does not drop itself
     */
    private static void ensureNotSelfDrop(final String droppedUserName) {
        final KVStoreUserPrincipal currentUserPrincipal =
                KVStoreUserPrincipal.getCurrentUser();
        if (currentUserPrincipal == null) {
            throw new IllegalCommandException(
                    "Could not identify current user");
        }
        if (droppedUserName.equals(currentUserPrincipal.getName())) {
            throw new IllegalCommandException(
                "A current online user cannot drop itself.");
        }
    }

    @Override
    protected MetadataType getMetadataType() {
        return MetadataType.SECURITY;
    }

    @Override
    protected Class<SecurityMetadata> getMetadataClass() {
        return SecurityMetadata.class;
    }

    @Override
    public boolean isExclusive() {
        return false;
    }

    @Override
    void preExecutionSave() {
        /* Nothing to do since the security metadata has been saved */
    }

    @Override
    public String getDefaultName() {
        return "Change SecurityMetadata";
    }

    @Override
    public void getCatalogLocks() {
        /*
         * Use the elasticity lock to coordinate the concurrent execution of
         * multiple SecurityMetadataPlans since they may read/update the
         * security metadata simultaneously. Also, the update of security
         * metadata will miss for some RepNodes if happens during topology
         * elasticity operation. Synchronize on the elasticity lock can help
         * prevent this.
         *
         * TODO: need to implement a lock only for security metadata plan?
         */
        planner.lockElasticity(getId(), getName());
        getPerTaskLocks();
    }

    /**
     * Get a PasswordHashDigest instance with default hash algorithm, hash
     * bytes, and iterations
     *
     * @param plainPassword the plain password
     * @return a PasswordHashDigest containing the hashed password and hashing
     * information
     */
    public PasswordHashDigest
        makeDefaultHashDigest(final char[] plainPassword) {

        /* TODO: fetch the parameter from global store configuration */
        final byte[] saltValue =
                PasswordHash.generateSalt(random, PasswordHash.SUGG_SALT_BYTES);
        return PasswordHashDigest.getHashDigest(PasswordHash.SUGG_ALGO,
                                                PasswordHash.SUGG_HASH_ITERS,
                                                PasswordHash.SUGG_SALT_BYTES,
                                                saltValue, plainPassword);
    }

    public static SecurityMetadataPlan
        createCreateUserPlan(AtomicInteger idGen,
                             String planName,
                             Planner planner,
                             String userName,
                             boolean isEnabled,
                             boolean isAdmin,
                             char[] plainPassword) {
        final String subPlanName =
                (planName != null) ? planName : "Create User";
        final SecurityMetadataPlan plan =
                new SecurityMetadataPlan(idGen, subPlanName, planner);

        plan.addTask(new AddUser(plan, userName, isEnabled, isAdmin,
                                 plainPassword));
        return plan;
    }

    public static SecurityMetadataPlan
        createChangeUserPlan(AtomicInteger idGen,
                             String planName,
                             Planner planner,
                             String userName,
                             Boolean isEnabled,
                             char[] plainPassword,
                             boolean retainPassword,
                             boolean clearRetainedPassword) {
        final String subPlanName =
                (planName != null) ? planName : "Change User";
        final SecurityMetadataPlan plan =
                new SecurityMetadataPlan(idGen, subPlanName, planner);

        plan.addTask(new ChangeUser(plan, userName, isEnabled, plainPassword,
                                    retainPassword, clearRetainedPassword));
        return plan;
    }

    public static SecurityMetadataPlan createDropUserPlan(AtomicInteger idGen,
                                                          String planName,
                                                          Planner planner,
                                                          String userName) {
        ensureNotSelfDrop(userName);

        final String subPlanName =
                (planName != null) ? planName : "Drop User";
        final SecurityMetadataPlan plan =
                new SecurityMetadataPlan(idGen, subPlanName, planner);

        plan.addTask(new RemoveUser(plan, userName));
        return plan;
    }

    @Override
    void stripForDisplay() {
    }
}
