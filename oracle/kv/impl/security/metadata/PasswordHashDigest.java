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
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import oracle.kv.impl.security.PasswordHash;

/**
 * Class for storing the password hashing information.
 */
public final class PasswordHashDigest implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    /* Salt value for hashing */
    private final byte[] saltValue;

    /* Hash algorithm for hashing the password */
    private final String hashAlgorithm;

    /* Iterations in hashing the password */
    private final int hashIterations;

    /* Hashed password for the user */
    private final byte[] hashedPassword;

    /* Length of bytes of salt value and password hash */
    private final int hashBytes;

    /* Creation time stamp for checking if this password is expired */
    private long createdTimeInMillis = System.currentTimeMillis();

    /* Life time for the password. It's unlimited if set to 0. */
    private long lifeTime = 0L;

    private PasswordHashDigest(final String hashAlgo,
                               final byte[] saltValue,
                               final int hashIters,
                               final byte[] hashedPasswd) {
        this.hashAlgorithm = hashAlgo;
        this.saltValue = saltValue;
        this.hashIterations = hashIters;
        this.hashBytes = hashedPasswd.length;
        this.hashedPassword = hashedPasswd;
    }

    /**
     * Verify whether the plain password has the same hashed value with the
     * stored hashed password when encrypted with stored parameters.
     *
     * @param plainPassword
     * @return true if matches
     */
    public boolean verifyPassword(final char[] plainPassword) {
        final byte[] newHashedPassword =
                getHashDigest(hashAlgorithm, hashIterations, hashBytes,
                              saltValue, plainPassword).hashedPassword;
        return Arrays.equals(this.hashedPassword, newHashedPassword);
    }

    /**
     * Get a PasswordHashDigest instance with full parameters.
     *
     * @param hashAlgorithm hash algorithm
     * @param hashIterations hash iterations
     * @param hashBytes length of bytes of salt value and hashed password
     * @param saltValue the salt value
     * @param plainPassword plain password
     * @return a PasswordHashDigest containing the hashed password and hashing
     * information
     */
    public static PasswordHashDigest getHashDigest(final String hashAlgorithm,
                                                   final int hashIterations,
                                                   final int hashBytes,
                                                   final byte[] saltValue,
                                                   final char[] plainPassword) {
        if ((hashAlgorithm == null) || hashAlgorithm.isEmpty()) {
            throw new IllegalArgumentException(
                "Hash algorithm must not be null or empty.");
        }
        if (hashIterations <= 0) {
            throw new IllegalArgumentException(
                "Hash iterations must be a non-negative integer: " +
                hashIterations);
        }
        if (hashBytes <= 0) {
            throw new IllegalArgumentException(
                "Hash bytes must be a non-negative integer: " + hashBytes);
        }
        if (saltValue == null || saltValue.length == 0) {
            throw new IllegalArgumentException(
                    "Salt value must not be null or empty.");
        }
        if (plainPassword == null || plainPassword.length == 0) {
            throw new IllegalArgumentException(
                    "Plain password must not be null or empty.");
        }

        Exception hashException = null;
        try {
            final byte[] hashedPasswd =
                    PasswordHash.pbeHash(plainPassword, hashAlgorithm,
                                         saltValue, hashIterations, hashBytes);

            return new PasswordHashDigest(hashAlgorithm, saltValue,
                                          hashIterations, hashedPasswd);

        } catch (NoSuchAlgorithmException e) {
            hashException = e;
        } catch (InvalidKeySpecException e) {
            hashException = e;
        }
        throw new IllegalStateException(
            "Failed to get hashed password.", hashException);
    }

    public boolean isExpired() {
        return lifeTime == 0 ? false : System.currentTimeMillis() >=
                                           (createdTimeInMillis + lifeTime);
    }

    public long getLifeTime() {
        return lifeTime;
    }

    public void setLifeTime(final long newLifeTime, TimeUnit timeUnit) {
        if (newLifeTime < 0) {
            throw new IllegalArgumentException(
                "Life time value must be large than zero: " + newLifeTime);
        }
        this.lifeTime = timeUnit.toMillis(newLifeTime);
    }

    /*
     * Refreshes the created time to current.
     */
    void refreshCreateTime() {
        createdTimeInMillis = System.currentTimeMillis();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 17 * prime + hashIterations;
        result = result * prime + hashAlgorithm.hashCode();
        result = result * prime + Arrays.hashCode(saltValue);
        result = result * prime + Arrays.hashCode(hashedPassword);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof PasswordHashDigest)) {
            return false;
        }
        final PasswordHashDigest other = (PasswordHashDigest) obj;
        return Arrays.equals(hashedPassword, other.hashedPassword) &&
               Arrays.equals(saltValue, other.saltValue) &&
               hashAlgorithm.equals(other.hashAlgorithm) &&
               hashIterations == other.hashIterations &&
               hashBytes == other.hashBytes;
    }

    @Override
    public PasswordHashDigest clone() {
        final PasswordHashDigest result =
                new PasswordHashDigest(hashAlgorithm, saltValue.clone(),
                                       hashIterations, hashedPassword.clone());
        result.createdTimeInMillis = createdTimeInMillis;
        result.lifeTime = lifeTime;
        return result;
    }
}
