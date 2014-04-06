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

import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Consistency.Time;
import oracle.kv.Consistency.Version;

import com.sleepycat.je.CommitToken;

/**
 * Translates between HA and KV consistency.
 */
public final class ConsistencyTranslator {

    /**
     * Private constructor to satisfy CheckStyle and to prevent
     * instantiation of this utility class.
     */
    private ConsistencyTranslator() {
        throw new AssertionError("Instantiated utility class " +
            ConsistencyTranslator.class);
    }

    /**
     * Maps a KV consistency to a HA consistency. The inverse of the
     * method below.
     */
    public static com.sleepycat.je.ReplicaConsistencyPolicy
        translate(Consistency consistency) {

        if (Consistency.ABSOLUTE.equals(consistency)) {

            /*
             * Requests with absolute consistency are simply directed at the
             * master, they do not have an HA equivalent.
             */
            throw new IllegalStateException("There is no translation for " +
                                            Consistency.ABSOLUTE.getName());
        } else if (Consistency.NONE_REQUIRED.equals(consistency) ||
            Consistency.NONE_REQUIRED_NO_MASTER.equals(consistency)) {

            return com.sleepycat.je.rep.NoConsistencyRequiredPolicy.
                NO_CONSISTENCY;
        } else if (consistency instanceof Time) {

            final Time c = (Time) consistency;
            return new com.sleepycat.je.rep.TimeConsistencyPolicy
                (c.getPermissibleLag(TimeUnit.MICROSECONDS),
                 TimeUnit.MICROSECONDS,
                 c.getTimeout(TimeUnit.MICROSECONDS),
                 TimeUnit.MICROSECONDS);
        } else if (consistency instanceof Consistency.Version) {
            final Consistency.Version c = (Version) consistency;
            final CommitToken commitToken =
                new CommitToken(c.getVersion().getRepGroupUUID(),
                                c.getVersion().getVersion());
            return new com.sleepycat.je.rep.CommitPointConsistencyPolicy
                       (commitToken,
                        c.getTimeout(TimeUnit.MICROSECONDS),
                        TimeUnit.MICROSECONDS);
        } else {
            throw new UnsupportedOperationException("unknown consistency: " +
                                                    consistency);
        }
    }

    /**
     * Maps a HA consistency to a KV consistency. The inverse of the
     * above method.
     */
    public static Consistency
        translate(com.sleepycat.je.ReplicaConsistencyPolicy consistency) {
        return translate(consistency, null);
    }

    /**
     * Returns the <code>oracle.kv.Consistency</code> that corresponds
     * to the given
     * <code>com.sleepycat.je.ReplicaConsistencyPolicy</code>. Note
     * that for some values of <code>ReplicaConsistencyPolicy</code>,
     * there is more than one corresponding KV
     * <code>Consistency</code>. For those cases, the caller must
     * supply the KV <code>Consistency</code> to return; where if
     * <code>null</code> is supplied, a default mapping is used to
     * determine the value to return.
     *
     * @param consistency the HA consistency from which the KV
     * consistency should be determined.
     *
     * @param kvConsistency KV <code>Consistency</code> value that
     * should be supplied when the HA consistency that is input has
     * more than one corresponding KV consistency value. If the HA
     * consistency value that is input corresponds to only one KV
     * consistency, then this parameter will be ignored. Alternatively,
     * if the HA consistency value that is input corresponds to more
     * than one KV consistency, then the following criteria is used to
     * determine the return value:
     *
     * <ul>
     * <li>If <code>null</code> is input for this parameter, a default
     * mapping is used to determine the return value.</li>
     * <li>If the value input for this parameter does not map to the given HA
     * consistency, then an <code>UnsupportedOperationException</code>
     * is thrown.</li>
     * <li>In all other cases, the value input for this parameter is
     * returned.</li>
     * </ul>
     *
     * @return the instance of <code>oracle.kv.Consistency</code> that
     * corresponds to the value of the <code>consistency</code>
     * parameter; or the value input for the
     * <code>kvConsistency</code> parameter if the criteria described
     * above is satisfied.
     */
    public static Consistency
        translate(com.sleepycat.je.ReplicaConsistencyPolicy consistency,
                  Consistency kvConsistency) {

        if (com.sleepycat.je.rep.NoConsistencyRequiredPolicy.NO_CONSISTENCY.
                equals(consistency)) {
            if (kvConsistency == null) {
                return Consistency.NONE_REQUIRED;
            } else if (!(Consistency.NONE_REQUIRED).equals(kvConsistency) &&
                !(Consistency.NONE_REQUIRED_NO_MASTER).equals(kvConsistency)) {
                    throw new UnsupportedOperationException
                        ("invalid consistency [KV consistency=" +
                         kvConsistency + ", HA consistency=" +
                         consistency + "]");
            }
            return kvConsistency;
        } else if (consistency instanceof
                com.sleepycat.je.rep.TimeConsistencyPolicy) {
            final com.sleepycat.je.rep.TimeConsistencyPolicy c =
                (com.sleepycat.je.rep.TimeConsistencyPolicy) consistency;
            return new Consistency.Time
                (c.getPermissibleLag(TimeUnit.MICROSECONDS),
                 TimeUnit.MICROSECONDS,
                 c.getTimeout(TimeUnit.MICROSECONDS),
                 TimeUnit.MICROSECONDS);
        } else if (consistency instanceof
                   com.sleepycat.je.rep.CommitPointConsistencyPolicy) {
            final com.sleepycat.je.rep.CommitPointConsistencyPolicy c =
                (com.sleepycat.je.rep.CommitPointConsistencyPolicy)
                    consistency;
            final CommitToken commitToken = c.getCommitToken();
            final oracle.kv.Version version =
                new oracle.kv.Version(commitToken.getRepenvUUID(),
                                             commitToken.getVLSN());
            return new Version
                       (version,
                        c.getTimeout(TimeUnit.MICROSECONDS),
                        TimeUnit.MICROSECONDS);
        } else {
            throw new UnsupportedOperationException
                ("unknown consistency: " + consistency);
        }
    }
}
