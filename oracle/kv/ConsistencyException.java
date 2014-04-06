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

/**
 * Thrown when a single or multiple-operation transaction fails because the
 * specified {@link Consistency} could not be met, within the allowed timeout
 * period.
 *
 * <p>The likelihood of this exception being thrown depends on the specified
 * {@code Consistency} and the general health of the KVStore system.  The
 * default consistency policy (specified by {@link
 * KVStoreConfig#getConsistency}) is {@link Consistency#NONE_REQUIRED}.  With
 * {@link Consistency#NONE_REQUIRED} (the default), {@link
 * Consistency#NONE_REQUIRED_NO_MASTER}, or {@link Consistency#ABSOLUTE}, this
 * exception will never be thrown.</p>
 *
 * <p>If the client overrides the default and specifies a {@link
 * Consistency.Time} or {@link Consistency.Version} setting, then this
 * exception will be thrown when the specified consistency requirement cannot
 * be satisfied within the timeout period associated with the consistency
 * setting.  If this exception is encountered frequently, it indicates that the
 * consistency policy requirements are too strict and cannot be met routinely
 * given the load being placed on the system and the hardware resources that
 * are available to service the load.</p>
 *
 * <p>Depending on the nature of the application, when this exception is thrown
 * the client may wish to
 * <ul>
 * <li>retry the read operation,</li>
 * <li>fall back to using a larger timeout or a less restrictive consistency
 * setting (for example, {@link Consistency#NONE_REQUIRED}), and resume using
 * the original consistency setting at a later time, or</li>
 * <li>give up and report an error at a higher level.</li>
 * </ul>
 * </p>
 */
public class ConsistencyException extends FaultException {

    private static final long serialVersionUID = 1L;

    private final Consistency consistency;

    /**
     * For internal use only.
     * @hidden
     */
    public ConsistencyException(Throwable cause, Consistency consistency) {
        super(cause, true /*isRemote*/);
        this.consistency = consistency;
    }

    /**
     * Returns the consistency policy that could not be satisfied.
     */
    public Consistency getConsistency() {
        return consistency;
    }
}
