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

package oracle.kv.impl.fault;

import oracle.kv.KVVersion;

/**
 * UnknownVersionException is thrown when a Topology with an unknown future
 * version is encountered during the upgrade of an object. The caller can take
 * appropriate action based upon the version information that's returned.
 */
public class UnknownVersionException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final KVVersion kvVersion;
    private final int supportedVersion;
    private final int unknownVersion;
    private final String className;

    public UnknownVersionException(String message,
                            String className,
                            int supportedVersion,
                            int unknownVersion) {
        super(message);

        this.kvVersion = KVVersion.CURRENT_VERSION;
        this.supportedVersion = supportedVersion;
        this.unknownVersion = unknownVersion;
        this.className = className;
    }

    @Override
    public String getMessage() {
        return super.getMessage() +
               " Class: " + className +
               " KVVersion: " + kvVersion.toString() +
               " Supported version:" + supportedVersion +
               " Unknown version:" + unknownVersion;
    }

    public KVVersion getKVVersion() {
        return kvVersion;
    }

    public int getUnknownVersion() {
        return unknownVersion;
    }

    public int getSupportedVersion() {
        return supportedVersion;
    }

    public String getClassName() {
        return className;
    }
}