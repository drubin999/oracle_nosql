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
 * This exception is thrown from methods where an authenticated user is 
 * attempting to perform an operation for which they are not authorized.  An
 * application that receives this exception typically should not retry the
 * operation.
 * <p>
 * Note that in this release, this exception should not occur through legitimate
 * use of the KVStore API.  Future release may introduce additional
 * authorization capabilities that may cause this exception to be encountered.
 *
 * @since 3.0
 */
public class UnauthorizedException extends KVSecurityException {

    private static final long serialVersionUID = 1L;

    /**
     * For internal use only.
     * @hidden
     */
    public UnauthorizedException(String msg) {
        super(msg);
    }

    /**
     * For internal use only.
     * @hidden
     */
    public UnauthorizedException(Throwable cause) {
        super(cause);
    }
}
