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

package oracle.kv.impl.api.parallelscan;

/**
 * @hidden
 * A hook interface used during ParallelScan processing. This is really for
 * debugging and unit tests.
 */
public interface ParallelScanHook {

    /* any _SHARD enums are unused since we don't do shard iteration. */
    public enum HookType {
        BEFORE_PROCESSING_SHARD,
            AFTER_PROCESSING_SHARD,
            BEFORE_PROCESSING_PARTITION,
            AFTER_PROCESSING_PARTITION,
            QUEUE_STALL_PUT,
            QUEUE_STALL_GET,
            BEFORE_EXECUTE_REQUEST
    };

    /*
     * This will generally be called within an assert so the return value
     * should be true unless the callback wants an AssertionError thrown.
     */
    public boolean callback(Thread thread, HookType hookType, String info);
}
