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

import oracle.kv.impl.topo.RepNodeId;

/**
 * Thrown when a request cannot be processed because it would exceed the
 * maximum number of active requests for a node as configured via
 * {@link KVStoreConfig#setRequestLimit}.
 *
 * This exception may simply indicate that the request limits are too strict
 * and need to be relaxed to more correctly reflect application behavior. It
 * may also indicate that there is a network issue with the communications path
 * to the node or that the node itself is having problems and needs attention.
 * In most circumstances, the KVS request dispatcher itself will handle node
 * failures automatically, so this exception should be pretty rare.
 * <p>
 * When encountering this exception it's best for the application to abandon
 * the request, and free up the thread, thus containing the failure and
 * allowing for more graceful service degradation. Freeing up the thread makes
 * it available for requests that can be processed by other healthy nodes.
 *
 * @see RequestLimitConfig
 */
public class RequestLimitException extends FaultException {

    private static final long serialVersionUID = 1L;

    /**
     * For internal use only.
     * @hidden
     */
    private RequestLimitException(String msg, boolean isRemote) {
        super(msg, isRemote);
    }

    /**
     * For internal use only.
     * @hidden
     */
    public static RequestLimitException create(RequestLimitConfig config,
                                               RepNodeId rnId,
                                               int activeRequests,
                                               int nodeRequests,
                                               boolean isRemote) {
        assert config != null;

        String msg = "Node limit exceeded at:" + rnId +
            " Active requests at node:" + nodeRequests +
            " Total active requests:" + activeRequests +
            " Request limit configuration:" + config.toString();

        return new RequestLimitException(msg, isRemote);
    }
}
