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
 * Describes how requests may be limited so that one or more nodes with long
 * service times don't end up consuming all available threads in the KVS
 * client.
 *
 * @see KVStoreConfig#setRequestLimit
 * @see RequestLimitException
 */
public class RequestLimitConfig {

    /**
     * The default values for limiting requests to a particular node.
     */
    public static final int DEFAULT_MAX_ACTIVE_REQUESTS = 100;
    public static final int DEFAULT_REQUEST_THRESHOLD_PERCENT = 90;
    public static final int DEFAULT_NODE_LIMIT_PERCENT = 80;

    /**
     *  The maximum number of active requests permitted by the KV client
     */
    private final int maxActiveRequests;

    /**
     *  The threshold at which request limiting is activated.
     */
    private final int requestThresholdPercent;

    /**
     * The above threshold expressed as a number of requests.
     */
    private final int requestThreshold;

    /**
     * The limit that must not be exceeded once the above threshold has been
     * crossed.
     */
    private final int nodeLimitPercent;

    /**
     * The above limit expressed as a number of requests.
     */
    private final int nodeLimit;

    private static final RequestLimitConfig defaultRequestLimitConfig =
        new RequestLimitConfig(DEFAULT_MAX_ACTIVE_REQUESTS,
                               DEFAULT_REQUEST_THRESHOLD_PERCENT,
                               DEFAULT_NODE_LIMIT_PERCENT);

    /**
     * Creates a request limiting configuration.
     *
     * The request limiting mechanism is only activated when the number of
     * active requests exceeds the threshold specified by the parameter
     * <code>requestThresholdPercent</code>. Both the threshold and limit
     * parameters below are expressed as a percentage of <code>
     * maxActiveRequests</code>.
     * <p>
     * When the mechanism is active the number of active requests to a node is
     * not allowed to exceed <code>nodeRequestLimitPercent</code>. Any new
     * requests that would exceed this limit are rejected and a <code>
     * RequestLimitException</code> is thrown.
     * <p>
     * For example, consider a configuration with maxActiveRequests=10,
     * requestThresholdPercent=80 and nodeLimitPercent=50. If 8 requests are
     * already active at the client, and a 9th request is received that would
     * be directed at a node which already has 5 active requests, it would
     * result in a <code>RequestLimitException</code> being thrown. If only
     * 7 requests were active at the client, the 8th request would be directed
     * at the node with 5 active requests and the request would be processed
     * normally.
     *
     * @param maxActiveRequests the maximum number of active requests permitted
     * by the KV client. This number is typically derived from the maximum
     * number of threads that the client has set aside for processing requests.
     * The default is 100. Note that the KVStore does not actually enforce this
     * maximum directly. It only uses this parameter as the basis for
     * calculating the requests limits to be enforced at a node.
     *
     * @param requestThresholdPercent the threshold computed as a percentage of
     * <code>maxActiveRequests</code> at which requests are limited. The
     * default is 90.
     *
     * @param nodeLimitPercent determines the maximum number of active requests
     * that can be associated with a node when the request limiting mechanism
     * is active. The default is 80.
     */
    public RequestLimitConfig(int maxActiveRequests,
                              int requestThresholdPercent,
                              int nodeLimitPercent) {

        if (maxActiveRequests <= 0) {
            throw new IllegalArgumentException("maxActiveRequests:" +
                                               maxActiveRequests +
                                               " must be positive");
        }
        this.maxActiveRequests = maxActiveRequests;

        if (requestThresholdPercent > 100) {
            throw new IllegalArgumentException("requestThresholdPercent: " +
                                               requestThresholdPercent +
                                               "cannot exceed 100");
        }
        this.requestThresholdPercent = requestThresholdPercent;
        requestThreshold =
            (maxActiveRequests * requestThresholdPercent) / 100;

        if (nodeLimitPercent > requestThresholdPercent) {
            String msg = "nodeLimitPercent: " + nodeLimitPercent +
            "cannot exceed requestThresholdPercent: " +
            requestThresholdPercent;
            throw new IllegalArgumentException(msg);
        }
        this.nodeLimitPercent = nodeLimitPercent;
        nodeLimit = (maxActiveRequests * nodeLimitPercent) / 100;
    }

    /**
     * Returns the maximum number of active requests permitted by the KVS
     * client.
     * <p>
     * The default value is 100.
     */
    public int getMaxActiveRequests() {
        return maxActiveRequests;
    }

    /**
     * Returns the percentage used to compute the active request threshold
     * above which the request limiting mechanism is activated.
     * <p>
     * The default value is 90.
     */
    public int getRequestThresholdPercent() {
        return requestThresholdPercent;
    }

    /**
     * Returns the threshold number of requests above which the request
     * limiting mechanism is activated.
     */
    public int getRequestThreshold() {
        return requestThreshold;
    }

    /**
     * Returns the percentage used to compute the maximum number of requests
     * that may be active at a node.
     * <p>
     * The default value is 80.
     */
    public int getNodeLimitPercent() {
        return nodeLimitPercent;
    }

    /**
     * Returns the maximum number of requests that may be active at a node.
     */
    public int getNodeLimit() {
        return nodeLimit;
    }

    @Override
    public String toString() {
        return String.format("maxActiveRequests=%d," +
                             " requestThresholdPercent=%d%%," +
                             " nodeLimitPercent=%d%%",
                             maxActiveRequests,
                             requestThresholdPercent,
                             nodeLimitPercent);
    }

    public static RequestLimitConfig getDefault() {
        return defaultRequestLimitConfig;
    }
}
