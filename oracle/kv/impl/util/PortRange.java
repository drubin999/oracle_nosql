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

import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Port ranges are expressed as a String property. Currently the format is
 * as follows:
 *
 *    firstPortNum,lastPortNum
 *
 * where the available ports are all those from first->last, inclusive. Over
 * time, we may want to  make this more sophisticated, so that one can express
 * something other than a consecutive range.
 *
 * The ports required by the various services are these:
 *
 * Admin:
 * 1) The command service port
 * 2) The port associated with the tracker listeners. All three listeners
 * share the same port.
 * 3) The login service if security is enabled
 *
 * Note that the above ports could have been combined but have deliberately
 * been left separate since the tracker listeners could see high traffic
 * in larger stores and could have different socket configuration
 * requirements.
 *
 * RepNode:
 * 1) The request handler service
 * 2) The rep node admin service
 * 3) The monitor service
 * 4) The login service if security is enabled
 *
 * StorageNode:
 * 1) The admin service
 * 2) The monitor service
 * 3) The login service if security is enabled
 * 4) A temporary port that appears to not go away on registration due to
 * an RMI bug.  This padding does no harm.
 *
 * This makes the calculation for the minimum number of ports in the range
 * PORTS_PER_SN + (PORTS_PER_RN * capacity) + [if admin, PORTS_PER_ADMIN].
 * When security is enabled, the per-component port requirements are increased
 * by the corresponding SECURE_PORTS_PER_XXX value.
 */

public class PortRange {

    /**
     * Denotes an unconstrained use of ports
     */
    public static final String UNCONSTRAINED = "0";
    public static final int PORTS_PER_SN = 3;
    public static final int PORTS_PER_RN = 3;
    public static final int PORTS_PER_ADMIN = 2;
    public static final int SECURE_PORTS_PER_SN = 1;
    public static final int SECURE_PORTS_PER_RN = 1;
    public static final int SECURE_PORTS_PER_ADMIN = 1;

    /**
     * Ensure that the HA portRange string is correct.
     */
    public static void validateHA(String portRange)
        throws IllegalArgumentException {

        String problem = portRange +
            " is not valid; format should be [firstPort,secondPort]";
        StringTokenizer tokenizer = new StringTokenizer(portRange, ",");
        if (tokenizer.countTokens() != 2) {
            throw new IllegalArgumentException(problem);
        }

        try {
            int firstPort = Integer.parseInt(tokenizer.nextToken());
            int secondPort = Integer.parseInt(tokenizer.nextToken());

            if ((firstPort < 1) || (secondPort < 1)) {
                throw new IllegalArgumentException(problem +
                    ", ports must be > 0");
            }

            if (firstPort > secondPort) {
                throw new IllegalArgumentException(problem +
                ", firstPort must be <= secondPort");
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(problem +
                                               ", not a valid number " +
                                               e.getMessage());
        }
    }

    /**
     * Validate the free range associated with
     * @param portRange
     * @throws IllegalArgumentException
     */
    public static void validateService(String portRange)
        throws IllegalArgumentException {

        if (isUnconstrained(portRange)) {
            return;
        }

        validateHA(portRange);
    }

    /**
     * Predicate to determine whether the port range is constrained
     */
    public static boolean isUnconstrained(String portRange) {
        return UNCONSTRAINED.equals(portRange);
    }

    /**
     * Ensures that <code>portRange</code> does not overlap
     * <code>otherRange</code>
     */
    public static void validateDisjoint(String portRange,
                                        String otherRange) {

        if (isUnconstrained(portRange) || isUnconstrained(otherRange)) {
            return;
        }

        final List<Integer> r1 = getRange(portRange);
        final List<Integer> r2 = getRange(otherRange);

        if ((r1.get(0) < r2.get(0)) && (r1.get(1) < r2.get(0))) {
            return;
        }

        if ((r2.get(0) < r1.get(0)) && (r2.get(1) < r1.get(0))) {
            return;
        }

        throw new IllegalArgumentException("Overlapping port ranges: " +
                                           portRange + " " + otherRange);
    }

    /**
     * Return the number of port numbers in the specified range, inclusive of
     * the range endpoints.
     *
     * @param portRange a comma-separated pair of point numbers or the
     * canonical {@link PortRange#UNCONSTRAINED} value.
     *
     * @return the size of the range
     *
     * @throws IllegalArgumentException if the port range string is not either
     * the canonical {@link PortRange#UNCONSTRAINED} value or a comma-separated
     * pair of tokens.
     * @throws NumberFormatException if either tokens in the range are not in
     * valid integer format
     */
    public static int rangeSize(String portRange) {
        if (isUnconstrained(portRange)) {
            return Integer.MAX_VALUE;
        }

        final List<Integer> r = getRange(portRange);
        return r.get(1) - r.get(0) + 1;
    }

    /**
     * Make sure there are enough ports for the storage node.  The
     * Storage Node itself is implied.  Number of ports required is noted
     * in the comments above, where PORTS_PER_XX members are declared.
     *
     * @throws IllegalArgumentException if the port range string is not either
     * the canonical {@link PortRange#UNCONSTRAINED} value or a comma-separated
     * pair of tokens, or if the range is smaller than the required capacity.
     * @throws NumberFormatException if either tokens in the range are not in
     * valid integer format
     */
    public static void validateSufficientPorts(String portRange,
                                               int capacity,
                                               boolean isSecure,
                                               boolean includeAdmin,
                                               int mgmtPorts) {
        int size = rangeSize(portRange);
        int required = isSecure ?
            (PORTS_PER_SN + SECURE_PORTS_PER_SN +
             ((PORTS_PER_RN + SECURE_PORTS_PER_SN) * capacity) +
             mgmtPorts +
             (includeAdmin ? (PORTS_PER_ADMIN + SECURE_PORTS_PER_ADMIN) : 0)) :
            (PORTS_PER_SN +
             (PORTS_PER_RN * capacity) + mgmtPorts +
             (includeAdmin ? PORTS_PER_ADMIN : 0));
        if (size < required) {
            throw new IllegalArgumentException
                               ("Service port range is too small. " +
                                size + " ports were specified and " + required +
                                " are required");
        }
    }

    /**
     * Returns an integer list doublet representing the end points of the
     * port range.
     *
     * @throws IllegalArgumentException if the port range string is not a
     *  comma separated pair of tokens
     * @throws NumberFormatException if either tokens in the range are not
     *  in valid integer format
     */
    public static List<Integer> getRange(String portRange) {

        if (isUnconstrained(portRange)) {
            throw new IllegalArgumentException("Unconstrained port range");
        }

        final StringTokenizer tokenizer = new StringTokenizer(portRange, ",");

        if (tokenizer.countTokens() != 2) {
            throw new IllegalArgumentException
                (portRange + " is not a valid port range expression");
        }

        return Arrays.asList(Integer.parseInt(tokenizer.nextToken()),
                             Integer.parseInt(tokenizer.nextToken()));
    }

    /**
     * @param portRange
     * @param port
     * @return true if port is within the portRange.
     */
    public static boolean contains(String portRange, int port) {
        List<Integer> range = getRange(portRange);
        return (range.get(0) <= port) && (range.get(1) >= port);
    }
}

