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

package oracle.kv.mgmt.jmx;

/**
 * This MBean represents the Storage Node's operational parameters.
 *
 * @since 2.0
 */
public interface StorageNodeMXBean {

    /**
     * Returns the StorageNodeId number of this Storage Node.
     */
    int getSnId();

    /**
     * Returns the reported service status of the Storage Node.
     */
    String getServiceStatus();

    /**
     * Returns true if this Storage Node hosts an Admin instance.
     */
    boolean isHostingAdmin();

    /**
     * Returns the pathname of the store's root directory.
     */
    String getRootDirPath();

    /**
     * Returns the configured name of the store to which this
     * Storage Node belongs.
     */
    String getStoreName();

    /**
     * Returns the range of port numbers available for assigning to Replication
     * Nodes that are hosted on this Storage Node.  A port is allocated
     * automatically from this range when a Replication Node is deployed.
     */
    String getHaPortRange();

    /**
     * Returns the name of the network interface used for communication between
     * Replication Nodes
     */
    String getHAHostname();

    /**
     * Returns the port number of the Storage Node's RMI registry.
     */
    int getRegistryPort();

    /**
     * Returns the number of Replication Nodes that can be hosted
     * on this Storage Node.
     */
    int getCapacity();

    /**
     * Returns the name associated with the network interface on which this
     * Storage Node's registry listens.
     */
    String getHostname();

    /**
     * Returns the maximum size of log files.
     */
    int getLogFileLimit();

    /**
     * Returns the number of log files that are kept.
     */
    int getLogFileCount();

    /**
     * Returns the http port used by the Admin Console web application.
     */
    int getAdminHttpPort();

    /**
     * Returns the amount of memory known to be available on this Storage Node,
     * in megabytes.
     */
    int getMemoryMB();

    /**
     * Returns the number of CPUs known to be available on this Storage Node.
     */
    int getNumCPUs();

    /**
     * Returns a list of file system mount points on which Replication Nodes
     * can be deployed
     */
    String getMountPoints();
}
