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

package oracle.kv.impl.mgmt.jmx;

import javax.management.MBeanNotificationInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.ObjectName;

import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.mgmt.jmx.StorageNodeMXBean;

public class StorageNode
    extends NotificationBroadcasterSupport
    implements StorageNodeMXBean {

    final private JmxAgent agent;
    final private MBeanServer server;
    private ObjectName oName;
    ServiceStatus status;
    long notifySequence = 1L;

    final private static String
        NOTIFY_SN_STATUS_CHANGE = "oracle.kv.storagenode.status";

    public StorageNode(JmxAgent agent, MBeanServer server) {
        this.agent =  agent;
        this.server = server;
        status = ServiceStatus.UNREACHABLE;
        register();
    }

    private void register() {

        /* There is only one StorageNode, so no need to identify it further. */
        StringBuffer buf = new StringBuffer(JmxAgent.DOMAIN);
        buf.append(":type=StorageNode");

        try {
            oName = new ObjectName(buf.toString());
        } catch (MalformedObjectNameException e) {
            throw new IllegalStateException
                ("Unexpected exception creating JMX ObjectName " +
                 buf.toString(), e);
        }

        try {
            server.registerMBean(this, oName);
        } catch (Exception e) {
            throw new IllegalStateException
                ("Unexpected exception registring MBean " + oName.toString(),
                 e);
        }
    }

    public void unregister() {
        if (oName != null) {
            try {
                server.unregisterMBean(oName);
            } catch (Exception e) {
                throw new IllegalStateException
                    ("Unexpected exception while unregistring MBean " +
                     oName.toString(), e);
            }
        }
    }

    public synchronized void setServiceStatus(ServiceStatus newStatus) {
        if (status.equals(newStatus)) {
            return;
        }

        Notification n = new Notification
            (NOTIFY_SN_STATUS_CHANGE, oName, notifySequence++,
             System.currentTimeMillis(),
             "The service status for the StorageNode has " +
             " changed to " + newStatus.toString() + ".");

        n.setUserData(newStatus.toString());

        sendNotification(n);

        status = newStatus;
    }

    /**
     * Send a RepNode's or Admin's notifications from the StorageNodeMBean.
     * This is convenient because RepNodes can come and go, and notification
     * subscriptions on Repnodes do not survive this death and resurrection.  A
     * client can subscribe to StorageNodeMBean events, and receive events for
     * all the RepNodes managed by this storageNode.
     */
    public synchronized void sendProxyNotification(Notification n) {

        sendNotification(n);
    }

    @Override
    public MBeanNotificationInfo[] getNotificationInfo() {

        return new MBeanNotificationInfo[]
        {
            new MBeanNotificationInfo
                (new String[]{NOTIFY_SN_STATUS_CHANGE},
                 Notification.class.getName(),
                 "Announce a change in this StorageNode's service status."),
            new MBeanNotificationInfo
                (new String[]{RepNode.NOTIFY_RN_STATUS_CHANGE},
                 Notification.class.getName(),
                 "Announce a change in a RepNode's service status."),
            new MBeanNotificationInfo
                (new String[]{RepNode.NOTIFY_SINGLE_TFLOOR},
                 Notification.class.getName(),
                 "Single-operation throughput floor violation notification."),
            new MBeanNotificationInfo
                (new String[]{RepNode.NOTIFY_SINGLE_LCEILING},
                 Notification.class.getName(),
                 "Single-operation latency ceiling violation notification."),
            new MBeanNotificationInfo
                (new String[]{RepNode.NOTIFY_MULTI_TFLOOR},
                 Notification.class.getName(),
                 "Multi-operation throughput floor violation notification."),
            new MBeanNotificationInfo
                (new String[]{RepNode.NOTIFY_MULTI_LCEILING},
                 Notification.class.getName(),
                 "Multi-operation latency ceiling violation notification."),
            new MBeanNotificationInfo
                (new String[]{Admin.NOTIFY_ADMIN_STATUS_CHANGE},
                 Notification.class.getName(),
                 "Announce a change in the Admin's service status"),
        };
    }

    @Override
    public String getServiceStatus() {
        return status.toString();
    }

    @Override
    public int getAdminHttpPort() {
        return agent.getAdminHttpPort();
    }

    @Override
    public boolean isHostingAdmin() {
        return agent.isHostingAdmin();
    }

    @Override
    public String getRootDirPath() {
        return agent.getRootDir();
    }

    @Override
    public String getStoreName() {
        return agent.getStoreName();
    }

    @Override
    public String getHostname() {
        return agent.getHostname();
    }

    @Override
    public int getRegistryPort() {
        return agent.getRegistryPort();
    }

    @Override
    public String getHAHostname() {
        return agent.getHAHostname();
    }

    @Override
    public int getCapacity() {
        return agent.getCapacity();
    }

    @Override
    public int getLogFileLimit() {
        return agent.getLogFileLimit();
    }

    @Override
    public int getLogFileCount() {
        return agent.getLogFileCount();
    }

    @Override
    public String getHaPortRange() {
        return agent.getSnHaPortRange();
    }

    @Override
    public int getSnId() {
        return agent.getSnId();
    }

    @Override
    public int getMemoryMB() {
        return agent.getMemoryMB();
    }

    @Override
    public int getNumCPUs() {
        return agent.getNumCpus();
    }

    @Override
    public String getMountPoints() {
        return agent.getMountPointsString();
    }
}
