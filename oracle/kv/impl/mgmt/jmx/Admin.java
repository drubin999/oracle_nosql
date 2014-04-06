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

import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.mgmt.jmx.AdminMXBean;

public class Admin
    extends NotificationBroadcasterSupport
    implements AdminMXBean {

    private AdminId aid;
    final private MBeanServer server;
    final private StorageNode sn;

    private ServiceStatus status;
    private AdminParams parameters;
    private boolean isMaster;
    private ObjectName oName;
    long notifySequence = 1L;

    final static String
        NOTIFY_ADMIN_STATUS_CHANGE = "oracle.kv.admin.status";

    public Admin(AdminParams ap, MBeanServer server, StorageNode sn) {
        this.server = server;
        /* If ap is null, then this is a bootstrap Admin. */
        this.aid = (ap == null ? new AdminId(0) : ap.getAdminId());
        this.sn = sn;
        status = ServiceStatus.UNREACHABLE;
        isMaster = false;
        setParameters(ap);
        register();
    }

    private void register() {
        StringBuffer buf = new StringBuffer(JmxAgent.DOMAIN);
        buf.append(":type=Admin");
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

    @Override
    public MBeanNotificationInfo[] getNotificationInfo() {
        return new MBeanNotificationInfo[]
        {
            new MBeanNotificationInfo
                (new String[]{NOTIFY_ADMIN_STATUS_CHANGE},
                 Notification.class.getName(),
                 "Announce a change in this Admin's service status"),
        };
    }

    public void setParameters(AdminParams ap) {
        parameters = ap;
        if (ap == null  || ap.getAdminId().equals(aid)) {
            return;
        }
        aid = ap.getAdminId();
    }

    public synchronized void setServiceStatus(ServiceStatus newStatus,
                                              boolean master) {

        if (status.equals(newStatus) && master == isMaster) {
            return;
        }

        Notification n = new Notification
            (NOTIFY_ADMIN_STATUS_CHANGE, oName, notifySequence++,
             System.currentTimeMillis(),
             "The service status for Admin " + getAdminId() +
             " changed to " + newStatus.toString() +
             (master ? " master" : ""));

        n.setUserData(newStatus.toString());

        sendNotification(n);

        /*
         * Also send it from the StorageNode. A client can observe this event
         * by subscribing ether to the StorageNode or to this RepNode.
         */
        sn.sendProxyNotification(n);

        status = newStatus;
        isMaster = master;
    }

    @Override
    public int getAdminId() {
        return aid.getAdminInstanceId();
    }

    @Override
    public String getServiceStatus() {
        return status.toString();
    }

    @Override
    public int getHttpPort() {
        return parameters == null ? 0 : parameters.getHttpPort();
    }

    @Override
    public int getLogFileLimit() {
        return parameters == null ? 0 : parameters.getLogFileLimit();
    }

    @Override
    public int getLogFileCount() {
        return parameters == null ? 0 : parameters.getLogFileCount();
    }

    @Override
    public long getPollPeriodMillis() {
        return parameters == null ? 0L : parameters.getPollPeriodMillis();
    }

    @Override
    public long getEventExpiryAge() {
        return parameters == null ? 0L : parameters.getEventExpiryAge();
    }

    @Override
    public boolean isMaster() {
        return isMaster;
    }
}
