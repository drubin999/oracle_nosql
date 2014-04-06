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

package oracle.kv.impl.monitor;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import oracle.kv.impl.util.registry.ServerSocketFactory;

public abstract class TrackerListenerImpl
    extends UnicastRemoteObject implements TrackerListener {

    private static final long serialVersionUID = 1L;

    /**
     * Items with timestamps earlier than this are of no
     * interest to this subscriber.
     */
    long interestingTime;

    public TrackerListenerImpl(ServerSocketFactory ssf,
                               long interestingTime)
        throws RemoteException {

        super(0, null, ssf);

        this.interestingTime = interestingTime;
    }

    /**
     * TODO: Check whether users of this constructor are legitimate test
     * users, or client side users where the server factory does not matter
     */
    public TrackerListenerImpl(long interestingTime)
        throws RemoteException {

        super(0);

        this.interestingTime = interestingTime;
    }

    public void setInterestingTime(long t) {
        interestingTime = t;
    }

    @Override
    public long getInterestingTime() {
        return interestingTime;
    }

    @Override
    public String toString() {
        return this.getClass().getName() + " intTime=" + interestingTime;
    }
}
