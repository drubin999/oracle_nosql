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

package oracle.kv.impl.test;

import java.rmi.RemoteException;

import oracle.kv.impl.util.registry.RemoteAPI;

/**
 * RemoteTestAPI is used conditionally in order to extend a service's
 * interface to include test-only functions.  Implementations of this interface
 * should live in test packages and be conditionally instantiated using
 * reflection, e.g.:
 * try {
 *   Class cl = Class.forName("ImplementsRemoteTestInterface");
 *   Constructor<?> c = cl.getConstructor(getClass());
 *   RemoteTestAPI rti = (RemoteTestAPI) c.newInstance(this);
 *   rti.start();
 * } catch (Exception ignored) {}
 *
 * Most implementations should extend RemoteTestBase which is implemented in
 * the test tree.
 */
public class RemoteTestAPI extends RemoteAPI {

    private final RemoteTestInterface remote;

    private RemoteTestAPI(RemoteTestInterface remote)
        throws RemoteException {

        super(remote);
        this.remote = remote;
    }

    public static RemoteTestAPI wrap(RemoteTestInterface remote)
        throws RemoteException {

        return new RemoteTestAPI(remote);
    }

    /**
     * Start the service. This call should create a binding.
     */
    public void start()
        throws RemoteException {

        remote.start(getSerialVersion());
    }

    /**
     * Stop the service.  This call should unbind the object.
     */
    public void stop()
        throws RemoteException {

        remote.stop(getSerialVersion());
    }

    /**
     * Tell the service to exit (calls System.exit(status)).  Callers should
     * expect this to always throw an exception because the service's process
     * will be unable to reply.
     *
     * @param status The exit status to use.
     */
    public void processExit(int status)
        throws RemoteException {

        remote.processExit(status, getSerialVersion());
    }

    /**
     * Tell the service to halt (calls Runtime.getRuntime.halt(status)).  This
     * differs from exit in that shutdown hooks are not run. Callers should
     * expect this to always throw an exception because the service's process
     * will be unable to reply.
     *
     * @param status The exit status to use.
     */
    public void processHalt(int status)
        throws RemoteException {

        remote.processHalt(status, getSerialVersion());
    }

    /**
     * Tell the service to invalidate it's JE environment, returning whether
     * invalidation was performed successfully.
     */
    public boolean processInvalidateEnvironment()
        throws RemoteException {

        return remote.processInvalidateEnvironment(getSerialVersion());
    }

    /**
     * Ask the implementation to set a TestHook.
     *
     * @param hook The TestHook instance to use.
     *
     * @param memberName The name of the member in the target class that holds
     * the TestHook instance.
     */
    public void setHook(TestHook<?> hook, String memberName)
        throws RemoteException {

        remote.setHook(hook, memberName, getSerialVersion());
    }
}
