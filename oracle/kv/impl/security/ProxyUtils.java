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
package oracle.kv.impl.security;

import java.lang.reflect.Method;
import java.rmi.Remote;
import java.util.HashSet;
import java.util.Set;

/**
 * A collection of common code used by proxy-building code in this package.
 */
final class ProxyUtils {

    /* Not instantiable */
    private ProxyUtils() {
    }

    static Set<Class<?>> findRemoteInterfaces(Class<?> implClass) {

        /*
         * Build a set of the most-derived interfaces that extend the
         * Remote interface.
         */
        final Set<Class<?>> remoteInterfaces = new HashSet<Class<?>>();
        for (Class<?> iface : implClass.getInterfaces()) {
            if (Remote.class.isAssignableFrom(iface)) {

                /* This interface either is, or extends Remote */
                for (Class<?> knownIface : remoteInterfaces) {
                    if (knownIface.isAssignableFrom(iface)) {
                        /* iface is a more derived class - replace knowIface */
                        remoteInterfaces.remove(knownIface);
                    }
                }
                remoteInterfaces.add(iface);
            }
        }

        return remoteInterfaces;
    }

    static Set<Method> findRemoteInterfaceMethods(Class<?> implClass) {

        /*
         * First, build a set of the most-derived interfaces that extend
         * the Remote interface.
         */
        final Set<Class<?>> remoteInterfaces = findRemoteInterfaces(implClass);

        /* Then collect the methods that are included in those interfaces */
        final Set<Method> remoteInterfaceMethods = new HashSet<Method>();
        for (Class<?> remoteInterface : remoteInterfaces) {
            for (Method method : remoteInterface.getMethods()) {
                remoteInterfaceMethods.add(method);
            }
        }

        return remoteInterfaceMethods;
    }

}
