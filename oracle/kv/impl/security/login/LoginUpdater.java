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
package oracle.kv.impl.security.login;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import oracle.kv.impl.param.ParameterListener;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;

/**
 * A class to help update the global and service-based security configuration
 * of components once there are changes of global and service parameters.
 */
public class LoginUpdater {

    private final Set<GlobalParamsUpdater> gpUpdaters =
        Collections.synchronizedSet(new HashSet<GlobalParamsUpdater>());

    private final Set<ServiceParamsUpdater> spUpdaters =
        Collections.synchronizedSet(new HashSet<ServiceParamsUpdater>());

    public void addGlobalParamsUpdaters(GlobalParamsUpdater... updaters) {
        for (final GlobalParamsUpdater updater : updaters) {
            gpUpdaters.add(updater);
        }
    }

    public void addServiceParamsUpdaters(ServiceParamsUpdater... updaters) {
        for (final ServiceParamsUpdater updater : updaters) {
            spUpdaters.add(updater);
        }
    }

    /**
     * Global parameter change listener. Registered globalParams updaters will
     * be notified of the changes.
     */
    public class GlobalParamsListener implements ParameterListener {

        @Override
        public void newParameters(ParameterMap oldMap, ParameterMap newMap) {
            if (oldMap != null) {
                final ParameterMap filtered =
                    oldMap.diff(newMap, true /* notReadOnly */).
                        filter(EnumSet.of(ParameterState.Info.SECURITY));
                if (filtered.size() == 0) {
                    return;
                }
            }
            for (final GlobalParamsUpdater gpUpdater : gpUpdaters) {
                gpUpdater.newGlobalParameters(newMap);
            }
        }
    }

    /**
     * Service parameter change listener. Registered serviceParams updaters
     * will be notified of the changes.
     */
    public class ServiceParamsListener implements ParameterListener {

        @Override
        public void newParameters(ParameterMap oldMap, ParameterMap newMap) {
            for (final ServiceParamsUpdater spUpdater : spUpdaters) {
                spUpdater.newServiceParameters(newMap);
            }
        }
    }

    /**
     * Updater interface for global parameters changes.
     */
    public interface GlobalParamsUpdater {
        void newGlobalParameters(ParameterMap map);
    }

    /**
     * Updater interface for service parameters changes.
     */
    public interface ServiceParamsUpdater {
        void newServiceParameters(ParameterMap map);
    }
}
