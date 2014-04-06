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

package oracle.kv.impl.param;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * This is a simple class that tracks ParameterListener objects that register
 * interest in notification that a parameter change has occurred.  A service
 * that has parameters that can be modified at run time will contain one of
 * these objects, and portions of that service can register ParameterListener
 * objects to be notified when a change occurs.
 *
 * See Admin.java and RepNodeService.java for examples of usage
 */
public class ParameterTracker {

    /**
     * Use a set to avoid the need to handle duplicates (LoggerUtils, notably).
     */
    private final Set<ParameterListener> listeners;

    public ParameterTracker() {
        listeners =
            Collections.synchronizedSet(new HashSet<ParameterListener>());
    }

    public void addListener(ParameterListener listener) {
        listeners.add(listener);
    }

    public void removeListener(ParameterListener listener) {
        listeners.remove(listener);
    }

    public void notifyListeners(ParameterMap oldMap, ParameterMap newMap) {
        for (ParameterListener listener : listeners) {
            listener.newParameters(oldMap, newMap);
        }
    }
}
