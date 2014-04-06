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

package oracle.kv.impl.rep.admin;

import java.io.Serializable;

import oracle.kv.KVVersion;
import oracle.kv.impl.rep.RepNode;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;

/**
 * Diagnostic and status information about a RepNode.
 */
public class RepNodeInfo implements Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private final EnvironmentConfig envConfig;
    private final EnvironmentStats envStats;

    /* Since R2 patch 1 */
    private final KVVersion version;

    RepNodeInfo(RepNode repNode) {
        version = KVVersion.CURRENT_VERSION;
        Environment env = repNode.getEnv(0L);
        if (env == null) {
            envConfig = null;
            envStats = null;
            return;
        }

        envConfig = env.getConfig();
        envStats = env.getStats(null);
    }

    public EnvironmentConfig getEnvConfig() {
        return envConfig;
    }

    public EnvironmentStats getEnvStats() {
        return envStats;
    }

    /**
     * Gets the RepNode's software version.
     * 
     * @return the RepNode's software version
     */
    public KVVersion getSoftwareVersion() {
        /*
         * If the version field is not filled-in we will assume that the
         * node it came from is running the initial R2 release (2.0.23).
         */
        return (version != null) ? version : KVVersion.R2_0_23;
    }

    @Override 
    public String toString() {
        return "Environment Configuration:\n" +  envConfig +
            "\n" + envStats;
    }
}
