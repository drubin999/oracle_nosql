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

import java.util.Properties;

import oracle.kv.impl.admin.param.RMISocketPolicyBuilder;
import oracle.kv.impl.admin.param.RepNetConfigBuilder;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.util.registry.ClearSocketPolicy;
import oracle.kv.impl.util.registry.RMISocketPolicy;

import com.sleepycat.je.rep.ReplicationNetworkConfig;

/**
 * Factory class for generating RMISocketPolicy instances and configuring
 * JE data channels.
 */
public class ClearTransport
    implements RMISocketPolicyBuilder, RepNetConfigBuilder {

    /**
     * Simple constructor, for use by newInstance().
     */
    public ClearTransport() {
    }

    /*
     * RMISocketPolicyBuilder interface methods
     */

    /**
     * Creates an instance of the RMISocketPolicy.
     */
    @Override
    public RMISocketPolicy makeSocketPolicy(SecurityParams securityParams,
                                            ParameterMap map)
        throws Exception {

        return new ClearSocketPolicy();
    }

    /**
     * Construct a set of properties for client access.
     */
    @Override
    public Properties getClientAccessProperties(SecurityParams sp,
                                                ParameterMap map) {
        return new Properties();
    }

    /*
     * RepNetConfigBuilder interface methods
     */

    @Override
    public Properties makeChannelProperties(SecurityParams sp,
                                            ParameterMap map) {

        final Properties props = new Properties();
        props.setProperty(ReplicationNetworkConfig.CHANNEL_TYPE, "basic");
        return props;
    }
}
