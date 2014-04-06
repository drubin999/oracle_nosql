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

package oracle.kv.impl.mgmt;

import java.lang.reflect.Constructor;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.util.ServiceStatusTracker;

/**
 * This factory produces an implementation of MgmtAgent based on the
 * configuration in the BootstrapParameters.
 */
public class MgmtAgentFactory {

    public static MgmtAgent getAgent(StorageNodeAgent sna,
                                     StorageNodeParams snp,
                                     ServiceStatusTracker tracker) {

        final Logger logger = sna.getLogger();
        assert(logger != null);
        final BootstrapParams bp = sna.getBootstrapParams();
        assert(bp != null);

        /*
         * If StorageNodeParams are provided, use them.  Otherwise, use
         * BootstratParams.
         */
        String mgmtClass;
        int pollingPort;
        String trapHost;
        int trapPort;

        if (snp != null) {
            mgmtClass = snp.getMgmtClass();
            pollingPort = snp.getMgmtPollingPort();
            trapHost = snp.getMgmtTrapHost();
            trapPort = snp.getMgmtTrapPort();
        } else {
            mgmtClass = bp.getMgmtClass();
            pollingPort = bp.getMgmtPollingPort();
            trapHost = bp.getMgmtTrapHost();
            trapPort = bp.getMgmtTrapPort();
        }
        assert(mgmtClass != null);

        if (MgmtUtil.verifyImplClassName(mgmtClass) != true) {
            throw new IllegalStateException
                ("The class name " + mgmtClass +
                 ", present in the config file, is not allowed.");
        }

        /*
         * Check to see if the current configuration matches the requested
         * configuration.  If it does, then we'll do nothing.
         */
        MgmtAgent currentImpl = sna.getMgmtAgent();
        if (currentImpl != null) {
            if (currentImpl.getClass().getName().equals(mgmtClass) &&
                currentImpl.checkParametersEqual(pollingPort, trapHost, trapPort)) {
                /*
                 * The requested configuration is already in force.  But there
                 * might be a new ServiceStatusTracker, so set that.
                 */
                currentImpl.setSnaStatusTracker(tracker);
                return currentImpl;
            }
            currentImpl.shutdown();
        }

        try {
            final Class<?> c = Class.forName(mgmtClass);
            final Constructor<?> ctor =
                c.getConstructor(StorageNodeAgent.class,
                                 Integer.TYPE,
                                 String.class, Integer.TYPE,
                                 ServiceStatusTracker.class);
            logger.info("Using mgmt class " + mgmtClass);
            return (MgmtAgent) ctor.newInstance(sna, pollingPort, trapHost,
                                                trapPort, tracker);
        } catch (Exception e) {
            logger.log(Level.WARNING,
                       "Can't instantiate MgmtAgent class " + mgmtClass, e);
        }
        /* Don't prevent the SNA from starting. */
        logger.info("Falling back to mgmt agent NoOpAgent.");
        return new NoOpAgent(sna, pollingPort, trapHost, trapPort, tracker);
    }
}
