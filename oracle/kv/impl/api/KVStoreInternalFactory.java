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

package oracle.kv.impl.api;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.registry.RegistryUtils.InterfaceType;

/**
 * Factory class used to produce handles to an existing KVStore.
 * This factory class in intended for internal use.
 */
public class KVStoreInternalFactory {

    /**
     * Get a handle to an existing KVStore for internal use.
     *
     * @param config the KVStore configuration parameters.
     * @param dispatcher a KVStore request dispatcher
     * @param logger a Logger instance
     * @throws IllegalArgumentException if an illegal configuration parameter
     * is specified.
     */
    public static KVStore getStore(KVStoreConfig config,
                                   RequestDispatcher dispatcher,
                                   LoginManager loginMgr,
                                   Logger logger)
        throws IllegalArgumentException {

        final long requestTimeoutMs =
                config.getRequestTimeout(TimeUnit.MILLISECONDS);
        final long readTimeoutMs =
            config.getSocketReadTimeout(TimeUnit.MILLISECONDS);
        if (requestTimeoutMs > readTimeoutMs) {
            final String format = "Invalid KVStoreConfig. " +
                "Request timeout: %,d ms exceeds " +
                "socket read timeout: %,d ms" ;
            throw new IllegalArgumentException
                (String.format(format, requestTimeoutMs, readTimeoutMs));
        }

        RegistryUtils.setRegistrySocketTimeouts
            ((int) config.getRegistryOpenTimeout(TimeUnit.MILLISECONDS),
             (int) config.getRegistryReadTimeout(TimeUnit.MILLISECONDS),
             config.getStoreName());

        final String csfName =
            ClientSocketFactory.factoryName(config.getStoreName(),
                                            RepNodeId.getPrefix(),
                                            InterfaceType.MAIN.
                                            interfaceName());
        final int openTimeoutMs =
            (int) config.getSocketOpenTimeout(TimeUnit.MILLISECONDS);

        ClientSocketFactory.configureStoreTimeout
            (csfName, openTimeoutMs, (int) readTimeoutMs);

        /*
         * construct a KVStore that is constrained in the usual way
         * w.r.t. internal namespace access.
         */
        return new KVStoreImpl(logger, dispatcher, config, loginMgr);
    }
}
