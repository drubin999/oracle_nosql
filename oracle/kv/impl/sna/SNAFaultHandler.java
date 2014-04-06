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

package oracle.kv.impl.sna;

import java.util.logging.Logger;

import oracle.kv.impl.fault.ClientAccessException;
import oracle.kv.impl.fault.InternalFaultException;
import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.fault.ProcessFaultHandler;

/**
 * Specializes the ProcessFaultHandler so that all thrown exceptions are
 * wrapped inside a SNAFaultException unless they originated with one of its
 * managed services.
 *
 */
public class SNAFaultHandler extends ProcessFaultHandler {

    public SNAFaultHandler(StorageNodeAgentImpl sna) {
        this(sna.getLogger());
    }

    public SNAFaultHandler(Logger logger) {
        super(logger, null);
    }

    /**
     * This method should never be called.
     */
    @Override
    protected void queueShutdownInternal(Throwable fault,
                                         ProcessExitCode exitCode) {

        throw new UnsupportedOperationException
            ("Method not implemented: " + "queueShutdown", fault);
    }

    /**
     * Wrap it inside a SNAFaultException.
     */
    @Override
    protected RuntimeException getThrowException(RuntimeException fault) {
        if (fault instanceof ClientAccessException) {
            return ((ClientAccessException) fault).getCause();
        }

        /* If the fault originated at another service, don't wrap it. */
        return((fault instanceof InternalFaultException) ?
                fault : new SNAFaultException(fault));
    }
}
