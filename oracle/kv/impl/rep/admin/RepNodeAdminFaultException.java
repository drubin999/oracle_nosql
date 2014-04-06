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

import oracle.kv.impl.fault.InternalFaultException;
import oracle.kv.impl.sna.SNAFaultException;

/**
 * Subclass of InternalFaultException used to indicate that the fault originated
 * in the RepNode when satisfying an administrative service request.
 * <p>
 * Note that the original service request may have been issued by the SNA or by
 * the Admin which uses the SNA as its proxy on the SN to perform a request on
 * on the RN on its behalf. So the Admin may see this exception even though it
 * actually issued this request to the SNA. A fault that originates in the SNA
 * results in a {@link SNAFaultException}.
 */
public class RepNodeAdminFaultException extends InternalFaultException {
    private static final long serialVersionUID = 1L;

    public RepNodeAdminFaultException(Throwable cause) {
        super(cause);
    }
}
