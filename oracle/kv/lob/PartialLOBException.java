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

package oracle.kv.lob;

import oracle.kv.FaultException;
import oracle.kv.impl.util.ObjectUtil;
import oracle.kv.lob.KVLargeObject.LOBState;

/**
 * Thrown when {@link KVLargeObject#getLOB} is invoked on a partial LOB. A
 * partial LOB is typically the result of an incomplete <code>deleteLOB</code>
 * or <code>putLOB</code> operation.
 *
 * <p>
 * The application can handle this exception and resume the incomplete
 * operation. For example, it can invoke <code>deleteLOB</code>, to delete a
 * LOB in the {@link LOBState#PARTIAL_DELETE} state or it can resume the failed
 * putLOB operation for a LOB in the {@link LOBState#PARTIAL_PUT} state.
 *
 * @see KVLargeObject
 *
 * @since 2.0
 */
@SuppressWarnings("javadoc")
public class PartialLOBException extends FaultException {

    private static final long serialVersionUID = 1L;

    private final LOBState partialState;

    /**
     * @hidden
     */
    public PartialLOBException(String msg,
                               LOBState partialState,
                               boolean isRemote) {
        super(msg, isRemote);

        ObjectUtil.checkNull("partialState", partialState);

        if (partialState == LOBState.COMPLETE) {
            final String emsg = "The value of partialState must denote a " +
                "partial state not " + partialState;
            throw new IllegalArgumentException(emsg);
        }

        this.partialState = partialState;
    }

    /**
     * Returns the state associated with the LOB. The state returned is one of
     * the partial states: {@link LOBState#PARTIAL_PUT},
     * {@link LOBState#PARTIAL_DELETE} or
     * {@link LOBState#PARTIAL_APPEND}.
     *
     * @since 2.1.55
     */
    public LOBState getPartialState() {
        return partialState;
    }

    /**
     * Returns true only if the exception resulted from a partially deleted LOB.
     *
     * @deprecated Use the getPartialStateMethod() instead
     */
    @Deprecated
    public boolean isPartiallyDeleted() {
        return LOBState.PARTIAL_DELETE == partialState;
    }
}
