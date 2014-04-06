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

import java.rmi.RemoteException;

import oracle.kv.FaultException;
import oracle.kv.impl.util.registry.RemoteAPI;

/**
 * Handles requests that have been directed to this RN by some
 * {@link RequestDispatcher}. It's ultimately responsible for the
 * execution of a request that originated at a KV Client.
 */
public class RequestHandlerAPI extends RemoteAPI {

    private final RequestHandler remote;

    private RequestHandlerAPI(RequestHandler remote)
        throws RemoteException {

        super(remote);
        this.remote = remote;
    }

    public static RequestHandlerAPI wrap(RequestHandler remote)
        throws RemoteException {

        return new RequestHandlerAPI(remote);
    }

    /**
     * Executes the request. It identifies the database that owns the keys
     * associated with the request and executes the request.
     *
     * <p>
     * The local request handler contains the retry logic for all failures that
     * can be handled locally. For example, a retry resulting from an
     * environment handle that was invalidated due to a hard recovery in the
     * midst of an operation. Exceptional situations that cannot be
     * handled internally are propagated back to the client.
     * <p>
     * It may not be possible to initiate execution of the request because the
     * request was misdirected and the RN does not own the key, or because the
     * request is for an update and the RN is not a master. In these cases,
     * it internally redirects the request to a more appropriate RN and returns
     * the response or exception as appropriate.
     *
     * @param request the request to be executed
     *
     * @return the response from the execution of the request
     */
    public Response execute(Request request)
        throws FaultException, RemoteException {

        return remote.execute(request);
    }
}
