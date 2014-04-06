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

import java.io.InputStream;

import oracle.kv.Version;

/**
 * Holds a Stream and Version that are associated with a LOB.
 *
 * <p>
 * An InputStreamVersion instance is returned by {@link KVLargeObject#getLOB}
 * as the current value (represented by the stream) and version associated with
 * a given LOB. The version and inputStream properties will always be non-null.
 * </p>
 * IOExceptions thrown by this stream may wrap KVStore exceptions as described
 * in the documentation for the {@link KVLargeObject#getLOB} method.
 *
 * @since 2.0
 */
public class InputStreamVersion {

    private final InputStream inputStream;
    private final Version version;

    /**
     * Used internally to create an object with an inputStream and version.
     */
    public InputStreamVersion(InputStream inputStream, Version version) {
        this.inputStream = inputStream;
        this.version = version;
    }

    /**
     * Returns the InputStream part of the InputStream and Version pair.
     */
    public InputStream getInputStream() {
        return inputStream;
    }

    /**
     * Returns the Version of the InputStream and Version pair.
     */
    public Version getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return "<InputStreamVersion " + inputStream + ' ' + version + '>';
    }
}