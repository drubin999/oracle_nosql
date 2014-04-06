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

import com.sleepycat.je.CacheMode;
import com.sleepycat.persist.model.Persistent;

@Persistent
public class CacheModeParameter extends Parameter {

    private static final long serialVersionUID = 1L;

    private CacheMode value;

    /* For DPL */
    public CacheModeParameter() {
    }

    public CacheModeParameter(String name, String val) {
        super(name);
        try {
            this.value =
                Enum.valueOf(CacheMode.class,
                             val.toUpperCase(java.util.Locale.ENGLISH));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid CacheMode format: " +
                                               val);
        }
    }

    public CacheModeParameter(String name, CacheMode value) {
        super(name);
        this.value = value;
    }

    public CacheMode asCacheMode() {
        return value;
    }

    @Override
    public Enum<?> asEnum() {
        return value;
    }

    @Override
    public String asString() {
        return value.toString();
    }

    @Override
    public ParameterState.Type getType() {
        return ParameterState.Type.CACHEMODE;
    }
}
