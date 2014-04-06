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

package oracle.kv.impl.rep.table;

import java.util.List;
import java.util.Set;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;
import com.sleepycat.je.SecondaryMultiKeyCreator;
import oracle.kv.impl.api.table.IndexImpl;

/**
 *
 */
public class IndexKeyCreator implements SecondaryKeyCreator,
                                        SecondaryMultiKeyCreator {

    private volatile IndexImpl index;
    /*
     * Keep this state to make access faster
     */
    private final boolean keyOnly;
    private final boolean isMultiKey;

    IndexKeyCreator(IndexImpl index) {
        this.index = index;
        this.keyOnly = index.isKeyOnly();
        this.isMultiKey = index.isMultiKey();
    }

    boolean primaryKeyOnly() {
        return keyOnly;
    }

    boolean isMultiKey() {
        return isMultiKey;
    }

    void setIndex(IndexImpl newIndex) {
        index = newIndex;
    }
    
    /* -- From SecondaryKeyCreator -- */

    @Override
    public boolean createSecondaryKey(SecondaryDatabase secondaryDb,
                                      DatabaseEntry key,
                                      DatabaseEntry data,
                                      DatabaseEntry result) {
        byte[] res =
            index.extractIndexKey(key.getData(),
                                  (data != null ? data.getData() : null),
                                  keyOnly);
        if (res != null) {
            result.setData(res);
            return true;
        }
        return false;
    }

    /* -- From SecondaryMultiKeyCreator -- */

    @Override
    public void createSecondaryKeys(SecondaryDatabase secondaryDb,
                                    DatabaseEntry key,
                                    DatabaseEntry data,
                                    Set<DatabaseEntry> results) {
        List<byte[]> res = index.extractIndexKeys(key.getData(),
                                                  data.getData(),
                                                  keyOnly);
        if (res != null) {
            for (byte[] bytes : res) {
                results.add(new DatabaseEntry(bytes));
            }
        }
    }

    @Override
    public String toString() {
        return "IndexKeyCreator[" + index.getName() + "]";
    }
}
