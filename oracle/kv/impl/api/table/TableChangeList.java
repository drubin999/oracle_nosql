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

package oracle.kv.impl.api.table;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;

/**
 * Container for table change instances. A sequence of changes can be
 * applied to a TableMetadata instance, via {@link TableMetadata#apply}
 * to make it more current.
 */
public class TableChangeList implements MetadataInfo,
                                        Iterable<TableChange>,
                                        Serializable {
    private static final long serialVersionUID = 1L;

    public static final MetadataInfo EMPTY_TABLE_INFO =
            new TableChangeList(Metadata.EMPTY_SEQUENCE_NUMBER, null);

    private final int sourceSeqNum;
    private final List<TableChange> changes;
    
    TableChangeList(int sourceSeqNum, List<TableChange> changes) {
        this.sourceSeqNum = sourceSeqNum;
        this.changes = changes;
    }
    
    @Override
    public MetadataType getType() {
        return MetadataType.TABLE;
    }

    @Override
    public int getSourceSeqNum() {
        return sourceSeqNum;
    }

    @Override
    public boolean isEmpty() {
        return (changes == null) ? true : changes.isEmpty();
    }

    @Override
    public Iterator<TableChange> iterator() {
        assert changes != null; // TODO- maybe make this an interator.?
        return changes.iterator();
    }

    @Override
    public String toString() {
        return "TableChangeList[" + sourceSeqNum + ", " +
               (isEmpty() ? "-" : changes.size()) + "]";
    }
}
