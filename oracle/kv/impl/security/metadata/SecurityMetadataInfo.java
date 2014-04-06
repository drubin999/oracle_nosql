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

package oracle.kv.impl.security.metadata;

import java.io.Serializable;
import java.util.List;

import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;

public class SecurityMetadataInfo implements MetadataInfo, Serializable {

    private static final long serialVersionUID = 1L;

    private final List<SecurityMDChange> changeList;
    private final String securityMetadataId;
    private final int sequenceNum;

    public static final SecurityMetadataInfo EMPTY_SECURITYMD_INFO =
        new SecurityMetadataInfo(null, -1, null);

    public SecurityMetadataInfo(final SecurityMetadata secMD,
                                final List<SecurityMDChange> changes) {
        this(secMD.getId(), secMD.getSequenceNumber(), changes);
    }

    public SecurityMetadataInfo(final String secMDId,
                                final int latestSeqNum,
                                final List<SecurityMDChange> changes) {
        this.securityMetadataId = secMDId;
        this.sequenceNum = latestSeqNum;
        this.changeList = changes;
    }

    @Override
    public MetadataType getType() {
        return MetadataType.SECURITY;
    }

    @Override
    public int getSourceSeqNum() {
        return sequenceNum;
    }

    @Override
    public boolean isEmpty() {
        return (changeList == null) || (changeList.isEmpty());
    }

    public String getSecurityMetadataId() {
        return securityMetadataId;
    }

    public List<SecurityMDChange> getChanges() {
        return changeList;
    }
}
