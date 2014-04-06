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

import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.security.metadata.SecurityMetadata.SecurityElement;
import oracle.kv.impl.security.metadata.SecurityMetadata.SecurityElementType;

/**
 * Class for recording a change of SecurityMetadata on its elements. The change
 * includes three types of operation so far: ADD, UPDATE and REMOVE. The type
 * of element operated on will also be recorded.
 */
public abstract class SecurityMDChange implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    public enum SecurityMDChangeType { ADD, UPDATE, REMOVE }

    protected int seqNum = Metadata.EMPTY_SEQUENCE_NUMBER;

    SecurityMDChange(final int seqNum) {
        this.seqNum = seqNum;
    }

    /*
     * Ctor with deferred sequence number setting.
     */
    private SecurityMDChange() {
    }

    /**
     * Get the sequence number of the security metadata change.
     */
    public int getSeqNum() {
        return seqNum;
    }

    public void setSeqNum(final int seqNum) {
        this.seqNum = seqNum;
    }

    /**
     * Get the type of element involved in the change
     */
    abstract SecurityElementType getElementType();

    abstract SecurityMDChangeType getChangeType();

    abstract SecurityElement getElement();

    abstract String getElementId();

    @Override
    public abstract SecurityMDChange clone();

    /**
     * The change of UPDATE.
     */
    public static class Update extends SecurityMDChange {

        private static final long serialVersionUID = 1L;

        SecurityElement element;

        public Update(final SecurityElement element) {
            this.element = element;
        }

        private Update(final int seqNum, final SecurityElement element) {
            super(seqNum);
            this.element = element;
        }

        @Override
        SecurityMDChangeType getChangeType() {
            return SecurityMDChangeType.UPDATE;
        }

        @Override
        SecurityElement getElement() {
            return element;
        }

        @Override
        String getElementId() {
            return element.getElementId();
        }

        @Override
        SecurityElementType getElementType() {
            return element.getElementType();
        }

        @Override
        public Update clone() {
            return new Update(seqNum, element.clone());
        }
    }

    /**
     * The change of ADD. It extends the CHANGE class since it shares almost
     * the same behavior and codes of CHANGE except the ChangeType.
     */
    public static class Add extends Update {

        private static final long serialVersionUID = 1L;

        public Add(final SecurityElement element) {
            super(element);
        }

        private Add(final int seqNum, final SecurityElement element) {
            super(seqNum, element);
        }

        @Override
        SecurityMDChangeType getChangeType() {
            return SecurityMDChangeType.ADD;
        }

        @Override
        public Add clone() {
            return new Add(seqNum, element.clone());
        }
    }

    /**
     * The change of REMOVE.
     */
    public static class Remove extends SecurityMDChange {

        private static final long serialVersionUID = 1L;

        private String elementId;
        private SecurityElementType elementType;

        public Remove(final String removedId,
                      final SecurityElementType eType) {
            this.elementId = removedId;
            this.elementType = eType;
        }

        private Remove(final int seqNum,
                       final String removedId,
                       final SecurityElementType eType) {
            super(seqNum);
            this.elementId = removedId;
            this.elementType = eType;
        }

        @Override
        SecurityMDChangeType getChangeType() {
            return SecurityMDChangeType.REMOVE;
        }

        @Override
        SecurityElement getElement() {
            return null;
        }

        @Override
        String getElementId() {
            return elementId;
        }

        @Override
        SecurityElementType getElementType() {
            return elementType;
        }

        @Override
        public Remove clone() {
            return new Remove(seqNum, elementId, elementType);
        }
    }
}
