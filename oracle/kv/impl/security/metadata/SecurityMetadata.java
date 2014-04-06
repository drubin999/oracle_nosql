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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.security.metadata.KVStoreUser.UserDescription;
import oracle.kv.impl.security.metadata.SecurityMDChange.SecurityMDChangeType;
import oracle.kv.impl.util.SerializationUtil;

/**
 * Class for security metadata. The class contains all elements of security
 * metadata, e.g., user data, role data, audit policy and other information
 * needed in future. Currently, only the KVStoreUser information is included.
 * <p>
 * The security metadata can be fully updated as whole. And it also supports
 * delta-update mechanism. A history change list is used to keep all changes
 * happened to the security metadata copy. All the changes can also be applied
 * to another copy to make it updated to the targeted status.
 * <p>
 * The SecurityMetadata is NOT thread safe. The concurrency protocol for it is
 * to always give out copies. Updates will be made on a new copy, and the old
 * one will be replaced as a whole. This can save the complex concurrency
 * control and corruption recovery mechanism.
 * <p>
 * On Admin side, the SecurityMetadata copy will be fetched from EntityStore
 * on each get. And the update will be performed in plans, which will be
 * executed serially. On the RepNode side, the access to SecurityMetadata is
 * coordinated by SecurityMetadataManager, in which concurrent updates and read
 * will be protected by "synchronized".
 */
public class SecurityMetadata implements Metadata<SecurityMetadataInfo>,
                                         Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The types of elements stored in security metadata.
     */
    public static enum SecurityElementType { KVSTOREUSER }

    private final String id;

    private final KVStoreUserMap kvstoreUserMap = new KVStoreUserMap(this);

    /* TODO: need to include AuditPolicy and Role in future */

    /* Used to identify which store the metadata belongs to */
    private final String kvstoreName;

    private final LinkedList<SecurityMDChange> changeList =
            new LinkedList<SecurityMDChange>();

    private int sequenceNumber = Metadata.EMPTY_SEQUENCE_NUMBER;

    /**
     * Create a SecurityMetadata instance with KVStore name. The ID is
     * "SecurityMD-" + currentTimeMillis by default.
     */
    public SecurityMetadata(final String storeName) {
        this(storeName, "SecurityMD-" + System.currentTimeMillis());
    }

    /**
     * Create a SecurityMetadata instance with specified KVStore name and ID.
     */
    public SecurityMetadata(final String storeName, final String id) {
        this.kvstoreName = storeName;
        this.id = id;
    }

    public String getKVStoreName() {
        return kvstoreName;
    }

    public String getId() {
        return id;
    }

    /**
     * Returns a KVStore user with the specified name.
     *
     * @param name user name
     * @return the user, null if no user with the specified name is found.
     */
    public KVStoreUser getUser(final String name) {
        if ((name == null) || name.isEmpty()) {
            return null;
        }

        final Collection<KVStoreUser> users = kvstoreUserMap.getAll();
        for (final KVStoreUser user : users) {
            if (name.equals(user.getName())) {
                return user;
            }
        }
        return null;
    }

    /**
     * Returns a KVStore user with the specified ID.
     *
     * @param uid user id
     * @return the user, null if no user with the specified ID is found.
     */
    public KVStoreUser getUserById(final String uid) {
        return kvstoreUserMap.get(uid);
    }

    /**
     * Returns all users.
     */
    public Collection<KVStoreUser> getAllUsers() {
        return kvstoreUserMap.getAll();
    }

    /**
     * Returns the internal KVStoreUser map.
     *
     * @return KVStoreUserMap
     */
    public KVStoreUserMap getKVStoreUserMap() {
        return kvstoreUserMap;
    }

    /**
     * Adds a new user into the map.
     *
     * @param user user instance
     * @return the newly added user
     */
    public KVStoreUser addUser(final KVStoreUser user) {
        return kvstoreUserMap.add(user);
    }

    /**
     * Removes a user with the specified ID. If the user does not exist, an
     * IllegalArgumentException will be thrown.
     *
     * @param uid user ID
     * @return the removed user if successful
     * @throw IllegalArgumentException if no user with specified ID is found
     */
    public KVStoreUser removeUser(final String uid) {
        return kvstoreUserMap.remove(uid);
    }

    /**
     * Updates an existing user of the specified ID with a new user data. If
     * the user does not exist, an IllegalArgumentException will be thrown.
     *
     * @param uid user ID
     * @param user user
     * @return the new user if sucessful
     * @throw IllegalArgumentException if no user with specified ID is found
     */
    public KVStoreUser updateUser(final String uid, final KVStoreUser user) {
        return kvstoreUserMap.update(uid, user);
    }

    /**
     * Verifies if the plain password works for the given user.
     *
     * @param userName user name
     * @param password the plain password to verify
     * @return true iff. the specified password works
     */
    public boolean verifyUserPassword(final String userName,
                                      final char[] password) {
        final KVStoreUser user = getUser(userName);
        if (user == null) {
            return false;
        }
        return user.verifyPassword(password);
    }

    /**
     * Returns the brief and detailed description of all users for showing. The
     * descriptions in the returned Map are ordered by user name.
     *
     * @return a SortedMap of user descriptions which are sorted by user name
     */
    public SortedMap<String, UserDescription> getUsersDescription() {
        final Collection<KVStoreUser> users = getAllUsers();

        /* Use TreeMap to order the desctiption by users' names */
        final TreeMap<String, UserDescription>
            userInfoMap = new TreeMap<String, UserDescription>();
        for (final KVStoreUser user : users) {
            userInfoMap.put(user.getName(), user.getDescription());
        }
        return userInfoMap;
    }

    /**
     * Applies a series of change on the security metadata. The changes applied
     * should be in increasing sequence number order.
     * <p>
     * Note that the object may be corrupted because of unexpected exceptions
     * while applying changes. For simplicity we do not implement recovering
     * codes for such failure. So a safe way to call the operation is to get
     * a deep copy of the object first, and then apply the changes on the copy.
     * If the applying failed, the corrupted copy could be simply discarded.
     *
     * @param changes list of SecurityMDChange
     * @return true if any update is made
     */
    public boolean apply(final List<SecurityMDChange> changes) {
        if (changes == null || changes.isEmpty()) {
            return false;
        }

        if (changes.get(0).getSeqNum() > (getSequenceNumber() + 1)) {
            throw new IllegalStateException(
                "Unexpected gap in security metadata sequence. Current " +
                "sequence = " + getSequenceNumber() + ", first change =" +
                changes.get(0).getSeqNum());
        }

        int changedCount = 0;
        for (final SecurityMDChange change : changes) {
            /* Skip the change already existed */
            if (change.getSeqNum() <= getSequenceNumber()) {
                continue;
            }

            if (change.getElementType() == SecurityElementType.KVSTOREUSER) {
                kvstoreUserMap.apply(change);
                changedCount++;
            } else {
                throw new IllegalArgumentException(
                    "Unknown security element type: " +
                    change.getElementType());
            }
        }
        return changedCount > 0;
    }

    @Override
    public MetadataType getType() {
        return MetadataType.SECURITY;
    }

    /**
     * Returns the current change sequence number associated with security
     * metadata. If no change has been logged, the
     * {@code Metadata.EMPTY_SEQUENCE_NUMBER} will be return.
     */
    @Override
    public int getSequenceNumber() {
        return sequenceNumber;
    }

    /**
     * Returns a list of security metadata changes starting with
     * <code>startSeqNum</code>. The list of changes will be wrapped into the
     * SecurityMetadataInfo object and will be used in the metadata framework.
     *
     * @param startSeqNum the inclusive start of the sequence of
     * changes to be returned
     *
     * @return the list of changes starting with startSeqNum and ending with
     * getSequenceNumber(). Return null if startSequenceNumber >
     * getSequenceNumber() or if the security metadata does not contain changes
     * at startSeqNum because they have been discarded.
     */
    @Override
    public SecurityMetadataInfo getChangeInfo(int startSeqNum) {
        return new SecurityMetadataInfo(this, getChanges(startSeqNum));
    }

    /**
     * Gets the first sequence number of the changes stored in the tracker.
     *
     * @return the first sequence number of the changes, or -1 if no change
     * exists yet.
     */
    public int getFirstChangeSeqNum() {
        return changeList.size() == 0 ? -1 : changeList.getFirst().getSeqNum();
    }

    /**
     * Obtain a list of all changes that have a seqNum of startSeqNum or higher.
     * Returns null if a change with seqNum == startSeqNum is not present in
     * our change list.
     *
     * @param startSeqNum starting sequence number
     * @return a list containing all qualified changes, or null if a change
     * with seqNum == startSeqNum is not present.
     */
    public List<SecurityMDChange> getChanges(final int startSeqNum) {
        /*
         * Return null if there's no change currently, or the starting seqNum
         * is out of the boundary of stored changes.
         */
        if (changeList.isEmpty()) {
            return null;
        }

        if (startSeqNum < getFirstChangeSeqNum() ||
            startSeqNum > changeList.getLast().getSeqNum()) {
            return null;
        }

        final List<SecurityMDChange> retChanges =
                new LinkedList<SecurityMDChange>();
        for (final SecurityMDChange change : changeList) {
            if (change.getSeqNum() >= startSeqNum) {
                retChanges.add(change.clone());
            }
        }
        return retChanges;
    }

    /**
     * Get all changes.
     */
    public List<SecurityMDChange> getChanges() {
        return getChanges((changeList.size() == 0) ?
                0 : changeList.getFirst().seqNum);
    }

    /**
     * Discard a part of changes whose seqNum is less then the newStartSeqNum.
     * This is to shorten the change list, and thus save memory.
     */
    public void discardChanges(final int newStartSeqNum) {
        final Iterator<SecurityMDChange> itr = changeList.iterator();
        while (itr.hasNext() && (itr.next().getSeqNum() < newStartSeqNum)) {
            itr.remove();
        }
    }

    /**
     * Create and return a deep copy of this SecurityMetadata object.
     *
     * @return the new SecurityMetadata instance
     */
    public SecurityMetadata getCopy() {
        final byte[] mdBytes = SerializationUtil.getBytes(this);
        return SerializationUtil.getObject(mdBytes, this.getClass());
    }

    @Override
    public String toString() {
        return String.format("SecurityMetadata id=%s seq# %d", id,
                             sequenceNumber);
    }

    /**
     * Bump the sequence number, and log the change into the list.
     */
    public void logChange(final SecurityMDChange change) {
        sequenceNumber++;
        change.setSeqNum(sequenceNumber);
        changeList.add(change);
    }

    /**
     * Map for storing elements of KVStoreUser.
     */
    public static class KVStoreUserMap extends ElementMap<KVStoreUser> {

        private static final long serialVersionUID = 1L;

        public KVStoreUserMap(final SecurityMetadata securityMD) {
            super(securityMD);
        }

        @Override
        String nextId() {
            /* A user id is a digital number with prefix of "u". */
            return "u" + (++idSequence);
        }

        @Override
        SecurityElementType getElementType() {
            return SecurityElementType.KVSTOREUSER;
        }
    }

    /**
     * Abstract class for composed element of security metadata.
     */
    protected abstract static class SecurityElement
        implements Serializable, Cloneable {

        private static final long serialVersionUID = 1L;

        /*
         * The unique element id. It's null if the element has not been
         * "added" to the ElementMap.
         */
        private String elementId;

        /**
         * Ctor with un-initialized elementId. The initialization of the
         * elementId will be deferred to, for example, when the element is
         * added in the storage container.
         */
        protected SecurityElement() {
        }

        protected SecurityElement(final SecurityElement other) {
            this.elementId = other.elementId;
        }

        public void setElementId(final String eId) {
            this.elementId = eId;
        }

        public String getElementId() {
            return elementId;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof SecurityElement)) {
                return false;
            }
            if (this == obj) {
                return true;
            }
            final SecurityElement other = (SecurityElement) obj;
            return (elementId == other.elementId ||
                    (elementId != null && elementId.equals(other.elementId)));
        }

        /**
         * Derived classes are required to calculate the hash code using their
         * own properties.
         */
        @Override
        public abstract int hashCode();

        @Override
        public abstract SecurityElement clone();

        public abstract SecurityElementType getElementType();
    }

    /**
     * Class holding a collection of SecurityElement. Use Map as the collection
     * to build a mapping between the element Ids and the elements.
     * <p>
     * Note that any operation modifying the elements may encounter unexpected
     * exceptions, and thus lead to corrupted data. The caller should handle
     * such cases.
     */
    protected abstract static class ElementMap<T extends SecurityElement>
        implements Serializable {

        private static final long serialVersionUID = 1L;

        private SecurityMetadata securityMD;

        private final HashMap<String, T> elementMap =
                new HashMap<String, T>();

        int idSequence;

        public ElementMap(final SecurityMetadata securityMD) {
            super();
            this.securityMD = securityMD;
        }

        T get(final String elementId) {
            return elementMap.get(elementId);
        }

        Collection<T> getAll() {
            return elementMap.values();
        }

        /**
         * Add a new element into the ElementMap. A new internal ElementID will
         * be generated and assigned to the new element.
         *
         * @param element the new element
         * @return the previous element will the same elementID if exists,
         * otherwise null will be returned.
         */
        T add(final T element) {
            return add(nextId(), element);
        }

        /**
         * Add a new element with its ElementID into the ElementMap. If the add
         * operation fails, i.e., an existing element with the same ID is found,
         * an IllegalStateException will be thrown. The caller should catch and
         * handle the exception.
         *
         * @param elementId ID of the element
         * @param element the new element
         * @return the previous element will the same ElementID if exists,
         * otherwise null will be returned.
         */
        T add(final String elementId, final T element) {
            element.setElementId(elementId);
            final T prev = elementMap.put(elementId, element);
            securityMD.logChange(new SecurityMDChange.Add(element));
            if (prev != null) {
                throw new IllegalStateException("Element " + prev +
                                                " was been overwritten by " +
                                                element);
            }
            return element;
        }

        T update(final String elementId, final T element) {
            if (elementMap.get(elementId) == null) {
                throw new IllegalArgumentException(
                    "Element " + elementId + " absent from security metadata.");
            }
            element.setElementId(elementId);
            final T prev = elementMap.put(elementId, element);
            assert prev != null;
            securityMD.logChange(new SecurityMDChange.Update(element));
            return element;
        }

        T remove(final String elementId) {
            final T prev = elementMap.remove(elementId);
            if (prev == null) {
                throw new IllegalArgumentException("Element " + elementId +
                                                   "was not found.");
            }
            securityMD.logChange(
                new SecurityMDChange.Remove(elementId, getElementType()));
            return prev;
        }

        /**
         * Apply a change to the current ElementMap object
         *
         * @param change the change of element
         */
        @SuppressWarnings("unchecked")
        void apply(final SecurityMDChange change) {
            final SecurityMDChangeType changeType = change.getChangeType();
            final String changeElementId = change.getElementId();

            switch (changeType) {
            case REMOVE:
                remove(changeElementId);
                break;

            case ADD:
                final String newId = nextId();
                if (!newId.equals(changeElementId)) {
                    throw new IllegalStateException(
                        "Element sequence out of sync; expected: " +
                         newId + " replayId: " + changeElementId);
                }
                add(newId, (T) change.getElement());
                break;

            case UPDATE:
                update(changeElementId, (T) change.getElement());
                break;

            default:
                throw new IllegalStateException(
                    "Unknown change type: " + changeType);
            }

            /*
             * SecurityMetadata should have the same sequence with the change
             * after applying it.
             */
            if (securityMD.getSequenceNumber() != change.getSeqNum()) {
                throw new IllegalStateException(
                    "Mismatched security metadata change sequence: log# " +
                    change.getSeqNum() + ", replay# " +
                    securityMD.getSequenceNumber());
            }
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            final int result = 17 * prime + elementMap.hashCode();
            return result * prime + idSequence;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            final ElementMap<?> other = (ElementMap<?>) obj;
            return (idSequence == other.idSequence) &&
                   elementMap.equals(other.elementMap);
        }

        abstract String nextId();
        abstract SecurityElementType getElementType();
    }
}
