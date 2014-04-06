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

package oracle.kv.impl.admin.param;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.topo.StorageNodeId;

import com.sleepycat.persist.model.Persistent;

/**
 * A StorageNodePool is just a set of StorageNodes identified by their
 * ResourceIds, which can be considered for deployment by the Planner.
 */
@Persistent
public class StorageNodePool implements Serializable, Iterable<StorageNodeId> {

    private static final long serialVersionUID = 1L;
	
    private String name;
    private Set<StorageNodeId> snIds;
    transient ReadWriteLock rwl = new ReentrantReadWriteLock();

    public StorageNodePool(String name) {
        this.name = name;
        snIds = new TreeSet<StorageNodeId>();
    }

    StorageNodePool() {
    }

    public String getName() {
        return name;
    }

    public List<StorageNodeId> getList() {
        return new ArrayList<StorageNodeId>(snIds);
    }

    public void add(StorageNodeId snId) {
        Lock w = rwl.writeLock();
        try {
            w.lock();
            if (snIds.contains(snId)) {
                throw new IllegalCommandException
                    ("SN Pool " + name + " already contains SN " + snId);
            }
            snIds.add(snId);
        } finally {
            w.unlock();
        }
    }

    public void remove(StorageNodeId snId) {
        Lock w = rwl.writeLock();
        try {
            w.lock();
            if (!snIds.contains(snId)) {
                throw new IllegalCommandException
                    ("SN Pool " + name + " does not contain SN " + snId);
            }
            snIds.remove(snId);
        } finally {
            w.unlock();
        }
    }

    public void clear() {
        Lock w = rwl.writeLock();
        try {
            w.lock();
            snIds.clear();
        } finally {
            w.unlock();
        }
    }

    /**
     * Calling freeze() on this StorageNodePool will prevent it from being
     * modified until thaw() is called.
     */
    public void freeze() {
        rwl.readLock().lock();
    }

    public void thaw() {
        rwl.readLock().unlock();
    }

    /**
     * This iterator will loop over and over the storage node list, and will
     * never return null.
     */
    public Iterator<StorageNodeId> getLoopIterator() {
        return new LoopIterator(this);
    }

    public int size() {
        return snIds.size();
    }

    public boolean contains(StorageNodeId snid) {
        return snIds.contains(snid);
    }

    @Override
    public Iterator<StorageNodeId> iterator() {
        return snIds.iterator();
    }

    /**
     * This iterator will loop over the storage node list, and will never
     * return null.
     */
    private static class LoopIterator implements Iterator<StorageNodeId> {
        private Iterator<StorageNodeId> listIter ;
        private final StorageNodePool pool;

        private LoopIterator(StorageNodePool pool) {

            if (pool.snIds.size() == 0) {
                throw new IllegalCommandException
                    ("Storage Node Pool " + pool.name + " is empty.");
            }
            this.pool = pool;
            listIter = pool.snIds.iterator();
        }

        @Override
        public boolean hasNext() {
            /* This loops, so there's always next item. */
            return true;
        }

        @Override
        public StorageNodeId next() {
            if (!listIter.hasNext()) {
                listIter = pool.snIds.iterator();
            }
            return listIter.next();
        }

        @Override
        public void remove() {
            /* We shouldn't be modifying the StorageNodePool. */
            throw new UnsupportedOperationException
                ("Intentionally unsupported");
        }
    }
}
