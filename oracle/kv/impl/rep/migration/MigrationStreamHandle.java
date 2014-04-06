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

package oracle.kv.impl.rep.migration;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Transaction;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.topo.PartitionId;

/**
 * A thread local handle to the migration stream.
 *
 * This class implements a thread local variable which is accessed during a
 * client operation. Specifically, when the client operation creates a
 * transaction it should initialize the thread local by calling
 * MigrationStreamHandle.initialize(). Then as the client operations are
 * executed the handle's addUpdate() and addDelete() methods should be invoked
 * for each update and delete operation respectively.
 *
 * At the end of the client operation but before txn.commit() is invoked, the
 * prepare() method on the handle should be invoked. The done() method must
 * always be called once client operations are completed.
 *
 * When initialize() is invoked, a check is made to see if the partition
 * that the operation affecting is being migrated. If so, the thread local
 * handle will forward the update and delete operations to the migration stream.
 * If no migration is active, the handle will do nothing.
 *
 * The base class does nothing. It provides the interface for the
 * normal (non-migrating) case.
 *
 * The forwarding handle is implemented by the MigratingHandle subclass.
 */
public class MigrationStreamHandle {

    private static final ThreadLocal<MigrationStreamHandle> handle =
            new ThreadLocal<MigrationStreamHandle>() {

        @Override
        protected synchronized MigrationStreamHandle initialValue() {

            /*
             * The thread local must be explicitly set by first invoking
             * MigrationStreamHandle.initialize().
             */
            throw new IllegalStateException("Handle not initialized");
        }
    };

    /*
     * Normal non-forwarding handle. This instance is a no-op and can be
     * shared.
     */
    private static final MigrationStreamHandle noop =
                                            new MigrationStreamHandle();

    /**
     * Returns the thread local migration stream handle. Note that this method
     * throws an IllegalStateException unless initialize() has been called.
     *
     * @return a stream handle
     */
    public static MigrationStreamHandle get() {
        return handle.get();
    }

    /**
     * Initializes the thread local migration stream handle.
     *
     * @param repNode the rep node
     * @param partitionId a partition ID
     * @param txn a transaction
     * @return a stream handle
     */
    public static MigrationStreamHandle initialize(RepNode repNode,
                                                   PartitionId partitionId,
                                                   Transaction txn) {
        assert checkForStaleHandle();

        final MigrationService service = repNode.getMigrationManager().
                                                        getMigrationService();
        final MigrationSource source =
                        (service == null) ? null :
                                            service.getSource(partitionId);

        final MigrationStreamHandle h =
                    (source == null) ? noop :
                                       new MigratingHandle(source, txn);
        handle.set(h);
        return h;
    }

    /**
     * Checks if the thread local is stale (left over from a previous stream).
     * If so an exception is thrown, otherwise true is returned.
     */
    private static boolean checkForStaleHandle() {
        try {
            MigrationStreamHandle h = handle.get();
            throw new IllegalStateException("Handle still around? " + h);
        } catch (IllegalStateException ise) {
            /* Expected */
            return true;
        }
    }

    private MigrationStreamHandle() {
    }

    /**
     * Inserts a PUT record into migration stream if partition migration
     * is in progress. Otherwise this method does nothing.
     *
     * @param key
     * @param value
     */
    public void addPut(DatabaseEntry key, DatabaseEntry value) {
        /* NOOP */
    }

    /**
     * Inserts a DELETE record into migration stream if partition migration
     * is in progress. Otherwise this method does nothing.
     *
     * @param key
     */
    public void addDelete(DatabaseEntry key) {
        /* NOOP */
    }

    /**
     * Inserts a PREPARE message into the migration stream if partition
     * migration is in progress. Otherwise this method does nothing. This
     * method should be invoked before the client transaction is committed.
     * The PREPARE message signals that the operations associated with this
     * transaction are about to be committed. No further operations can be
     * added once prepared.
     */
    public void prepare() {
        /* NOOP */
    }

    /**
     * Signals that this operations associated with this transaction are done.
     * Depending on the transaction's outcome, a COMMIT or ABORT message is
     * inserted into the migration stream.
     */
    public void done() {
        /* NOOP */
        handle.remove();
    }

    @Override
    public String toString() {
        return "MigrationStreamHandle[]";
    }

    /**
     * Subclass for when migration is taking place.
     */
    private static class MigratingHandle extends MigrationStreamHandle {

        private final MigrationSource source;

        /* Transaction associated with this thread. */
        private final Transaction txn;

        /*
         * Number of DB operations that have been sent. Not all add*() calls
         * will result in sent messages due to key filtering.
         */
        private int opsSent = 0;

        /* True if prepare() has been called. */
        private boolean prepared = false;

        /* True if done() has been called. */
        private boolean done = false;

        private MigratingHandle(MigrationSource source, Transaction txn) {
            super();
            this.source = source;
            this.txn = txn;
        }

        @Override
        public void addPut(DatabaseEntry key, DatabaseEntry value) {
            assert !prepared;
            assert key != null;
            assert value != null;
            if (source.sendPut(txn.getId(), key, value)) {
                opsSent++;
            }
        }

        @Override
        public void addDelete(DatabaseEntry key) {
            assert !prepared;
            assert key != null;
            if (source.sendDelete(txn.getId(), key)) {
                opsSent++;
            }
        }

        @Override
        public void prepare() {
            assert !prepared;
            assert !done;

            /*
             * Don't bother sending PREPARE (or COMMIT or ABORT) messages if no
             * DB operations have been sent.
             */
            if (opsSent > 0) {
                source.sendPrepare(txn.getId());
            }
            prepared = true;
        }

        @Override
        public void done() {
            assert !done;

            try {
                if (opsSent > 0) {
                    source.sendResolution(txn.getId(),
                                          txn.getState().
                                           equals(Transaction.State.COMMITTED));
                }
            } finally {
                done = true;
                /* Remove handle */
                super.done();
            }
        }

        @Override
        public String toString() {
            return "MigratingHandle[" + prepared + ", " + done +
                   ", " + opsSent + "]";
        }
    }
}
