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

package oracle.kv.impl.util;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.persist.EntityStore;

/**
 * Utility class for managing transactions
 */
public class TxnUtil {

    /**
     * A wrapper for Transaction.abort that:
     *
     * 1) Ignores an IAE and ISE resulting from a closed environment. The
     * environment is typically closed by some other thread that may have
     * detected a environment failure and closed the environment as part of
     * its exception handling.
     *
     * 2) Ignore UnknownMasterException and ReplicaWriteException if the
     * transaction is already closed. These exceptions may result from a
     * master->replica transition.
     *
     * 3) Skips the abort operation altogether when the environment is invalid,
     * since otherwise the environment invalidation exception is thrown again.
     * This can be problematic if the abort is in a finally clause and the
     * exception was already handled in the immediately preceding catch clause.
     */
    public static void abort(Transaction transaction) {

        if (transaction == null) {
            return;
        }

        final Environment env = DbInternal.getEnvironment(transaction);

        try {
            if ((env != null) && env.isValid()) {
                /*
                 * Only abort if environment is open and valid to avoid
                 * spurious exceptions.
                 */
                transaction.abort();
            }
        } catch (IllegalArgumentException iae) {
            ignoreIfClosed(env, iae);
        } catch (IllegalStateException ise) {
            ignoreIfClosedOrAbort(transaction, env, ise);
        } catch (UnknownMasterException ume) {
            ignoreIfClosedOrAbort(transaction, env, ume);
        } catch (ReplicaWriteException rwe) {
            ignoreIfClosedOrAbort(transaction, env, rwe);
        }
    }

    /**
     * Checks whether the environment has been closed, and if not, throws
     * the specified exception.
     */
    private static void ignoreIfClosed(Environment env, RuntimeException re) {
        assert env != null;

        if (env.isValid()) {
            throw re;
        }
    }

    /**
     * Checks whether the environment has been closed or the transaction is in
     * MUST_ABORT, or has already been aborted, and if not, throws the
     * specified exception.
     */
    private static void ignoreIfClosedOrAbort(Transaction transaction,
                                              Environment env,
                                              RuntimeException re) {
        assert env != null;

        if (Transaction.State.MUST_ABORT.equals(transaction.getState()) ||
            Transaction.State.ABORTED.equals(transaction.getState())) {
            return;
        }

        if (!env.isValid()) {
            return;
        }

        throw re;
    }

    /**
     * Close the cursor, but suppress ISE resulting from the database already
     * being closed. This can happen when a RN is being shutdown, or during
     * partition migration when a partition database is closed after the
     * partition has been migrated.
     *
     * @param cursor the cursor to be closed
     */
    public static void close(Cursor cursor) {
        try {
            cursor.close();
        } catch (IllegalStateException ise) {
            /* Ignore, db closed */
        }
    }

    /**
     * Closes the database and ignores exceptions that may result from
     * asynchronous closes of the environment. The method effectively
     * suppresses any runtime exception that results from an invalid
     * environment all other exceptions are propagated out since they cannot be
     * explained in the context of a valid environment.
     *
     * @param logger used to log message related to exceptions when closing
     * the database
     * @param db the database to be closed
     * @param dbType a descriptive string to be used in log messages, since
     * the db.getDatabaseName() can provoke an exception. This would be worth
     * fixing in JE.
     *
     * @return null if the database was closed cleanly. Otherwise it
     * returns the suppressed exception
     */
    public static RuntimeException close(Logger logger,
                                         final Database db,
                                         String dbType) {

       final Environment env = db.getEnvironment();

       if  (env == null) {
           /* Environment was closed and consequently the db */
           return null;
       }

        return new WithDbCloseExceptionHandler() {

            @Override
            void closeInternal() {
                db.close();
            }

        }.close(logger, env, dbType);
    }

    /**
     * As above, except that it represents the close of an EntityStore
     *
     * @param logger used to log message related to exceptions when closing
     * the entity store
     * @param env the environment associated with the entity store, it must
     * not be null
     * @param store the store to be closed
     * @param entityType description string describing the store being closed
     *
     * @return null if the store was closed cleanly. Otherwise it returns the
     * suppressed exception
     */
    public static RuntimeException close(Logger logger,
                                         Environment env,
                                         final EntityStore store,
                                         String entityType) {

        return new WithDbCloseExceptionHandler() {

            @Override
            void closeInternal() {
                store.close();
            }

        }.close(logger, env, entityType);
    }

    /**
     * Utility class defines the exception handling around the close of a
     * database or an entity store.
     */
    private static abstract class WithDbCloseExceptionHandler {
        abstract void closeInternal();

        RuntimeException close(Logger logger,
                               Environment env,
                               String dbType) {
            try {
                closeInternal();
                return null;
            } catch (IllegalStateException e) {

                /*
                 * If there are open cursors associated with the database
                 * handle. Ignore the exception unconditionally.
                 */
                logCloseException(logger, dbType, e);
                return e;
            } catch (RuntimeException re) {

                /*
                 * EFE, as well as NPE exceptions are handled here.
                 */
                if (env.isValid()) {
                    /* Unknown failure, propagate the RE */
                    logger.log(Level.SEVERE,
                               "Unexpected exception closing " + dbType +
                               " database.",
                               re);
                    throw re;
                }

                logCloseException(logger, dbType, re);
                return re;
            }
        }


        /**
         * Utility message to log a consistent INFO level message for benign
         * exceptions on a database close.
         */
        private void logCloseException(Logger logger,
                                       String dbType,
                                       RuntimeException re) {
            logger.info("Ignoring exception closing " + dbType + " database " +
                        " Exception:" + re.getClass().getName() +
                        " Message:" + re.getMessage());
        }
    }
}
