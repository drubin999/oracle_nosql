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

package oracle.kv.impl.rep;

import java.util.logging.Logger;

import oracle.kv.KVVersion;
import oracle.kv.impl.fault.ProcessFaultException;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.impl.util.VersionUtil;

import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

/**
 * Maintains the local and replicated version information, consolidating the
 * information whenever necessary.
 */
public class VersionManager {

    /* The name of the local database used to store version information. */
    private static final String VERSION_DATABASE_NAME = "VersionDatabase";
    private static final String VERSION_KEY = "LocalVersion";
    private static final DatabaseEntry VKEY_ENTRY = new DatabaseEntry();

    static {
        StringBinding.stringToEntry(VERSION_KEY, VKEY_ENTRY);
    }

    /* The RN whose version information is being managed. */
    private final RepNode repNode;

    private final Logger logger;

    public VersionManager(Logger logger, RepNode repNode) {
        this.logger = logger;
        this.repNode = repNode;
    }

    /**
     * Invoked at RN startup to check that the code version matches the version
     * stored in the RN's environment. This method is invoked as soon as the
     * environment is opened before any access to its contents. If the
     * version database contains an older version than the current code
     * version, it updates the version database with KVVersion.CURRENT_VERSION.
     *
     * @param env the environment associated with the RN. Note the deliberate
     * typing of the parameter as Environment instead of ReplicateEnvironment
     * to emphasize that no changes are made to any replicated databases.
     */
    void checkCompatibility(Environment env) {

        Database vdb = null;

        try {
            vdb = openDb(env);

            final KVVersion localVersion = getLocalVersion(vdb);

            if (localVersion != null) {

                /* If the old version is the same, nothing to do */
                if (localVersion.equals(KVVersion.CURRENT_VERSION)) {
                    return;
                }

                /* Check for upgrade (or downgrade) compatibility. */
                try {
                    VersionUtil.checkUpgrade(localVersion);
                } catch (IllegalStateException ise) {
                    throw new ProcessFaultException(ise.getMessage(), ise);
                }
            }

            /* Note that the version change may be a patch downgrade */
            repNode.versionChange(env, localVersion);

            /*
             * Finally, install the current version after any earlier changes
             * have been completed.
             */
            final DatabaseEntry vdata = new DatabaseEntry(SerializationUtil.
                getBytes(KVVersion.CURRENT_VERSION));
            final OperationStatus status = vdb.put(null, VKEY_ENTRY, vdata);
            if (status != OperationStatus.SUCCESS) {
                throw new IllegalStateException("Could not install new version");
            }

            /*
             * Ensure it's in stable storage, since the version database is
             * non-transactional.
             */
            DbInternal.getEnvironmentImpl(env).getLogManager().flush();
            logger.info("Local Environment version updated to: " +
                        KVVersion.CURRENT_VERSION +
                        " Previous version: " +
                        ((localVersion == null) ?
                         " none" :
                         localVersion.getVersionString()));
        } finally {
            /*
             * Note that since the version database is a non-transactional
             * local database, there is no transaction to abort and undo
             * changes, so the sequence of changes must be carefully organized
             * so they can be retried at a higher level without an undo. That
             * is, they must be idempotent.
             */
            if (vdb != null) {
                TxnUtil.close(logger, vdb, "version");
            }
        }
    }

    /**
     * Returns the current version stored in the version database.
     *
     * @param env the RN's environment
     *
     * @return the KVVersion or null
     */
    public static KVVersion getLocalVersion(Logger logger, Environment env) {
        Database db = null;

        try {
            db = openDb(env);
            return getLocalVersion(db);
        } finally {
            if (db != null) {
                TxnUtil.close(logger, db, "version");
            }
        }
    }

    /* Returns the version from the version database. */
    private static KVVersion getLocalVersion(Database versionDB) {
        final DatabaseEntry vdata = new DatabaseEntry();
        final OperationStatus status =
            versionDB.get(null, VKEY_ENTRY, vdata, LockMode.DEFAULT);

        return (status == OperationStatus.SUCCESS) ?
            SerializationUtil.getObject(vdata.getData(), KVVersion.class) :
            null;
    }

    /**
     * Returns a handle to the version DB.
     */
    private static Database openDb(Environment env) {
        final DatabaseConfig dbConfig =
            new DatabaseConfig().
                setTransactional(false). /* local db cannot be transactional */
                setAllowCreate(true).
                setReplicated(false);

        return env.openDatabase(null, VERSION_DATABASE_NAME, dbConfig);
    }
}
