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
package oracle.kv.impl.security.filestore;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import oracle.kv.impl.security.PasswordStore;
import oracle.kv.impl.security.PasswordStoreException;
import oracle.kv.impl.security.util.SecurityUtils;

/**
 * Open-source implementation of PasswordStore.
 * The FileStore uses a single file to house a PasswordStore.  The store
 * content is currently saved as clear text.
 */
public class FileStore implements PasswordStore {

    /* The File containing the password store */
    private final File storeLocation;

    /* The data structure containing the secrets */
    private SecretHash secretHash = null;

    /* If true, the store needs to be written back to the file */
    private boolean modified = false;

    /**
     * There are two types of information stored in a password store:
     * secrets and logins
     * A "secret" is a named password, whereas a login is a pair of
     * user/password associated with a database as a unique key.
     *
     * The following constants are conventions established to allow
     * tracking of both logins and secrets in the same hash structure.
     *
     * The login keys look like
     *     login.user.<database>
     *     login.password.<database>
     * and secrets look like this:
     *     secret.<user-specified-alias>
     */
    private static final String LOGIN_USER_PREFIX = "login.user.";
    private static final String LOGIN_PW_PREFIX = "login.password.";
    private static final String SECRET_PREFIX = "secret.";

    /**
     * Constructor
     * Prepare for access to a file store, which might not yet exist.
     */
    FileStore(File storeFile) {
        this.storeLocation = storeFile;
    }

    /**
     * Create a new file store.  The file where the store will be stored
     *        must not exist yet.
     * @param passphrase a passphrase for the store.  This option is not
     *        currently supported and results in an
     *        UnsupportedOperationException.
     * @throw IllegalStateException if this FileStore has already had
     *        a create or open operation executed.
     * @throw UnsupportedOperationException if a non-null passphrase is
     *        specified
     * @throw IOException if there are IO errors accessing the file store
     * @throw PasswordStoreException if there errors in the password store
     *        itself.
     */
    @Override
    public boolean create(char[] passphrase) throws IOException {

        assertNotInitialized();

        if (passphrase != null) {
            throw new UnsupportedOperationException(
                "Passphrases are not supported");
        }

        if (storeLocation.exists()) {
            throw new PasswordStoreException(
                "A file already exists at this location");
        }

        File parentDir = storeLocation.getParentFile();
        if (parentDir == null) {
            parentDir = new File(".");
        }

        if (!parentDir.exists() || !parentDir.isDirectory()) {
            throw new PasswordStoreException(
                "The directory for the password file does not exist");
        }

        if (!parentDir.canWrite()) {
            throw new PasswordStoreException(
                "The directory for the password file is not writable");
        }

        final SecretHash newSecretHash = new SecretHash();

        try {
            newSecretHash.write(storeLocation);
        } catch (IOException ioe) {
            // TBD: special interpretation of any exceptions?
            throw ioe;
        }

        if (!SecurityUtils.makeOwnerAccessOnly(storeLocation)) {
            throw new PasswordStoreException(
                "Unable to set access permissions for file. " +
                "Correct manually before using the password store");
        }

        /* Make sure we can read it back */
        try {
            newSecretHash.read(storeLocation);
        } catch (IOException ioe) {
            /* Not clear how this could happen with a newly created store */
            throw new PasswordStoreException(
                "Error retrieving passwords from file", ioe);
        }

        this.secretHash = newSecretHash;
        return true;
    }

    /**
     * Open an existing filestore.
     * @throw IllegalStateException if this FileStore has already had
     *        a create or open operation executed.
     * @throw UnsupportedOperationException if a non-null passphrase is
     *        specified
     * @throw IOException if there are IO errors accessing the store
     */
    @Override
    public boolean open(char[] passphrase) throws IOException {

        assertNotInitialized();

        if (!storeLocation.exists()) {
            throw new PasswordStoreException(
                "No file exists at this location");
        }

        if (passphrase != null) {
            throw new UnsupportedOperationException(
                "Passphrases are not supported by this implementation");
        }

        final SecretHash newSecretHash = new SecretHash();
        try {
            newSecretHash.read(storeLocation);
        } catch (IOException ioe) {
            throw ioe;
        }
        this.secretHash = newSecretHash;

        return true;
    }

    @Override
    public Collection<String> getSecretAliases() throws IOException {

        assertInitialized();

        final Set<String> secretAliases = new HashSet<String>();
        final Iterator<String> e = secretHash.aliases();
        while (e.hasNext()) {
            String alias = e.next();
            if (alias.startsWith(SECRET_PREFIX)) {
                alias = alias.substring(SECRET_PREFIX.length());
                secretAliases.add(alias);
            }
        }

        return secretAliases;
    }

    @Override
    public char[] getSecret(String alias) throws IOException {

        assertInitialized();

        return secretHash.getSecret(SECRET_PREFIX + alias);
    }

    @Override
    public boolean setSecret(String alias, char[] secret)throws IOException  {

        assertInitialized();

        final String internalAlias = SECRET_PREFIX + alias;
        final boolean updated = secretHash.containsAlias(internalAlias);
        secretHash.setSecret(internalAlias, secret);
        modified = true;
        return updated;
    }

    @Override
    public boolean deleteSecret(String alias) throws IOException {

        assertInitialized();

        final String internalAlias = SECRET_PREFIX + alias;
        if (secretHash.containsAlias(internalAlias)) {
            secretHash.deleteSecret(internalAlias);
            modified = true;
            return true;
        }
        return false;
    }

    @Override
    public Collection<LoginId> getLogins() throws IOException {

        assertInitialized();

        final Set<LoginId> logins = new HashSet<LoginId>();

        final Iterator<String> e = secretHash.aliases();
        while (e.hasNext()) {
            final String alias = e.next();
            if (alias.startsWith(LOGIN_USER_PREFIX)) {
                final String db = alias.substring(LOGIN_USER_PREFIX.length());
                final String user = new String(secretHash.getSecret(alias));
                logins.add(new LoginId(db, user));
            }
        }

        return logins;
    }

    @Override
    public boolean setLogin(LoginId login, char[] password) throws IOException {

        assertInitialized();

        final String dbAlias = LOGIN_USER_PREFIX + login.getDatabase();
        final boolean exists = secretHash.containsAlias(dbAlias);
        secretHash.setSecret(dbAlias, login.getUser().toCharArray());
        secretHash.setSecret(LOGIN_PW_PREFIX + login.getDatabase(),
                             password);
        modified = true;
        return exists;
    }

    @Override
    public LoginId getLoginId(String database) throws IOException {

        assertInitialized();

        final char[] user = secretHash.getSecret(LOGIN_USER_PREFIX + database);
        if (user == null) {
            return null;
        }
        return new LoginId(database, new String(user));
    }

    @Override
    public char[] getLoginSecret(String database) throws IOException {

        assertInitialized();

        return secretHash.getSecret(LOGIN_PW_PREFIX + database);
    }

    @Override
    public boolean deleteLogin(String db) {

        assertInitialized();

        if (secretHash.getSecret(LOGIN_USER_PREFIX + db) == null &&
            secretHash.getSecret(LOGIN_PW_PREFIX + db) == null) {
            return false;
        }

        secretHash.deleteSecret(LOGIN_USER_PREFIX + db);
        secretHash.deleteSecret(LOGIN_PW_PREFIX + db);
        modified = true;
        return true;
    }

    /**
     * Provides an implementation for setPassphrase().  However, only
     * null passphrases are accepted by this implementation.
     * @throw UnsupportedOperationException if a non-null passphrase is
     *        specified
     */
    @Override
    public boolean setPassphrase(char[] passphrase)
        throws IOException {

        assertInitialized();

        if (passphrase != null) {
            throw new UnsupportedOperationException(
                "Passphrases are not supported");
        }

        return true;
    }

    /**
     * Save a modified password store.
     */
    @Override
    public void save() throws IOException {

        assertInitialized();

        if (secretHash == null) {
            throw new IllegalStateException(
                "Password store has not been initialized");
        }

        if (modified) {
            secretHash.write(storeLocation);
            modified = false;
        }
    }

    /**
     * Discard this store.
     */
    @Override
    public void discard() {
        if (secretHash != null) {
            secretHash.discard();
        }
    }

    /**
     * Check whether a passphrase is required to access the store.
     * @return true if a passphrase is required.  The current implementation
     * returns false in all cases
     */
    @Override
    public boolean requiresPassphrase() throws IOException {

        return false;
    }

    /**
     * Check whether a passphrase is valid to use.
     * @return true if the passphrase is valid.  The current implementation
     * does not support passphrases, so this returns false unless the supplied
     * passphrase is null.
     */
    @Override
    public boolean isValidPassphrase(char[] passphrase) {
        return passphrase == null;
    }

    @Override
    public boolean exists() throws IOException {
        return storeLocation.exists();
    }

    private void assertNotInitialized() {
        if (secretHash != null) {
            throw new IllegalStateException(
                "Password store already initialized");
        }
    }

    private void assertInitialized() {
        if (secretHash == null) {
            throw new IllegalStateException(
                "Password store not yet initialized");
        }
    }

    private static final class SecretHash {
        private HashMap<String, char[]> secretData =
            new HashMap<String, char[]>();

        private static final String PASSWORD_STORE_KEY = "Password Store:";

        /**
         * Constructor.
         */
        SecretHash() {
        }

        /**
         * Return an iterator over the secretData keys.
         */
        private Iterator<String> aliases() {
            return secretData.keySet().iterator();
        }

        /**
         * Create or update a mapping of the specified key to the
         * specified secret value.
         */
        private void setSecret(String alias, char[] secret) {
            secretData.put(alias,  Arrays.copyOf(secret, secret.length));
        }

        /**
         * Return the current secret associated with the alias.
         */
        private char[] getSecret(String alias) {
            final char[] secret = secretData.get(alias);
            if (secret == null) {
                return null;
            }

            /* return a copy */
            return Arrays.copyOf(secret, secret.length);
        }

        /*
         * Check whether a secret exists named by the specified alias
         */
        private boolean containsAlias(String alias) {
            return secretData.containsKey(alias);
        }

        /* Remove the secret mapping for the specified alias. */
        private boolean deleteSecret(String alias) {
            final char[] secret = secretData.remove(alias);
            if (secret == null) {
                return false;
            }
            Arrays.fill(secret, ' ');
            return true;
        }

        private void discard() {
            discardSecretData();
        }

        /* "Zero" out all of the entries as a precaution. */
        private void discardSecretData() {
            final Iterator<char[]> iter = secretData.values().iterator();
            while (iter.hasNext()) {
                final char[] secret = iter.next();
                Arrays.fill(secret, ' ');
            }
        }

        /**
         * Read a password store into memory
         */
        synchronized void read(File f) throws IOException {
            discardSecretData();

            final BufferedReader br = new BufferedReader(new FileReader(f));
            final String keyLine = br.readLine();

            try {
                if (keyLine == null ||
                    !keyLine.startsWith(PASSWORD_STORE_KEY)) {
                    throw new PasswordStoreException(
                        "The file does not appear to contain a password store");
                }

                while (true) {
                    final String input = br.readLine();
                    if (input == null) {
                        break;
                    }
                    final String[] kv = input.split("=");
                    /* Expect a key and a value */
                    if (kv.length == 2) {
                        final String alias = kv[0];
                        final char[] secret = kv[1].toCharArray();
                        secretData.put(alias, secret);
                    }
                }
            } finally {
                br.close();
            }
        }

        synchronized void write(File f) throws IOException {
            final PrintWriter writer = new PrintWriter(f);

            writer.println(PASSWORD_STORE_KEY);

            final Set<String> keys = secretData.keySet();
            for (String alias : keys) {
                final char[] secret = secretData.get(alias);
                writer.print(alias + "=");
                /*
                 * Keep separate to take advantange of char[] support.
                 * Simple string concatenation doesn't print the chars
                 */
                writer.println(secret);
            }

            writer.close();
        }
    }
}
