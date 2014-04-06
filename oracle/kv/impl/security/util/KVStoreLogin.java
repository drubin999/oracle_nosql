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

package oracle.kv.impl.security.util;

import static oracle.kv.KVSecurityConstants.AUTH_PWDFILE_PROPERTY;
import static oracle.kv.KVSecurityConstants.AUTH_USERNAME_PROPERTY;
import static oracle.kv.KVSecurityConstants.AUTH_WALLET_PROPERTY;
import static oracle.kv.KVSecurityConstants.SECURITY_FILE_PROPERTY;
import static oracle.kv.KVSecurityConstants.TRANSPORT_PROPERTY;
import static oracle.kv.impl.security.ssl.SSLConfig.KEYSTORE_FILE;
import static oracle.kv.impl.security.ssl.SSLConfig.TRUSTSTORE_FILE;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.AuthenticationRequiredException;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreException;
import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.ReauthenticateHandler;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.security.PasswordManager;
import oracle.kv.impl.security.PasswordStore;
import oracle.kv.impl.security.login.AdminLoginManager;
import oracle.kv.impl.security.login.RepNodeLoginManager;
import oracle.kv.impl.security.ssl.SSLConfig;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.util.shell.ShellInputReader;

/**
 * Helper class for loading user login information from the security file, and
 * to facilitate the login behavior of client utilities like CommandShell. Note
 * the class is NOT thread-safe.
 * <p>
 * The security file may contain any of the properties defined in
 * {@link SSLConfig} as well as the following authentication properties:
 * <ul>
 * <li>oracle.kv.auth.username</li>
 * <li>oracle.kv.auth.wallet.dir</li>
 * <li>oracle.kv.auth.pwdfile.file</li>
 * <li>oracle.kv.auth.pwdfile.manager</li>
 * </ul>
 * <p>
 * Note that if the oracle.kv.transport is set in the security file, we will
 * assume that the security file contains all necessary transport configuration
 * information provided by users.
 * TODO: the statement above is out-of-date
 */
public class KVStoreLogin {

    public static final String PWD_MANAGER = "oracle.kv.auth.pwdfile.manager";

    private static final String WALLET_MANAGER_CLASS =
            "oracle.kv.impl.security.wallet.WalletManager";
    private static final String DEFAULT_FILESTORE_MANAGER_CLASS =
            "oracle.kv.impl.security.filestore.FileStoreManager";

    private String userName;
    private String securityFilePath;
    private Properties securityProps = null;
    private ShellInputReader reader = null;

    private static final Set<String> fileProperties = new HashSet<String>();
    static {
        fileProperties.add(SECURITY_FILE_PROPERTY);
        fileProperties.add(AUTH_WALLET_PROPERTY);
        fileProperties.add(AUTH_PWDFILE_PROPERTY);
        fileProperties.add(KEYSTORE_FILE);
        fileProperties.add(TRUSTSTORE_FILE);
    }

    /**
     * Build a kvstore login without user and login information.
     */
    public KVStoreLogin() {
        this(null, null);
    }

    public KVStoreLogin(final String user, final String security) {
        userName = user;
        securityFilePath = security;
    }

    public String getUserName() {
        return userName;
    }

    public void updateLoginInfo(final String user, final String security) {
        this.userName = user;
        this.securityFilePath = security;
        loadSecurityProperties();
    }

    public Properties getSecurityProperties() {
        return securityProps;
    }

    /**
     * Read the settings of the security file into the inner properties. Note
     * that if the "oracle.kv.transport" is specified in the security file, all
     * transport config information is assumed to be contained in the file.
     *
     * @throws IllegalStateException if anything wrong happens while loading the
     * file
     */
    public void loadSecurityProperties() {
        /*
         * If security file is not set, try read it from system property of
         * oracle.kv.security
         */
        if (securityFilePath == null) {
            securityFilePath =
                System.getProperty(SECURITY_FILE_PROPERTY);
        }

        securityProps = createSecurityProperties(securityFilePath);

        /* If user is not set, try to read from security file */
        if (securityProps != null && userName == null) {
            userName = securityProps.getProperty(AUTH_USERNAME_PROPERTY);
        }

        if (securityFilePath != null && !foundSSLTransport()) {
            throw new IllegalArgumentException(
                "Warning: login is specified without setting SSL transport.");
        }
    }

    /**
     * We delay the instantiation of ShellInputReader as late as when it is
     * really needed, because a background task will stop if it tries to
     * instantiate the ShellInputReaders as described in [#23075]. This can
     * help alleviate the situation when a background task can read password
     * credentials from the security file.
     */
    private ShellInputReader getReader() {
        if (reader == null) {
            reader = new ShellInputReader(System.in, System.out);
        }
        return reader;
    }

    /* For test */
    protected void setReader(final ShellInputReader reader) {
        this.reader = reader;
    }

    /**
     * Get password credentials for login. If user/password is not specified or
     * cannot be read from the security file, it will be required to read from
     * the shell input reader.
     *
     * @throws IOException if fails to get username or password from shell
     * input reader
     */
    public PasswordCredentials makeShellLoginCredentials()
        throws IOException {
        if (userName == null) {
            userName = getReader().readLine("Login as:");
        }

        char[] passwd = retrievePassword(userName, securityProps);
        if (passwd == null) {
            passwd = getReader().readPassword(userName + "'s password:");
        }
        return new PasswordCredentials(userName, passwd);
    }

    /**
     * Get password credentials according to internal security properties.
     */
    public PasswordCredentials getLoginCredentials() {
        return makeLoginCredentials(securityProps);
    }

    public String getSecurityFilePath() {
        return securityFilePath;
    }

    /**
     * Check if the transport type is set to SSl. Both the settings from system
     * and security properties will be checked.
     */
    public boolean foundSSLTransport() {
        String transportType = securityProps == null ?
                               null :
                               securityProps.getProperty(TRANSPORT_PROPERTY);
        return (transportType != null &&
                transportType.equals(SecurityParams.TRANS_TYPE_SSL));
    }

    /**
     * Check if the security properties loaded from file contain transport
     * settings by looking for the definition of "oracle.kv.tranport". In this
     * case, we assume the security properties contain all config information.
     */
    public boolean hasTransportSettings() {
        return securityProps != null &&
               securityProps.getProperty(TRANSPORT_PROPERTY) != null;
    }


    /**
     * Initialize the RMI policy and the registryCSF according to the transport
     * settings in the store login.
     */
    public void prepareRegistryCSF() {
        if (hasTransportSettings()) {
            ClientSocketFactory.setRMIPolicy(getSecurityProperties());
        }
        RegistryUtils.initRegistryCSF();
    }

    /**
     * Get password credentials for login from the security properties. If
     * either the oracle.kv.auth.username is not set, or password of the
     * specified user is not found in the store, null will be returned.
     */
    public static PasswordCredentials
        makeLoginCredentials(final Properties securityProps) {
        if (securityProps == null) {
            return null;
        }

        final String user = securityProps.getProperty(AUTH_USERNAME_PROPERTY);
        if (user == null) {
            return null;
        }

        final char[] passwd = retrievePassword(user, securityProps);
        return passwd == null ? null : new PasswordCredentials(user, passwd);
    }

    /**
     * Create a security properties object by reading the settings in the
     * security file.
     */
    public static Properties createSecurityProperties(final String security) {
        if (security == null) {
            return null;
        }

        final File securityFile = new File(security);
        FileInputStream fis = null;

        try {
            fis = new FileInputStream(securityFile);
            final Properties securityProps = new Properties();
            securityProps.load(fis);
            resolveRelativePaths(securityProps, securityFile);
            return securityProps;
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage());
        } finally {
            try {
                if (fis != null) {
                    fis.close();
                }
            } catch (IOException e) /* CHECKSTYLE:OFF */ {
                /* Ignore */
            } /* CHECKSTYLE:ON */
        }
    }

    /**
     * Given a set of Properties loaded from the file sourceFile, examine the
     * property settings that name a file or directory, and for each one that
     * is a relative path name, update the property setting to be an absolulte
     * path name by interpreting the path as relative to the directory
     * containing sourceFile.
     */
    private static void resolveRelativePaths(Properties securityProps,
                                             File sourceFile) {
        final File sourceDir = sourceFile.getAbsoluteFile().getParentFile();
        for (final String propName : securityProps.stringPropertyNames()) {
            if (fileProperties.contains(propName)) {
                final String propVal = securityProps.getProperty(propName);
                File propFile = new File(propVal);
                if (!propFile.isAbsolute()) {
                    propFile  = new File(sourceDir, propVal);
                    securityProps.setProperty(propName, propFile.getPath());
                }
            }
        }
    }

    /**
     * Try to retrieve the password from the password store constructed from
     * the security properties. If wallet.dir is set, the password will be
     * fetched using wallet store, otherwise file password store is used.
     */
    private static char[] retrievePassword(final String user,
                                           final Properties securityProps) {
        if (user == null || securityProps == null) {
            return null;
        }

        PasswordManager pwdManager = null;
        PasswordStore pwdStore = null;

        try {
            final String walletDir =
                securityProps.getProperty(AUTH_WALLET_PROPERTY);
            if (walletDir != null && !walletDir.isEmpty()) {
                pwdManager = PasswordManager.load(WALLET_MANAGER_CLASS);
                pwdStore = pwdManager.getStoreHandle(new File(walletDir));
            } else {
                String mgrClass = securityProps.getProperty(PWD_MANAGER);
                if (mgrClass == null || mgrClass.isEmpty()) {
                    mgrClass = DEFAULT_FILESTORE_MANAGER_CLASS;
                }
                final String pwdFile =
                    securityProps.getProperty(AUTH_PWDFILE_PROPERTY);
                if (pwdFile == null || pwdFile.isEmpty()) {
                    return null;
                }
                pwdManager = PasswordManager.load(mgrClass);
                pwdStore = pwdManager.getStoreHandle(new File(pwdFile));
            }
            pwdStore.open(null); /* must be autologin */
            return pwdStore.getSecret(user);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (pwdStore != null) {
                pwdStore.discard();
            }
        }
    }

    /**
     * Makes a reauthenticate handler with the given credentialsProvider.
     *
     * @param credsProvider credentials provider
     * @return a reauthenticate handler, null if the credentialsProvider is
     * null
     */
    public static ReauthenticateHandler
        makeReauthenticateHandler(final CredentialsProvider credsProvider) {

        return credsProvider == null ?
               null :
               new ReauthenticateHandler() {

                   @Override
                   public void reauthenticate(KVStore kvstore)
                       throws FaultException,
                              AuthenticationFailureException,
                              AuthenticationRequiredException {
                       final LoginCredentials creds =
                           credsProvider.getCredentials();
                       kvstore.login(creds);
                   }
                };
    }

    /**
     * Obtain an AdminLoginManager with given host and port.
     *
     * @param host host
     * @param port port
     * @param pwdCreds password credentials
     * @return an AdminLoginManager instance if non-null credentials were
     * provided and the login operation was successful, or null otherwise.
     * @throw oracle.kv.AuthenticationFailureException if credentials are
     * supplied, but are invalid
     */
    public static AdminLoginManager
        getAdminLoginMgr(final String host,
                         final int port,
                         final PasswordCredentials pwdCreds)
        throws AuthenticationFailureException {

        return getAdminLoginMgr(new String[] { host + ":" + port  }, pwdCreds);
    }

    /**
     * Obtain an AdminLoginManager with given host:port pairs.
     *
     * @param hostPorts the host:port pairs
     * @param pwdCreds password credentials
     * @return an AdminLoginManager instance if non-null credentials were
     * provided and the login operation was successful, or null otherwise.
     * @throw oracle.kv.AuthenticationFailureException if credentials are
     * supplied, but are invalid
     * @throw oracle.kv.AuthenticationFailureException if credentials are
     * supplied, but are invalid
     */
    public static AdminLoginManager
        getAdminLoginMgr(final String[] hostPorts,
                         final PasswordCredentials pwdCreds)
        throws AuthenticationFailureException {

        if (pwdCreds != null) {
            final AdminLoginManager loginMgr =
                new AdminLoginManager(pwdCreds.getUsername(), true);
            if (loginMgr.bootstrap(hostPorts, pwdCreds)) {
                return loginMgr;
            }
        }
        return null;
    }

    /**
     * Obtain a RepNodeLoginManager with given host and port.
     *
     * @param host host
     * @param port port
     * @param pwdCreds password credentials
     * @param storeName the KVStore store name, if known, or else null
     * @return a RepNodeLoginManager instance if non-null credentials were
     * provided and the login operation was successful, or null otherwise.
     * @throw oracle.kv.AuthenticationFailureException if credentials are
     * supplied, but are invalid
     */
    public static RepNodeLoginManager
        getRepNodeLoginMgr(final String host,
                           final int port,
                           final PasswordCredentials pwdCreds,
                           final String storeName)
        throws AuthenticationFailureException {

        return getRepNodeLoginMgr(new String[] { host + ":" + port  },
                                  pwdCreds, storeName);
    }

    /**
     * Obtain a RepNodeLoginManager with given host:port pairs.
     *
     * @param hostPorts the host:port pairs
     * @param pwdCreds password credentials
     * @param storeName the KVStore store name, if known, or else null
     * @return a RepNodeLoginManager instance if non-null credentials were
     * provided and the login operation was successful, or null otherwise
     * @throw oracle.kv.AuthenticationFailureException if credentials are
     * supplied, but are invalid
     */
    public static RepNodeLoginManager
        getRepNodeLoginMgr(final String[] hostPorts,
                           final PasswordCredentials pwdCreds,
                           final String storeName)
        throws AuthenticationFailureException {

        if (pwdCreds != null) {
            try {
                final RepNodeLoginManager loginMgr =
                    new RepNodeLoginManager(pwdCreds.getUsername(), true);
                loginMgr.bootstrap(hostPorts, pwdCreds, storeName);
                return loginMgr;
            } catch (KVStoreException kvse) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
        }
        return null;
    }

    /**
     * Credentials provider used for constructing a reauthenticate handler. A
     * class implements this interface ought to provide login credentials for
     * use in reauthenticattion.
     */
    public interface CredentialsProvider {
        LoginCredentials getCredentials();
    }

    /**
     * A class provides the login credential via reading the password in the
     * store indicated in the store security properties.
     */
    public static class StoreLoginCredentialsProvider
        implements CredentialsProvider {
        private final Properties props;

        public StoreLoginCredentialsProvider(Properties securityProps) {
            this.props = securityProps;
        }

        @Override
        public LoginCredentials getCredentials() {
            return makeLoginCredentials(props);
        }
    }
}
