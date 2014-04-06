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

import static oracle.kv.impl.admin.param.SecurityParams.TRANS_TYPE_SSL;
import static oracle.kv.impl.security.PasswordManager.WALLET_MANAGER_CLASS;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterState.Info;
import oracle.kv.impl.security.PasswordManager;
import oracle.kv.impl.security.PasswordStore;
import oracle.kv.impl.security.PasswordStoreException;
import oracle.kv.impl.security.ssl.SSLConfig;
import oracle.kv.impl.security.util.ConsolePasswordReader;
import oracle.kv.impl.security.util.PasswordReader;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.util.shell.Shell;

/**
 * SecurityConfigCreator implements the core operations for creating a
 * security configuration.  It is referenced by both makebootconfig and
 * securityconfig.
 */
public class SecurityConfigCreator {
    /*
     * The list of preferred protocols.  Both KV and JE SSL implementations
     * will filter out any that are not supported.  If none are supported,
     * an exception will be thrown.
     */
    private static final String PREFERRED_PROTOCOLS = "TLSv1.2,TLSv1.1,TLSv1";

    /* This is a Java-specified requirement */
    private static final int MIN_STORE_PASSPHRASE_LEN = 6;
    private String kvRoot;
    private final IOHelper ioHelper;
    private final ParsedConfig config;
    /* derived from config */
    private final String pwdMgrClass;

    private static final String SHARED_KEY_ALIAS = "shared";
    private static final String SSL_CERT_DN = "CN=NoSQL";
    private static final String SSL_CLIENT_PEER = "CN=NoSQL";
    private static final String SSL_SERVER_PEER = "CN=NoSQL";
    private static final String INTERNAL_AUTH_SSL = "ssl";
    private static final String CERT_MODE_SHARED = "shared";
    private static final String CERT_MODE_SERVER = "server";

    private static final String PWD_ALIAS_KEYSTORE = "keystore";

    /**
     * Configuration class.
     */
    public static class ParsedConfig {
        private String pwdmgr;
        private String secDir;
        private char[] ksPassword;
        private String certMode;
        private List<ParamSetting> userParams = new ArrayList<ParamSetting>();

        public class ParamSetting {
            final ParameterState pstate;
            final String transportName;
            final String paramName;
            final String paramValue;

            ParamSetting(ParameterState pstate,
                         String transportName,
                         String paramName,
                         String paramValue) {
                this.pstate = pstate;
                this.transportName = transportName;
                this.paramName = paramName;
                this.paramValue = paramValue;
            }
        }

        public void setPwdmgr(String pwdmgrVal) {
            pwdmgr = pwdmgrVal;
        }

        public String getPwdmgr() {
            return pwdmgr;
        }

        public void setKeystorePassword(char[] ksPwdVal) {
            ksPassword = ksPwdVal;
        }

        public char[] getKeystorePassword() {
            return ksPassword;
        }

        public void setSecurityDir(String securityDir) {
            secDir = securityDir;
        }

        public String getSecurityDir() {
            return secDir;
        }

        public void setCertMode(String certModeVal) {
            if (certModeVal != null &&
                !(CERT_MODE_SHARED.equals(certModeVal)) &&
                !(CERT_MODE_SERVER.equals(certModeVal))) {
                throw new IllegalArgumentException(
                    "The value '" + certModeVal + "' is not a valid " +
                    "certificate mode.  Only " + CERT_MODE_SHARED + " and " +
                    CERT_MODE_SERVER + " are allowed.");
            }
            certMode = certModeVal;
        }

        public String getCertMode() {
            return certMode;
        }

        /**
         * @throw IllegalArgumentException if the parameter setting is not
         * valid.
         */
        public void addParam(String paramSetting) {

            final int equalIdx = paramSetting.indexOf("=");
            if (equalIdx < 0) {
                throw new IllegalArgumentException(
                    "Invalid parameter setting - missing '='");
            }

            final String param = paramSetting.substring(0, equalIdx);
            final String value = paramSetting.substring(equalIdx + 1);
            final String[] paramSplit = param.split(":");

            if (paramSplit.length > 2) {
                throw new IllegalArgumentException(
                    "Invalid parameter name format: " + param);
            }

            final String paramName = paramSplit[paramSplit.length - 1];
            final String transport =
                (paramSplit.length > 1) ? paramSplit[0] : null;
            final ParameterState pstate = ParameterState.lookup(paramName);

            if (pstate == null) {
                throw new IllegalArgumentException(
                    "The name " + paramName + " is not a valid parameter name");
            }

            if (!(pstate.appliesTo(Info.SECURITY) ||
                  pstate.appliesTo(Info.TRANSPORT))) {
                throw new IllegalArgumentException(
                    "The name " + paramName + " is not a valid parameter for " +
                    "a security configuration");
            }

            if (transport != null) {
                if (!pstate.appliesTo(Info.TRANSPORT)) {
                    throw new IllegalArgumentException(
                        paramName + " is not a transport parameter");
                }

                if (!(ParameterState.SECURITY_TRANSPORT_CLIENT.equals(
                          transport) ||
                      ParameterState.SECURITY_TRANSPORT_INTERNAL.equals(
                          transport) ||
                      ParameterState.SECURITY_TRANSPORT_JE_HA.equals(
                          transport))) {
                    throw new IllegalArgumentException(
                        transport + " is not a valid transport name");
                }
            }

            userParams.add(
                new ParamSetting(pstate, transport, paramName, value));
        }

        public List<ParamSetting> getUserParams() {
            return userParams;
        }

        public void populateDefaults() {
            if (getSecurityDir() == null) {
                setSecurityDir(FileNames.SECURITY_CONFIG_DIR);
            }
            if (getCertMode() == null) {
                setCertMode(CERT_MODE_SHARED);
            }
        }
    }

    /**
     * This functionality wants to be accessed through a couple of paths.
     * provide an intermediate interface that can adapt to either path.
     */
    public interface IOHelper {

        /**
         * Read a password from the user.
         */
        char[] readPassword(String prompt) throws IOException;

        /**
         * Print a line of output.
         */
        void println(String s);
    }

    /**
     * IOHelper for use in the context of a Shell environment.
     */
    static class ShellIOHelper implements IOHelper {
        private Shell shell;
        private PasswordReader passwordReader;

        ShellIOHelper(Shell shell) {
            this.shell = shell;
            passwordReader = null;
            if (shell instanceof SecurityShell) {
                passwordReader = ((SecurityShell) shell).getPasswordReader();
            }
            if (passwordReader == null) {
                passwordReader = new ConsolePasswordReader();
            }
        }

        @Override
        public char[] readPassword(String prompt) throws IOException {
            return passwordReader.readPassword(prompt);
        }

        @Override
        public void println(String s) {
            shell.println(s);
        }
    }

    /**
     * Generic IO Helper
     */
    static class GenericIOHelper implements IOHelper {
        private PrintStream printStream;
        private PasswordReader passwordReader;

        GenericIOHelper(PrintStream printStream) {
            this(printStream, new ConsolePasswordReader());
        }

        GenericIOHelper(PrintStream printStream,
                        PasswordReader passwordReader) {
            this.printStream = printStream;
            this.passwordReader = passwordReader;
        }

        @Override
        public char[] readPassword(String prompt) throws IOException {
            return passwordReader.readPassword(prompt);
        }

        @Override
        public void println(String s) {
            printStream.println(s);
        }
    }

    public SecurityConfigCreator(String kvRoot,
                                 ParsedConfig parsedConfig,
                                 IOHelper ioHelper) {
        this.kvRoot = kvRoot;
        this.ioHelper = ioHelper;
        this.config = parsedConfig;
        this.pwdMgrClass =
            SecurityConfigCommand.getPwdmgrClass(config.getPwdmgr());
    }

    /**
     * Gather input from the user and create the requisite security files.
     * @return true if the operation was successful
     */
    public boolean createConfig() throws PasswordStoreException, Exception {

        config.populateDefaults();

        /* Get a PasswordManager instance */
        final PasswordManager pwdMgr = resolvePwdMgr();
        if (pwdMgr == null) {
            return false;
        }

        /* Make sure the security directory exists - create if needed */
        final File securityDir = prepareSecurityDir();
        if (securityDir == null) {
            return false;
        }

        char[] keyStorePassword = config.getKeystorePassword();
        if (keyStorePassword != null) {
            if (!validKeystorePassword(keyStorePassword)) {
                return false;
            }
        } else {
            /* Get a keystore password - the user may need to know this */
            try {
                keyStorePassword = promptForKeyStorePassword();
                if (keyStorePassword == null) {
                    ioHelper.println("No keystore password specified");
                    return false;
                }
            } catch (IOException ioe) {
                ioHelper.println("I/O error reading password: " +
                                 ioe.getMessage());
                return false;
            }
        }

        /* Prepare the SecurityParams object */
        final SecurityParams sp = makeSecurityParams();

        /* Create the keystore file */
        final Properties keyStoreProperties = new Properties();
        keyStoreProperties.setProperty(SecurityUtils.KEY_DISTINGUISHED_NAME,
                                       SSL_CERT_DN);
        SecurityUtils.initKeyStore(securityDir,
                                   sp,
                                   keyStorePassword,
                                   keyStoreProperties);

        /*
         * Create the password store, which will hold the password for the
         * keystore.
         */
        final PasswordStore pwdStore = makePasswordStore(securityDir, sp);
        pwdStore.setSecret(PWD_ALIAS_KEYSTORE, keyStorePassword);
        pwdStore.save();
        pwdStore.discard();

        /*
         * Now that the password store and keystore have successfully been
         * created, build the security.xml file that ties it all together.
         */
        final File securityXmlFile =
            new File(securityDir.getPath(), FileNames.SECURITY_CONFIG_FILE);
        ConfigUtils.createSecurityConfig(sp, securityXmlFile);

        /*
         * Now build a security file that captures the salient bits
         * that the customer needs in order to connect to the KVStore.
         */
        final File securityFile =
            new File(securityDir.getPath(), FileNames.CLIENT_SECURITY_FILE);
        final Properties securityProps = sp.getClientAccessProps();

        /*
         * The client access properties have a trustStore setting that
         * references the store.trust file.  Update it to reference the
         * client.trust file.
         */
        final String trustStoreRef =
            securityProps.getProperty(SSLConfig.TRUSTSTORE_FILE);
        if (trustStoreRef != null) {
            /*
             * There is a truststore file that is needed for secure access.
             * Make a copy of it and update the property to refer to the client
             * copy of the file.
             */
            final File srcFile =
                new File(securityDir, trustStoreRef);
            final File destFile =
                new File(securityDir, FileNames.CLIENT_TRUSTSTORE_FILE);

            SecurityUtils.copyOwnerWriteFile(srcFile, destFile);
            securityProps.put(SSLConfig.TRUSTSTORE_FILE,
                              FileNames.CLIENT_TRUSTSTORE_FILE);
        }

        final String securityComment =
            "Security property settings for communication with " +
            "KVStore servers";
        ConfigUtils.storeProperties(securityProps, securityComment,
                                    securityFile);

        /* summarize the configuration for the user */

        final List<File> createdFiles = findSecurityFiles(securityDir);
        ioHelper.println("Created files");
        for (File f : createdFiles) {
            ioHelper.println("    " + f.getPath());
        }

        return true;
    }

    private List<File> findSecurityFiles(File securityDir) {
        final List<File> result = new ArrayList<File>();
        findFiles(securityDir, result);
        return result;
    }

    private void findFiles(File securityDir, List<File> found) {
        for (File f : securityDir.listFiles()) {
            if (f.isDirectory()) {
                findFiles(f, found);
            } else {
                found.add(f);
            }
        }
    }

    /**
     * Resolve the pwdMgrClass to a PasswordManager instance.
     * @return an instance of the password manager class, or null if a problem
     * was encountered
     */
    private PasswordManager resolvePwdMgr() {
        try {
            return PasswordManager.load(pwdMgrClass);
        } catch (ClassNotFoundException cnfe) {
            ioHelper.println("Unable to locate password manager class '" +
                             pwdMgrClass + "'");
            return null;
        } catch (Exception exc) {
            /*
             * There are lots of ways this could fail, none of them likely.
             */
            ioHelper.println("Creation of password manager class failed: " +
                             exc.getMessage());
            return null;
        }
    }

    /**
     * Ensure the existence of the security directory based on the kvRoot
     * input.
     * @return a File object identifying the security directory if all
     *   goes according to plan, or null otherwise.
     */
    private File prepareSecurityDir() {
        /* Check out kvRoot */
        File kvRootDir = new File(kvRoot);
        if (!kvRootDir.exists()) {
            ioHelper.println(
                "The directory " + kvRootDir.getPath() + " does not exist");
            return null;
        }
        if (!kvRootDir.isAbsolute()) {
            kvRootDir = kvRootDir.getAbsoluteFile();
            kvRoot = kvRootDir.getPath();
        }

        /* Then create the security directory if needed */
        final File securityDir = new File(kvRoot, config.getSecurityDir());
        if (securityDir.exists()) {
            if (!directoryEmpty(securityDir)) {
                ioHelper.println("The directory " + securityDir.getPath() +
                                 " exists and is not empty");
                return null;
            }
        } else {
            if (!securityDir.mkdir()) {
                ioHelper.println("Unable to create the directory " +
                                 securityDir.getPath());
                return null;
            }
        }
        return securityDir;
    }

    /**
     * Check whether the specified directory is empty.
     * @param dir a directory, which must exist
     * @return true if the directory is empty
     */
    private boolean directoryEmpty(File dir) {
        final String[] entries = dir.list();
        return entries != null && entries.length == 0;
    }

    private PasswordStore makePasswordStore(File securityDir,
                                            SecurityParams sp) {

        PasswordManager pwdMgr = null;
        try {
            pwdMgr = PasswordManager.load(pwdMgrClass);
        } catch (ClassNotFoundException cnfe) {
            /*
             * This should already have been checked, but we have to deal with
             * the exception, so...
             */
            ioHelper.println("Unable to locate password manager class '" +
                             pwdMgrClass + "'");
            return null;
        } catch (Exception exc) {
            /*
             * There are lots of ways this could fail, none of them likely.
             */
            ioHelper.println("Creation of password manager class failed: " +
                                 exc.getMessage());
            return null;
        }

        final File storeLoc =
            new File(securityDir,
                     new File(pwdMgrClass.equals(WALLET_MANAGER_CLASS) ?
                              sp.getWalletDir() :
                              sp.getPasswordFile()).getPath());
        final PasswordStore pwdStore = pwdMgr.getStoreHandle(storeLoc);

        try {
            /* Create password store as auto-login */
            pwdStore.create(null);
            return pwdStore;
        } catch (IOException ioe) {
            ioHelper.println("Error creating password store: " +
                             ioe.getMessage());
        }
        return null;
    }

    private SecurityParams makeSecurityParams() {
        final SecurityParams sp = new SecurityParams();

        /* Security is enabled */
        sp.setSecurityEnabled(true);

        if (pwdMgrClass.equals(WALLET_MANAGER_CLASS)) {
            sp.setWalletDir(FileNames.WALLET_DIR);
        } else {
            sp.setPasswordFile(FileNames.PASSWD_FILE);
            sp.setPasswordClass(pwdMgrClass);
        }

        sp.setKeystorePasswordAlias(PWD_ALIAS_KEYSTORE);

        sp.setKeystoreFile(FileNames.KEYSTORE_FILE);
        sp.setTruststoreFile(FileNames.TRUSTSTORE_FILE);

        /* Set the internal authentication mechanism */
        sp.setInternalAuth(INTERNAL_AUTH_SSL);

        /* Set the certificate mode */
        sp.setCertMode(CERT_MODE_SHARED);

        /* Set client transport */
        sp.addTransportMap(ParameterState.SECURITY_TRANSPORT_CLIENT);
        sp.setTransType(
            ParameterState.SECURITY_TRANSPORT_CLIENT,
            TRANS_TYPE_SSL);
        sp.setTransServerKeyAlias(
            ParameterState.SECURITY_TRANSPORT_CLIENT,
            SHARED_KEY_ALIAS);
        sp.setTransServerIdentityAllowed(
            ParameterState.SECURITY_TRANSPORT_CLIENT,
            "dnmatch(" + SSL_SERVER_PEER + ")");

        /*
         * We only set the ClientAllowProtocols because we might need to be
         * accessed by applications that can't set their protocol level,
         * which would prevent them from connecting to us.
         */
        sp.setTransClientAllowProtocols(
            ParameterState.SECURITY_TRANSPORT_CLIENT,
            PREFERRED_PROTOCOLS);

        /* Set internal transport */
        sp.addTransportMap(ParameterState.SECURITY_TRANSPORT_INTERNAL);
        sp.setTransType(
            ParameterState.SECURITY_TRANSPORT_INTERNAL,
            TRANS_TYPE_SSL);
        sp.setTransServerKeyAlias(
            ParameterState.SECURITY_TRANSPORT_INTERNAL,
            SHARED_KEY_ALIAS);
        sp.setTransClientKeyAlias(
            ParameterState.SECURITY_TRANSPORT_INTERNAL,
            SHARED_KEY_ALIAS);
        sp.setTransClientAuthRequired(
            ParameterState.SECURITY_TRANSPORT_INTERNAL,
            true);
        sp.setTransClientIdentityAllowed(
            ParameterState.SECURITY_TRANSPORT_INTERNAL,
            "dnmatch(" + SSL_CLIENT_PEER + ")");
        sp.setTransServerIdentityAllowed(
            ParameterState.SECURITY_TRANSPORT_INTERNAL,
            "dnmatch(" + SSL_SERVER_PEER + ")");
        /* See above for reason why we only apply this on the client side */
        sp.setTransClientAllowProtocols(
            ParameterState.SECURITY_TRANSPORT_INTERNAL,
            PREFERRED_PROTOCOLS);

        /* Set JE HA transport */
        sp.addTransportMap(ParameterState.SECURITY_TRANSPORT_JE_HA);
        sp.setTransType(
            ParameterState.SECURITY_TRANSPORT_JE_HA,
            TRANS_TYPE_SSL);
        sp.setTransServerKeyAlias(
            ParameterState.SECURITY_TRANSPORT_JE_HA,
            SHARED_KEY_ALIAS);
        sp.setTransClientAuthRequired(
            ParameterState.SECURITY_TRANSPORT_JE_HA,
            true);
        sp.setTransClientIdentityAllowed(
            ParameterState.SECURITY_TRANSPORT_JE_HA,
            "dnmatch(" + SSL_CLIENT_PEER + ")");
        sp.setTransServerIdentityAllowed(
            ParameterState.SECURITY_TRANSPORT_JE_HA,
            "dnmatch(" + SSL_SERVER_PEER + ")");

        /*
         * JE doesn't support the notion of client vs. server and we don't
         * have a need to support non-Oracle applications, so set both client
         * and server to use the preferred protocols.
         */
        sp.setTransAllowProtocols(
            ParameterState.SECURITY_TRANSPORT_JE_HA,
            PREFERRED_PROTOCOLS);

        /*
         * Apply any user-specified settings
         */
        final ParameterMap pmap = sp.getMap();
        final List<ParsedConfig.ParamSetting> settings = config.getUserParams();
        for (ParsedConfig.ParamSetting setting : settings) {
            final ParameterState pstate = setting.pstate;
            if (pstate.appliesTo(Info.TRANSPORT)) {
                if (setting.transportName == null) {
                    for (ParameterMap tmap : sp.getTransportMaps()) {
                        tmap.setParameter(setting.paramName,
                                          setting.paramValue);
                    }
                } else {
                    final ParameterMap tmap =
                        sp.getTransportMap(setting.transportName);
                    tmap.setParameter(setting.paramName, setting.paramValue);
                }
            } else {
                pmap.setParameter(setting.paramName, setting.paramValue);
            }
        }

        return sp;
    }

    private char[] promptForKeyStorePassword()
        throws IOException {

        while (true) {
            final char[] pwd = ioHelper.readPassword(
                "Enter a password for the Java KeyStore:");
            if (pwd == null || pwd.length == 0) {
                return null;
            }
            if (!validKeystorePassword(pwd)) {
                continue;
            }
            final char[] pwd2 = ioHelper.readPassword(
                "Re-enter the KeyStore password for verification:");
            if (pwd2 != null && SecurityUtils.passwordsMatch(pwd, pwd2)) {
                return pwd;
            }
            ioHelper.println("The passwords do not match");
        }
    }

    private boolean validKeystorePassword(char[] pwd) {
        /*
         * Standard requirement for keystore implementations is 6 character
         * minimum, though some implementations might add additional
         * requirements.
         */
        if (pwd.length < MIN_STORE_PASSPHRASE_LEN) {
            ioHelper.println("The keystore password must be at least " +
                             MIN_STORE_PASSPHRASE_LEN + " characters long");
            return false;
        }
        return true;
    }


}
