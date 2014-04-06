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

package oracle.kv.impl.security.ssl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;

import oracle.kv.impl.util.registry.RMISocketPolicy;
import oracle.kv.impl.util.registry.ssl.SSLSocketPolicy;

import com.sleepycat.je.rep.net.SSLAuthenticator;
import com.sleepycat.je.rep.utilint.net.AliasKeyManager;

/**
 * SSL configuration support.
 */
public class SSLConfig {

    public static final String ENABLED_CIPHER_SUITES =
        "oracle.kv.ssl.ciphersuites";
    public static final String ENABLED_PROTOCOLS =
        "oracle.kv.ssl.protocols";
    public static final String CLIENT_AUTHENTICATOR =
        "oracle.kv.ssl.clientAuthenticator";
    public static final String SERVER_HOST_VERIFIER =
        "oracle.kv.ssl.hostnameVerifier";
    public static final String KEYSTORE_FILE =
        "oracle.kv.ssl.keyStore";
    public static final String KEYSTORE_TYPE =
        "oracle.kv.ssl.keyStoreType";
    public static final String KEYSTORE_ALIAS =
        "oracle.kv.ssl.keyStoreAlias";
    public static final String TRUSTSTORE_FILE =
        "oracle.kv.ssl.trustStore";
    public static final String TRUSTSTORE_TYPE =
        "oracle.kv.ssl.trustStoreType";

    /*
     * The following references to JE classes are in string literal form to
     * avoid the need for having kvclient.jar depend on je.jar.
     */
    static final String JE_SSL_DN_AUTHENTICATOR_CLASS =
        "com.sleepycat.je.rep.utilint.net.SSLDNAuthenticator";
    static final String JE_SSL_DN_HOST_VERIFIER_CLASS =
        "com.sleepycat.je.rep.utilint.net.SSLDNHostVerifier";
    static final String JE_SSL_STD_HOST_VERIFIER_CLASS =
        "com.sleepycat.je.rep.utilint.net.SSLStdHostVerifier";

    /**
     * AllProps should contain all of the known SSL configuration properties
     */
    private static final Set<String> allProps = new HashSet<String>();
    static {
        allProps.add(ENABLED_CIPHER_SUITES);
        allProps.add(ENABLED_PROTOCOLS);
        allProps.add(CLIENT_AUTHENTICATOR);
        allProps.add(SERVER_HOST_VERIFIER);
        allProps.add(KEYSTORE_FILE);
        allProps.add(KEYSTORE_TYPE);
        allProps.add(KEYSTORE_ALIAS);
        allProps.add(TRUSTSTORE_FILE);
        allProps.add(TRUSTSTORE_TYPE);
    }

    private final Properties props;
    private char[] keystorePassword = null;

    /**
     * Create an SSLConfig object, using the provided properties to define
     * the configuration.
     * @param sslProps a property set, which must not be null
     */
    public SSLConfig(Properties sslProps) {
        this.props = sslProps;

        if (sslProps != null) {
            validateProperties(sslProps);
        }
    }

    /**
     * Check that the specified properties are valid for SSL configuration.
     * Properties outside of the oracle.kv.ssl namespace are ignored.
     *
     * @param sslProps a set of SSL configuration properties
     * @return a Properties object containing only those properties belonging
     * to the oracle.kv.ssl namespace.
     * @throws IllegalArgumentException if any of the non-ignored input
     * properties are invalid
     */
    public static Properties validateProperties(Properties sslProps)
        throws IllegalArgumentException {

        final Properties returnProps = new Properties();

        for (String propName : sslProps.stringPropertyNames()) {
            if (propName.startsWith("oracle.kv.ssl.")) {
                if (!allProps.contains(propName)) {
                    throw new IllegalArgumentException(
                        propName + " is not a supported SSL property");
                }
                returnProps.setProperty(propName,
                                        sslProps.getProperty(propName));
            }
        }

        final String truststore = sslProps.getProperty(TRUSTSTORE_FILE);
        if (truststore != null) {
            final File truststoreFile = new File(truststore);
            if (!truststoreFile.isAbsolute()) {
                throw new IllegalArgumentException(
                    "The truststore file must be specified " +
                    "using an absolute pathname.");
            }

            /* TBD: enforce this here or defer? */
            /*
            if (!truststoreFile.exists()) {
                throw new IllegalArgumentException(
                    "The specified truststore file " + truststore +
                    " does not exist.");
            }
            */
        }

        return returnProps;
    }

    /**
     * Set the keystore password in preparation for SSLContext creation.
     */
    public synchronized void setKeystorePassword(char[] newKsPassword) {

        if (this.keystorePassword != null) {
            Arrays.fill(this.keystorePassword, ' ');
        }
        if (newKsPassword == null) {
            this.keystorePassword = null;
        } else {
            this.keystorePassword =
                Arrays.copyOf(newKsPassword, newKsPassword.length);
        }
    }

    /**
     * Construct an RMISocketPolicy for client access.
     * @throw IllegalStateException if an exception occurs while constructing
     * a socket policy from the configuration.
     */
    public RMISocketPolicy makeClientSocketPolicy()
        throws IllegalStateException {

        try {
            return new SSLSocketPolicy(null, makeSSLControl(false));
        } catch (Exception e) {
            throw new IllegalStateException(
                "Exception while initializing SSL configuration", e);
        }
    }

    /**
     * Construct an SSLControl object for the client or server end of
     * an SSL connection.
     *
     * @throws IOException if there is a problem reading from the key store or
     *         trust store files.
     * @throws KeyStoreException if there is a problem with the contents of
     *         the key store or trust store contents.
     */
    public SSLControl makeSSLControl(boolean isServer)
        throws KeyStoreException, IOException {

        SSLParameters sslParams = new SSLParameters();

        final String allowCipherSuites = getProp(ENABLED_CIPHER_SUITES);
        if (allowCipherSuites != null) {
            final String[] allowCipherSuiteList =
                trim(allowCipherSuites.split(","));
            sslParams.setCipherSuites(allowCipherSuiteList);
        }

        final String allowProtocols = getProp(ENABLED_PROTOCOLS);
        if (allowProtocols != null) {
            final String[] allowProtocolList = trim(allowProtocols.split(","));
            sslParams.setProtocols(allowProtocolList);
        }

        final SSLContext sslContext = makeSSLContext(makeAuth(), isServer);

        SSLAuthenticator authenticator = null;
        HostnameVerifier hostVerifier = null;
        if (isServer) {
            final String clientIdentityAllowed = getProp(CLIENT_AUTHENTICATOR);
            if (clientIdentityAllowed != null) {
                authenticator = makeAuthenticator(clientIdentityAllowed);
                sslParams.setNeedClientAuth(true);
            }
        } else {
            final String serverIdentityAllowed = getProp(SERVER_HOST_VERIFIER);
            if (serverIdentityAllowed != null) {
                hostVerifier = makeHostVerifier(serverIdentityAllowed);
            }
        }

        /*
         * filter the cipherSuites and protocols to limit to those available
         * in the current environment.
         */
        sslParams = filterSSLParameters(sslParams, sslContext);

        return new SSLControl(sslParams, sslContext,
                              hostVerifier, authenticator);
    }

    /**
     * Filter SSLParameter configuration to respect the supported
     * configuration capabilities of the context.
     */
    private static SSLParameters filterSSLParameters(
        SSLParameters configParams, SSLContext filterContext)
        throws IllegalArgumentException {

        final SSLParameters suppParams =
            filterContext.getSupportedSSLParameters();

        /* Filter the cipher suite selection */
        String[] configCipherSuites = configParams.getCipherSuites();
        if (configCipherSuites != null) {
            final String[] suppCipherSuites = suppParams.getCipherSuites();
            configCipherSuites =
                filterConfig(configCipherSuites, suppCipherSuites);
            if (configCipherSuites.length == 0) {
                throw new IllegalArgumentException(
                    "None of the configured SSL cipher suites are supported " +
                    "by the environment.");
            }
        }

        /* Filter the protocol selection */
        String[] configProtocols = configParams.getProtocols();
        if (configProtocols != null) {
            final String[] suppProtocols = suppParams.getProtocols();
            configProtocols = filterConfig(configProtocols, suppProtocols);
            if (configProtocols.length == 0) {
                throw new IllegalArgumentException(
                    "None of the configured SSL protocols are supported " +
                    "by the environment.");
            }
        }

        /* Construct a new object with the updated settings */
        final SSLParameters newParams =
            new SSLParameters(configCipherSuites, configProtocols);
        newParams.setNeedClientAuth(configParams.getNeedClientAuth());

        return newParams;
    }

    private static SSLAuthenticator makeAuthenticator(
        String peerIdentityAllowed) {

        final InstanceInfo<SSLAuthenticator> authInfo =
            makeAuthenticatorInfo(peerIdentityAllowed);
        return authInfo.impl;
    }

    static InstanceInfo<SSLAuthenticator> makeAuthenticatorInfo(
        String peerIdentityAllowed) {

        final String peerIdent = peerIdentityAllowed.trim();
        if (peerIdent.startsWith("dnmatch(") && peerIdent.endsWith(")")) {
            final String match =
                peerIdent.substring("dnmatch(".length(),
                                    peerIdent.length() - 1);

            return new InstanceInfo<SSLAuthenticator> (
                new SSLPatternAuthenticator(match),
                JE_SSL_DN_AUTHENTICATOR_CLASS, match);
        }
        throw new IllegalArgumentException(
            peerIdent  + " is not a valid server peer constraint.");
    }

    private HostnameVerifier makeHostVerifier(String peerIdentityAllowed) {

        final InstanceInfo<HostnameVerifier> verInfo =
            makeHostVerifierInfo(peerIdentityAllowed);
        return verInfo.impl;
    }

    static InstanceInfo<HostnameVerifier> makeHostVerifierInfo(
        String peerIdentityAllowed) {

        final String peerIdent = peerIdentityAllowed.trim();
        if ("hostname".equals(peerIdent)) {
            return new InstanceInfo<HostnameVerifier>(
                new SSLStdHostVerifier(),
                JE_SSL_STD_HOST_VERIFIER_CLASS, null);
        } else if (peerIdent.startsWith("dnmatch(") &&
                   peerIdent.endsWith(")")) {
            final String match =
                peerIdent.substring("dnmatch(".length(),
                                    peerIdent.length() - 1);
            return new InstanceInfo<HostnameVerifier>(
                new SSLPatternVerifier(match),
                JE_SSL_DN_HOST_VERIFIER_CLASS, match);
        }
        throw new IllegalArgumentException(
            peerIdent  + " is not a valid client peer constraint.");
    }

    private SSLAuthInfo makeAuth() {
        final SSLAuthInfo sslAuth = new SSLAuthInfo();

        final String ks = getProp(KEYSTORE_FILE);
        if (ks != null) {
            sslAuth.keyStore = new File(ks);
        }

        final String ksType = getProp(KEYSTORE_TYPE);
        if (ksType != null) {
            sslAuth.keyStoreType = ksType;
        }

        final char[] ksPwChars = this.keystorePassword;
        if (ksPwChars != null) {
            sslAuth.keyStorePassword = ksPwChars;
        }

        final String keyAlias = getProp(KEYSTORE_ALIAS);
        if (keyAlias != null) {
            sslAuth.keyStoreAlias = keyAlias;
        }

        final String ts = getProp(TRUSTSTORE_FILE);
        if (ts != null) {
            sslAuth.trustStore = new File(ts);
        }

        final String tsType = getProp(TRUSTSTORE_TYPE);
        if (tsType != null) {
            sslAuth.trustStoreType = tsType;
        }

        return sslAuth;
    }

    /**
     * Build an SSLContext out of the available information in the authInfo
     * object.
     *
     * @param authInfo must be non-null
     * @throws IOException if there is a problem reading from the key store or
     *         trust store files.
     * @throws KeyStoreException if there is a problem with the contents of
     *         the key store or trust store contents.
     */
    private SSLContext makeSSLContext(SSLAuthInfo authInfo, boolean isServer)
        throws KeyStoreException, IOException {

        try {
            KeyManager[] kmList = null;
            TrustManager[] tmList = null;

            /* Construct SSL key-related information */

            if (authInfo.keyStore != null &&
                authInfo.keyStorePassword != null) {

                String ksType = authInfo.keyStoreType;
                if (ksType == null) {
                    ksType = KeyStore.getDefaultType();
                }

                final KeyStore ks = KeyStore.getInstance(ksType);
                ks.load(new FileInputStream(authInfo.keyStore),
                        authInfo.keyStorePassword);

                /*
                 * If an alias was specified, it better be present in the
                 * keyStore.
                 */
                final String alias = authInfo.keyStoreAlias;
                if (alias != null && !ks.containsAlias(alias)) {
                    throw new IllegalArgumentException(
                        "Alias " + alias + " not found in " +
                        authInfo.keyStore);
                }

                final KeyManagerFactory kmf =
                    KeyManagerFactory.getInstance("SunX509");
                kmf.init(ks, authInfo.keyStorePassword);
                kmList = kmf.getKeyManagers();

                if (alias != null) {
                    /*
                     * If an alias was specified, we need to construct an
                     * AliasKeyManager, which will delegate to the correct
                     * underlying KeyManager, which we need to locate.
                     */
                    X509ExtendedKeyManager x509KeyManager = null;
                    for (KeyManager km : kmList) {
                        if (km instanceof X509ExtendedKeyManager) {
                            x509KeyManager = (X509ExtendedKeyManager) km;
                            break;
                        }
                    }

                    if (x509KeyManager == null) {
                        throw new IllegalStateException(
                            "Unable to locate an X509ExtendedKeyManager " +
                            "corresponding to keyStore " + authInfo.keyStore);
                    }

                    kmList = new KeyManager[] {
                        new AliasKeyManager(x509KeyManager,
                                            (isServer ? alias : null),
                                            (isServer ? null : alias)) };
                }
            }

            if (authInfo.trustStore != null) {
                String tsType = authInfo.trustStoreType;
                if (tsType == null) {
                    tsType = KeyStore.getDefaultType();
                }

                final KeyStore ts = KeyStore.getInstance(tsType);
                final char[] tsPw = (authInfo.trustStorePassword != null) ?
                    authInfo.trustStorePassword : null;
                ts.load(new FileInputStream(authInfo.trustStore), tsPw);

                final TrustManagerFactory tmf =
                    TrustManagerFactory.getInstance("SunX509");
                tmf.init(ts);

                tmList = tmf.getTrustManagers();
            }

            final SSLContext ctx = SSLContext.getInstance("TLS");

            ctx.init(kmList, tmList, null /* SecureRandom */);

            return ctx;
        } catch (NoSuchAlgorithmException nsae) {
            throw new KeyStoreException(nsae);
        } catch (UnrecoverableKeyException urke) {
            throw new KeyStoreException(urke);
        } catch (KeyManagementException kme) {
            throw new KeyStoreException(kme);
        } catch (CertificateException ce) {
            throw new KeyStoreException(ce);
        }
    }

    /**
     * Class containing a pairing of Strings, representing a class and
     * a constructor argument.
     */
    static final class InstanceInfo<T> {
        final T impl;
        final String jeImplClass;
        final String jeImplParams;

        private InstanceInfo(T impl,
                             String jeImplClass,
                             String jeImplParams) {
            this.impl = impl;
            this.jeImplClass = jeImplClass;
            this.jeImplParams = jeImplParams;
        }
    }

    /**
     * Return the named property, with whitespace trimmed and empty strings
     * replaced with null.
     */
    private String getProp(String key) {
        return trim(props.getProperty(key));
    }

    /**
     * Unlike String.trim(), this supports a null input and returns null
     * if the input String is empty after removing leading and trailing
     * whitespace.
     */
    private String trim(String input) {
        if (input == null) {
            return null;
        }

        final String trimmed = input.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        return trimmed;
    }

    /**
     * Trim an array of strings, where each string entry is individually
     * trimmed using trim(), and then null entries are removed.  A resulting
     * array of length 0 causes a null return value.
     */
    private String[] trim(String[] input) {
        if (input == null) {
            return null;
        }

        final String[] result = new String[input.length];
        int count = 0;
        for (String s : input) {
            final String t = trim(s);
            if (t != null) {
                result[count++] = t;
            }
        }
        if (count == 0) {
            return null;
        }
        return Arrays.copyOf(result, count);
    }

    /**
     * Return the intersection of configChoices and supported
     */
    private static String[] filterConfig(String[] configChoices,
                                         String[] supported) {

        final ArrayList<String> keep = new ArrayList<String>();
        for (String choice : configChoices) {
            for (String supp : supported) {
                if (choice.equals(supp)) {
                    keep.add(choice);
                    break;
                }
            }
        }
        return keep.toArray(new String[keep.size()]);
    }

    /**
     * For test access
     */
    char[] getKeystorePassword() {
        return keystorePassword;
    }

    /**
     * A simple set of fields for SSL authentication-related configuration
     * information. Depending on context, some or all of the fields of this
     * class may be null.
     */
    private class SSLAuthInfo {
        private File keyStore;
        private String keyStoreType;
        private char[] keyStorePassword;
        private String keyStoreAlias;

        private File trustStore;
        private String trustStoreType;
        private char[] trustStorePassword;
    }
}
