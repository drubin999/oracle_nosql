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

package oracle.kv;

/**
 * The KVSecurityConstants interface defines constants used for security
 * configuration. These are most commonly use when populating a set if
 * properties to be passed to {@link KVStoreConfig#setSecurityProperties},
 * but may be used as a reference when configuring a security property file.
 *
 * @since 3.0
 */
public interface KVSecurityConstants {

    /**
     * The name of the property that identifies a security property
     * configuration file to be read when a KVStoreConfig is created, as a
     * set of overriding property definitions.
     */
    public static final String SECURITY_FILE_PROPERTY = "oracle.kv.security";

    /**
     * The name of the property used by KVStore to determine the network
     * mechanism to be used when communicating with Oracle NoSQL DB
     * servers.
     */
    public static final String TRANSPORT_PROPERTY = "oracle.kv.transport";

    /**
     * The value of the {@link #TRANSPORT_PROPERTY} setting that enables the use
     * of SSL/TLS communication.  This property has the value
     * {@value #SSL_TRANSPORT_NAME}.
     */
    public static final String SSL_TRANSPORT_NAME = "ssl";

    /**
     * The name of the property used to control what SSL/TLS cipher suites are
     * acceptable for use. This has the value
     * {@value #SSL_CIPHER_SUITES_PROPERTY}. The property value is a
     * comma-separated list of SSL/TLS cipher suite names. Refer to your Java
     * documentation for the list of valid values.
     */
    public static final String SSL_CIPHER_SUITES_PROPERTY =
        "oracle.kv.ssl.ciphersuites";
        
    /**
     * The name of the property used to control what SSL/TLS procotols are
     * acceptable for use. This has the value {@value #SSL_PROTOCOLS_PROPERTY}.
     * The property value is a comma-separated list of SSL/TLS protocol names.
     * Refer to your Java documentation for the list of valid values.
     */
    public static final String SSL_PROTOCOLS_PROPERTY =
        "oracle.kv.ssl.protocols";

    /**
     * The name of the property used to specify a verification step to
     * be performed when connecting to a NoSQL DB server when using SSL/TLS.
     * This has the value {@value #SSL_HOSTNAME_VERIFIER_PROPERTY}. The only
     * verification step currently supported is the "dnmatch" verifier.
     * <p>
     * The dnmatch verifier must be specified in the form 
     * "dnmatch(distinguished-name)", where distinguished-name must be the
     * NoSQL DB server certificate's distinguished name. For a typical secure
     * deployment this should be "dnmatch(CN=NoSQL)".
     */
    public static final String SSL_HOSTNAME_VERIFIER_PROPERTY =
        "oracle.kv.ssl.hostnameVerifier";

    /**
     * The name of the property to identify the location of a Java
     * truststore file that validates the SSL/TLS certificates used
     * by the NoSQL DB server. This has the value
     * {@value #SSL_TRUSTSTORE_FILE_PROPERTY}. The property setting must be
     * set to an absolute path for the file. If this property is not set,
     * a system property setting of javax.net.ssl.trustStore will be used.
     */
    public static final String SSL_TRUSTSTORE_FILE_PROPERTY =
        "oracle.kv.ssl.trustStore";

    /**
     * The name of the property to identify the type of Java
     * truststore that is referenced by the
     * {@link #SSL_TRUSTSTORE_FILE_PROPERTY} property.  This is only needed if
     * using a non-default truststore type, and the specified type must be a
     * type supported by your Java implementation. This has the value
     * {@value #SSL_TRUSTSTORE_TYPE_PROPERTY}.
     */
    public static final String SSL_TRUSTSTORE_TYPE_PROPERTY =
        "oracle.kv.ssl.trustStoreType";

    /**
     * The name of a property to specify a username for authentication.
     * This has the value {@value #AUTH_USERNAME_PROPERTY}.
     */
    public static final String AUTH_USERNAME_PROPERTY =
        "oracle.kv.auth.username";

    /**
     * The name of the property that identifies an Oracle Wallet directory
     * containing the password of the user to authenticate. This is only used
     * in the Enterprise Edition of the product. This has the value
     * {@value #AUTH_WALLET_PROPERTY}.
     */
    public static final String AUTH_WALLET_PROPERTY =
        "oracle.kv.auth.wallet.dir";

    /**
     * The name of the property that identifies a password store file containing
     * the password of the user to authenticate. This has the value
     * {@value #AUTH_PWDFILE_PROPERTY}.
     */
    public static final String AUTH_PWDFILE_PROPERTY =
        "oracle.kv.auth.pwdfile.file";
}
