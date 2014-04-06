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

import java.security.Principal;
import java.security.cert.Certificate;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;

/**
 * SSL socket authentication implementation using standard server host
 * certificate authentication.
 */
public class SSLStdHostVerifier implements HostnameVerifier {

    private static final int ALTNAME_DNS = 2;
    private static final int ALTNAME_IP  = 7;

    public SSLStdHostVerifier() {
    }

    @Override
    public boolean verify(String targetHost, SSLSession sslSession) {

        if (targetHost == null) {
            return false;
        }

        Principal principal = null;
        Certificate[] peerCerts = null;
        try {
            principal = sslSession.getPeerPrincipal();
            peerCerts = sslSession.getPeerCertificates();
        } catch (SSLPeerUnverifiedException pue) {
            return false;
        }

        if (principal != null && principal instanceof X500Principal) {
            final X500Principal x500Principal = (X500Principal) principal;
            final String name = x500Principal.getName("RFC1779");
            if (targetHost.equalsIgnoreCase(name)) {
                return true;
            }
        }

        /* Check for SubjectAlternativeNames */
        if (peerCerts[0] instanceof java.security.cert.X509Certificate) {

            final X509Certificate peerCert =
                (java.security.cert.X509Certificate) peerCerts[0];

            Collection<List<?>> altNames = null;
            try {
                altNames = peerCert.getSubjectAlternativeNames();
            } catch (CertificateParsingException cpe) /* CHECKSTYLE:OFF */ {
                // TODO: think about communicating this error
                // Should add a logger to the constructor
            } /* CHECKSTYLE:ON */

            if (altNames != null) {
                for (List<?> altName : altNames) {
                    /*
                     * altName will be a 2-element list, with the first being
                     * the name type and the second being the "name".  For
                     * DNS and IP entries, the "name" will be a string.
                     */
                    final int nameType = ((Integer) altName.get(0)).intValue();
                    if (nameType == ALTNAME_IP || nameType == ALTNAME_DNS) {
                        final String nameValue = (String) altName.get(1);
                        if (targetHost.equals(nameValue)) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }
}
