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

package oracle.kv.impl.rep.login;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.Principal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.security.auth.Subject;

import oracle.kv.impl.security.KVStoreRole;
import oracle.kv.impl.security.KVStoreRolePrincipal;
import oracle.kv.impl.security.KVStoreUserPrincipal;
import oracle.kv.impl.security.login.LoginSession;

/**
 * KVSession captures the state of a login session as stored in the persistent
 * security section of the store.
 */
public class KVSession {

    /*
     * The data format of this object.  We provide for the ability to create
     * read, update and delete of version 1 objects and to read update and
     * delete (no create) version 2.  If a version 2 of this format is created,
     * it must only add fields at the end.  If an object of version 2 is read,
     * we de-serialize only the V1 portion and retain the remainder as a
     * byte array, and add it to the end of the object when re-writing.
     */
    static final int VERSION = 1;
    static final int NEXT_VERSION = 2;

    /**
     * The source version of this object.
     */
    private byte version;

    /**
     * The key of this session
     */
    private byte[] sessionId;

    /**
     * The internal user id associated with the session.
     */
    private String userId;

    /**
     * The user name associated with the session
     */
    private String userName;

    /**
     * The KVStore roles associated with the session
     */
    private KVStoreRole[] roles;

    /**
     * The address of the host from which the user login originated
     */
    private String clientHost;

    /**
     * The expiration  time of the session
     */
    private long sessionExpire;

    /**
     * A possibly null byte array that should be appended to the output when
     * writing.
     */
    byte[] remainder;

    private KVSession(byte version) {
        this.version = version;
    }

    /**
     * Creates a KVSession object from a LoginSession.
     * @throws IllegalArgumentException if the session is null or does not
     * have an associated KVStoreUserPrincipal
     */
    public KVSession(LoginSession session)
        throws IllegalArgumentException {

        if (session == null) {
            throw new IllegalArgumentException("session may not be null");
        }

        this.version = VERSION;
        final byte[] idValue = session.getId().getValue();
        this.sessionId = Arrays.copyOf(idValue, idValue.length);
        final Subject subj = session.getSubject();

        final KVStoreUserPrincipal userPrinc =
            KVStoreUserPrincipal.getSubjectUser(subj);
        if (userPrinc == null) {
            throw new IllegalArgumentException(
                "No user principal associated with the login session");
        }
        this.userId = userPrinc.getUserId();
        this.userName = userPrinc.getName();
        this.roles = KVStoreRolePrincipal.getSubjectRoles(subj);
        if (this.roles == null) {
            this.roles = new KVStoreRole[0];
        }
        this.clientHost = session.getClientHost();
        this.sessionExpire = session.getExpireTime();
    }

    public byte[] toByteArray()
        throws IOException {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutputStream dos = new DataOutputStream(baos);
        dos.write(version);

        dos.write(sessionId.length);
        dos.write(sessionId, 0, sessionId.length);

        if (userId == null) {
            dos.writeBoolean(false);
        } else {
            dos.writeBoolean(true);
            dos.writeUTF(userId);
        }
        if (userName == null) {
            dos.writeBoolean(false);
        } else {
            dos.writeBoolean(true);
            dos.writeUTF(userName);
        }
        if (clientHost == null) {
            dos.writeBoolean(false);
        } else {
            dos.writeBoolean(true);
            dos.writeUTF(clientHost);
        }
        dos.write(roles.length);
        for (int i = 0; i < roles.length; i++) {
            dos.writeUTF(roles[i].name());
        }
        dos.writeLong(sessionExpire);

        /*
         * If there is any remainder (because we read a version 2 object), put
         * the remainder on the output stream.
         */
        if (remainder != null) {
            dos.write(remainder, 0, remainder.length);
        }
        dos.close();

        return baos.toByteArray();
    }

    public static KVSession fromByteArray(byte[] data)
        throws IOException {

        final ByteArrayInputStream bais = new ByteArrayInputStream(data);
        final DataInputStream dis = new DataInputStream(bais);

        @SuppressWarnings("unused")
        final int version = dis.readByte();

        if (version != VERSION && version != NEXT_VERSION) {
            throw new IOException("Unsupported version: " + version);
        }

        final KVSession session = new KVSession((byte) version);

        final int sidLen = dis.readByte();
        session.sessionId = new byte[sidLen];
        dis.readFully(session.sessionId);

        final boolean haveUserId = dis.readBoolean();
        session.userId = haveUserId ? dis.readUTF() : null;

        final boolean haveUserName = dis.readBoolean();
        session.userName = haveUserName ? dis.readUTF() : null;

        final boolean haveClientHost = dis.readBoolean();
        session.clientHost = haveClientHost ? dis.readUTF() : null;

        final int nRoles = dis.readByte();
        session.roles = new KVStoreRole[nRoles];
        for (int i = 0; i < nRoles; i++) {
            session.roles[i] = Enum.valueOf(KVStoreRole.class, dis.readUTF());
        }

        session.sessionExpire = dis.readLong();

        if (version == NEXT_VERSION) {
            /*
             * If we are reading a session written in the next version of the
             * format, it may differ only in the presence of additional fields.
             * Capture the remaining data so that it can be included if this 
             * object is re-written.
             */
            final byte[] buffer = new byte[data.length];
            final int bytesRead = dis.read(buffer);
            if (bytesRead > 0) {
                session.remainder = Arrays.copyOfRange(buffer, 0, bytesRead);
            }
        } else {
            int read = dis.read(new byte[1]);
            if (read > 0) {
                throw new IOException("Encountered unexpected data");
            }
        }

        dis.close();

        return session;
    }

    public byte[] getSessionId() {
        return sessionId;
    }

    public long getSessionExpire() {
        return sessionExpire;
    }

    public void setSessionExpire(long expireTime) {
        sessionExpire = expireTime;
    }

    public LoginSession makeLoginSession() {

        final LoginSession sess =
            new LoginSession(new LoginSession.Id(sessionId),
                             makeSubject(), clientHost, true);
        sess.setExpireTime(sessionExpire);
        return sess;
    }

    /* For testing */
    void setVersion(byte version) {
        this.version = version;
    }

    private Subject makeSubject() {

        final Set<Principal> princs = new HashSet<Principal>();

        princs.add(new KVStoreUserPrincipal(userName, userId));

        for (KVStoreRole role : roles) {
            princs.add(KVStoreRolePrincipal.get(role));
        }

        final Set<Object> publicCreds = new HashSet<Object>();
        final Set<Object> privateCreds = new HashSet<Object>();

        return new Subject(true, princs, publicCreds, privateCreds);
    }
}
