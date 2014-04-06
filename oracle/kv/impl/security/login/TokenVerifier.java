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

package oracle.kv.impl.security.login;

import javax.security.auth.Subject;

import oracle.kv.impl.security.SessionAccessException;

/**
 * Provides a common mechanism for looking up user capabilities based on tokens.
 * This classes combines the use of TokenResolver and TokenCache in order to
 * provide low-overhead verification of tokens, on average.
 */

public class TokenVerifier {

    private final TokenResolver tokenResolver;
    private volatile TokenCache tokenCache;

    /**
     * A class for configuring the token cache.
     */
    public static class CacheConfig {
        private final int capacity;
        private final long timeoutMs;

        /**
         * Creates a new token cache configuration.
         */
        public CacheConfig(int capacity, long timeoutMs) {
            this.capacity = capacity;
            this.timeoutMs = timeoutMs;
        }

        /**
         * Returns the cache capacity, terms of the number of entries.
         */
        public int getCapacity() {
            return capacity;
        }

        /**
         * Returns the cache entry timeout, in units of MS.
         */
        public long getTimeout() {
            return timeoutMs;
        }
    }

    /**
     * Creates a new TokenVerifier.
     * @param cacheConfig if null, no cache is to be used.  Otherwise, provides
     *    configuration information for the token cache.
     * @param tokenResolver the resolver instance to use to determine a login
     *    identity base on a login token.
     */
    public TokenVerifier(CacheConfig cacheConfig, TokenResolver tokenResolver) {
        this.tokenResolver = tokenResolver;
        this.tokenCache = (cacheConfig == null) ?
            null :
            new TokenCache(cacheConfig.getCapacity(), cacheConfig.getTimeout(),
                           tokenResolver);
    }

    /**
     * Determine the user capabilities associated with the input LoginToken.
     * @param token a LoginToken acquired from the system
     * @result a Subject that reflects the user capabilities if the token is
     *    valid or null if not valid.
     * @throw SessionAccessException if an operational failure prevents
     *    reliable verification
     */
    public Subject verifyToken(LoginToken token)
        throws SessionAccessException {

        Subject subject =
            (null == tokenCache) ? null : tokenCache.lookup(token);

        if (subject != null) {
            return subject;
        }

        subject = tokenResolver.resolve(token);

        if (subject != null && tokenCache != null) {
            tokenCache.add(token, subject);
        }

        return subject;
    }

    /**
     * Update the tokenCache with a new size.
     *
     * @param newSize
     * @return true if the size is updated, or false if it remains the same
     */
    public boolean updateLoginCacheSize(final int newSize) {
        if (newSize == tokenCache.getCacheSize()) {
            return false;
        }
        final long entryLifeTime = tokenCache.getEntryLifeTime();
        final TokenCache oldCache = tokenCache;
        tokenCache = new TokenCache(newSize, entryLifeTime, tokenResolver);

        /* shut down background processing on the old cache */
        if (oldCache != null) {
            oldCache.stop(false);
        }
        return true;
    }

    /**
     * Update the tokenCache entry lifetime.
     *
     * @param newTimeoutInMillis
     * @return true if the lifetime is updated, or false if it remains the same
     */
    public boolean updateLoginCacheTimeout(final long newTimeoutInMillis) {
        if (tokenCache == null ||
            newTimeoutInMillis == tokenCache.getEntryLifeTime()) {
            return false;
        }
        tokenCache.setEntryLifeTime(newTimeoutInMillis);
        return true;
    }
}
