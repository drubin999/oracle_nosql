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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.security.auth.Subject;

import oracle.kv.impl.security.SessionAccessException;

/**
 * Provides a cache by which LoginTokens are associated with Subjects.  the
 * cache is configured with a maximum capacity and maximum lifetime that an
 * entry may exist in the cache. The goal is to allow security validations
 * to normally occur very quickly, but to time out cached information that
 * may be stale.
 * <p>
 * The implementation supports configuration such that when a lookup is
 * performed for a token and the cached token is found to be valid, but has
 * been present in the cache for > 50% of the valid cache lifetime, the token
 * is queued to be refreshed as a background activity.  Tokens that are found
 * to still be valid have a new entry created with a new lifetime start.  This
 * allows frequently used tokens to not incur a pause for token validation.
 */

public class TokenCache {

    /* The maximum number of outstanding refresh requests */
    private static final int REFRESH_QUEUE_MAX = 100;

    /* Load factor for the session map */
    private static final float LOAD_FACTOR = 0.6f;

    /* Map of session ID to information about the session */
    private final LinkedHashMap<SessionId, SessionEntry> sessionMap;

    /* The maximum capacity for the cache */
    private final int capacity;

    /* Maximum lifetime for a SessionEntry in ms */
    private volatile long entryLifetimeMax;

    /*
     * A possibly null background refresher.  It is created only if a
     * TokenResolver instance is provided.
     */
    private final EntryRefresher refresher;

    /**
     * Construct a TokenCache.  If a TokenResolver is supplied, the cache will
     * attempt to refresh cache entries in the background if they are accessed
     * by a caller in the latter portion of the entry lifetime.
     *
     * @param capacity the maximum number of entries that are allowed to be
     *   maintained in the cache at any one time.
     * @param entryLifetime the maximum duration that an entry may reside
     *   in the cache, specified in units of milliseconds.  If the value
     *   specified is less than or equal to 0 then there is no fixed lifetime
     *   and eviction from the cache is based solely on LRU eviction.
     * @param resolver a token resolver, which is used to asynchronously
     *   refresh the cache, if provided.  This is null allowable.
     */
    @SuppressWarnings("serial")
    public TokenCache(int capacity,
                      long entryLifetime,
                      TokenResolver resolver) {

        /*
         * Create the sessionMap as a LinkedHashMap() using accessOrder
         * rules, which allows us to use this as an LRU list.
         */
        this.capacity = capacity;
        this.entryLifetimeMax = entryLifetime;

        this.sessionMap = new LinkedHashMap<SessionId, SessionEntry>(
            capacity, LOAD_FACTOR, true) {
            @Override
            protected boolean removeEldestEntry(
                Map.Entry<SessionId, SessionEntry> entry) {
                return size() > TokenCache.this.capacity;
            }
        };

        this.refresher =
            (resolver == null) ? null : new EntryRefresher(resolver);
    }

    /**
     * Look up an entry in the token cache.
     */
    public Subject lookup(LoginToken token) {
        final SessionId id = token.getSessionId();

        SessionEntry entry = null;
        synchronized (sessionMap) {
            entry = sessionMap.get(id);
        }
        if (entry == null) {
            return null;
        }
        final long now = System.currentTimeMillis();

        if (entryLifetimeMax > 0) {
            if (now > (entry.getCreateTime() + entryLifetimeMax)) {
                removeEntry(id);
                return null;
            }

            if (refresher != null) {
                /*
                 * If the entry is at least half way to the point of expiration,
                 * mark it for refresh in hopes that we don't require a
                 * synchronous refresh later on.
                 */
                if (now > (entry.getCreateTime() + entryLifetimeMax / 2)) {
                    refresher.queueForRefresh(entry);
                }
            }
        }

        return entry.getSubject();
    }

    /**
     * Add a resolved token to the token cache.
     */
    public void add(LoginToken token, Subject subject) {
        final SessionId id = token.getSessionId();
        synchronized (sessionMap) {
            sessionMap.put(id, new SessionEntry(token, subject));
        }
    }

    /**
     * Notifies the token cache to stop background execution and waits
     * for completion if the wait parameter is set to true.
     * @param wait set to true if the cache should attempt to wait for
     *   background execution to complete before returning.
     */
    public void stop(boolean wait) {
        if (refresher != null) {
            refresher.stop(wait);
        }
    }

    /**
     * Returns the configured entry lifetime for the cache.
     */
    public long getEntryLifeTime() {
        return entryLifetimeMax;
    }

    /**
     * Sets the configured entry lifetime for the cache.
     */
    public void setEntryLifeTime(final long lifeTimeInMillis) {
        this.entryLifetimeMax = lifeTimeInMillis;
    }

    /**
     * Returns the configured maximum number of entries in the cache.
     */
    public int getCacheSize() {
        return capacity;
    }

    /**
     * Returns statistics regarding cache refresh, for testing purposes.
     */
    public EntryRefreshStats getRefreshStats() {
        return (refresher == null) ?
            new EntryRefreshStats(0) :
            refresher.getRefreshStats();
    }

    /*
     * Internal implementation methods
     */

    private void removeEntry(SessionId id) {
        synchronized (sessionMap) {
            sessionMap.remove(id);
        }
    }

    /**
     * The cache entry object.
     */
    private final class SessionEntry {

        /* The token whose resolution is cached */
        private final LoginToken token;

        /* The cached resolution of the token */
        private final Subject subject;

        /* The time at which the entry was created */
        private final long createTime;

        /*
         * A flag indicating whether the entry is previously been successfully
         * added to the refresh queue.  Entries are not added a second time.
         */
        private boolean queuedForRefresh;

        private SessionEntry(LoginToken token, Subject subject) {
            this.token = token;
            this.subject = subject;
            this.createTime = System.currentTimeMillis();
            this.queuedForRefresh = false;
        }

        private long getCreateTime() {
            return createTime;
        }

        private LoginToken getToken() {
            return token;
        }

        private Subject getSubject() {
            return subject;
        }

        private void setQueuedForRefresh() {
            queuedForRefresh = true;
        }

        private boolean isQueuedForRefresh() {
            return queuedForRefresh;
        }
    }

    /**
     * Statistics regarding refresh activity.
     */
    public static final class EntryRefreshStats {
        private int refreshAttempts;

        public EntryRefreshStats(int refreshAttempts) {
            this.refreshAttempts = refreshAttempts;
        }

        public int getRefreshAttempts() {
            return refreshAttempts;
        }
    }

    /**
     * The cach entry refresh implementation.
     */
    private final class EntryRefresher implements Runnable {

        /* Internal state flat to signal an intent for execution to stop */
        private volatile boolean terminated = false;

        /* A token resolver to use for entry refresh */
        private final TokenResolver resolver;

        /* A queue of entries that require refresh */
        private final BlockingQueue<SessionEntry> refreshQueue;

        /* The thread that performs the refresh activity */
        private final Thread refresherThread;

        /* A count of refresh attempts, for testing purposes */
        private volatile int entryRefreshAttempts;

        private EntryRefresher(TokenResolver resolver) {
            this.entryRefreshAttempts = 0;
            this.resolver = resolver;
            this.refreshQueue =
                new LinkedBlockingQueue<SessionEntry>(REFRESH_QUEUE_MAX);
            final String threadName = "TokenRefresh";
            this.refresherThread = new Thread(this, threadName);
            refresherThread.setDaemon(true);
            refresherThread.start();
        }

        /**
         * Attempt to stop the background activity for the refresher.
         * @param wait if true, the the method attempts to wait for the
         *   background thread to finish background activity.
         */
        private void stop(boolean wait) {
            /* Set the flag to notify the run loop that it should exit */
            terminated = true;

            /* Then give it a reason to notice the flag */
            refresherThread.interrupt();

            if (wait) {
                try {
                    refresherThread.join();
                } catch (InterruptedException ie) /* CHECKSTYLE:OFF */ {
                } /* CHECKSTYLE:ON */
            }
        }

        /**
         * Add the entry to the refresh queue if possible.
         * @param entry a SessionEntry whose token should be resolved as a
         *   background activity.
         */
        private void queueForRefresh(SessionEntry entry) {
            synchronized (entry) {
                if (!entry.isQueuedForRefresh()) {
                    if (refreshQueue.offer(entry)) {
                        entry.setQueuedForRefresh();
                    }
                }
            }
        }

        private EntryRefreshStats getRefreshStats() {
            return new EntryRefreshStats(entryRefreshAttempts);
        }

        /**
         * The Runnable entrypoint.
         */
        @Override
        public void run() {
            while (!terminated) {
                try {
                    final SessionEntry entry = refreshQueue.take();
                    entryRefreshAttempts++;
                    final Subject resolved = resolver.resolve(entry.getToken());
                    if (resolved != null) {
                        add(entry.getToken(), resolved);
                    }
                } catch (SessionAccessException sae) /* CHECKSTYLE:OFF */ {
                    /*
                     * We could try to requeue for later access, but that
                     * could be immediatedly, so just let it go.
                     */
                } /* CHECKSTYLE:ON */
                catch (InterruptedException ie) /* CHECKSTYLE:OFF */ {
                    /* We are probably being asked to terminate */
                } /* CHECKSTYLE:ON */
                catch (RuntimeException rte) /* CHECKSTYLE:OFF */ {
                    /*
                     * It's unclear what happened, but ignore it. We expect
                     * that any errors were logged by the resolver.
                     */
                } /* CHECKSTYLE:ON */
            }
        }
    }
}
