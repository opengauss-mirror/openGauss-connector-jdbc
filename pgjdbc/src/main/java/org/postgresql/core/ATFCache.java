/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.postgresql.core;

import org.postgresql.PGProperty;
import org.postgresql.util.LinkedListCache;

import java.sql.SQLException;
import java.util.Properties;

/**
 * Cache for ATF queries.
 *
 * @author shuaikangzhou
 * @since 2025-12-18
 */
public class ATFCache {
    /**
     * Cache for ATF queries at the transaction level.
     */
    public final LinkedListCache<ATFCachedQuery> transactionCache;

    /**
     * Cache for ATF queries at the session level.
     */
    public final LinkedListCache<ATFCachedQuery> sessionCache;
    private long xid = -1; // transaction id associated with the cache
    private boolean isRecovery = false; // is in recovery mode
    private final int maxCacheNum; // max size of session cache
    private final int maxCacheSize; // max size of session cache in bytes
    private boolean isSessionCacheOverLimit = false; // is session cache over limit
    private boolean isTransactionCacheOverLimit = false; // is transaction cache over limit

    /**
     * Create a new ATFCache.
     *
     * @param info the connection properties
     * @throws SQLException if an error occurs
     */
    public ATFCache(Properties info) throws SQLException {
        maxCacheNum = Math.max(0, PGProperty.ATF_STATEMENT_CACHE_QUERIES.getInt(info));
        maxCacheSize = Math.max(0, PGProperty.ATF_STATEMENT_CACHE_SIZE_MIB.getInt(info) * 1024 * 1024);
        sessionCache = new LinkedListCache<ATFCachedQuery>(
            maxCacheNum,
            maxCacheSize,
            new LinkedListCache.EvictAction<ATFCachedQuery>() {
                @Override
                public void evict(ATFCachedQuery cachedQuery) {
                    cachedQuery.clear();
                }
            }
        );
        transactionCache = new LinkedListCache<ATFCachedQuery>(
            maxCacheNum,
            maxCacheSize,
            new LinkedListCache.EvictAction<ATFCachedQuery>() {
                @Override
                public void evict(ATFCachedQuery cachedQuery) {
                    if (cachedQuery.isSessionQuery()) {
                        // if it's a session query, move it to session cache
                        moveCacheToSession(cachedQuery);
                        return;
                    }
                    cachedQuery.clear();
                }
            }
        );
    }


    private void moveCacheToSession(ATFCachedQuery cachedQuery) {
        Query[] subQueries = cachedQuery.query.getSubqueries();
        if (subQueries != null) {
            for (Query subQuery : subQueries) {
                String sql = subQuery.getNativeSql();
                if (cachedQuery.isSessionQuery(sql)) {
                    ATFCachedQuery newAtfCachedQuery = new ATFCachedQuery(
                        subQuery, cachedQuery.parameterList, cachedQuery.flags, cachedQuery.isAutoCommit()
                    );
                    sessionCache.append(newAtfCachedQuery);
                }
            }
        } else {
            sessionCache.append(cachedQuery);
        }
    }

    /**
     * Cache the query.
     *
     * @param cachedQuery the query to cache
     */
    public void cacheQuery(ATFCachedQuery cachedQuery) {
        if (isSessionCacheOverLimit || isTransactionCacheOverLimit) {
            return;
        }
        if (cachedQuery.isAutoCommit()) {
            if (cachedQuery.isSessionQuery()) {
                // session level query
                if (!sessionCache.append(cachedQuery)) {
                    isSessionCacheOverLimit = true;
                }
                return;
            } else {
                return;
            }
        }
        if (!transactionCache.append(cachedQuery)) {
            isTransactionCacheOverLimit = true;
        }
        if (xid == -1) {
            xid = cachedQuery.getXid();
        }
    }

    /**
     * Get the number of elements in the cache.
     *
     * @return The number of elements in the cache.
     */
    public int size() {
        return sessionCache.size() + transactionCache.size();
    }

    /**
     * Clear the cache.
     */
    public void clear() {
        if (!isRecovery) {
            transactionCache.clear();
            isTransactionCacheOverLimit = false;
        }
    }

    /**
     * Close the cache.
     */
    public void close() {
        transactionCache.clear();
        sessionCache.clear();
        isTransactionCacheOverLimit = false;
        isSessionCacheOverLimit = false;
    }

    /*
     * Set the recovery mode of the cache.
     *
     * @param isRecovery true if the cache is in recovery mode, false otherwise.
     */
    public void setIsRecovery(boolean isRecovery) {
        this.isRecovery = isRecovery;
    }

    /**
     * Check if the cache is in recovery mode.
     *
     * @return true if the cache is in recovery mode, false otherwise.
     */
    public boolean getIsRecovery() {
        return isRecovery;
    }

    /**
     * Get the transaction id associated with the cache.
     *
     * @return The transaction id associated with the cache.
     */
    public long getXid() {
        return xid;
    }

    /**
     * Check if the transaction cache is over the limit.
     *
     * @return true if the transaction cache is over the limit, false otherwise.
     */
    public boolean isTransactionCacheOverLimit() {
        return isTransactionCacheOverLimit;
    }

    /**
     * Check if the session cache is over the limit.
     *
     * @return true if the session cache is over the limit, false otherwise.
     */
    public boolean isSessionCacheOverLimit() {
        return isSessionCacheOverLimit;
    }

    /**
     * Get the maximum number of elements in the cache.
     *
     * @return The maximum number of elements in the cache.
     */
    public int getMaxCacheNum() {
        return maxCacheNum;
    }

    /**
     * Get the maximum size of elements in the cache.
     *
     * @return The maximum size of elements in the cache.
     */
    public int getMaxCacheSize() {
        return maxCacheSize;
    }
}
