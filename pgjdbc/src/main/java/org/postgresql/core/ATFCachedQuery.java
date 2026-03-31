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

import org.postgresql.util.CanEstimateSize;

/**
 * Cached query for ATF.
 *
 * @author shuaikangzhou
 * @since 2025-10-18
 */
public class ATFCachedQuery implements CanEstimateSize {
    /**
     * The query to be cached.
     */
    public final Query query;

    /**
     * The parameter list of the query.
     */
    public final ParameterList parameterList;

    /**
     * The flags of the query.
     */
    public final int flags;

    /**
     * The queries of the batch.
     */
    public final Query[] queries;

    /**
     * The parameter lists of the batch.
     */
    public final ParameterList[] parameterLists;
    private final boolean isBatch;
    private final boolean isAutoCommit;

    /**
     * Create a new ATFCachedQuery.
     *
     * @param query the query
     * @param parameterList the parameter list
     * @param flags the flags
     * @param isAutoCommit true if the query is auto-commit, false otherwise.
     */
    public ATFCachedQuery(Query query, ParameterList parameterList, int flags, boolean isAutoCommit) {
        // we need to create copies of our query and parameterList, otherwise the values can be changed
        this.query = query.copy();
        if (parameterList != null) {
            this.parameterList = parameterList.copy();
        } else {
            this.parameterList = null;
        }
        this.flags = flags;
        this.isAutoCommit = isAutoCommit;
        queries = null;
        parameterLists = null;
        isBatch = false;
    }

    /**
     * Create a new ATFCachedQuery.
     *
     * @param querys the querys
     * @param parameterLists the parameter lists
     * @param flags the flags
     * @param isAutoCommit true if the query is auto-commit, false otherwise.
     */
    public ATFCachedQuery(Query[] querys, ParameterList[] parameterLists, int flags, boolean isAutoCommit) {
        // we need to create copies of our query and parameterList, otherwise the values can be changed
        this.queries = new Query[querys.length];
        for (int i = 0; i < querys.length; i++) {
            this.queries[i] = querys[i].copy();
        }
        this.parameterLists = new ParameterList[parameterLists.length];
        for (int i = 0; i < parameterLists.length; i++) {
            this.parameterLists[i] = parameterLists[i].copy();
        }
        this.flags = flags;
        isBatch = true;
        query = null;
        parameterList = null;
        this.isAutoCommit = isAutoCommit;
    }

    /**
     * Clear the query resources.
     */
    public void clear() {
        if (isBatch) {
            for (Query q : queries) {
                q.close();
            }
        } else {
            if (query != null) {
                query.close();
            }
        }
    }

    /**
     * Set the isRecovery flag for the query result.
     *
     * @param isRecovery true if the query is recovery, false otherwise.
     */
    public void setIsRecovery(boolean isRecovery) {
        if (isBatch) {
            for (Query q : queries) {
                q.getQueryResult().setIsRecovery(isRecovery);
            }
        } else {
            if (query != null) {
                query.getQueryResult().setIsRecovery(isRecovery);
            }
        }
    }

    /**
     * Set the isSessionQuery flag for the query result.
     *
     * @param isSessionQuery true if the query is session query, false otherwise.
     */
    public void setIsSessionQuery(boolean isSessionQuery) {
        if (isBatch) {
            for (Query q : queries) {
                q.getQueryResult().setIsSessionQuery(isSessionQuery);
            }
        } else {
            if (query != null) {
                query.getQueryResult().setIsSessionQuery(isSessionQuery);
            }
        }
    }

    /**
     * Set the isLastQuery flag for the query result.
     *
     * @param isLastQuery true if the query is the last query, false otherwise.
     */
    public void setIsLastQuery(boolean isLastQuery) {
        if (isBatch) {
            for (Query q : queries) {
                q.getQueryResult().setIsLastQuery(isLastQuery);
            }
        } else {
            if (query != null) {
                query.getQueryResult().setIsLastQuery(isLastQuery);
            }
        }
    }

    /**
     * Check if the query is session query.
     *
     * @return true if the query is session query, false otherwise.
     */
    public boolean isSessionQuery() {
        if (isBatch || query == null) {
            return false;
        }

        String sql = query.getNativeSql();

        if (query.getSubqueries() == null) {
            return isSessionQuery(sql);
        } else {
            boolean isSession = false;
            for (Query subquery : query.getSubqueries()) {
                isSession = isSessionQuery(subquery.getNativeSql());
                if (isSession) {
                    break;
                }
            }
            return isSession;
        }
    }


    private String formatSql(String sql) {
        char[] aChars = sql.toCharArray();
        int i = 0;
        // skip comments and whitespace
        while (i < aChars.length) {
            char aChar = aChars[i];
            if (Character.isWhitespace(aChar)) {
                i++;
                continue;
            } else if (aChar == '-') {
                // possibly -- style comment
                int newI = Parser.parseLineComment(aChars, i);
                if (newI != i) {
                    i = newI + 1;
                    continue;
                }
            } else if (aChar == '/') {
                // possibly /* */ style comment
                int newI = Parser.parseBlockComment(aChars, i);
                if (newI != i) {
                    i = newI + 1;
                    continue;
                }
            } else {
                break;
            }
        }
        if (i >= aChars.length) {
            return "";
        }
        return sql.substring(i).toUpperCase(java.util.Locale.ENGLISH);
    }

    /**
     * Check if the query is session query.
     *
     * @param sql the native sql text.
     * @return true if the query is session query, false otherwise.
     */
    public boolean isSessionQuery(String sql) {
        String remaining = formatSql(sql);

        if (remaining == null
            || remaining.startsWith("SET LOCAL")
            || remaining.startsWith("SET TRANSACTION")) {
            return false;
        }

        if (remaining.startsWith("SET")
            || remaining.startsWith("RESET")
            || remaining.startsWith("DISCARD")
            || remaining.startsWith("PREPARE")
            || remaining.startsWith("DEALLOCATE")
            ||remaining.startsWith("LISTEN")
            || remaining.startsWith("UNLISTEN")
            || remaining.startsWith("LOAD")) {
            return true;
        }

        return false;
    }

    /**
     * Get the XID of the query.
     *
     * @return the XID of the query.
     */
    public long getXid() {
        if (isBatch) {
            return queries[0].getQueryResult().getXid();
        } else {
            return query.getQueryResult().getXid();
        }
    }

    /**
     * Get the size of the query. Only include the size of the query text and the result size.
     *
     * @return the size of the query.
     */
    @Override
    public long getSize() {
        if (isBatch) {
            // 2 bytes per char, revise with Java 9's compact strings
            return queries.length * (queries[0].getNativeSql().length() * 2L + queries[0].getQueryResult().getSize());
        } else {
            return query.getNativeSql().length() * 2L + query.getQueryResult().getSize();
        }
    }

    /**
     * Check if the query is auto-commit.
     *
     * @return true if the query is auto-commit, false otherwise.
     */
    public boolean isAutoCommit() {
        return isAutoCommit;
    }

    /**
     * Check if the query is batch.
     *
     * @return true if the query is batch, false otherwise.
     */
    public boolean isBatch() {
        return isBatch;
    }

    @Override
    public String toString() {
        if (isBatch) {
            return "ATFCachedQuery{"
            + "querys=" + queries[0]
            + ", isBatch=" + isBatch
            + ", queryResult=" + queries[0].getQueryResult()
            + ", parameterLists=" + parameterLists[0]
            + ", flags=" + flags
            + '}';
        } else {
            return "ATFCachedQuery{"
            + "query=" + query
            + ", queryResult=" + query.getQueryResult()
            + ", parameterList=" + parameterList
            + ", flags=" + flags
            + '}';
        }
    }
}
