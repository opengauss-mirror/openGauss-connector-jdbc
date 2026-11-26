/*
 * Copyright (c) 2015, PostgreSQL Global Development Group
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * All rights reserved.
 *
 * Licensed under the 2-Clause BSD License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/license/bsd-2-clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.postgresql.core;

import org.postgresql.jdbc.PreferQueryMode;
import org.postgresql.util.LruCache;

import java.sql.SQLException;
import java.util.List;

/**
 * Creates an instance of {@link CachedQuery} for a given connection.
 */
class CachedQueryCreateAction implements LruCache.CreateAction<Object, CachedQuery> {
  private static final String[] EMPTY_RETURNING = new String[0];
  private final QueryExecutor queryExecutor;

  CachedQueryCreateAction(QueryExecutor queryExecutor) {
    this.queryExecutor = queryExecutor;
  }

  @Override
  public CachedQuery create(Object key) throws SQLException {
    assert key instanceof String || key instanceof BaseQueryKey
        : "Query key should be String or BaseQueryKey. Given " + key.getClass() + ", sql: "
        + String.valueOf(key);
    BaseQueryKey queryKey;
    String parsedSql;
    if (key instanceof BaseQueryKey) {
      queryKey = (BaseQueryKey) key;
      parsedSql = queryKey.sql;
    } else {
      queryKey = null;
      parsedSql = (String) key;
    }
    if (key instanceof String || queryKey.escapeProcessing) {
      parsedSql =
          Parser.replaceProcessing(parsedSql, true, queryExecutor.getStandardConformingStrings());
    }
    boolean isFunction;
    boolean isACompatibilityFunction;
    if (key instanceof CallableQueryKey) {
      JdbcCallParseInfo callInfo =
          Parser.modifyJdbcCall(parsedSql, queryExecutor.getStandardConformingStrings(),
              queryExecutor.getServerVersionNum(), queryExecutor.getProtocolVersion());
      parsedSql = callInfo.getSql();
      isFunction = callInfo.isFunction();
      isACompatibilityFunction = callInfo.isACompatibilityFunction();
    } else {
      isFunction = false;
      isACompatibilityFunction = false;
    }
    boolean isParameterized = key instanceof String || queryKey.isParameterized;
    boolean splitStatements = isParameterized || queryExecutor.getPreferQueryMode().compareTo(PreferQueryMode.EXTENDED) >= 0;

    String[] returningColumns;
    if (key instanceof QueryWithReturningColumnsKey) {
      returningColumns = ((QueryWithReturningColumnsKey) key).columnNames;
    } else {
      returningColumns = EMPTY_RETURNING;
    }

    List<NativeQuery> queries = Parser.parseJdbcSql(parsedSql,
        queryExecutor.getStandardConformingStrings(), isParameterized, splitStatements,
        queryExecutor.isReWriteBatchedInsertsEnabled(), queryExecutor.getQuoteReturningIdentifiers(),
        returningColumns);

    Query query = queryExecutor.wrap(queries);
    return new CachedQuery(key, query, isFunction, isACompatibilityFunction);
  }
}
