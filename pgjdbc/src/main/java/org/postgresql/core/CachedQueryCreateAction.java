/*
 * Copyright (c) 2015, PostgreSQL Global Development Group
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.
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
