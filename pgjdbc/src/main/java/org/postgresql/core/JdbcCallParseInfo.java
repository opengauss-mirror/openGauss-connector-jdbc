/*
 * Copyright (c) 2015, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.core;

/**
 * Contains parse flags from {@link Parser#modifyJdbcCall(String, boolean, int, int)}.
 */
public class JdbcCallParseInfo {
  private final String sql;
  private final boolean isFunction;
  private final boolean isOracleCompatibilityFunction;

  public JdbcCallParseInfo(String sql, boolean isFunction, boolean isOracleCompatibilityFunction) {
    this.sql = sql;
    this.isFunction = isFunction;
    this.isOracleCompatibilityFunction = isOracleCompatibilityFunction;
  }

  /**
   * SQL in a native for certain backend version.
   *
   * @return SQL in a native for certain backend version
   */
  public String getSql() {
    return sql;
  }

  /**
   * Returns if given SQL is a function.
   *
   * @return {@code true} if given SQL is a function
   */
  public boolean isFunction() {
    return isFunction;
  }

  public boolean isOracleCompatibilityFunction() {
    return isOracleCompatibilityFunction;
  }

}
