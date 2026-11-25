/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
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

package org.postgresql;

import org.postgresql.core.Field;

import java.sql.SQLException;

public interface PGResultSetMetaData {

  /**
   * Returns the underlying column name of a query result, or "" if it is unable to be determined.
   *
   * @param column column position (1-based)
   * @return underlying column name of a query result
   * @throws SQLException if something wrong happens
   * @since 8.0
   */
  String getBaseColumnName(int column) throws SQLException;

  /**
   * Returns the underlying table name of query result, or "" if it is unable to be determined.
   *
   * @param column column position (1-based)
   * @return underlying table name of query result
   * @throws SQLException if something wrong happens
   * @since 8.0
   */
  String getBaseTableName(int column) throws SQLException;

  /**
   * Returns the underlying schema name of query result, or "" if it is unable to be determined.
   *
   * @param column column position (1-based)
   * @return underlying schema name of query result
   * @throws SQLException if something wrong happens
   * @since 8.0
   */
  String getBaseSchemaName(int column) throws SQLException;

  /**
   * Is a column Text or Binary?
   *
   * @param column column position (1-based)
   * @return 0 if column data foramt is TEXT, or 1 if BINARY
   * @throws SQLException if something wrong happens
   * @see Field#BINARY_FORMAT
   * @see Field#TEXT_FORMAT
   * @since 9.4
   */
  int getFormat(int column) throws SQLException;
}
