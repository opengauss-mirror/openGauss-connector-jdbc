/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
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
