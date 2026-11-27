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

import java.sql.SQLException;

/**
 * This interface defines the public PostgreSQL extensions to java.sql.Statement. All Statements
 * constructed by the PostgreSQL driver implement PGStatement.
 */
public interface PGStatement {
  // We can't use Long.MAX_VALUE or Long.MIN_VALUE for java.sql.date
  // because this would break the 'normalization contract' of the
  // java.sql.Date API.
  // The follow values are the nearest MAX/MIN values with hour,
  // minute, second, millisecond set to 0 - this is used for
  // -infinity / infinity representation in Java
  long DATE_POSITIVE_INFINITY = 9223372036825200000L;
  long DATE_NEGATIVE_INFINITY = -9223372036832400000L;
  long DATE_POSITIVE_SMALLER_INFINITY = 185543533774800000L;
  long DATE_NEGATIVE_SMALLER_INFINITY = -185543533774800000L;


  /**
   * Returns the Last inserted/updated oid.
   *
   * @return OID of last insert
   * @throws SQLException if something goes wrong
   * @since 7.3
   */
  long getLastOID() throws SQLException;

  /**
   * Turn on the use of prepared statements in the server (server side prepared statements are
   * unrelated to jdbc PreparedStatements) As of build 302, this method is equivalent to
   * <code>setPrepareThreshold(1)</code>.
   *
   * @param flag use server prepare
   * @throws SQLException if something goes wrong
   * @since 7.3
   * @deprecated As of build 302, replaced by {@link #setPrepareThreshold(int)}
   */
  @Deprecated
  void setUseServerPrepare(boolean flag) throws SQLException;

  /**
   * Checks if this statement will be executed as a server-prepared statement. A return value of
   * <code>true</code> indicates that the next execution of the statement will be done as a
   * server-prepared statement, assuming the underlying protocol supports it.
   *
   * @return true if the next reuse of this statement will use a server-prepared statement
   */
  boolean isUseServerPrepare();

  /**
   * <p>Sets the reuse threshold for using server-prepared statements.</p>
   *
   * <p>If <code>threshold</code> is a non-zero value N, the Nth and subsequent reuses of a
   * PreparedStatement will use server-side prepare.</p>
   *
   * <p>If <code>threshold</code> is zero, server-side prepare will not be used.</p>
   *
   * <p>The reuse threshold is only used by PreparedStatement and CallableStatement objects; it is
   * ignored for plain Statements.</p>
   *
   * @param threshold the new threshold for this statement
   * @throws SQLException if an exception occurs while changing the threshold
   * @since build 302
   */
  void setPrepareThreshold(int threshold) throws SQLException;

  /**
   * Gets the server-side prepare reuse threshold in use for this statement.
   *
   * @return the current threshold
   * @see #setPrepareThreshold(int)
   * @since build 302
   */
  int getPrepareThreshold();
}
