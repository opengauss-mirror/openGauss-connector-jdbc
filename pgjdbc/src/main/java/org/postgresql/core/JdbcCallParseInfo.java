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

/**
 * Contains parse flags from {@link Parser#modifyJdbcCall(String, boolean, int, int)}.
 */
public class JdbcCallParseInfo {
  private final String sql;
  private final boolean isFunction;
  private final boolean isACompatibilityFunction;

  public JdbcCallParseInfo(String sql, boolean isFunction, boolean isACompatibilityFunction) {
    this.sql = sql;
    this.isFunction = isFunction;
    this.isACompatibilityFunction = isACompatibilityFunction;
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

  public boolean isACompatibilityFunction() {
    return isACompatibilityFunction;
  }

}
