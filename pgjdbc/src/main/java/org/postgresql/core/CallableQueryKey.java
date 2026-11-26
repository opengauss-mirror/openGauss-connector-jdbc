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
 * Serves as a cache key for {@link java.sql.CallableStatement}.
 * Callable statements require some special parsing before use (due to JDBC {@code {?= call...}}
 * syntax, thus a special cache key class is used to trigger proper parsing for callable statements.
 */
class CallableQueryKey extends BaseQueryKey {

  CallableQueryKey(String sql) {
    super(sql, true, true);
  }

  @Override
  public String toString() {
    return "CallableQueryKey{"
        + "sql='" + sql + '\''
        + ", isParameterized=" + isParameterized
        + ", escapeProcessing=" + escapeProcessing
        + '}';
  }

  @Override
  public int hashCode() {
    return super.hashCode() * 31;
  }

  @Override
  public boolean equals(Object o) {
    // Nothing interesting here, overriding equals to make hashCode and equals paired
    return super.equals(o);
  }
}
