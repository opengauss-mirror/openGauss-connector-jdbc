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

package org.postgresql.core;

import java.util.Arrays;

/**
 * Cache key for a query that have some returning columns.
 * {@code columnNames} should contain non-quoted column names.
 * The parser will quote them automatically.
 * <p>There's a special case of {@code columnNames == new String[]{"*"}} that means all columns
 * should be returned. {@link Parser} is aware of that and does not quote {@code *}</p>
 */
class QueryWithReturningColumnsKey extends BaseQueryKey {
  public final String[] columnNames;
  private int size; // query length cannot exceed MAX_INT

  QueryWithReturningColumnsKey(String sql, boolean isParameterized, boolean escapeProcessing,
      String[] columnNames) {
    super(sql, isParameterized, escapeProcessing);
    if (columnNames == null) {
      // TODO: teach parser to fetch key columns somehow when no column names were given
      columnNames = new String[]{"*"};
    }
    this.columnNames = columnNames;
  }

  @Override
  public long getSize() {
    int sizeOfThis = this.size;
    if (sizeOfThis != 0) {
      return sizeOfThis;
    }
    sizeOfThis = (int) super.getSize();
    if (columnNames != null) {
        sizeOfThis += 16L; // array itself
      for (String columnName: columnNames) {
          sizeOfThis += columnName.length() * 2L; // 2 bytes per char, revise with Java 9's compact strings
      }
    }
    this.size = sizeOfThis;
    return sizeOfThis;
  }

  @Override
  public String toString() {
    return "QueryWithReturningColumnsKey{"
        + "sql='" + sql + '\''
        + ", isParameterized=" + isParameterized
        + ", escapeProcessing=" + escapeProcessing
        + ", columnNames=" + Arrays.toString(columnNames)
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    QueryWithReturningColumnsKey that = (QueryWithReturningColumnsKey) o;

    // Probably incorrect - comparing Object[] arrays with Arrays.equals
    return Arrays.equals(columnNames, that.columnNames);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + Arrays.hashCode(columnNames);
    return result;
  }
}
