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
