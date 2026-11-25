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
