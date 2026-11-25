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

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.List;

/**
 * Internal to the driver class, please do not use in the application.
 *
 * <p>The class simplifies creation of ResultHandler delegates: it provides default implementation
 * for the interface methods</p>
 */
public class ResultHandlerDelegate implements ResultHandler {
  private final ResultHandler delegate;

  public ResultHandlerDelegate(ResultHandler delegate) {
    this.delegate = delegate;
  }

  @Override
  public void handleResultRows(Query fromQuery, Field[] fields, List<byte[][]> tuples,
      ResultCursor cursor) {
    if (delegate != null) {
      delegate.handleResultRows(fromQuery, fields, tuples, cursor);
    }
  }

  @Override
  public void handleCommandStatus(String status, long updateCount, long insertOID) {
    if (delegate != null) {
      delegate.handleCommandStatus(status, updateCount, insertOID);
    }
  }

  @Override
  public void handleWarning(SQLWarning warning) {
    if (delegate != null) {
      delegate.handleWarning(warning);
    }
  }

  @Override
  public void handleError(SQLException error) {
    if (delegate != null) {
      delegate.handleError(error);
    }
  }

  @Override
  public void handleCompletion() throws SQLException {
    if (delegate != null) {
      delegate.handleCompletion();
    }
  }

  @Override
  public void secureProgress() {
    if (delegate != null) {
      delegate.secureProgress();
    }
  }

  @Override
  public SQLException getException() {
    if (delegate != null) {
      return delegate.getException();
    }
    return null;
  }

  @Override
  public SQLWarning getWarning() {
    if (delegate != null) {
      return delegate.getWarning();
    }
    return null;
  }
}
