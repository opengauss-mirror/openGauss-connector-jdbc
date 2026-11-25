/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
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
// Copyright (c) 2004, Open Cloud Limited.

package org.postgresql.core;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.List;

/**
 * Empty implementation of {@link ResultHandler} interface.
 * {@link SQLException#setNextException(SQLException)} has {@code O(N)} complexity,
 * so this class tracks the last exception object to speedup {@code setNextException}.
 */
public class ResultHandlerBase implements ResultHandler {
  // Last exception is tracked to avoid O(N) SQLException#setNextException just in case there
  // will be lots of exceptions (e.g. all batch rows fail with constraint violation or so)
  private SQLException firstException;
  private SQLException lastException;

  private SQLWarning firstWarning;
  private SQLWarning lastWarning;

  @Override
  public void handleResultRows(Query fromQuery, Field[] fields, List<byte[][]> tuples,
      ResultCursor cursor) {
  }

  @Override
  public void handleCommandStatus(String status, long updateCount, long insertOID) {
  }

  @Override
  public void secureProgress() {
  }

  @Override
  public void handleWarning(SQLWarning warning) {
    if (firstWarning == null) {
      firstWarning = lastWarning = warning;
      return;
    }
    lastWarning.setNextException(warning);
    lastWarning = warning;
  }

  @Override
  public void handleError(SQLException error) {
    if (firstException == null) {
      firstException = lastException = error;
      return;
    }
    lastException.setNextException(error);
    lastException = error;
  }

  @Override
  public void handleCompletion() throws SQLException {
    if (firstException != null) {
      throw firstException;
    }
  }

  @Override
  public SQLException getException() {
    return firstException;
  }

  @Override
  public SQLWarning getWarning() {
    return firstWarning;
  }
}
