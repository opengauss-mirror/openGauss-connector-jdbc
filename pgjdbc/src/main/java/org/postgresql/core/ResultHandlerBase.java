/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
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
