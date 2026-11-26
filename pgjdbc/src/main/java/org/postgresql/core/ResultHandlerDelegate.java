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
