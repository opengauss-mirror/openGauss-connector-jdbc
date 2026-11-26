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

package org.postgresql.jdbc;

import org.postgresql.core.Utils;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.SQLException;
import java.sql.Savepoint;

public class PSQLSavepoint implements Savepoint {

  private boolean _isValid;
  private boolean _isNamed;
  private int _id;
  private String _name;

  public PSQLSavepoint(int id) {
    _isValid = true;
    _isNamed = false;
    _id = id;
  }

  public PSQLSavepoint(String name) {
    _isValid = true;
    _isNamed = true;
    _name = name;
  }

  public int getSavepointId() throws SQLException {
    if (!_isValid) {
      throw new PSQLException(GT.tr("Cannot reference a savepoint after it has been released."),
          PSQLState.INVALID_SAVEPOINT_SPECIFICATION);
    }

    if (_isNamed) {
      throw new PSQLException(GT.tr("Cannot retrieve the id of a named savepoint."),
          PSQLState.WRONG_OBJECT_TYPE);
    }

    return _id;
  }

  public String getSavepointName() throws SQLException {
    if (!_isValid) {
      throw new PSQLException(GT.tr("Cannot reference a savepoint after it has been released."),
          PSQLState.INVALID_SAVEPOINT_SPECIFICATION);
    }

    if (!_isNamed) {
      throw new PSQLException(GT.tr("Cannot retrieve the name of an unnamed savepoint."),
          PSQLState.WRONG_OBJECT_TYPE);
    }

    return _name;
  }

  public void invalidate() {
    _isValid = false;
  }

  public String getPGName() throws SQLException {
    if (!_isValid) {
      throw new PSQLException(GT.tr("Cannot reference a savepoint after it has been released."),
          PSQLState.INVALID_SAVEPOINT_SPECIFICATION);
    }

    if (_isNamed) {
      // We need to quote and escape the name in case it
      // contains spaces/quotes/etc.
      //
      return Utils.escapeIdentifier(null, _name).toString();
    }

    return "JDBC_SAVEPOINT_" + _id;
  }
}
