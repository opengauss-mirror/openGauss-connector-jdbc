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
