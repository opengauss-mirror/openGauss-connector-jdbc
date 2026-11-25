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


package org.postgresql.util;

import java.sql.SQLException;

public class PSQLException extends SQLException {

  private ServerErrorMessage _serverError;

  public PSQLException(String msg, PSQLState state, Throwable cause) {
    super(msg, state == null ? null : state.getState(), cause);
  }
  
  public PSQLException(String msg,PSQLState state,int errorcode,Throwable cause)
  {
      super(msg, state == null ? null : state.getState(),errorcode);
      initCause(cause);
  }
  
  public PSQLException(String msg, PSQLState state) {
    super(msg, state == null ? null : state.getState());
  }
  
  public PSQLException(String msg, PSQLState state,int errorcode)
  {
      this(msg, state,errorcode, null);
  }

  public PSQLException(ServerErrorMessage serverError) {
    super(serverError.toString(), serverError.getSQLState(),
        (serverError.getERRORCODE() == null) ? 0 : Integer.parseInt(serverError.getERRORCODE()));
    _serverError = serverError;
  }

  public ServerErrorMessage getServerErrorMessage() {
    return _serverError;
  }
}
