/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
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
    super(serverError.toString(), serverError.getSQLState(), Integer.parseInt(serverError.getERRORCODE()));
    _serverError = serverError;
  }

  public ServerErrorMessage getServerErrorMessage() {
    return _serverError;
  }
}
