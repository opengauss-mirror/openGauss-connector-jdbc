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
