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


package org.postgresql.util;

import java.sql.SQLWarning;

public class PSQLWarning extends SQLWarning {

  private ServerErrorMessage serverError;

  public PSQLWarning(ServerErrorMessage err) {
    super(err.toString(), err.getSQLState());
    this.serverError = err;
  }

  public String getMessage() {
    return serverError.getMessage();
  }

  public ServerErrorMessage getServerErrorMessage() {
    return serverError;
  }
}
