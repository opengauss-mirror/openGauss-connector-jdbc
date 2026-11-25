/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
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

package org.postgresql.core.v3;

import org.postgresql.copy.CopyDual;
import org.postgresql.util.PSQLException;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Queue;

public class CopyDualImpl extends CopyOperationImpl implements CopyDual {
  private Queue<byte[]> received = new LinkedList<byte[]>();

  public void writeToCopy(byte[] data, int off, int siz) throws SQLException {
    queryExecutor.writeToCopy(this, data, off, siz);
  }

  public void flushCopy() throws SQLException {
    queryExecutor.flushCopy(this);
  }

  public long endCopy() throws SQLException {
    return queryExecutor.endCopy(this);
  }

  public byte[] readFromCopy() throws SQLException {
    if (received.isEmpty()) {
      queryExecutor.readFromCopy(this, true);
    }

    return received.poll();
  }

  @Override
  public byte[] readFromCopy(boolean block) throws SQLException {
    if (received.isEmpty()) {
      queryExecutor.readFromCopy(this, block);
    }

    return received.poll();
  }

  @Override
  public void handleCommandStatus(String status) throws PSQLException {
  }

  protected void handleCopydata(byte[] data) {
    received.add(data);
  }
}
