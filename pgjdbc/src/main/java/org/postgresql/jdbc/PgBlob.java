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

import org.postgresql.largeobject.LargeObject;

import java.sql.SQLException;

public class PgBlob extends AbstractBlobClob implements java.sql.Blob {

  public PgBlob(org.postgresql.core.BaseConnection conn, long oid) throws SQLException {
    super(conn, oid);
  }

  public synchronized java.io.InputStream getBinaryStream(long pos, long length)
      throws SQLException {
    checkFreed();
    LargeObject subLO = getLo(false).copy();
    addSubLO(subLO);
    if (pos > Integer.MAX_VALUE) {
      subLO.seek64(pos - 1, LargeObject.SEEK_SET);
    } else {
      subLO.seek((int) pos - 1, LargeObject.SEEK_SET);
    }
    return subLO.getInputStream(length);
  }

  public synchronized int setBytes(long pos, byte[] bytes) throws SQLException {
    return setBytes(pos, bytes, 0, bytes.length);
  }

  public synchronized int setBytes(long pos, byte[] bytes, int offset, int len)
      throws SQLException {
    assertPosition(pos);
    getLo(true).seek((int) (pos - 1));
    getLo(true).write(bytes, offset, len);
    return len;
  }
}
