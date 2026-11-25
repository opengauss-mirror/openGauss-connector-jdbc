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

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.SQLException;

public class PgClob extends AbstractBlobClob implements java.sql.Clob {
  public PgClob(org.postgresql.core.BaseConnection conn, long oid) throws java.sql.SQLException {
    super(conn, oid);
  }

  public synchronized Reader getCharacterStream(long pos, long length) throws SQLException {
    checkFreed();
    throw org.postgresql.Driver.notImplemented(this.getClass(), "getCharacterStream(long, long)");
  }

  public synchronized int setString(long pos, String str) throws SQLException {
    checkFreed();
    throw org.postgresql.Driver.notImplemented(this.getClass(), "setString(long,str)");
  }

  public synchronized int setString(long pos, String str, int offset, int len) throws SQLException {
    checkFreed();
    throw org.postgresql.Driver.notImplemented(this.getClass(), "setString(long,String,int,int)");
  }

  public synchronized java.io.OutputStream setAsciiStream(long pos) throws SQLException {
    checkFreed();
    throw org.postgresql.Driver.notImplemented(this.getClass(), "setAsciiStream(long)");
  }

  public synchronized java.io.Writer setCharacterStream(long pos) throws SQLException {
    checkFreed();
    throw org.postgresql.Driver.notImplemented(this.getClass(), "setCharacteStream(long)");
  }

  public synchronized InputStream getAsciiStream() throws SQLException {
    return getBinaryStream();
  }

  public synchronized Reader getCharacterStream() throws SQLException {
    Charset connectionCharset = Charset.forName(conn.getEncoding().name());
    return new InputStreamReader(getBinaryStream(), connectionCharset);
  }

  public synchronized String getSubString(long i, int j) throws SQLException {
    assertPosition(i, j);
    getLo(false).seek((int) i - 1);
    return new String(getLo(false).read(j), StandardCharsets.UTF_8);
  }

  /**
   * For now, this is not implemented.
   */
  public synchronized long position(String pattern, long start) throws SQLException {
    checkFreed();
    throw org.postgresql.Driver.notImplemented(this.getClass(), "position(String,long)");
  }

  /**
   * This should be simply passing the byte value of the pattern Blob.
   */
  public synchronized long position(Clob pattern, long start) throws SQLException {
    checkFreed();
    throw org.postgresql.Driver.notImplemented(this.getClass(), "position(Clob,start)");
  }
}
