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
