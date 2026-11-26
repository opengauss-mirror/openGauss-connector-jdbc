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
