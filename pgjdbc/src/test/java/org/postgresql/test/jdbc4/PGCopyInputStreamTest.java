/*
 * Copyright (c) 2007, PostgreSQL Global Development Group
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

package org.postgresql.test.jdbc4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.postgresql.PGConnection;
import org.postgresql.copy.PGCopyInputStream;
import org.postgresql.core.ServerVersion;
import org.postgresql.test.TestUtil;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PGCopyInputStreamTest {
  private Connection _conn;
  private PGCopyInputStream sut;
  private String copyParams;

  @Before
  public void setUp() throws Exception {
    _conn = TestUtil.openDB();
    TestUtil.createTable(_conn, "cpinstreamtest", "i int");
    if (TestUtil.haveMinimumServerVersion(_conn, ServerVersion.v9_0)) {
      copyParams = "(FORMAT CSV, HEADER false)";
    } else {
      copyParams = "CSV";
    }
  }

  @After
  public void tearDown() throws SQLException {
    silentlyCloseStream(sut);
    TestUtil.dropTable(_conn, "cpinstreamtest");
    TestUtil.closeDB(_conn);
  }

  @Test
  public void testReadBytesCorrectlyHandlesEof() throws SQLException, IOException {
    insertSomeData();

    sut = new PGCopyInputStream((PGConnection) _conn,
        "COPY cpinstreamtest (i) TO STDOUT WITH " + copyParams);

    byte[] buf = new byte[2]; // large enough to read everything on the next step
    assertTrue(sut.read(buf) > 0);
    while (sut.read(buf) > 0) {
      continue;
    }
    assertEquals(-1, sut.read(buf));
  }

  @Test
  public void testReadBytesCorrectlyReadsDataInChunks() throws SQLException, IOException {
    insertSomeData();

    sut = new PGCopyInputStream((PGConnection) _conn,
        "COPY (select i from cpinstreamtest order by i asc) TO STDOUT WITH " + copyParams);

    byte[] buf = new byte[2]; // small enough to read in multiple chunks
    StringBuilder result = new StringBuilder(100);
    int chunks = 0;
    while (sut.read(buf) > 0) {
      result.append(new String(buf));
      ++chunks;
    }

    assertEquals(4, chunks);
    assertEquals("0\n1\n2\n3\n", result.toString());
  }

  @Test
  public void testStreamCanBeClosedAfterReadUp() throws SQLException, IOException {
    insertSomeData();

    sut = new PGCopyInputStream((PGConnection) _conn,
        "COPY (select i from cpinstreamtest order by i asc) TO STDOUT WITH " + copyParams);

    byte[] buff = new byte[2];
    while (sut.read(buff) > 0) {
      ;
    }

    sut.close();
  }

  private void silentlyCloseStream(PGCopyInputStream sut) {
    if (sut != null) {
      try {
        if (sut.isActive()) {
          sut.close();
        }
      } catch (IOException e) {
        // intentionally ignore
      }
    }
  }

  private void insertSomeData() throws SQLException {
    PreparedStatement pstmt = _conn.prepareStatement("insert into cpinstreamtest (i) values (?)");
    for (int i = 0; i < 4; ++i) {
      pstmt.setInt(1, i);
      pstmt.addBatch();
    }
    pstmt.executeBatch();
  }
}
