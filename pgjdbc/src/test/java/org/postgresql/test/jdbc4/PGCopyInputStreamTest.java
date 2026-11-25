/*
 * Copyright (c) 2007, PostgreSQL Global Development Group
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
