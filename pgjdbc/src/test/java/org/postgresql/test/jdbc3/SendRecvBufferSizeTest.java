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

package org.postgresql.test.jdbc3;

import org.postgresql.test.TestUtil;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class SendRecvBufferSizeTest {

  private Connection _conn;

  @Before
  public void setUp() throws Exception {
    System.setProperty("sendBufferSize", "1024");
    System.setProperty("receiveBufferSize", "1024");

    _conn = TestUtil.openDB();
    Statement stmt = _conn.createStatement();
    stmt.execute("CREATE TEMP TABLE hold(a int)");
    stmt.execute("INSERT INTO hold VALUES (1)");
    stmt.execute("INSERT INTO hold VALUES (2)");
    stmt.close();
  }

  @After
  public void tearDown() throws SQLException {
    Statement stmt = _conn.createStatement();
    stmt.execute("DROP TABLE hold");
    stmt.close();
    TestUtil.closeDB(_conn);
  }


  // dummy test
  @Test
  public void testSelect() throws SQLException {
    Statement stmt = _conn.createStatement();
    stmt.execute("select * from hold");
    stmt.close();
  }

}
