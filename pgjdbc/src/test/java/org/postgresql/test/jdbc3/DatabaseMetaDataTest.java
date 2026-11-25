/*
 * Copyright (c) 2008, PostgreSQL Global Development Group
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.postgresql.test.TestUtil;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;

public class DatabaseMetaDataTest {

  private Connection _conn;

  @Before
  public void setUp() throws Exception {
    _conn = TestUtil.openDB();
    Statement stmt = _conn.createStatement();
    stmt.execute("CREATE DOMAIN mydom AS int");
    stmt.execute("CREATE TABLE domtab (a mydom)");
  }

  @After
  public void tearDown() throws Exception {
    Statement stmt = _conn.createStatement();
    stmt.execute("DROP TABLE domtab");
    stmt.execute("DROP TYPE mydom CASCADE");
    TestUtil.closeDB(_conn);
  }

  @Test
  public void testGetColumnsForDomain() throws Exception {
    DatabaseMetaData dbmd = _conn.getMetaData();

    ResultSet rs = dbmd.getColumns("%", "%", "domtab", "%");
    assertTrue(rs.next());
    assertEquals("a", rs.getString("COLUMN_NAME"));
    assertEquals(Types.DISTINCT, rs.getInt("DATA_TYPE"));
    assertEquals("mydom", rs.getString("TYPE_NAME"));
    assertEquals(Types.INTEGER, rs.getInt("SOURCE_DATA_TYPE"));
    assertTrue(!rs.next());
  }

}
