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

package org.postgresql.test.jdbc4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.postgresql.core.ServerVersion;

import org.postgresql.test.TestUtil;
import org.postgresql.test.jdbc2.BaseTest4;

import org.junit.Assume;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JsonbTest extends BaseTest4 {

  @Override
  public void setUp() throws Exception {
    super.setUp();
    Assume.assumeTrue("jsonb requires PostgreSQL 9.4+", TestUtil.haveMinimumServerVersion(con, ServerVersion.v9_4));
    TestUtil.createTable(con, "jsonbtest", "detail jsonb");
    Statement stmt = con.createStatement();
    stmt.executeUpdate("INSERT INTO jsonbtest (detail) VALUES ('{\"a\": 1}')");
    stmt.executeUpdate("INSERT INTO jsonbtest (detail) VALUES ('{\"b\": 1}')");
    stmt.executeUpdate("INSERT INTO jsonbtest (detail) VALUES ('{\"c\": 1}')");
    stmt.close();
  }

  @Override
  public void tearDown() throws SQLException {
    TestUtil.dropTable(con, "jsonbtest");
    super.tearDown();
  }

  @Test
  public void testJsonbNonPreparedStatement() throws SQLException {
    Statement stmt = con.createStatement();

    ResultSet rs = stmt.executeQuery("SELECT count(1) FROM jsonbtest WHERE detail ? 'a' = false;");
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    rs.close();
    stmt.close();
  }

  @Test
  public void testJsonbPreparedStatement() throws SQLException {
    PreparedStatement stmt = con.prepareStatement("SELECT count(1) FROM jsonbtest WHERE detail ?? 'a' = false;");
    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    rs.close();
    stmt.close();
  }
}
