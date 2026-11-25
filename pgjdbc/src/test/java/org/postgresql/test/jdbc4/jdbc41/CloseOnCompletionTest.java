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

package org.postgresql.test.jdbc4.jdbc41;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.postgresql.test.TestUtil;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class CloseOnCompletionTest {
  private Connection _conn;

  @Before
  public void setUp() throws Exception {
    _conn = TestUtil.openDB();
    TestUtil.createTable(_conn, "table1", "id integer");
  }

  @After
  public void tearDown() throws SQLException {
    TestUtil.dropTable(_conn, "table1");
    TestUtil.closeDB(_conn);
  }

  /**
   * Test that the statement is not automatically closed if we do not ask for it.
   */
  @Test
  public void testWithoutCloseOnCompletion() throws SQLException {
    Statement stmt = _conn.createStatement();

    ResultSet rs = stmt.executeQuery(TestUtil.selectSQL("table1", "*"));
    rs.close();
    assertFalse(stmt.isClosed());
  }

  /**
   * Test the behavior of closeOnCompletion with a single result set.
   */
  @Test
  public void testSingleResultSet() throws SQLException {
    Statement stmt = _conn.createStatement();
    stmt.closeOnCompletion();

    ResultSet rs = stmt.executeQuery(TestUtil.selectSQL("table1", "*"));
    rs.close();
    assertTrue(stmt.isClosed());
  }

  /**
   * Test the behavior of closeOnCompletion with a multiple result sets.
   */
  @Test
  public void testMultipleResultSet() throws SQLException {
    Statement stmt = _conn.createStatement();
    stmt.closeOnCompletion();

    stmt.execute(TestUtil.selectSQL("table1", "*") + ";" + TestUtil.selectSQL("table1", "*") + ";");
    ResultSet rs = stmt.getResultSet();
    rs.close();
    assertFalse(stmt.isClosed());
    stmt.getMoreResults();
    rs = stmt.getResultSet();
    rs.close();
    assertTrue(stmt.isClosed());
  }

  /**
   * Test that when execution does not produce any result sets, closeOnCompletion has no effect
   * (spec).
   */
  @Test
  public void testNoResultSet() throws SQLException {
    Statement stmt = _conn.createStatement();
    stmt.closeOnCompletion();

    stmt.executeUpdate(TestUtil.insertSQL("table1", "1"));
    assertFalse(stmt.isClosed());
  }
}
