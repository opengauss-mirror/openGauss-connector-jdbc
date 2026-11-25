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

package org.postgresql.test.extensions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.postgresql.core.ServerVersion;
import org.postgresql.jdbc.PreferQueryMode;
import org.postgresql.test.jdbc2.BaseTest4;

import org.junit.Assume;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

// SELECT 'hstore'::regtype::oid
// SELECT 'hstore[]'::regtype::oid

public class HStoreTest extends BaseTest4 {

  @Override
  public void setUp() throws Exception {
    super.setUp();
    Assume.assumeTrue("server has installed hstore", isHStoreEnabled(con));
    Assume.assumeFalse("hstore is not supported in simple protocol only mode",
        preferQueryMode == PreferQueryMode.SIMPLE);
    assumeMinimumServerVersion("hstore requires PostgreSQL 8.3+", ServerVersion.v8_3);
  }

  private static boolean isHStoreEnabled(Connection conn) {
    try {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT 'a=>1'::hstore::text");
      rs.close();
      stmt.close();
      return true;
    } catch (SQLException sqle) {
      return false;
    }
  }

  @Test
  public void testHStoreSelect() throws SQLException {
    PreparedStatement pstmt = con.prepareStatement("SELECT 'a=>1,b=>2'::hstore");
    ResultSet rs = pstmt.executeQuery();
    assertEquals(getCompareOfMapType(), rs.getMetaData().getColumnClassName(1));
    assertTrue(rs.next());
    String str = rs.getString(1);
    if (!("\"a\"=>\"1\", \"b\"=>\"2\"".equals(str) || "\"b\"=>\"2\", \"a\"=>\"1\"".equals(str))) {
      fail("Expected " + "\"a\"=>\"1\", \"b\"=>\"2\"" + " but got " + str);
    }
    Map<String, String> correct = new HashMap<String, String>();
    correct.put("a", "1");
    correct.put("b", "2");
    assertEquals(correct, rs.getObject(1));
  }

  @Test
  public void testHStoreSelectNullValue() throws SQLException {
    PreparedStatement pstmt = con.prepareStatement("SELECT 'a=>NULL'::hstore");
    ResultSet rs = pstmt.executeQuery();
    assertEquals(getCompareOfMapType(), rs.getMetaData().getColumnClassName(1));
    assertTrue(rs.next());
    assertEquals("\"a\"=>NULL", rs.getString(1));
    Map<String, Object> correct = Collections.singletonMap("a", null);
    assertEquals(correct, rs.getObject(1));
  }

  @Test
  public void testHStoreSend() throws SQLException {
    Map<String, Integer> correct = Collections.singletonMap("a", 1);
    PreparedStatement pstmt = con.prepareStatement("SELECT ?::text");
    pstmt.setObject(1, correct);
    ResultSet rs = pstmt.executeQuery();
    assertEquals(String.class.getName(), rs.getMetaData().getColumnClassName(1));
    assertTrue(rs.next());
    assertEquals("\"a\"=>\"1\"", rs.getString(1));
  }

  @Test
  public void testHStoreUsingPSSetObject4() throws SQLException {
    Map<String, Integer> correct = Collections.singletonMap("a", 1);
    PreparedStatement pstmt = con.prepareStatement("SELECT ?::text");
    pstmt.setObject(1, correct, Types.OTHER, -1);
    ResultSet rs = pstmt.executeQuery();
    assertEquals(String.class.getName(), rs.getMetaData().getColumnClassName(1));
    assertTrue(rs.next());
    assertEquals("\"a\"=>\"1\"", rs.getString(1));
  }

  @Test
  public void testHStoreSendEscaped() throws SQLException {
    Map<String, String> correct = Collections.singletonMap("a", "t'e\ns\"t");
    PreparedStatement pstmt = con.prepareStatement("SELECT ?");
    pstmt.setObject(1, correct);
    ResultSet rs = pstmt.executeQuery();
    assertEquals(getCompareOfMapType(), rs.getMetaData().getColumnClassName(1));
    assertTrue(rs.next());
    assertEquals(correct, rs.getObject(1));
    assertEquals("\"a\"=>\"t'e\ns\\\"t\"", rs.getString(1));
  }

  private static String getCompareOfMapType() {
    return Object.class.getName();
  }
}
