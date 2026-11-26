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
