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

package org.postgresql.test.jdbc2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.postgresql.test.TestUtil;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;

/*
 * Test for getObject
 */
public class GetXXXTest {
  private Connection con = null;

  @Before
  public void setUp() throws Exception {
    con = TestUtil.openDB();
    TestUtil.createTempTable(con, "test_interval",
        "initial timestamp with time zone, final timestamp with time zone");
    PreparedStatement pstmt = con.prepareStatement("insert into test_interval values (?,?)");
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.DAY_OF_YEAR, -1);

    pstmt.setTimestamp(1, new Timestamp(cal.getTime().getTime()));
    pstmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
    assertEquals(1, pstmt.executeUpdate());
    pstmt.close();
  }

  @After
  public void tearDown() throws Exception {
    TestUtil.dropTable(con, "test_interval");
    con.close();
  }

  @Test
  public void testGetObject() throws SQLException {
    Statement stmt = con.createStatement();
    ResultSet rs = stmt.executeQuery("select (final-initial) as diff from test_interval");
    while (rs.next()) {
      String str = rs.getString(1);

      assertNotNull(str);
      Object obj = rs.getObject(1);
      assertNotNull(obj);
    }
  }

  @Test
  public void testGetUDT() throws SQLException {
    Statement stmt = con.createStatement();
    ResultSet rs = stmt.executeQuery("select (final-initial) as diff from test_interval");

    while (rs.next()) {
      // make this return a PGobject
      Object obj = rs.getObject(1, new HashMap<String, Class<?>>());

      // it should not be an instance of PGInterval
      assertTrue(obj instanceof org.postgresql.util.PGInterval);

    }

  }

}
