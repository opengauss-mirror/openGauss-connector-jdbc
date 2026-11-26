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

import org.postgresql.test.TestUtil;

import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/*
 * Tests for using non-zero setFetchSize().
 */
public class ServerCursorTest extends BaseTest4 {

  @Override
  public void setUp() throws Exception {
    super.setUp();
    TestUtil.createTable(con, "test_fetch", "value integer,data bytea");
    con.setAutoCommit(false);
  }

  @Override
  public void tearDown() throws SQLException {
    con.rollback();
    con.setAutoCommit(true);
    TestUtil.dropTable(con, "test_fetch");
    super.tearDown();
  }

  protected void createRows(int count) throws Exception {
    PreparedStatement stmt = con.prepareStatement("insert into test_fetch(value,data) values(?,?)");
    for (int i = 0; i < count; ++i) {
      stmt.setInt(1, i + 1);
      stmt.setBytes(2, DATA_STRING.getBytes("UTF8"));
      stmt.executeUpdate();
    }
    con.commit();
  }

  // Test regular cursor fetching
  @Test
  public void testBasicFetch() throws Exception {
    assumeByteaSupported();
    createRows(1);

    PreparedStatement stmt =
        con.prepareStatement("declare test_cursor cursor for select * from test_fetch");
    stmt.execute();

    stmt = con.prepareStatement("fetch forward from test_cursor");
    ResultSet rs = stmt.executeQuery();
    while (rs.next()) {
      // there should only be one row returned
      assertEquals("query value error", 1, rs.getInt(1));
      byte[] dataBytes = rs.getBytes(2);
      assertEquals("binary data got munged", DATA_STRING, new String(dataBytes, "UTF8"));
    }

  }

  // Test binary cursor fetching
  @Test
  public void testBinaryFetch() throws Exception {
    assumeByteaSupported();
    createRows(1);

    PreparedStatement stmt =
        con.prepareStatement("declare test_cursor binary cursor for select * from test_fetch");
    stmt.execute();

    stmt = con.prepareStatement("fetch forward from test_cursor");
    ResultSet rs = stmt.executeQuery();
    while (rs.next()) {
      // there should only be one row returned
      byte[] dataBytes = rs.getBytes(2);
      assertEquals("binary data got munged", DATA_STRING, new String(dataBytes, "UTF8"));
    }

  }

  //CHECKSTYLE: OFF
  // This string contains a variety different data:
  // three japanese characters representing "japanese" in japanese
  // the four characters "\000"
  // a null character
  // the seven ascii characters "english"
  private static final String DATA_STRING = "\u65E5\u672C\u8A9E\\000\u0000english";
  //CHECKSTYLE: ON

}
