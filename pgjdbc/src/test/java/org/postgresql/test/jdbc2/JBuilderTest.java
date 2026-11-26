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

import static org.junit.Assert.assertNotNull;

import org.postgresql.test.TestUtil;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/*
 * Some simple tests to check that the required components needed for JBuilder stay working
 *
 */
public class JBuilderTest {

  // Set up the fixture for this testcase: the tables for this test.
  @Before
  public void setUp() throws Exception {
    Connection con = TestUtil.openDB();

    TestUtil.createTable(con, "test_c", "source text,cost money,imageid int4");

    TestUtil.closeDB(con);
  }

  // Tear down the fixture for this test case.
  @After
  public void tearDown() throws Exception {
    Connection con = TestUtil.openDB();
    TestUtil.dropTable(con, "test_c");
    TestUtil.closeDB(con);
  }

  /*
   * This tests that Money types work. JDBCExplorer barfs if this fails.
   */
  @Test
  public void testMoney() throws Exception {
    Connection con = TestUtil.openDB();

    Statement st = con.createStatement();
    ResultSet rs = st.executeQuery("select cost from test_c");
    assertNotNull(rs);

    while (rs.next()) {
      rs.getDouble(1);
    }

    rs.close();
    st.close();

    TestUtil.closeDB(con);
  }
}
