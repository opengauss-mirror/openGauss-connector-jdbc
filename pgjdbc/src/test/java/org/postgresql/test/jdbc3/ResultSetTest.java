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

package org.postgresql.test.jdbc3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.postgresql.test.TestUtil;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ResultSetTest {

  private Connection _conn;

  @Before
  public void setUp() throws Exception {
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

  @Test
  public void testHoldableResultSet() throws SQLException {
    Statement stmt = _conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
        ResultSet.HOLD_CURSORS_OVER_COMMIT);

    _conn.setAutoCommit(false);
    stmt.setFetchSize(1);

    ResultSet rs = stmt.executeQuery("SELECT a FROM hold ORDER BY a");

    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));

    _conn.commit();

    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertTrue(!rs.next());

    rs.close();
    stmt.close();
  }

}
