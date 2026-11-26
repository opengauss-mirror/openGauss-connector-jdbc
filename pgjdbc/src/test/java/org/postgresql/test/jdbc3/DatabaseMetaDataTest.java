/*
 * Copyright (c) 2008, PostgreSQL Global Development Group
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
