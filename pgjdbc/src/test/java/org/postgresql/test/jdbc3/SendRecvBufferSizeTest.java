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

import org.postgresql.test.TestUtil;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class SendRecvBufferSizeTest {

  private Connection _conn;

  @Before
  public void setUp() throws Exception {
    System.setProperty("sendBufferSize", "1024");
    System.setProperty("receiveBufferSize", "1024");

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


  // dummy test
  @Test
  public void testSelect() throws SQLException {
    Statement stmt = _conn.createStatement();
    stmt.execute("select * from hold");
    stmt.close();
  }

}
