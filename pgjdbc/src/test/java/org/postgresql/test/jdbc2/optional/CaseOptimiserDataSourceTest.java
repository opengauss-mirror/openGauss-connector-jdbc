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

package org.postgresql.test.jdbc2.optional;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.postgresql.core.BaseConnection;
import org.postgresql.ds.common.BaseDataSource;
import org.postgresql.jdbc2.optional.SimpleDataSource;
import org.postgresql.test.TestUtil;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * DataSource test to ensure the BaseConnection is configured with column sanitiser disabled.
 */
public class CaseOptimiserDataSourceTest {
  private BaseDataSource bds;
  protected Connection conn;

  @Before
  public void setUp() throws SQLException {
    Connection conn = getDataSourceConnection();
    assertTrue(conn instanceof BaseConnection);
    BaseConnection bc = (BaseConnection) conn;
    assertTrue("Expected state [TRUE] of base connection configuration failed test.",
        bc.isColumnSanitiserDisabled());
    Statement insert = conn.createStatement();
    TestUtil.createTable(conn, "allmixedup",
        "id int primary key, \"DESCRIPTION\" varchar(40), \"fOo\" varchar(3)");
    insert.execute(TestUtil.insertSQL("allmixedup", "1,'mixed case test', 'bar'"));
    insert.close();
    conn.close();
  }

  @After
  public void tearDown() throws SQLException {
    Connection conn = getDataSourceConnection();
    Statement drop = conn.createStatement();
    drop.execute("drop table allmixedup");
    drop.close();
    conn.close();
    bds.setDisableColumnSanitiser(false);
  }

  /*
   * Test to ensure a datasource can be configured with the column sanitiser optimisation. This test
   * checks for a side effect of the sanitiser being disabled. The column is not expected to be
   * found.
   */
  @Test
  public void testDataSourceDisabledSanitiserPropertySucceeds() throws SQLException {
    String label = "FOO";
    Connection conn = getDataSourceConnection();
    PreparedStatement query =
        conn.prepareStatement("select * from allmixedup");
    if (0 < TestUtil.findColumn(query, label)) {
      fail(String.format("Did not expect to find the column with the label [%1$s].", label));
    }
    query.close();
    conn.close();
  }

  /**
   * Gets a connection from the current BaseDataSource.
   */
  protected Connection getDataSourceConnection() throws SQLException {
    if (bds == null) {
      initializeDataSource();
    }
    return bds.getConnection();
  }

  protected void initializeDataSource() {
    if (bds == null) {
      bds = new SimpleDataSource();
      setupDataSource(bds);
      bds.setDisableColumnSanitiser(true);
    }
  }

  public static void setupDataSource(BaseDataSource bds) {
    bds.setServerName(TestUtil.getServer());
    bds.setPortNumber(TestUtil.getPort());
    bds.setDatabaseName(TestUtil.getDatabase());
    bds.setUser(TestUtil.getUser());
    bds.setPassword(TestUtil.getPassword());
    bds.setPrepareThreshold(TestUtil.getPrepareThreshold());
    bds.setLoggerLevel(TestUtil.getLogLevel());
    bds.setLoggerFile(TestUtil.getLogFile());
    bds.setProtocolVersion(TestUtil.getProtocolVersion());
  }
}
