/*
 * Copyright (c) 2017, PostgreSQL Global Development Group
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

package org.postgresql.localtimedate;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.postgresql.core.ServerVersion;

import org.junit.After;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.postgresql.test.jdbc2.BaseTest4;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collection;

@RunWith(Parameterized.class)
public class SetObject310InfinityTests extends BaseTest4 {

  public SetObject310InfinityTests(BinaryMode binaryMode) {
    setBinaryMode(binaryMode);
  }

  @Parameterized.Parameters(name = "binary = {0}")
  public static Iterable<Object[]> data() {
    Collection<Object[]> ids = new ArrayList<>(2);
    for (BinaryMode binaryMode : BinaryMode.values()) {
      ids.add(new Object[]{binaryMode});
    }
    return ids;
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    Assume.assumeTrue("PostgreSQL 8.3 does not support 'infinity' for 'date'",
        TestUtil.haveMinimumServerVersion(con, ServerVersion.v8_4));
    super.setUp();
    TestUtil.createTable(con, "table1", "timestamp_without_time_zone_column timestamp without time zone,"
            + "timestamp_with_time_zone_column timestamp with time zone,"
            + "date_column date"
    );
  }

  @After
  public void tearDown() throws SQLException {
    TestUtil.dropTable(con, "table1");
    super.tearDown();
  }

  @Test
  public void testTimestamptz() throws SQLException {
    runTestforType(OffsetDateTime.MAX, OffsetDateTime.MIN, "timestamp_without_time_zone_column", null);
  }

  @Test
  public void testTimestamp() throws SQLException {
    runTestforType(LocalDateTime.MAX, LocalDateTime.MIN, "timestamp_without_time_zone_column", null);
  }

  @Test
  public void testDate() throws SQLException {
    runTestforType(LocalDate.MAX, LocalDate.MIN, "date_column", null);
  }

  private void runTestforType(Object max, Object min, String columnName, Integer type) throws SQLException {
    insert(max, columnName, type);
    String readback = readString(columnName);
    assertEquals("infinity", readback);
    delete();

    insert(min, columnName, type);
    readback = readString(columnName);
    assertEquals("-infinity", readback);
    delete();
  }

  private void insert(Object data, String columnName, Integer type) throws SQLException {
    PreparedStatement ps = con.prepareStatement(TestUtil.insertSQL("table1", columnName, "?"));
    try {
      if (type != null) {
        ps.setObject(1, data, type);
      } else {
        ps.setObject(1, data);
      }
      assertEquals(1, ps.executeUpdate());
    } finally {
      ps.close();
    }
  }

  private String readString(String columnName) throws SQLException {
    Statement st = con.createStatement();
    try {
      ResultSet rs = st.executeQuery(TestUtil.selectSQL("table1", columnName));
      try {
        assertNotNull(rs);
        assertTrue(rs.next());
        return rs.getString(1);
      } finally {
        rs.close();
      }
    } finally {
      st.close();
    }
  }

  private void delete() throws SQLException {
    Statement st = con.createStatement();
    try {
      st.execute("DELETE FROM table1");
    } finally {
      st.close();
    }
  }

}
