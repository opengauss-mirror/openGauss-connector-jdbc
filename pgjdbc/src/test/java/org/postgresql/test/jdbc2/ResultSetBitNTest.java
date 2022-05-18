/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc2;

import org.junit.Test;
import org.postgresql.test.TestUtil;
import org.postgresql.util.PGobject;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/*
 * ResultSet tests.
 */
public class ResultSetBitNTest extends BaseTest4 {
  @Override
  public void setUp() throws Exception {
    super.setUp();
    TestUtil.createTable(con, "test_bit", "id bit, id1 bit(1), id2 bit(10)");
    String insertOne = "insert into test_bit (id, id1, id2) values ('1', b'0', b'1111100000')";
    try (Statement stmt = con.createStatement()) {
      stmt.executeUpdate(insertOne);
    }
  }

  @Override
  public void tearDown() throws SQLException {
    TestUtil.dropTable(con, "test_bit");
    super.tearDown();
  }

  @Override
  protected void updateProperties(Properties props) {
    super.updateProperties(props);
    props.setProperty("loggerLevel", "TRACE");
    props.setProperty("bitToString", "true");
  }

  @Test
  public void testInsertDirectly() throws SQLException {
    String insertSql = "insert into test_bit (id, id1, id2) values ('1', b'0', b'1111100000')";
    try (Statement st = con.createStatement()) {
      int number = st.executeUpdate(insertSql);
      assertEquals(1, number);
    }
  }

  @Test
  public void testInsertByPreparedStatementObj() throws SQLException {
    String insertSql = "insert into test_bit (id, id1, id2) values (?::bit, ?, ?)";
    try (PreparedStatement ps = con.prepareStatement(insertSql)) {
      ps.setObject(1, "1");
      ps.setObject(2, "0", Types.BIT);
      ps.setObject(3, "0000011111", Types.BIT);
      boolean success = ps.execute();
      assertFalse(success);
      int num = ps.getUpdateCount();
      assertEquals(1, num);
    }
  }

  @Test
  public void testInsertByPreparedStatementString() throws SQLException {
    String insertSql = "insert into test_bit (id, id1, id2) values (?::bit,?::bit,?)";
    try (PreparedStatement ps = con.prepareStatement(insertSql)) {
      ps.setString(1, "1");
      ps.setString(2, "0");
      PGobject pObj = new PGobject();
      pObj.setType("bit");
      pObj.setValue("1010111111");
      ps.setObject(3, pObj);
      boolean success = ps.execute();
      assertFalse(success);
      int num = ps.getUpdateCount();
      assertEquals(1, num);
    }
  }

  @Test
  public void testSelectDirectly() throws SQLException {
      String sql = "select id, id1, id2 from test_bit";
      try (Statement st = con.createStatement()) {
        try (ResultSet rs = st.executeQuery(sql)) {
          assertTrue(rs.next());
          Object obj = rs.getObject(1);
          assertEquals("1", obj);
          Object obj1 = rs.getObject(2);
          assertEquals("0", obj1);
          Object obj2 = rs.getObject(3);
          assertEquals("1111100000", obj2);
        }
      }
  }
}
