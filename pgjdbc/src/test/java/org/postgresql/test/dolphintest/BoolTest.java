package org.postgresql.test.dolphintest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.test.TestUtil;
import org.postgresql.test.jdbc2.BaseTest4B;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * This test-case is only for JDBC4 boolean methods. Take a look at
 */
public class BoolTest extends BaseTest4B {
  /*
   * Tests int to boolean methods in ResultSet
   */
  @Test
  public void testIntToBoolean() throws Exception {
    TestUtil.createTable(con, "test_bool", "id tinyint(1)");

    PreparedStatement pstmt = con.prepareStatement("INSERT INTO test_bool VALUES (?)");
    pstmt.setObject(1, 2, Types.INTEGER);
    pstmt.executeUpdate();

    pstmt.setObject(1, 0, Types.INTEGER);
    pstmt.executeUpdate();

    pstmt.setObject(1, -1, Types.INTEGER);
    pstmt.executeUpdate();

    pstmt.setObject(1, -3, Types.INTEGER);
    pstmt.executeUpdate();

    Statement stmt = con.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT id FROM test_bool");

    assertTrue(rs.next());
    Boolean b = rs.getBoolean(1);
    assertNotNull(b);
    assertEquals(true, b);

    assertTrue(rs.next());
    Boolean b2 = rs.getBoolean(1);
    assertNotNull(b2);
    assertEquals(false, b2);

    assertTrue(rs.next());
    Boolean b3 = rs.getBoolean(1);
    assertNotNull(b3);
    assertEquals(true, b3);

    assertTrue(rs.next());
    Boolean b4 = rs.getBoolean(1);
    assertNotNull(b4);
    assertEquals(false, b4);

    TestUtil.dropTable(con, "test_bool");
  }

  /*
   * Tests bit(1) to boolean methods in ResultSet
   */
  @Test
  public void testBit1ToBoolean() throws Exception {
    assumeMiniOgVersion("opengauss 6.0.0",6,0,0);
    TestUtil.createTable(con, "test_bool", "id bit(1)");

    PreparedStatement pstmt1 = con.prepareStatement("INSERT INTO test_bool VALUES (1)");
    pstmt1.executeUpdate();

    PreparedStatement pstmt2 = con.prepareStatement("INSERT INTO test_bool VALUES (0)");
    pstmt2.executeUpdate();

    Statement stmt = con.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT id FROM test_bool");

    assertTrue(rs.next());
    Boolean b = rs.getBoolean(1);
    assertNotNull(b);
    assertEquals(true, b);

    assertTrue(rs.next());
    Boolean b2 = rs.getBoolean(1);
    assertNotNull(b2);
    assertEquals(false, b2);

    TestUtil.dropTable(con, "test_bool");
  }

  /*
   * Tests bit(4) to boolean methods in ResultSet
   */
  @Test
  public void testBit4ToBoolean() throws Exception {
    assumeMiniOgVersion("opengauss 6.0.0",6,0,0);
    TestUtil.createTable(con, "test_bool", "id bit(4)");

    PreparedStatement pstmt1 = con.prepareStatement("INSERT INTO test_bool VALUES (0011)");
    pstmt1.executeUpdate();

    PreparedStatement pstmt2 = con.prepareStatement("INSERT INTO test_bool VALUES (0000)");
    pstmt2.executeUpdate();

    Statement stmt = con.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT id FROM test_bool");

    assertTrue(rs.next());
    Boolean b = rs.getBoolean(1);
    assertNotNull(b);
    assertEquals(true, b);

    assertTrue(rs.next());
    Boolean b2 = rs.getBoolean(1);
    assertNotNull(b2);
    assertEquals(false, b2);

    TestUtil.dropTable(con, "test_bool");
  }

  /*
   * Tests String to boolean methods in ResultSet
   */
  @Test
  public void testStrToBoolean() throws Exception {
    TestUtil.createTable(con, "test_bool", "id varchar");

    PreparedStatement pstmt = con.prepareStatement("INSERT INTO test_bool VALUES (?)");
    pstmt.setObject(1, "T");
    pstmt.executeUpdate();

    pstmt.setObject(1, "y");
    pstmt.executeUpdate();

    pstmt.setObject(1, "N");
    pstmt.executeUpdate();

    pstmt.setObject(1, "f");
    pstmt.executeUpdate();

    Statement stmt = con.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT id FROM test_bool");

    assertTrue(rs.next());
    Boolean b = rs.getBoolean(1);
    assertNotNull(b);
    assertEquals(true, b);

    assertTrue(rs.next());
    Boolean b2 = rs.getBoolean(1);
    assertNotNull(b2);
    assertEquals(true, b2);

    assertTrue(rs.next());
    Boolean b3 = rs.getBoolean(1);
    assertNotNull(b3);
    assertEquals(false, b3);

    assertTrue(rs.next());
    Boolean b4 = rs.getBoolean(1);
    assertNotNull(b4);
    assertEquals(false, b4);

    TestUtil.dropTable(con, "test_bool");
  }

  /*
   * Tests float to boolean methods in ResultSet
   */
  @Test
  public void testFloatToBoolean() throws Exception {
    TestUtil.createTable(con, "test_bool", "id float8");

    PreparedStatement pstmt = con.prepareStatement("INSERT INTO test_bool VALUES (?)");
    pstmt.setObject(1, 5.334);
    pstmt.executeUpdate();

    pstmt.setObject(1, -6.882);
    pstmt.executeUpdate();

    pstmt.setObject(1, 7.996e45);
    pstmt.executeUpdate();

    pstmt.setObject(1, -1.227e98);
    pstmt.executeUpdate();

    pstmt.setObject(1, -1.0);
    pstmt.executeUpdate();

    pstmt.setObject(1, 0.0);
    pstmt.executeUpdate();

    Statement stmt = con.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT id FROM test_bool");

    assertTrue(rs.next());
    Boolean b = rs.getBoolean(1);
    assertNotNull(b);
    assertEquals(true, b);

    assertTrue(rs.next());
    Boolean b2 = rs.getBoolean(1);
    assertNotNull(b2);
    assertEquals(false, b2);

    assertTrue(rs.next());
    Boolean b3 = rs.getBoolean(1);
    assertNotNull(b3);
    assertEquals(true, b3);

    assertTrue(rs.next());
    Boolean b4 = rs.getBoolean(1);
    assertNotNull(b4);
    assertEquals(false, b4);

    assertTrue(rs.next());
    Boolean b5 = rs.getBoolean(1);
    assertNotNull(b5);
    assertEquals(true, b5);

    assertTrue(rs.next());
    Boolean b6 = rs.getBoolean(1);
    assertNotNull(b6);
    assertEquals(false, b6);

    TestUtil.dropTable(con, "test_bool");
  }

  /*
   * Tests binary to boolean methods in ResultSet
   */
  @Test
  public void testBinaryToBoolean() throws Exception {
    TestUtil.createTable(con, "test_bool", "id binary(1)");

    PreparedStatement pstmt = con.prepareStatement("INSERT INTO test_bool VALUES (?)");
    pstmt.setObject(1, "5", Types.BINARY);
    pstmt.executeUpdate();

    pstmt.setObject(1, "0", Types.BINARY);
    pstmt.executeUpdate();

    Statement stmt = con.createStatement();
    String sql = "set bytea_output=escape;";
    stmt.execute(sql);
    ResultSet rs = stmt.executeQuery("SELECT id FROM test_bool");

    assertTrue(rs.next());
    Boolean b = rs.getBoolean(1);
    assertNotNull(b);
    assertEquals(true, b);

    assertTrue(rs.next());
    Boolean b2 = rs.getBoolean(1);
    assertNotNull(b2);
    assertEquals(false, b2);

    TestUtil.dropTable(con, "test_bool");
  }
}