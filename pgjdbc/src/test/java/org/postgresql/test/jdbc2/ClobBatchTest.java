/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc2;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.test.TestUtil;

import javax.sql.rowset.serial.SerialClob;
import java.io.IOException;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Some simple tests based on problems reported by users. Hopefully these will help prevent previous
 * problems from re-occurring ;-)
 */
public class ClobBatchTest {
  private static final int LOOP = 0; // LargeObject API using loop
  private static final int NATIVE_STREAM = 1; // LargeObject API using OutputStream

  private Connection con;

  @Before
  public void setUp() throws Exception {
    Properties props = new Properties();
    // blob and clob can exchange must set string is not varchar!
//    props.setProperty(PGProperty.STRING_TYPE.getName(), "unspecified");
    con = TestUtil.openDB(props);
    TestUtil.createTable(con, "testclob", "id int,lo clob");
    con.setAutoCommit(false);
  }

  @After
  public void tearDown() throws Exception {
    con.setAutoCommit(true);
    TestUtil.dropTable(con, "testclob");
    TestUtil.closeDB(con);
  }
  
  @Test
  public void testNormalInsert() throws SQLException, IOException {
    String sql = "insert into testclob (id, lo) values " + " (?, ?)";
    Clob data = new SerialClob("abcd".toCharArray());
    int id = 0;
    try (PreparedStatement ps = con.prepareStatement(sql)) {
      ps.setInt(1, id);
      ps.setClob(2, data);
      ps.execute();
    }
    
    String query = "select id, lo from testclob where id = 0";
    try (Statement st = con.createStatement()) {
      try (ResultSet rs = st.executeQuery(query)) {
        assertTrue(rs.next());
        int id1 = rs.getInt(1);
        assertEquals(id, id1);
        Clob data1 = rs.getClob(2);
        assertEquals(data.getSubString(1, 4), data1.getSubString(1, 4));
      }
    }
  }
  
  @Test
  public void testSetNull() throws Exception {
    PreparedStatement pstmt = con.prepareStatement("INSERT INTO testclob(lo) VALUES (?)");

    pstmt.setClob(1, (Clob) null);
    pstmt.executeUpdate();

    pstmt.setNull(1, Types.CLOB);
    pstmt.executeUpdate();

    pstmt.setObject(1, null, Types.CLOB);
    pstmt.executeUpdate();
    
    pstmt.setObject(1, "");
    pstmt.executeUpdate();
  }
  
  @Test
  public void testInsertNullBatch() throws SQLException {
    String sql = "insert into testclob (id, lo) values " + " (?, ?)";
    Object[] inputs = {null, ""};
    try (PreparedStatement ps = con.prepareStatement(sql)) {
      ps.setInt(1, 0);
      ps.setObject(2, null, Types.CLOB);
      ps.addBatch();
      ps.setInt(1, 1);
      ps.setObject(2, "");
      ps.addBatch();
      ps.executeBatch();
    }
    
    String query = "select id, lo from testclob";
    try (Statement st = con.createStatement()) {
      try (ResultSet rs = st.executeQuery(query)) {
        while (rs.next()) {
          Clob data1 = rs.getClob(2);
          if (data1 != null) {
            assertTrue(data1.length() == 0);
          }
        }
      }
    }
  }
}