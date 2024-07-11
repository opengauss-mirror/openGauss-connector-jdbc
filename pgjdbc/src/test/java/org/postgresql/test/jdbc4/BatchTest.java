package org.postgresql.test.jdbc4;

import org.junit.Test;
import org.postgresql.test.TestUtil;
import org.postgresql.test.jdbc2.BaseTest4;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;

import static org.junit.Assert.*;

/**
 * This test-case is only for JDBC4 execute batch methods. Take a look at
 */
public class BatchTest extends BaseTest4 {
  /*
   * Tests execute batch
   */
  @Test
  public void testExecuteBatch() throws Exception {
    TestUtil.createTable(con, "t_bytea", "id bigint,col1 bytea");

    PreparedStatement pstmt = con.prepareStatement("INSERT INTO t_bytea VALUES (?,?)");
    pstmt.setInt(1, 1);
    pstmt.setNull(2, Types.BINARY);
    pstmt.addBatch();
    pstmt.setInt(1,2);
    pstmt.setBytes(2,new byte[]{0x00,0x01});
    pstmt.addBatch();
    pstmt.setInt(1, 3);
    pstmt.setNull(2,Types.OTHER);
    pstmt.addBatch();
    pstmt.executeBatch();

    Statement stmt = con.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT col1 FROM t_bytea");

    assertTrue(rs.next());
    String o1 = rs.getString(1);
    assertNull(o1);

    assertTrue(rs.next());
    String o2 = rs.getString(1);
    assertNotNull(o2);
    assertEquals("\\x0001", o2);

    assertTrue(rs.next());
    String b3 = rs.getString(1);
    assertNull(b3);

    TestUtil.dropTable(con, "t_bytea");
  }
}