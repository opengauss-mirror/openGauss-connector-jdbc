package org.postgresql.test.dolphintest;

import org.junit.Test;
import org.postgresql.test.TestUtil;
import org.postgresql.test.jdbc2.BaseTest4B;

import java.sql.PreparedStatement;
import java.sql.Types;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class IntTest extends BaseTest4B {
    /*
     * Tests tinyint1 to boolean
     */
    @Test
    public void testTinyint1() throws Exception {
        TestUtil.createTable(con, "test_tinyint", "id tinyint(1)");

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO test_tinyint VALUES (?)");
        pstmt.setObject(1, 18, Types.INTEGER);
        pstmt.executeUpdate();

        pstmt.setObject(1, 106, Types.INTEGER);
        pstmt.executeUpdate();

        pstmt.setObject(1, -1, Types.INTEGER);
        pstmt.executeUpdate();

        pstmt.setObject(1, 0, Types.INTEGER);
        pstmt.executeUpdate();

        pstmt.setObject(1, -10, Types.INTEGER);
        pstmt.executeUpdate();

        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT id FROM test_tinyint");

        assertTrue(rs.next());
        Object r1 = rs.getObject(1);
        assertNotNull(r1);
        assertEquals(true, r1);

        assertTrue(rs.next());
        Object r2 = rs.getObject(1);
        assertNotNull(r2);
        assertEquals(true, r2);

        assertTrue(rs.next());
        Object r3 = rs.getObject(1);
        assertNotNull(r3);
        assertEquals(true, r3);

        assertTrue(rs.next());
        Object r4 = rs.getObject(1);
        assertNotNull(r4);
        assertEquals(false, r4);

        assertTrue(rs.next());
        Object r5 = rs.getObject(1);
        assertNotNull(r5);
        assertEquals(false, r5);

        TestUtil.dropTable(con, "test_tinyint");
    }

    /*
     * Tests tinyint2
     */
    @Test
    public void testTinyint2() throws Exception {
        TestUtil.createTable(con, "test_tinyint2", "id tinyint(2),id2 smallint(1)");

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO test_tinyint2 VALUES (?,?)");
        pstmt.setObject(1, 25, Types.INTEGER);
        pstmt.setObject(2, 36, Types.INTEGER);
        pstmt.executeUpdate();

        pstmt.setObject(1, -24, Types.INTEGER);
        pstmt.setObject(2, -54, Types.INTEGER);
        pstmt.executeUpdate();

        pstmt.setObject(1, 0, Types.INTEGER);
        pstmt.setObject(2, 0, Types.INTEGER);
        pstmt.executeUpdate();

        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT id,id2 FROM test_tinyint2");

        assertTrue(rs.next());
        Object r11 = rs.getObject(1);
        assertNotNull(r11);
        assertEquals(25, r11);
        Object r12 = rs.getObject(2);
        assertNotNull(r12);
        assertEquals(36, r12);

        assertTrue(rs.next());
        Object r21 = rs.getObject(1);
        assertNotNull(r21);
        assertEquals(-24, r21);
        Object r22 = rs.getObject(2);
        assertNotNull(r22);
        assertEquals(-54, r22);

        assertTrue(rs.next());
        Object r31 = rs.getObject(1);
        assertNotNull(r31);
        assertEquals(0, r31);
        Object r32 = rs.getObject(2);
        assertNotNull(r32);
        assertEquals(0, r32);

        TestUtil.dropTable(con, "test_tinyint2");
    }

    /*
     * Tests int type
     */
    @Test
    public void testIntType() throws Exception {
        TestUtil.createTable(con, "test_int", "c1 int1,c2 int2,c3 int4,"
                + "c4 int8,uc1 uint1,uc2 uint2,uc3 uint4,uc4 uint8");
        try (Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM test_int")) {
            ResultSetMetaData rsmd = rs.getMetaData();
            assertEquals(8, rsmd.getColumnCount());
            assertEquals(Types.TINYINT, rsmd.getColumnType(1));
            assertEquals(Types.SMALLINT, rsmd.getColumnType(2));
            assertEquals(Types.INTEGER, rsmd.getColumnType(3));
            assertEquals(Types.BIGINT, rsmd.getColumnType(4));
            assertEquals(Types.TINYINT, rsmd.getColumnType(5));
            assertEquals(Types.SMALLINT, rsmd.getColumnType(6));
            assertEquals(Types.INTEGER, rsmd.getColumnType(7));
            assertEquals(Types.BIGINT, rsmd.getColumnType(8));
        } finally {
            TestUtil.dropTable(con, "test_int");
        }
    }
}
