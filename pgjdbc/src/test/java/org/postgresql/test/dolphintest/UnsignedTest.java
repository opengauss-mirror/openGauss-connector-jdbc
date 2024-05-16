package org.postgresql.test.dolphintest;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.postgresql.test.TestUtil;
import org.postgresql.test.jdbc2.BaseTest4B;

import java.math.BigInteger;
import java.sql.*;

import static org.junit.Assert.*;

public class UnsignedTest extends BaseTest4B {
    /**
     * test uint1 type
     * @throws Exception
     */
    @Test
    public void testUint1() throws SQLException {
        TestUtil.createTable(con, "test_unit1", "id uint1");

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO test_unit1 VALUES (?)");
        pstmt.setObject(1, 234, Types.SMALLINT);
        pstmt.executeUpdate();

        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT id FROM test_unit1");

        assertTrue(rs.next());
        Object r1 = rs.getObject(1);
        assertNotNull(r1);
        assertEquals(234, r1);

        TestUtil.dropTable(con, "test_unit1");
    }

    /**
     * test uint2 type
     * @throws Exception
     */
    @Test
    public void testUint2() throws SQLException {
        TestUtil.createTable(con, "test_unit2", "id uint2");

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO test_unit2 VALUES (?)");
        pstmt.setObject(1, 65518, Types.INTEGER);
        pstmt.executeUpdate();

        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT id FROM test_unit2");

        assertTrue(rs.next());
        Object r1 = rs.getObject(1);
        assertNotNull(r1);
        assertEquals(65518, r1);

        TestUtil.dropTable(con, "test_unit2");
    }

    /**
     * test uint4 type
     * @throws Exception
     */
    @Test
    public void testUint4() throws SQLException {
        TestUtil.createTable(con, "test_unit4", "id uint4");

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO test_unit4 VALUES (?)");
        long l = 4294967282L;
        pstmt.setObject(1, l, Types.BIGINT);
        pstmt.executeUpdate();

        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT id FROM test_unit4");

        assertTrue(rs.next());
        Object r1 = rs.getObject(1);
        assertNotNull(r1);
        assertEquals(l, r1);

        TestUtil.dropTable(con, "test_unit4");
    }

    /**
     * test uint8 type
     * @throws Exception
     */
    @Test
    public void testUint8() throws SQLException {
        TestUtil.createTable(con, "test_unit8", "id uint8");

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO test_unit8 VALUES (?)");
        BigInteger b = new BigInteger("9223372036859999999");
        pstmt.setObject(1, b, Types.NUMERIC);
        pstmt.executeUpdate();

        BigInteger b2 = new BigInteger("15223372036859999999");
        pstmt.setObject(1, b2, Types.NUMERIC);
        pstmt.executeUpdate();

        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT id FROM test_unit8");

        assertTrue(rs.next());
        Object r1 = rs.getObject(1);
        assertNotNull(r1);
        assertEquals(b, r1);

        assertTrue(rs.next());
        Object r2 = rs.getObject(1);
        assertNotNull(r2);
        assertEquals(b2, r2);

        TestUtil.dropTable(con, "test_unit8");
    }
}
