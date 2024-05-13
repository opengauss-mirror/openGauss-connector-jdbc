package org.postgresql.test.jdbc4;

import org.junit.Test;
import org.postgresql.test.TestUtil;
import org.postgresql.test.jdbc2.BaseTest4;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class UnsignedTest extends BaseTest4 {

    /**
     * test uint8 type
     * @throws Exception
     */
    @Test
    public void testUint8() throws SQLException {
        TestUtil.createTable(con, "test_unit8", "id uint8");

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO test_unit8 VALUES (?)");
        BigDecimal b = new BigDecimal("9223372036859999999");
        pstmt.setObject(1, b, Types.NUMERIC);
        pstmt.executeUpdate();

        BigDecimal b2 = new BigDecimal("15223372036859999999");
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
