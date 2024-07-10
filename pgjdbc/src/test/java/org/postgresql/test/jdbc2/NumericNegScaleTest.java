package org.postgresql.test.jdbc2;

import org.junit.Test;
import org.postgresql.test.TestUtil;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.postgresql.jdbc.TypeInfoCache.FLOATSCALE;
import static org.postgresql.jdbc.TypeInfoCache.NUMERIC_MAX_DISPLAYSIZE;

public class NumericNegScaleTest extends BaseTest4 {
    @Test
    public void testFloat() throws Exception {
        TestUtil.execute("set behavior_compat_options='float_as_numeric'", con);
        TestUtil.createTable(con, "test_float", "col1 float(1), col2 float(23), col3 float(126)");

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO test_float VALUES (?,?,?)");
        BigDecimal n1 = new BigDecimal("1.8355454812");

        pstmt.setObject(1, n1, Types.NUMERIC);
        pstmt.setObject(2, n1, Types.NUMERIC);
        pstmt.setObject(3, n1, Types.NUMERIC);
        pstmt.executeUpdate();

        n1 = BigDecimal.valueOf(1234567890);
        pstmt.setObject(1, n1, Types.NUMERIC);
        pstmt.setObject(2, n1, Types.NUMERIC);
        pstmt.setObject(3, n1, Types.NUMERIC);
        pstmt.executeUpdate();

        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM test_float");
        int[] precision_arr = {0, 1, 23, 126};
        for (int i=1; i<4; i++)
        {
            assertEquals(rs.getMetaData().getPrecision(i), precision_arr[i]);
            assertEquals(rs.getMetaData().getScale(i), FLOATSCALE);
            assertEquals(rs.getMetaData().getColumnDisplaySize(i), NUMERIC_MAX_DISPLAYSIZE);
        }

        assertTrue(rs.next());
        Object r1 = rs.getObject(1);
        assertNotNull(r1);
        assertEquals(BigDecimal.valueOf(2), r1);
        Object r2 = rs.getObject(2);
        assertNotNull(r2);
        assertEquals(BigDecimal.valueOf(1.835545), r2);
        Object r3 = rs.getObject(3);
        assertNotNull(r3);
        assertEquals(BigDecimal.valueOf(1.8355454812), r3);

        assertTrue(rs.next());
        r1 = rs.getObject(1);
        assertNotNull(r1);
        assertEquals(BigDecimal.valueOf(1000000000), r1);
        r2 = rs.getObject(2);
        assertNotNull(r2);
        assertEquals(BigDecimal.valueOf(1234568000), r2);
        r3 = rs.getObject(3);
        assertNotNull(r3);
        assertEquals(BigDecimal.valueOf(1234567890), r3);

        rs.close();
        TestUtil.execute("set behavior_compat_options=''", con);
        TestUtil.dropTable(con, "test_float");
    }

    @Test
    public void testNegScale() throws Exception {
        TestUtil.createTable(con, "test_neg_scale", "col1 numeric(4,-3), col2 numeric(3,-4)");

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO test_neg_scale VALUES (?,?)");
        BigDecimal n1 = new BigDecimal("1.8355454812");

        pstmt.setObject(1, n1, Types.NUMERIC);
        pstmt.setObject(2, n1, Types.NUMERIC);
        pstmt.executeUpdate();

        n1 = BigDecimal.valueOf(1234567);
        pstmt.setObject(1, n1, Types.NUMERIC);
        pstmt.setObject(2, n1, Types.NUMERIC);
        pstmt.executeUpdate();

        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM test_neg_scale");

        int[][] typmod_arr = {{0}, {4, -3}, {3, -4}};
        for (int i=1; i<3; i++)
        {
            assertEquals(rs.getMetaData().getPrecision(i), typmod_arr[i][0]);
            assertEquals(rs.getMetaData().getScale(i), typmod_arr[i][1]);
            assertEquals(rs.getMetaData().getColumnDisplaySize(i), 8);
        }

        assertTrue(rs.next());
        Object r1 = rs.getObject(1);
        assertNotNull(r1);
        assertEquals(BigDecimal.valueOf(0), r1);
        Object r2 = rs.getObject(2);
        assertNotNull(r2);
        assertEquals(BigDecimal.valueOf(0), r2);

        assertTrue(rs.next());
        r1 = rs.getObject(1);
        assertNotNull(r1);
        assertEquals(BigDecimal.valueOf(1235000), r1);
        r2 = rs.getObject(2);
        assertNotNull(r2);
        assertEquals(BigDecimal.valueOf(1230000), r2);

        rs.close();
        TestUtil.dropTable(con, "test_neg_scale");
    }
}

