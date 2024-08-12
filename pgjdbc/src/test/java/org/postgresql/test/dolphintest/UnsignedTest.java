package org.postgresql.test.dolphintest;

import org.junit.Assert;
import org.junit.Test;

import org.postgresql.test.TestUtil;
import org.postgresql.test.jdbc2.BaseTest4B;

import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Array;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class UnsignedTest extends BaseTest4B {
    /**
     * test uint1 type
     * @throws Exception
     */
    @Test
    public void testUint1() throws SQLException {
        TestUtil.createTable(con, "test_unit1", "id uint1");

        try (PreparedStatement pstmt = con.prepareStatement("INSERT INTO test_unit1 VALUES (?)")) {
            pstmt.setObject(1, 234, Types.SMALLINT);
            pstmt.executeUpdate();
        }

        try (Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT id FROM test_unit1");) {
            assertTrue(rs.next());
            Object r1 = rs.getObject(1);
            assertNotNull(r1);
            assertEquals(234, r1);
        } finally {
            TestUtil.dropTable(con, "test_unit1");
        }
    }

    /**
     * test uint2 type
     * @throws Exception
     */
    @Test
    public void testUint2() throws SQLException {
        TestUtil.createTable(con, "test_unit2", "id uint2");

        try (PreparedStatement pstmt = con.prepareStatement("INSERT INTO test_unit2 VALUES (?)")) {
            pstmt.setObject(1, 65518, Types.INTEGER);
            pstmt.executeUpdate();
        }

        try (Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT id FROM test_unit2")) {
            assertTrue(rs.next());
            Object r1 = rs.getObject(1);
            assertNotNull(r1);
            assertEquals(65518, r1);
        } finally {
            TestUtil.dropTable(con, "test_unit2");
        }
    }

    /**
     * test uint4 type
     * @throws Exception
     */
    @Test
    public void testUint4() throws SQLException {
        TestUtil.createTable(con, "test_unit4", "id uint4");

        try (PreparedStatement pstmt = con.prepareStatement("INSERT INTO test_unit4 VALUES (?)")) {
            pstmt.setObject(1, 4294967282L, Types.BIGINT);
            pstmt.executeUpdate();
        }

        try (Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT id FROM test_unit4")) {
            assertTrue(rs.next());
            Object r1 = rs.getObject(1);
            assertNotNull(r1);
            assertEquals(4294967282L, r1);
        } finally {
            TestUtil.dropTable(con, "test_unit4");
        }
    }

    /**
     * test uint8 type
     * @throws Exception
     */
    @Test
    public void testUint8() throws SQLException {
        TestUtil.createTable(con, "test_unit8", "id uint8");
        BigInteger b = new BigInteger("9223372036859999999");
        BigInteger b2 = new BigInteger("15223372036859999999");
        try (PreparedStatement pstmt = con.prepareStatement("INSERT INTO test_unit8 VALUES (?)")) {
            pstmt.setObject(1, b, Types.NUMERIC);
            pstmt.executeUpdate();

            pstmt.setObject(1, b2, Types.NUMERIC);
            pstmt.executeUpdate();
        }

        try (Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT id FROM test_unit8")) {
            assertTrue(rs.next());
            Object r1 = rs.getObject(1);
            assertNotNull(r1);
            assertEquals(b, r1);

            assertTrue(rs.next());
            Object r2 = rs.getObject(1);
            assertNotNull(r2);
            assertEquals(b2, r2);
        } finally {
            TestUtil.dropTable(con, "test_unit8");
        }
    }

    @Test
    public void testCreateArrayOfUint1() throws SQLException {
        try (PreparedStatement pstmt = con.prepareStatement("SELECT ?::uint1[]")) {
            Object[] in = new Object[3];
            in[0] = 0;
            in[1] = 88;
            in[2] = 115;
            pstmt.setArray(1, con.createArrayOf("uint1", in));
            try (ResultSet rs = pstmt.executeQuery()) {
                Assert.assertTrue(rs.next());
                Array arr = rs.getArray(1);
                Object[] out = (Object[]) arr.getArray();

                Assert.assertEquals(3, out.length);
                Assert.assertEquals(0, Integer.parseInt(out[0].toString()));
                Assert.assertEquals(88, Integer.parseInt(out[1].toString()));
                Assert.assertEquals(115, Integer.parseInt(out[2].toString()));
            }
        }
    }

    @Test
    public void testCreateArrayOfUint2() throws SQLException {
        try (PreparedStatement pstmt = con.prepareStatement("SELECT ?::uint2[]")) {
            Short[] in = new Short[3];
            in[0] = 0;
            in[1] = 12654;
            in[2] = 30035;
            pstmt.setArray(1, con.createArrayOf("uint2", in));
            try (ResultSet rs = pstmt.executeQuery()) {
                Assert.assertTrue(rs.next());
                Array arr = rs.getArray(1);
                Short[] out = (Short[]) arr.getArray();

                Assert.assertEquals(3, out.length);
                Assert.assertEquals(0, out[0].shortValue());
                Assert.assertEquals(12654, out[1].shortValue());
                Assert.assertEquals(30035, out[2].shortValue());
            }
        }
    }

    @Test
    public void testCreateArrayOfUint4() throws SQLException {
        try (PreparedStatement pstmt = con.prepareStatement("SELECT ?::uint4[]")) {
            Integer[] in = new Integer[2];
            in[0] = 0;
            in[1] = 1994967295;
            pstmt.setArray(1, con.createArrayOf("uint4", in));
            try (ResultSet rs = pstmt.executeQuery()) {
                Assert.assertTrue(rs.next());
                Array arr = rs.getArray(1);
                Integer[] out = (Integer[]) arr.getArray();

                Assert.assertEquals(2, out.length);
                Assert.assertEquals(0, out[0].intValue());
                Assert.assertEquals(1994967295, out[1].intValue());
            }
        }
    }

    @Test
    public void testCreateArrayOfUint8() throws SQLException {
        try (PreparedStatement pstmt = con.prepareStatement("SELECT ?::uint8[]")) {
            Long[] in = new Long[2];
            in[0] = 0L;
            in[1] = 32458765334567556L;
            pstmt.setArray(1, con.createArrayOf("uint8", in));
            try (ResultSet rs = pstmt.executeQuery()) {
                Assert.assertTrue(rs.next());
                Array arr = rs.getArray(1);
                Object[] out = (Object[]) arr.getArray();
                Long[] outLong = new Long[out.length];
                for (int i = 0; i < out.length; i++) {
                    outLong[i] = Long.valueOf(out[i].toString());
                }

                Assert.assertEquals(2, out.length);
                Assert.assertEquals(0L, outLong[0].longValue());
                Assert.assertEquals(32458765334567556L, outLong[1].longValue());
            }
        }
    }
}
