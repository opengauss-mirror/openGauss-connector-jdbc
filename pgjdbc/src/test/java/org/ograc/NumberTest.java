/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.ograc;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * test number
 *
 * @author zhangting
 * @since  2026-09-15
 */
public class NumberTest {
    private static final String TABLE = "ograc_number_t";

    private Connection conn;

    @Before
    public void setUp() throws Exception {
        conn = TestUtil.openDB();
        TestUtil.dropTable(conn, TABLE);
        try (Statement st = conn.createStatement()) {
            st.execute("create table " + TABLE + "("
                    + "id int primary key, "
                    + "c_smallint smallint, "
                    + "c_int integer, "
                    + "c_bigint bigint, "
                    + "c_real real, "
                    + "c_double double precision, "
                    + "c_numeric numeric(18,6), "
                    + "c_decimal decimal(12,4))");
        }
    }

    @After
    public void tearDown() {
        try {
            if (conn != null && !conn.isClosed()) {
                TestUtil.dropTable(conn, TABLE);
            }
        } catch (SQLException ignore) {
            // ignore
        }
        TestUtil.closeQuietly(conn);
    }

    @Test
    public void testSmallintRoundTripAndBounds() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_smallint) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setShort(2, Short.MIN_VALUE);
            ps.executeUpdate();
            ps.setInt(1, 2);
            ps.setShort(2, Short.MAX_VALUE);
            ps.executeUpdate();
            ps.setInt(1, 3);
            ps.setShort(2, (short) 0);
            ps.executeUpdate();
        }
        assertEquals(Short.MIN_VALUE, getShort(1));
        assertEquals(Short.MAX_VALUE, getShort(2));
        assertEquals(0, getShort(3));
    }

    @Test
    public void testIntegerRoundTripAndBounds() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_int) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setInt(2, Integer.MIN_VALUE);
            ps.executeUpdate();
            ps.setInt(1, 2);
            ps.setInt(2, Integer.MAX_VALUE);
            ps.executeUpdate();
            ps.setInt(1, 3);
            ps.setInt(2, -12345);
            ps.executeUpdate();
        }
        assertEquals(Integer.MIN_VALUE, getInt(1));
        assertEquals(Integer.MAX_VALUE, getInt(2));
        assertEquals(-12345, getInt(3));
    }

    @Test
    public void testBigintRoundTripAndBounds() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_bigint) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setLong(2, Long.MIN_VALUE);
            ps.executeUpdate();
            ps.setInt(1, 2);
            ps.setLong(2, Long.MAX_VALUE);
            ps.executeUpdate();
            ps.setInt(1, 3);
            ps.setLong(2, 4200000000L);
            ps.executeUpdate();
        }
        assertEquals(Long.MIN_VALUE, getLong(1));
        assertEquals(Long.MAX_VALUE, getLong(2));
        assertEquals(4200000000L, getLong(3));
    }

    @Test
    public void testRealAndDoubleRoundTrip() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_real, c_double) values (?, ?, ?)")) {
            ps.setInt(1, 1);
            ps.setFloat(2, 1.5f);
            ps.setDouble(3, 2.5d);
            ps.executeUpdate();
            ps.setInt(1, 2);
            ps.setFloat(2, -0.125f);
            ps.setDouble(3, -987654321.125d);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "select c_real, c_double from " + TABLE + " where id = ?")) {
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(1.5f, rs.getFloat(1), 0.0001f);
                assertEquals(2.5d, rs.getDouble(2), 0.0000001d);
            }
            ps.setInt(1, 2);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(-0.125f, rs.getFloat(1), 0.0001f);
                assertEquals(-987654321.125d, rs.getDouble(2), 0.001d);
            }
        }
    }

    @Test
    public void testNumericAndDecimalRoundTrip() throws SQLException {
        BigDecimal numeric = new BigDecimal("123456789.123456");
        BigDecimal decimal = new BigDecimal("9876.5432");
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_numeric, c_decimal) values (?, ?, ?)")) {
            ps.setInt(1, 1);
            ps.setBigDecimal(2, numeric);
            ps.setBigDecimal(3, decimal);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "select c_numeric, c_decimal from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(0, numeric.compareTo(rs.getBigDecimal(1)));
                assertEquals(0, decimal.compareTo(rs.getBigDecimal(2)));
            }
        }
    }

    @Test
    public void testNumericScaleAndZero() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_numeric, c_decimal) values (?, ?, ?)")) {
            ps.setInt(1, 1);
            ps.setBigDecimal(2, BigDecimal.ZERO);
            ps.setBigDecimal(3, new BigDecimal("0.0000"));
            ps.executeUpdate();
            ps.setInt(1, 2);
            ps.setBigDecimal(2, new BigDecimal("-1.000001"));
            ps.setBigDecimal(3, new BigDecimal("-0.0001"));
            ps.executeUpdate();
        }
        assertEquals(0, BigDecimal.ZERO.compareTo(getNumeric(1)));
        assertEquals(0, new BigDecimal("-1.000001").compareTo(getNumeric(2)));
        assertEquals(0, new BigDecimal("-0.0001").compareTo(getDecimal(2)));
    }

    @Test
    public void testNullNumberColumns() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE
                        + "(id, c_smallint, c_int, c_bigint, c_real, c_double, c_numeric, c_decimal)"
                        + " values (?,?,?,?,?,?,?,?)")) {
            ps.setInt(1, 1);
            ps.setNull(2, Types.SMALLINT);
            ps.setNull(3, Types.INTEGER);
            ps.setNull(4, Types.BIGINT);
            ps.setNull(5, Types.REAL);
            ps.setNull(6, Types.DOUBLE);
            ps.setNull(7, Types.NUMERIC);
            ps.setNull(8, Types.DECIMAL);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "select c_smallint, c_int, c_bigint, c_real, c_double, c_numeric, c_decimal"
                        + " from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(0, rs.getShort(1));
                assertTrue(rs.wasNull());
                assertEquals(0, rs.getInt(2));
                assertTrue(rs.wasNull());
                assertEquals(0L, rs.getLong(3));
                assertTrue(rs.wasNull());
                assertEquals(0f, rs.getFloat(4), 0.0f);
                assertTrue(rs.wasNull());
                assertEquals(0d, rs.getDouble(5), 0.0d);
                assertTrue(rs.wasNull());
                assertNull(rs.getBigDecimal(6));
                assertTrue(rs.wasNull());
                assertNull(rs.getBigDecimal(7));
                assertTrue(rs.wasNull());
            }
        }
    }

    @Test
    public void testNumberGetAsStringAndObject() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_int, c_bigint, c_numeric) values (?, ?, ?, ?)")) {
            ps.setInt(1, 1);
            ps.setInt(2, 42);
            ps.setLong(3, 99L);
            ps.setBigDecimal(4, new BigDecimal("3.140000"));
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "select c_int, c_bigint, c_numeric from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("42", rs.getString(1));
                assertEquals("99", rs.getString(2));
                assertEquals(0, new BigDecimal("3.140000").compareTo(new BigDecimal(rs.getString(3))));
                Object intObj = rs.getObject(1);
                assertTrue(intObj instanceof Number);
                assertEquals(42, ((Number) intObj).intValue());
                Object bigintObj = rs.getObject(2);
                assertTrue(bigintObj instanceof Number);
                assertEquals(99L, ((Number) bigintObj).longValue());
                Object decimalObj = rs.getObject(3);
                assertTrue(decimalObj instanceof BigDecimal);
                assertEquals(0, new BigDecimal("3.140000").compareTo((BigDecimal) decimalObj));
            }
        }
    }

    @Test
    public void testNumberSetFromString() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_int, c_bigint, c_double, c_numeric) values (?,?,?,?,?)")) {
            ps.setInt(1, 1);
            ps.setString(2, "100");
            ps.setString(3, "200");
            ps.setString(4, "3.5");
            ps.setString(5, "9.875000");
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "select c_int, c_bigint, c_double, c_numeric from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(100, rs.getInt(1));
                assertEquals(200L, rs.getLong(2));
                assertEquals(3.5d, rs.getDouble(3), 0.0001d);
                assertEquals(0, new BigDecimal("9.875000").compareTo(rs.getBigDecimal(4)));
            }
        }
    }

    @Test
    public void testAllNumberColumnsTogether() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE
                        + "(id, c_smallint, c_int, c_bigint, c_real, c_double, c_numeric, c_decimal)"
                        + " values (?,?,?,?,?,?,?,?)")) {
            ps.setInt(1, 1);
            ps.setShort(2, (short) 11);
            ps.setInt(3, 22);
            ps.setLong(4, 33L);
            ps.setFloat(5, 4.5f);
            ps.setDouble(6, 6.75d);
            ps.setBigDecimal(7, new BigDecimal("8.123456"));
            ps.setBigDecimal(8, new BigDecimal("9.0001"));
            assertEquals(1, ps.executeUpdate());
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "select c_smallint, c_int, c_bigint, c_real, c_double, c_numeric, c_decimal"
                        + " from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(11, rs.getShort(1));
                assertEquals(22, rs.getInt(2));
                assertEquals(33L, rs.getLong(3));
                assertEquals(4.5f, rs.getFloat(4), 0.0001f);
                assertEquals(6.75d, rs.getDouble(5), 0.0001d);
                assertEquals(0, new BigDecimal("8.123456").compareTo(rs.getBigDecimal(6)));
                assertEquals(0, new BigDecimal("9.0001").compareTo(rs.getBigDecimal(7)));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testUpdateAndCompareNumberValues() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_int, c_numeric) values (1, 10, 1.5)")) {
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "update " + TABLE + " set c_int = ?, c_numeric = ? where id = ?")) {
            ps.setInt(1, 20);
            ps.setBigDecimal(2, new BigDecimal("2.500000"));
            ps.setInt(3, 1);
            assertEquals(1, ps.executeUpdate());
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_int, c_numeric from " + TABLE
                        + " where c_int > ? and c_numeric >= ?")) {
            ps.setInt(1, 15);
            ps.setBigDecimal(2, new BigDecimal("2.5"));
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(20, rs.getInt(1));
                assertEquals(0, new BigDecimal("2.500000").compareTo(rs.getBigDecimal(2)));
                assertFalse(rs.next());
            }
        }
    }

    private short getShort(int id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("select c_smallint from " + TABLE + " where id = ?")) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return rs.getShort(1);
            }
        }
    }

    private int getInt(int id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("select c_int from " + TABLE + " where id = ?")) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return rs.getInt(1);
            }
        }
    }

    private long getLong(int id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("select c_bigint from " + TABLE + " where id = ?")) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return rs.getLong(1);
            }
        }
    }

    private BigDecimal getNumeric(int id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("select c_numeric from " + TABLE + " where id = ?")) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return rs.getBigDecimal(1);
            }
        }
    }

    private BigDecimal getDecimal(int id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("select c_decimal from " + TABLE + " where id = ?")) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return rs.getBigDecimal(1);
            }
        }
    }
}
