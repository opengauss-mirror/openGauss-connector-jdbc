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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * test uint
 *
 * @author zhangting
 * @since  2026-10-16
 */
public class UnsignedTest {
    private static final String TABLE = "ograc_unsigned_t";
    private static final long UINT32_MIN = 0L;
    private static final long UINT32_MAX = 0xffffffffL;

    private Connection conn;

    @Before
    public void setUp() throws Exception {
        conn = TestUtil.openDB();
        TestUtil.dropTable(conn, TABLE);
        try (Statement st = conn.createStatement()) {
            st.execute("create table " + TABLE + "("
                    + "id int primary key, "
                    + "c_uint uint, "
                    + "c_binary_uint32 binary_uint32, "
                    + "c_int_unsigned integer unsigned)");
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
    public void testUintBasicValues() throws SQLException {
        insertUint(1, 0);
        insertUint(2, 1);
        insertUint(3, 100);
        assertEquals(0L, getUint(1));
        assertEquals(1L, getUint(2));
        assertEquals(100L, getUint(3));
    }

    @Test
    public void testBinaryUint32Bounds() throws SQLException {
        insertBinaryUint32(1, UINT32_MIN);
        insertBinaryUint32(2, UINT32_MAX);
        assertEquals(UINT32_MIN, getBinaryUint32(1));
        assertEquals(UINT32_MAX, getBinaryUint32(2));
    }

    @Test
    public void testIntegerUnsignedBounds() throws SQLException {
        insertIntUnsigned(1, UINT32_MIN);
        insertIntUnsigned(2, UINT32_MAX);
        assertEquals(UINT32_MIN, getIntUnsigned(1));
        assertEquals(UINT32_MAX, getIntUnsigned(2));
    }

    @Test
    public void testUnsignedValuesAboveSignedIntMax() throws SQLException {
        long value = 3_000_000_000L;
        insertRow(1, value, value, value);
        assertEquals(value, getUint(1));
        assertEquals(value, getBinaryUint32(1));
        assertEquals(value, getIntUnsigned(1));
    }

    @Test
    public void testAllUnsignedColumnsTogether() throws SQLException {
        insertRow(1, 10L, 20L, 30L);
        try (PreparedStatement ps = conn.prepareStatement(
                "select c_uint, c_binary_uint32, c_int_unsigned from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(10L, rs.getLong(1));
                assertEquals(20L, rs.getLong(2));
                assertEquals(30L, rs.getLong(3));
                assertEquals(10L, rs.getLong("c_uint"));
                assertEquals(20L, rs.getLong("c_binary_uint32"));
                assertEquals(30L, rs.getLong("c_int_unsigned"));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testBinaryUint32BatchInsert() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_binary_uint32) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setLong(2, UINT32_MIN);
            ps.addBatch();
            ps.setInt(1, 2);
            ps.setLong(2, UINT32_MAX);
            ps.addBatch();
            int[] counts = ps.executeBatch();
            assertEquals(2, counts.length);
        }
        assertEquals(UINT32_MIN, getBinaryUint32(1));
        assertEquals(UINT32_MAX, getBinaryUint32(2));
    }

    @Test
    public void testUnsignedNullAndWasNull() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_uint, c_binary_uint32, c_int_unsigned) values (?,?,?,?)")) {
            ps.setInt(1, 1);
            ps.setNull(2, Types.BIGINT);
            ps.setNull(3, Types.BIGINT);
            ps.setNull(4, Types.BIGINT);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "select c_uint, c_binary_uint32, c_int_unsigned from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(0L, rs.getLong(1));
                assertTrue(rs.wasNull());
                assertEquals(0L, rs.getLong(2));
                assertTrue(rs.wasNull());
                assertEquals(0L, rs.getLong(3));
                assertTrue(rs.wasNull());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
            }
        }
    }

    @Test
    public void testUnsignedGetAsOtherNumericTypes() throws SQLException {
        insertRow(1, 42L, 84L, 126L);
        try (PreparedStatement ps = conn.prepareStatement(
                "select c_uint, c_binary_uint32, c_int_unsigned from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(42, rs.getInt(1));
                assertEquals(84, rs.getInt(2));
                assertEquals(126, rs.getInt(3));
                assertEquals(0, new BigDecimal("42").compareTo(BigDecimal.valueOf(rs.getLong(1))));
                assertEquals("84", rs.getString(2));
                assertEquals(126.0d, rs.getLong(3), 0.0d);
            }
        }
    }

    @Test
    public void testUnsignedSetFromStringAndObject() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_uint, c_binary_uint32, c_int_unsigned) values (?,?,?,?)")) {
            ps.setInt(1, 1);
            ps.setString(2, "7");
            ps.setObject(3, 8L);
            ps.setObject(4, "9");
            ps.executeUpdate();
        }
        assertEquals(7L, getUint(1));
        assertEquals(8L, getBinaryUint32(1));
        assertEquals(9L, getIntUnsigned(1));
    }

    @Test
    public void testUnsignedResultSetMetaData() throws SQLException {
        insertRow(1, 1L, 2L, 3L);
        try (PreparedStatement ps = conn.prepareStatement(
                "select c_uint, c_binary_uint32, c_int_unsigned from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                ResultSetMetaData md = rs.getMetaData();
                assertEquals(3, md.getColumnCount());
                assertEquals("C_UINT", md.getColumnName(1));
                assertEquals("C_BINARY_UINT32", md.getColumnName(2));
                assertEquals("C_INT_UNSIGNED", md.getColumnName(3));
                assertTrue(rs.next());
            }
        }
    }

    @Test
    public void testStatementLiteralInsert() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("insert into " + TABLE
                    + "(id, c_uint, c_binary_uint32, c_int_unsigned) values (1, 0, 4294967295, 65535)");
        }
        assertEquals(0L, getUint(1));
        assertEquals(UINT32_MAX, getBinaryUint32(1));
        assertEquals(65535L, getIntUnsigned(1));
    }

    private void insertUint(int id, long value) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_uint) values (?, ?)")) {
            ps.setInt(1, id);
            ps.setLong(2, value);
            ps.executeUpdate();
        }
    }

    private void insertBinaryUint32(int id, long value) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_binary_uint32) values (?, ?)")) {
            ps.setInt(1, id);
            ps.setLong(2, value);
            ps.executeUpdate();
        }
    }

    private void insertIntUnsigned(int id, long value) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_int_unsigned) values (?, ?)")) {
            ps.setInt(1, id);
            ps.setLong(2, value);
            ps.executeUpdate();
        }
    }

    private void insertRow(int id, long uintVal, long binaryUint32Val, long intUnsignedVal) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_uint, c_binary_uint32, c_int_unsigned) values (?,?,?,?)")) {
            ps.setInt(1, id);
            ps.setLong(2, uintVal);
            ps.setLong(3, binaryUint32Val);
            ps.setLong(4, intUnsignedVal);
            ps.executeUpdate();
        }
    }

    private long getUint(int id) throws SQLException {
        return getLongColumn(id, "c_uint");
    }

    private long getBinaryUint32(int id) throws SQLException {
        return getLongColumn(id, "c_binary_uint32");
    }

    private long getIntUnsigned(int id) throws SQLException {
        return getLongColumn(id, "c_int_unsigned");
    }

    private long getLongColumn(int id, String column) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "select " + column + " from " + TABLE + " where id = ?")) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return rs.getLong(1);
            }
        }
    }
}
