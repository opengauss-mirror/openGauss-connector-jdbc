/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.
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
 * test int
 *
 * @author zhangting
 * @since  2026-09-23
 */
public class IntTest {
    private static final String TABLE = "ograc_int_t";

    private Connection conn;

    @Before
    public void setUp() throws Exception {
        conn = TestUtil.openDB();
        TestUtil.dropTable(conn, TABLE);
        try (Statement st = conn.createStatement()) {
            st.execute("create table " + TABLE + "("
                    + "id int primary key, "
                    + "c_smallint smallint, "
                    + "c_int int, "
                    + "c_integer integer, "
                    + "c_bigint bigint)");
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
    public void testSmallintBounds() throws SQLException {
        insertSmallint(1, Short.MIN_VALUE);
        insertSmallint(2, Short.MAX_VALUE);
        insertSmallint(3, (short) 0);
        insertSmallint(4, (short) -1);
        assertEquals(Short.MIN_VALUE, getSmallint(1));
        assertEquals(Short.MAX_VALUE, getSmallint(2));
        assertEquals(0, getSmallint(3));
        assertEquals(-1, getSmallint(4));
    }

    @Test
    public void testIntBounds() throws SQLException {
        insertInt(1, Integer.MIN_VALUE);
        insertInt(2, Integer.MAX_VALUE);
        insertInt(3, 0);
        insertInt(4, -42);
        assertEquals(Integer.MIN_VALUE, getInt(1));
        assertEquals(Integer.MAX_VALUE, getInt(2));
        assertEquals(0, getInt(3));
        assertEquals(-42, getInt(4));
    }

    @Test
    public void testIntegerAliasSameAsInt() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_integer) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setInt(2, 123456);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_integer from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(123456, rs.getInt(1));
                assertEquals(123456, rs.getInt("c_integer"));
            }
        }
    }

    @Test
    public void testBigintBounds() throws SQLException {
        insertBigint(1, Long.MIN_VALUE);
        insertBigint(2, Long.MAX_VALUE);
        insertBigint(3, 0L);
        insertBigint(4, 3000000000L);
        assertEquals(Long.MIN_VALUE, getBigint(1));
        assertEquals(Long.MAX_VALUE, getBigint(2));
        assertEquals(0L, getBigint(3));
        assertEquals(3000000000L, getBigint(4));
    }

    @Test
    public void testAllIntColumnsTogether() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_smallint, c_int, c_integer, c_bigint) values (?,?,?,?,?)")) {
            ps.setInt(1, 1);
            ps.setShort(2, (short) 11);
            ps.setInt(3, 22);
            ps.setInt(4, 33);
            ps.setLong(5, 44L);
            assertEquals(1, ps.executeUpdate());
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "select c_smallint, c_int, c_integer, c_bigint from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(11, rs.getShort(1));
                assertEquals(22, rs.getInt(2));
                assertEquals(33, rs.getInt(3));
                assertEquals(44L, rs.getLong(4));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testIntNullAndWasNull() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_smallint, c_int, c_integer, c_bigint) values (?,?,?,?,?)")) {
            ps.setInt(1, 1);
            ps.setNull(2, Types.SMALLINT);
            ps.setNull(3, Types.INTEGER);
            ps.setNull(4, Types.INTEGER);
            ps.setNull(5, Types.BIGINT);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "select c_smallint, c_int, c_integer, c_bigint from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(0, rs.getShort(1));
                assertTrue(rs.wasNull());
                assertEquals(0, rs.getInt(2));
                assertTrue(rs.wasNull());
                assertEquals(0, rs.getInt(3));
                assertTrue(rs.wasNull());
                assertEquals(0L, rs.getLong(4));
                assertTrue(rs.wasNull());
                assertNull(rs.getObject(2));
                assertTrue(rs.wasNull());
            }
        }
    }

    @Test
    public void testIntSetFromStringAndLong() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_int, c_bigint) values (?, ?, ?)")) {
            ps.setInt(1, 1);
            ps.setString(2, "100");
            ps.setString(3, "200");
            ps.executeUpdate();
            ps.setInt(1, 2);
            ps.setLong(2, 300L);
            ps.setLong(3, 400L);
            ps.executeUpdate();
        }
        assertEquals(100, getInt(1));
        assertEquals(200L, getBigint(1));
        assertEquals(300, getInt(2));
        assertEquals(400L, getBigint(2));
    }

    @Test
    public void testIntGetAsOtherNumericTypes() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_smallint, c_int, c_bigint) values (1, 12, 34, 56)")) {
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "select c_smallint, c_int, c_bigint from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(12, rs.getInt(1));
                assertEquals(12L, rs.getLong(1));
                assertEquals("12", rs.getString(1));
                assertEquals(0,
                        new BigDecimal("34").compareTo(BigDecimal.valueOf(rs.getLong(2))));
                assertEquals(56, rs.getInt(3));
                assertEquals(56L, rs.getLong(3));
                assertEquals(56.0d, rs.getLong(3), 0.0d);
                Object obj = rs.getObject(2);
                assertTrue(obj instanceof Number);
                assertEquals(34, ((Number) obj).intValue());
            }
        }
    }

    @Test
    public void testIntBatchInsert() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_int) values (?, ?)")) {
            for (int i = 1; i <= 5; i++) {
                ps.setInt(1, i);
                ps.setInt(2, i * 10);
                ps.addBatch();
            }
            int[] counts = ps.executeBatch();
            assertEquals(5, counts.length);
        }
        try (PreparedStatement ps = conn.prepareStatement("select id, c_int from " + TABLE + " order by id")) {
            try (ResultSet rs = ps.executeQuery()) {
                for (int i = 1; i <= 5; i++) {
                    assertTrue(rs.next());
                    assertEquals(i, rs.getInt(1));
                    assertEquals(i * 10, rs.getInt(2));
                }
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testIntUpdateAndWhere() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_int, c_bigint) values (1, 10, 100)")) {
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "update " + TABLE + " set c_int = ?, c_bigint = ? where id = ? and c_int = ?")) {
            ps.setInt(1, 20);
            ps.setLong(2, 200L);
            ps.setInt(3, 1);
            ps.setInt(4, 10);
            assertEquals(1, ps.executeUpdate());
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "select c_int, c_bigint from " + TABLE + " where c_int between ? and ?")) {
            ps.setInt(1, 15);
            ps.setInt(2, 25);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(20, rs.getInt(1));
                assertEquals(200L, rs.getLong(2));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testIntArithmeticInSql() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_int, c_bigint) values (1, 7, 3)")) {
            ps.executeUpdate();
        }
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "select c_int + c_bigint, c_int * 2, c_bigint - 1 from " + TABLE + " where id = 1")) {
            assertTrue(rs.next());
            assertEquals(10L, rs.getLong(1));
            assertEquals(14, rs.getInt(2));
            assertEquals(2L, rs.getLong(3));
        }
    }

    private void insertSmallint(int id, short value) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_smallint) values (?, ?)")) {
            ps.setInt(1, id);
            ps.setShort(2, value);
            ps.executeUpdate();
        }
    }

    private void insertInt(int id, int value) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_int) values (?, ?)")) {
            ps.setInt(1, id);
            ps.setInt(2, value);
            ps.executeUpdate();
        }
    }

    private void insertBigint(int id, long value) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_bigint) values (?, ?)")) {
            ps.setInt(1, id);
            ps.setLong(2, value);
            ps.executeUpdate();
        }
    }

    private short getSmallint(int id) throws SQLException {
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

    private long getBigint(int id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("select c_bigint from " + TABLE + " where id = ?")) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return rs.getLong(1);
            }
        }
    }
}
