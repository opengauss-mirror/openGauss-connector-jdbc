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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * test raw type
 *
 * @author zhangting
 * @since  2026-09-17
 */
public class RawTest {
    private static final String TABLE = "ograc_raw_t";
    private static final int RAW_MAX_LEN = 64;

    private Connection conn;

    @Before
    public void setUp() throws Exception {
        conn = TestUtil.openDB();
        TestUtil.dropTable(conn, TABLE);
        try (Statement st = conn.createStatement()) {
            st.execute("create table " + TABLE + "(id int primary key, c_raw raw(" + RAW_MAX_LEN + "))");
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
    public void testRawRoundTrip() throws SQLException {
        byte[] data = new byte[]{0x01, 0x02, 0x03, 0x04};
        insertRaw(1, data);
        assertArrayEquals(data, getRaw(1));
    }

    @Test
    public void testRawGetByColumnLabel() throws SQLException {
        byte[] data = new byte[]{0x10, 0x20, 0x30};
        insertRaw(1, data);
        try (PreparedStatement ps = conn.prepareStatement("select c_raw from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertArrayEquals(data, rs.getBytes("c_raw"));
                assertArrayEquals(data, rs.getBytes("C_RAW"));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testRawRepresentativeValues() throws SQLException {
        byte[][] values = {
                new byte[]{0x00},
                new byte[]{(byte) 0xFF},
                new byte[]{0x01, 0x02, (byte) 0xFE, (byte) 0xFF, 0x00},
                new byte[]{0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F}
        };
        for (int i = 0; i < values.length; i++) {
            insertRaw(i + 1, values[i]);
        }
        for (int i = 0; i < values.length; i++) {
            assertArrayEquals(values[i], getRaw(i + 1));
        }
    }

    @Test
    public void testRawNullAndWasNull() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_raw) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setNull(2, Types.BINARY);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_raw from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertNull(rs.getBytes(1));
                assertTrue(rs.wasNull());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
            }
        }
    }

    @Test
    public void testRawEmptyValue() throws SQLException {
        byte[] empty = new byte[0];
        insertRaw(1, empty);
        try (PreparedStatement ps = conn.prepareStatement("select c_raw from " + TABLE + " where id = 1");
             ResultSet rs = ps.executeQuery()) {
            assertTrue(rs.next());
            assertNull(rs.getBytes(1));
            assertTrue(rs.wasNull());
            assertNull(rs.getObject(1));
            assertTrue(rs.wasNull());
        }
    }

    @Test
    public void testRawSetObjectAndGetObject() throws SQLException {
        byte[] data = new byte[]{0x05, 0x06, 0x07};
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_raw) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setObject(2, data);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "select c_raw from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                Object obj = rs.getObject(1);
                assertNotNull(obj);
                assertTrue(obj instanceof String);
                assertEquals("050607", obj);
                assertArrayEquals(data, rs.getBytes(1));
            }
        }
    }

    @Test
    public void testRawViaBinaryStream() throws SQLException {
        byte[] data = new byte[32];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_raw) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setBinaryStream(2, new ByteArrayInputStream(data), data.length);
            assertEquals(1, ps.executeUpdate());
        }
        assertArrayEquals(data, getRaw(1));
    }

    @Test
    public void testRawGetBinaryStream() throws SQLException, IOException {
        byte[] data = new byte[]{0x11, 0x22, 0x33, 0x44};
        insertRaw(1, data);
        try (PreparedStatement ps = conn.prepareStatement("select c_raw from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                try (InputStream in = rs.getBinaryStream(1);
                     ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                    assertNotNull(in);
                    byte[] buf = new byte[16];
                    int n;
                    while ((n = in.read(buf)) >= 0) {
                        if (n > 0) {
                            out.write(buf, 0, n);
                        }
                    }
                    assertArrayEquals(data, out.toByteArray());
                }
            }
        }
    }

    @Test
    public void testRawGetAsHexString() throws SQLException {
        byte[] data = new byte[]{0x01, 0x0A, (byte) 0xAB, (byte) 0xFF};
        insertRaw(1, data);
        try (PreparedStatement ps = conn.prepareStatement("select c_raw from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("010AABFF", rs.getString(1));
            }
        }
    }

    @Test
    public void testRawMaxLength() throws SQLException {
        byte[] data = new byte[RAW_MAX_LEN];
        Arrays.fill(data, (byte) 0x5A);
        data[0] = 0x01;
        data[data.length - 1] = 0x02;
        insertRaw(1, data);
        byte[] got = getRaw(1);
        assertEquals(RAW_MAX_LEN, got.length);
        assertArrayEquals(data, got);
    }

    @Test
    public void testRawBatchInsert() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_raw) values (?, ?)")) {
            for (int i = 1; i <= 3; i++) {
                ps.setInt(1, i);
                ps.setBytes(2, new byte[]{(byte) i, (byte) (i + 16)});
                ps.addBatch();
            }
            assertEquals(3, ps.executeBatch().length);
        }
        assertArrayEquals(new byte[]{1, 17}, getRaw(1));
        assertArrayEquals(new byte[]{2, 18}, getRaw(2));
        assertArrayEquals(new byte[]{3, 19}, getRaw(3));
    }

    @Test
    public void testRawUpdate() throws SQLException {
        insertRaw(1, new byte[]{1, 2, 3});
        byte[] updated = new byte[]{9, 8, 7, 6};
        try (PreparedStatement ps = conn.prepareStatement("update " + TABLE + " set c_raw = ? where id = ?")) {
            ps.setBytes(1, updated);
            ps.setInt(2, 1);
            assertEquals(1, ps.executeUpdate());
        }
        assertArrayEquals(updated, getRaw(1));
    }

    @Test
    public void testRawResultSetMetaData() throws SQLException {
        insertRaw(1, new byte[]{0x01});
        try (PreparedStatement ps = conn.prepareStatement("select c_raw from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                ResultSetMetaData md = rs.getMetaData();
                assertEquals(1, md.getColumnCount());
                assertEquals("C_RAW", md.getColumnName(1));
                assertTrue(rs.next());
            }
        }
    }

    private void insertRaw(int id, byte[] data) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_raw) values (?, ?)")) {
            ps.setInt(1, id);
            ps.setBytes(2, data);
            ps.executeUpdate();
        }
    }

    private byte[] getRaw(int id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("select c_raw from " + TABLE + " where id = ?")) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return rs.getBytes(1);
            }
        }
    }
}
