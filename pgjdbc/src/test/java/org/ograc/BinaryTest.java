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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
import static org.junit.Assert.fail;

/**
 * test binary type
 *
 * @author zhangting
 * @since  2026-09-05
 */
public class BinaryTest {
    private static final String TABLE = "ograc_binary_t";

    private Connection conn;

    @Before
    public void setUp() throws Exception {
        conn = TestUtil.openDB();
        TestUtil.dropTable(conn, TABLE);
        try (Statement st = conn.createStatement()) {
            st.execute("create table " + TABLE + "("
                    + "id int primary key, "
                    + "c_binary binary(8), "
                    + "c_varbinary varbinary(64), "
                    + "c_bytea bytea, "
                    + "c_raw raw(64), "
                    + "c_blob blob)");
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
    public void testBinaryFixedLengthRoundTrip() throws SQLException {
        byte[] data = new byte[]{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
        try (PreparedStatement ps = conn.prepareStatement("insert into "
                + TABLE + "(id, c_binary) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setBytes(2, data);
            assertEquals(1, ps.executeUpdate());
        }
        assertArrayEquals(data, getBytes(1, "c_binary"));
    }

    @Test
    public void testBinaryShorterValue() throws SQLException {
        byte[] data = new byte[]{0x0A, 0x0B, 0x0C};
        try (PreparedStatement ps = conn.prepareStatement("insert into "
                + TABLE + "(id, c_binary) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setBytes(2, data);
            assertEquals(1, ps.executeUpdate());
        }
        byte[] got = getBytes(1, "c_binary");
        assertNotNull(got);
        assertTrue(got.length >= data.length);
        assertArrayEquals(data, Arrays.copyOf(got, data.length));
    }

    @Test
    public void testVarbinaryRoundTrip() throws SQLException {
        byte[] data = new byte[]{0x11, 0x22, 0x33, 0x44, 0x55};
        try (PreparedStatement ps = conn.prepareStatement("insert into "
                + TABLE + "(id, c_varbinary) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setBytes(2, data);
            assertEquals(1, ps.executeUpdate());
        }
        assertArrayEquals(data, getBytes(1, "c_varbinary"));
    }

    @Test
    public void testByteaRoundTrip() throws SQLException {
        byte[] data = new byte[]{0x01, 0x02, (byte) 0xFE, (byte) 0xFF, 0x00};
        try (PreparedStatement ps = conn.prepareStatement("insert into "
                + TABLE + "(id, c_bytea) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setBytes(2, data);
            assertEquals(1, ps.executeUpdate());
        }
        assertArrayEquals(data, getBytes(1, "c_bytea"));
    }

    @Test
    public void testRawRoundTrip() throws SQLException {
        byte[] data = new byte[]{0x10, 0x20, 0x30, 0x40};
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_raw) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setBytes(2, data);
            assertEquals(1, ps.executeUpdate());
        }
        assertArrayEquals(data, getBytes(1, "c_raw"));
    }

    @Test
    public void testAllBinaryColumnsTogether() throws SQLException {
        byte[] fixed = new byte[]{1, 2, 3, 4, 5, 6, 7, 8};
        byte[] varb = new byte[]{9, 10, 11};
        byte[] bytea = new byte[]{12, 13};
        byte[] raw = new byte[]{14, 15, 16, 17};
        try (PreparedStatement ps = conn.prepareStatement("insert into "
                + TABLE + "(id, c_binary, c_varbinary, c_bytea, c_raw) values (?,?,?,?,?)")) {
            ps.setInt(1, 1);
            ps.setBytes(2, fixed);
            ps.setBytes(3, varb);
            ps.setBytes(4, bytea);
            ps.setBytes(5, raw);
            assertEquals(1, ps.executeUpdate());
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "select c_binary, c_varbinary, c_bytea, c_raw from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertArrayEquals(fixed, rs.getBytes(1));
                assertArrayEquals(varb, rs.getBytes(2));
                assertArrayEquals(bytea, rs.getBytes(3));
                assertArrayEquals(raw, rs.getBytes(4));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testBinaryNull() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_binary, c_varbinary, c_bytea, c_raw) values (?,?,?,?,?)")) {
            ps.setInt(1, 1);
            ps.setNull(2, Types.BINARY);
            ps.setNull(3, Types.VARBINARY);
            ps.setNull(4, Types.BINARY);
            ps.setNull(5, Types.BINARY);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "select c_binary, c_varbinary, c_bytea, c_raw from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertNull(rs.getBytes(1));
                assertTrue(rs.wasNull());
                assertNull(rs.getBytes(2));
                assertTrue(rs.wasNull());
                assertNull(rs.getBytes(3));
                assertTrue(rs.wasNull());
                assertNull(rs.getBytes(4));
                assertTrue(rs.wasNull());
            }
        }
    }

    @Test
    public void testEmptyBinaryValues() throws SQLException {
        byte[] empty = new byte[0];
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_varbinary, c_bytea, c_raw) values (?, ?, ?, ?)")) {
            ps.setInt(1, 1);
            ps.setBytes(2, empty);
            ps.setBytes(3, empty);
            ps.setBytes(4, empty);
            assertEquals(1, ps.executeUpdate());
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "select c_varbinary, c_bytea, c_raw from " + TABLE + " where id = 1");
             ResultSet rs = ps.executeQuery()) {
            assertTrue(rs.next());
            assertNull(rs.getBytes(1));
            assertTrue(rs.wasNull());
            assertNull(rs.getBytes(2));
            assertTrue(rs.wasNull());
            assertNull(rs.getBytes(3));
            assertTrue(rs.wasNull());
        }
    }

    @Test
    public void testBinarySetObjectAndGetObject() throws SQLException {
        byte[] data = new byte[]{0x01, 0x02, 0x03};
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_bytea) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setObject(2, data);
            assertEquals(1, ps.executeUpdate());
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_bytea from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                Object obj = rs.getObject(1);
                assertNotNull(obj);
                assertTrue(obj instanceof java.sql.Blob);
                java.sql.Blob blob = (java.sql.Blob) obj;
                assertArrayEquals(data, blob.getBytes(1, (int) blob.length()));
                assertArrayEquals(data, rs.getBytes(1));
            }
        }
    }

    @Test
    public void testBinaryViaBinaryStream() throws SQLException {
        byte[] data = new byte[64];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_varbinary) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setBinaryStream(2, new ByteArrayInputStream(data), data.length);
            assertEquals(1, ps.executeUpdate());
        }
        assertArrayEquals(data, getBytes(1, "c_varbinary"));
    }

    @Test
    public void testUpdateBinary() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_bytea) values (1, ?)")) {
            ps.setBytes(1, new byte[]{1, 2, 3});
            ps.executeUpdate();
        }
        byte[] updated = new byte[]{9, 8, 7, 6};
        try (PreparedStatement ps = conn.prepareStatement("update " + TABLE + " set c_bytea = ? where id = 1")) {
            ps.setBytes(1, updated);
            assertEquals(1, ps.executeUpdate());
        }
        assertArrayEquals(updated, getBytes(1, "c_bytea"));
    }

    @Test
    public void testBinaryBatchInsert() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_varbinary) values (?, ?)")) {
            for (int i = 1; i <= 3; i++) {
                ps.setInt(1, i);
                ps.setBytes(2, new byte[]{(byte) i, (byte) (i + 10)});
                ps.addBatch();
            }
            assertEquals(3, ps.executeBatch().length);
        }
        assertArrayEquals(new byte[]{1, 11}, getBytes(1, "c_varbinary"));
        assertArrayEquals(new byte[]{2, 12}, getBytes(2, "c_varbinary"));
        assertArrayEquals(new byte[]{3, 13}, getBytes(3, "c_varbinary"));
    }

    @Test
    public void testBinaryGetByLabel() throws SQLException {
        byte[] data = new byte[]{0x5A, 0x5B};
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_bytea) values (1, ?)")) {
            ps.setBytes(1, data);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_bytea from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertArrayEquals(data, rs.getBytes("c_bytea"));
                assertArrayEquals(data, rs.getBytes("C_BYTEA"));
            }
        }
    }

    @Test
    public void testLargeBytea() throws SQLException {
        byte[] data = new byte[2 * 1024];
        Arrays.fill(data, (byte) 0x7F);
        data[0] = 0x01;
        data[data.length - 1] = 0x02;
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_bytea) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setBytes(2, data);
            assertEquals(1, ps.executeUpdate());
        }
        byte[] got = getBytes(1, "c_bytea");
        assertEquals(data.length, got.length);
        assertArrayEquals(data, got);
    }

    @Test
    public void testSetBinaryStreamWithIntLength() throws Exception {
        byte[] data = streamBytes(1, 2, 3, 4, 5);
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_bytea) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setBinaryStream(2, new ByteArrayInputStream(data), data.length);
            assertEquals(1, ps.executeUpdate());
        }
        assertArrayEquals(data, readColumnViaBinaryStream(1, "c_bytea"));
    }

    @Test
    public void testSetBinaryStreamWithLongLength() throws Exception {
        byte[] data = streamBytes(10, 20, 30, 40);
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_bytea) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setBinaryStream(2, new ByteArrayInputStream(data), (long) data.length);
            assertEquals(1, ps.executeUpdate());
        }
        assertArrayEquals(data, readColumnViaBinaryStream(1, "c_bytea"));
    }

    @Test
    public void testSetBinaryStreamWithoutLength() throws Exception {
        byte[] data = streamBytes(0xAA, 0xBB, 0xCC);
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_bytea) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setBinaryStream(2, new ByteArrayInputStream(data));
            assertEquals(1, ps.executeUpdate());
        }
        assertArrayEquals(data, readColumnViaBinaryStream(1, "c_bytea"));
    }

    @Test
    public void testSetBinaryStreamPartialLength() throws Exception {
        byte[] full = streamBytes(1, 2, 3, 4, 5, 6, 7, 8);
        int length = 4;
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_bytea) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setBinaryStream(2, new ByteArrayInputStream(full), length);
            assertEquals(1, ps.executeUpdate());
        }
        assertArrayEquals(Arrays.copyOf(full, length), readColumnViaBinaryStream(1, "c_bytea"));
    }

    @Test
    public void testGetBinaryStreamByIndexAndLabel() throws Exception {
        byte[] data = streamBytes(9, 8, 7, 6);
        insertBytea(1, data);
        try (PreparedStatement ps = conn.prepareStatement("select c_bytea from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                InputStream byIndex = rs.getBinaryStream(1);
                try {
                    assertArrayEquals(data, readAll(byIndex));
                } finally {
                    byIndex.close();
                }
            }
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_bytea from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                InputStream byLabel = rs.getBinaryStream("c_bytea");
                try {
                    assertArrayEquals(data, readAll(byLabel));
                } finally {
                    byLabel.close();
                }
            }
        }
    }

    @Test
    public void testSetBinaryStreamOnBlobColumn() throws Exception {
        byte[] data = streamBytes(11, 22, 33, 44, 55);
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_blob) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setBinaryStream(2, new ByteArrayInputStream(data), data.length);
            assertEquals(1, ps.executeUpdate());
        }
        assertArrayEquals(data, readColumnViaBinaryStream(1, "c_blob"));
    }

    @Test
    public void testBlobGetBinaryStream() throws Exception {
        byte[] data = streamBytes(1, 2, 3, 4, 5, 6, 7);
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_blob) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setBinaryStream(2, new ByteArrayInputStream(data), data.length);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_blob from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                Blob blob = rs.getBlob(1);
                assertNotNull(blob);
                InputStream in = blob.getBinaryStream();
                try {
                    assertArrayEquals(data, readAll(in));
                } finally {
                    in.close();
                }
            }
        }
    }

    @Test
    public void testEmptyBinaryStream() throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_bytea) values (?, ?)")) {
            ps.setInt(1, 1);
            try {
                ps.setBinaryStream(2, new ByteArrayInputStream(new byte[0]), 0);
                fail("expected SQLException for setBinaryStream length 0");
            } catch (SQLException expected) {
                assertTrue(expected.getMessage().toLowerCase().contains("invalid"));
            }
        }
    }

    @Test
    public void testNullBinaryStream() throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_bytea) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setNull(2, Types.BINARY);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_bytea from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertNull(rs.getBinaryStream(1));
                assertTrue(rs.wasNull());
            }
        }
    }

    @Test
    public void testUpdateWithBinaryStream() throws Exception {
        insertBytea(1, streamBytes(1, 1, 1));
        byte[] updated = streamBytes(9, 9, 9, 9);
        try (PreparedStatement ps = conn.prepareStatement("update " + TABLE + " set c_bytea = ? where id = 1")) {
            ps.setBinaryStream(1, new ByteArrayInputStream(updated), updated.length);
            assertEquals(1, ps.executeUpdate());
        }
        assertArrayEquals(updated, readColumnViaBinaryStream(1, "c_bytea"));
    }

    @Test
    public void testBinaryStreamBatchInsert() throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_bytea) values (?, ?)")) {
            for (int i = 1; i <= 3; i++) {
                byte[] data = streamBytes(i, i * 2, i * 3);
                ps.setInt(1, i);
                ps.setBinaryStream(2, new ByteArrayInputStream(data), data.length);
                ps.addBatch();
            }
            assertEquals(3, ps.executeBatch().length);
        }
        assertArrayEquals(streamBytes(1, 2, 3), readColumnViaBinaryStream(1, "c_bytea"));
        assertArrayEquals(streamBytes(2, 4, 6), readColumnViaBinaryStream(2, "c_bytea"));
        assertArrayEquals(streamBytes(3, 6, 9), readColumnViaBinaryStream(3, "c_bytea"));
    }

    @Test
    public void testLargeBinaryStreamRoundTrip() throws Exception {
        byte[] data = new byte[2 * 1024];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i & 0xFF);
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_bytea) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setBinaryStream(2, new ByteArrayInputStream(data), data.length);
            assertEquals(1, ps.executeUpdate());
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_bytea from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                InputStream in = rs.getBinaryStream(1);
                try {
                    byte[] got = readAll(in);
                    assertEquals(data.length, got.length);
                    assertArrayEquals(data, got);
                } finally {
                    in.close();
                }
            }
        }
    }

    @Test
    public void testBinaryStreamThenGetBytesConsistent() throws Exception {
        byte[] data = streamBytes(0x01, 0x02, (byte) 0xFE, (byte) 0xFF);
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_bytea) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setBinaryStream(2, new ByteArrayInputStream(data), data.length);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_bytea from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertArrayEquals(data, rs.getBytes(1));
            }
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_bytea from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                InputStream in = rs.getBinaryStream(1);
                try {
                    assertArrayEquals(data, readAll(in));
                } finally {
                    in.close();
                }
            }
        }
    }

    @Test
    public void testBothColumnsWithBinaryStream() throws Exception {
        byte[] a = streamBytes(1, 2, 3);
        byte[] b = streamBytes(4, 5, 6, 7);
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_bytea, c_blob) values (?, ?, ?)")) {
            ps.setInt(1, 1);
            ps.setBinaryStream(2, new ByteArrayInputStream(a), a.length);
            ps.setBinaryStream(3, new ByteArrayInputStream(b), b.length);
            assertEquals(1, ps.executeUpdate());
        }
        assertArrayEquals(a, readColumnViaBinaryStream(1, "c_bytea"));
        assertArrayEquals(b, readColumnViaBinaryStream(1, "c_blob"));
    }

    private byte[] getBytes(int id, String column) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "select " + column + " from " + TABLE + " where id = ?")) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return rs.getBytes(1);
            }
        }
    }

    private void insertBytea(int id, byte[] data) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_bytea) values (?, ?)")) {
            ps.setInt(1, id);
            ps.setBytes(2, data);
            ps.executeUpdate();
        }
    }

    private byte[] readColumnViaBinaryStream(int id, String column) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(
                "select " + column + " from " + TABLE + " where id = ?")) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                InputStream in = rs.getBinaryStream(1);
                assertNotNull(in);
                try {
                    return readAll(in);
                } finally {
                    in.close();
                }
            }
        }
    }

    private static byte[] streamBytes(int... values) {
        byte[] data = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            data[i] = (byte) values[i];
        }
        return data;
    }

    private static byte[] readAll(InputStream in) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[4096];
        int n;
        while ((n = in.read(buf)) >= 0) {
            out.write(buf, 0, n);
        }
        return out.toByteArray();
    }
}
