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
import java.io.IOException;
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

/**
 * test blob
 *
 * @author zhangting
 * @since  2026-09-05
 */
public class BlobTest {
    private static final String TABLE = "ograc_blob_t";

    private Connection conn;

    @Before
    public void setUp() throws Exception {
        conn = TestUtil.openDB();
        TestUtil.dropTable(conn, TABLE);
        try (Statement st = conn.createStatement()) {
            st.execute("create table " + TABLE + "("
                    + "id int primary key, "
                    + "c_blob blob, "
                    + "c_bytea bytea)");
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
    public void testSetBytesAndGetBytesOnBlob() throws SQLException {
        byte[] data = new byte[]{0x01, 0x02, 0x03, (byte) 0xFE, (byte) 0xFF};
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_blob) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setBytes(2, data);
            assertEquals(1, ps.executeUpdate());
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_blob from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertArrayEquals(data, rs.getBytes(1));
                assertArrayEquals(data, rs.getBytes("c_blob"));
            }
        }
    }

    @Test
    public void testSetBytesAndGetBytesOnBytea() throws SQLException {
        byte[] data = new byte[]{0x10, 0x20, 0x30, 0x40};
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_bytea) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setBytes(2, data);
            assertEquals(1, ps.executeUpdate());
        }
        assertArrayEquals(data, getBytea(1));
    }

    @Test
    public void testSetBlobWithInputStream() throws SQLException {
        byte[] data = new byte[]{0x0A, 0x0B, 0x0C, 0x0D};
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_blob) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setBlob(2, new ByteArrayInputStream(data));
            assertEquals(1, ps.executeUpdate());
        }
        assertArrayEquals(data, getBlobBytes(1));
    }

    @Test
    public void testSetBlobWithInputStreamAndLength() throws SQLException {
        byte[] data = new byte[]{1, 2, 3, 4, 5, 6, 7, 8};
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_blob) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setBlob(2, new ByteArrayInputStream(data), data.length);
            assertEquals(1, ps.executeUpdate());
        }
        assertArrayEquals(data, getBlobBytes(1));
    }

    @Test
    public void testSetBlobWithConnectionCreateBlob() throws SQLException {
        byte[] data = new byte[]{9, 8, 7, 6, 5};
        Blob blob = conn.createBlob();
        blob.setBytes(1, data);
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_blob) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setBlob(2, blob.getBinaryStream(), blob.length());
            assertEquals(1, ps.executeUpdate());
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_blob from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                Blob got = rs.getBlob(1);
                assertNotNull(got);
                assertEquals(data.length, got.length());
                assertArrayEquals(data, got.getBytes(1, (int) got.length()));
            }
        } finally {
            blob.free();
        }
    }

    @Test
    public void testGetBlobApi() throws Exception {
        byte[] data = new byte[]{1, 2, 3, 4, 5, 6};
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_blob) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setBytes(2, data);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_blob from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                Blob blob = rs.getBlob("c_blob");
                assertNotNull(blob);
                assertEquals(6, blob.length());
                assertArrayEquals(new byte[]{2, 3, 4}, blob.getBytes(2, 3));
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
    public void testSetBinaryStreamAndGetBinaryStream() throws Exception {
        byte[] data = new byte[512];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i & 0xFF);
        }
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_blob) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setBinaryStream(2, new ByteArrayInputStream(data), data.length);
            assertEquals(1, ps.executeUpdate());
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_blob from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                try (InputStream in = rs.getBinaryStream(1)) {
                    assertArrayEquals(data, readAll(in));
                }
            }
        }
    }

    @Test
    public void testEmptyBlob() throws SQLException {
        byte[] empty = new byte[0];
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_blob, c_bytea) values (?, ?, ?)")) {
            ps.setInt(1, 1);
            ps.setBytes(2, empty);
            ps.setBytes(3, empty);
            assertEquals(1, ps.executeUpdate());
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_blob, c_bytea from " + TABLE + " where id = 1");
             ResultSet rs = ps.executeQuery()) {
            assertTrue(rs.next());
            assertNull(rs.getBytes(1));
            assertTrue(rs.wasNull());
            assertNull(rs.getBlob(1));
            assertTrue(rs.wasNull());
            assertNull(rs.getBytes(2));
            assertTrue(rs.wasNull());
        }
    }

    @Test
    public void testNullBlob() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_blob, c_bytea) values (?, ?, ?)")) {
            ps.setInt(1, 1);
            ps.setNull(2, Types.BLOB);
            ps.setNull(3, Types.BINARY);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "select c_blob, c_bytea from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertNull(rs.getBytes(1));
                assertTrue(rs.wasNull());
                assertNull(rs.getBlob(1));
                assertTrue(rs.wasNull());
                assertNull(rs.getBytes(2));
                assertTrue(rs.wasNull());
            }
        }
    }

    @Test
    public void testBlobAndByteaTogether() throws SQLException {
        byte[] blobData = new byte[]{1, 1, 2, 3, 5, 8};
        byte[] byteaData = new byte[]{13, 21, 34};
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_blob, c_bytea) values (?, ?, ?)")) {
            ps.setInt(1, 1);
            ps.setBytes(2, blobData);
            ps.setBytes(3, byteaData);
            assertEquals(1, ps.executeUpdate());
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "select c_blob, c_bytea from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertArrayEquals(blobData, rs.getBytes(1));
                assertArrayEquals(byteaData, rs.getBytes(2));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testUpdateBlob() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_blob) values (1, ?)")) {
            ps.setBytes(1, new byte[]{1, 2, 3});
            ps.executeUpdate();
        }
        byte[] updated = new byte[]{9, 9, 9, 9};
        try (PreparedStatement ps = conn.prepareStatement("update " + TABLE + " set c_blob = ? where id = 1")) {
            ps.setBytes(1, updated);
            assertEquals(1, ps.executeUpdate());
        }
        assertArrayEquals(updated, getBlobBytes(1));
    }

    @Test
    public void testBlobBatchInsert() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_blob) values (?, ?)")) {
            for (int i = 1; i <= 3; i++) {
                ps.setInt(1, i);
                ps.setBytes(2, new byte[]{(byte) i, (byte) (i * 2)});
                ps.addBatch();
            }
            assertEquals(3, ps.executeBatch().length);
        }
        assertArrayEquals(new byte[]{1, 2}, getBlobBytes(1));
        assertArrayEquals(new byte[]{2, 4}, getBlobBytes(2));
        assertArrayEquals(new byte[]{3, 6}, getBlobBytes(3));
    }

    @Test
    public void testLargeBlobRoundTrip() throws SQLException {
        byte[] data = new byte[2 * 1024];
        Arrays.fill(data, (byte) 0x5A);
        data[0] = 0x01;
        data[data.length - 1] = 0x02;
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_blob) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setBinaryStream(2, new ByteArrayInputStream(data), data.length);
            assertEquals(1, ps.executeUpdate());
        }
        byte[] got = getBlobBytes(1);
        assertEquals(data.length, got.length);
        assertEquals(0x01, got[0]);
        assertEquals(0x02, got[got.length - 1] & 0xFF);
        assertArrayEquals(data, got);
    }

    @Test
    public void testGetObjectReturnsBinary() throws SQLException {
        byte[] data = new byte[]{7, 8, 9};
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_blob) values (1, ?)")) {
            ps.setBytes(1, data);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_blob from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                Object obj = rs.getObject(1);
                assertNotNull(obj);
                if (obj instanceof byte[]) {
                    assertArrayEquals(data, (byte[]) obj);
                } else if (obj instanceof Blob) {
                    Blob blob = (Blob) obj;
                    assertArrayEquals(data, blob.getBytes(1, (int) blob.length()));
                } else {
                    assertArrayEquals(data, rs.getBytes(1));
                }
            }
        }
    }

    private byte[] getBlobBytes(int id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("select c_blob from " + TABLE + " where id = ?")) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return rs.getBytes(1);
            }
        }
    }

    private byte[] getBytea(int id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("select c_bytea from " + TABLE + " where id = ?")) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return rs.getBytes(1);
            }
        }
    }

    private static byte[] readAll(InputStream in) throws SQLException {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buf = new byte[1024];
            int n;
            while ((n = in.read(buf)) >= 0) {
                out.write(buf, 0, n);
            }
            return out.toByteArray();
        } catch (IOException e) {
            throw new SQLException("failed to read stream", e);
        }
    }
}
