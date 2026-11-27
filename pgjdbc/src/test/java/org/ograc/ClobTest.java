/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * All rights reserved.
 *
 * Licensed under the 2-Clause BSD License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/license/bsd-2-clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ograc;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Reader;
import java.io.StringReader;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * test clob
 *
 * @author zhangting
 * @since  2026-09-12
 */
public class ClobTest {
    private static final String TABLE = "ograc_clob_t";

    private Connection conn;

    @Before
    public void setUp() throws Exception {
        conn = TestUtil.openDB();
        TestUtil.dropTable(conn, TABLE);
        try (Statement st = conn.createStatement()) {
            st.execute("create table " + TABLE + "("
                    + "id int primary key, "
                    + "c_clob clob, "
                    + "c_text text)");
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
    public void testSetStringAndGetStringOnClob() throws SQLException {
        String value = "hello-clob!@#";
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_clob) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setString(2, value);
            assertEquals(1, ps.executeUpdate());
        }
        assertEquals(value, getClobString(1));
    }

    @Test
    public void testSetStringAndGetStringOnText() throws SQLException {
        String value = "hello-text";
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_text) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setString(2, value);
            assertEquals(1, ps.executeUpdate());
        }
        assertEquals(value, getText(1));
    }

    @Test
    public void testSetClobWithReader() throws SQLException {
        String value = "clob-from-reader";
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_clob) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setClob(2, new StringReader(value));
            assertEquals(1, ps.executeUpdate());
        }
        assertEquals(value, getClobString(1));
    }

    @Test
    public void testSetClobWithReaderAndLength() throws SQLException {
        String value = "clob-with-length-12345";
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_clob) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setClob(2, new StringReader(value), value.length());
            assertEquals(1, ps.executeUpdate());
        }
        assertEquals(value, getClobString(1));
    }

    @Test
    public void testSetClobWithConnectionCreateClob() throws SQLException {
        String value = "createClob-api";
        Clob clob = conn.createClob();
        clob.setString(1, value);
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_clob) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setClob(2, clob.getCharacterStream(), clob.length());
            assertEquals(1, ps.executeUpdate());
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_clob from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                Clob got = rs.getClob(1);
                assertNotNull(got);
                assertEquals(value.length(), got.length());
                assertEquals(value, got.getSubString(1, (int) got.length()));
            }
        } finally {
            clob.free();
        }
    }

    @Test
    public void testGetClobApi() throws Exception {
        String value = "abcdef";
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_clob) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setString(2, value);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_clob from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                Clob clob = rs.getClob("c_clob");
                assertNotNull(clob);
                assertEquals(6, clob.length());
                assertEquals("bcd", clob.getSubString(2, 3));
                Reader reader = clob.getCharacterStream();
                try {
                    assertEquals(value, readAll(reader));
                } finally {
                    reader.close();
                }
            }
        }
    }

    @Test
    public void testSetCharacterStreamAndGetCharacterStream() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 200; i++) {
            sb.append("line-").append(i).append('\n');
        }
        String value = sb.toString();
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_clob) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setCharacterStream(2, new StringReader(value), value.length());
            assertEquals(1, ps.executeUpdate());
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_clob from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                Reader reader = rs.getCharacterStream(1);
                try {
                    assertEquals(value, readAll(reader));
                } finally {
                    reader.close();
                }
            }
        }
    }

    @Test
    public void testEmptyClob() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_clob, c_text) values (?, ?, ?)")) {
            ps.setInt(1, 1);
            ps.setString(2, "");
            ps.setString(3, "");
            assertEquals(1, ps.executeUpdate());
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_clob, c_text from " + TABLE + " where id = 1");
             ResultSet rs = ps.executeQuery()) {
            assertTrue(rs.next());
            assertNull(rs.getString(1));
            assertTrue(rs.wasNull());
            assertNull(rs.getClob(1));
            assertTrue(rs.wasNull());
            assertNull(rs.getString(2));
            assertTrue(rs.wasNull());
        }
    }

    @Test
    public void testNullClob() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_clob, c_text) values (?, ?, ?)")) {
            ps.setInt(1, 1);
            ps.setNull(2, Types.CLOB);
            ps.setNull(3, Types.LONGVARCHAR);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "select c_clob, c_text from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertNull(rs.getString(1));
                assertTrue(rs.wasNull());
                assertNull(rs.getClob(1));
                assertTrue(rs.wasNull());
                assertNull(rs.getString(2));
                assertTrue(rs.wasNull());
            }
        }
    }

    @Test
    public void testClobAndTextTogether() throws SQLException {
        String clobValue = "clob-side";
        String textValue = "text-side";
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_clob, c_text) values (?, ?, ?)")) {
            ps.setInt(1, 1);
            ps.setString(2, clobValue);
            ps.setString(3, textValue);
            assertEquals(1, ps.executeUpdate());
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "select c_clob, c_text from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(clobValue, rs.getString(1));
                assertEquals(textValue, rs.getString(2));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testUpdateClob() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_clob) values (1, ?)")) {
            ps.setString(1, "old");
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement("update " + TABLE + " set c_clob = ? where id = 1")) {
            ps.setString(1, "new-value");
            assertEquals(1, ps.executeUpdate());
        }
        assertEquals("new-value", getClobString(1));
    }

    @Test
    public void testClobBatchInsert() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_clob) values (?, ?)")) {
            for (int i = 1; i <= 3; i++) {
                ps.setInt(1, i);
                ps.setString(2, "v" + i);
                ps.addBatch();
            }
            assertEquals(3, ps.executeBatch().length);
        }
        assertEquals("v1", getClobString(1));
        assertEquals("v2", getClobString(2));
        assertEquals("v3", getClobString(3));
    }

    @Test
    public void testLargeClobRoundTrip() throws SQLException {
        StringBuilder sb = new StringBuilder(64 * 1024);
        while (sb.length() < 64 * 1024) {
            sb.append("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789");
        }
        String value = sb.toString();
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_clob) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setCharacterStream(2, new StringReader(value), value.length());
            assertEquals(1, ps.executeUpdate());
        }
        String got = getClobString(1);
        assertEquals(value.length(), got.length());
        assertEquals(value, got);
    }

    @Test
    public void testGetObjectForClob() throws SQLException {
        String value = "object-clob";
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_clob) values (1, ?)")) {
            ps.setString(1, value);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_clob from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                Object obj = rs.getObject(1);
                assertNotNull(obj);
                if (obj instanceof String) {
                    assertEquals(value, obj);
                } else if (obj instanceof Clob) {
                    Clob clob = (Clob) obj;
                    assertEquals(value, clob.getSubString(1, (int) clob.length()));
                } else {
                    assertEquals(value, rs.getString(1));
                }
            }
        }
    }

    @Test
    public void testClobUnicodeAndSpecialChars() throws SQLException {
        String value = "αβγ €\n\t\\\"'";
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_clob) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setString(2, value);
            assertEquals(1, ps.executeUpdate());
        }
        assertEquals(value, getClobString(1));
    }

    private String getClobString(int id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("select c_clob from " + TABLE + " where id = ?")) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return rs.getString(1);
            }
        }
    }

    private String getText(int id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("select c_text from " + TABLE + " where id = ?")) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return rs.getString(1);
            }
        }
    }

    private static String readAll(Reader reader) throws Exception {
        StringBuilder sb = new StringBuilder();
        char[] buf = new char[1024];
        int n;
        while ((n = reader.read(buf)) >= 0) {
            sb.append(buf, 0, n);
        }
        return sb.toString();
    }
}
