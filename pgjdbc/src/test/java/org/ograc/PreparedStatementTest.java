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

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
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
 * test PreparedStatement
 *
 * @author zhangting
 * @since  2026-09-17
 */
public class PreparedStatementTest {
    private static final String TABLE = "ograc_ps_t";

    private Connection conn;

    @Before
    public void setUp() throws Exception {
        conn = TestUtil.openDB();
        TestUtil.dropTable(conn, TABLE);
        try (Statement st = conn.createStatement()) {
            st.execute("create table " + TABLE + "("
                    + "id int primary key, "
                    + "c_bool boolean, "
                    + "c_smallint smallint, "
                    + "c_int int, "
                    + "c_bigint bigint, "
                    + "c_real real, "
                    + "c_double double precision, "
                    + "c_decimal decimal(12,4), "
                    + "c_varchar varchar(128), "
                    + "c_text text, "
                    + "c_bytea bytea, "
                    + "c_date date, "
                    + "c_ts timestamp, "
                    + "c_int_arr integer[], "
                    + "c_varchar_arr varchar(64)[])");
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
    public void testSetBoolean() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_bool) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setBoolean(2, true);
            assertEquals(1, ps.executeUpdate());
            ps.setInt(1, 2);
            ps.setBoolean(2, false);
            assertEquals(1, ps.executeUpdate());
        }
        assertEquals(true, getBoolean(1));
        assertEquals(false, getBoolean(2));
    }

    @Test
    public void testSetByteAndShort() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_smallint, c_int) values (?, ?, ?)")) {
            ps.setInt(1, 1);
            ps.setByte(2, (byte) 12);
            ps.setShort(3, (short) 1234);
            assertEquals(1, ps.executeUpdate());
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "select c_smallint, c_int from " + TABLE + " where id = ?")) {
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(12, rs.getByte(1));
                assertEquals(1234, rs.getShort(2));
            }
        }
    }

    @Test
    public void testSetIntAndLong() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_int, c_bigint) values (?, ?, ?)")) {
            ps.setInt(1, 1);
            ps.setInt(2, Integer.MAX_VALUE);
            ps.setLong(3, 4200000000L);
            assertEquals(1, ps.executeUpdate());
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "select c_int, c_bigint from " + TABLE + " where id = ?")) {
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(Integer.MAX_VALUE, rs.getInt(1));
                assertEquals(4200000000L, rs.getLong(2));
            }
        }
    }

    @Test
    public void testSetFloatAndDouble() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_real, c_double) values (?, ?, ?)")) {
            ps.setInt(1, 1);
            ps.setFloat(2, 1.25f);
            ps.setDouble(3, 3.1415926535d);
            assertEquals(1, ps.executeUpdate());
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "select c_real, c_double from " + TABLE + " where id = ?")) {
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(1.25f, rs.getFloat(1), 0.0001f);
                assertEquals(3.1415926535d, rs.getDouble(2), 0.0000001d);
            }
        }
    }

    @Test
    public void testSetBigDecimal() throws SQLException {
        BigDecimal value = new BigDecimal("123456.7890");
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_decimal) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setBigDecimal(2, value);
            assertEquals(1, ps.executeUpdate());
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_decimal from " + TABLE + " where id = ?")) {
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(0, value.compareTo(rs.getBigDecimal(1)));
            }
        }
    }

    @Test
    public void testSetString() throws SQLException {
        String value = "hello-ograc !@#";
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_varchar) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setString(2, value);
            assertEquals(1, ps.executeUpdate());
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_varchar from " + TABLE + " where id = ?")) {
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(value, rs.getString(1));
            }
        }
    }

    @Test
    public void testSetBytes() throws SQLException {
        byte[] value = new byte[]{0x01, 0x02, (byte) 0xFE, (byte) 0xFF};
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_bytea) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setBytes(2, value);
            assertEquals(1, ps.executeUpdate());
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_bytea from " + TABLE + " where id = ?")) {
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertArrayEquals(value, rs.getBytes(1));
            }
        }
    }

    @Test
    public void testSetDateTimeTimestamp() throws SQLException {
        Date date = Date.valueOf("2025-06-29");
        Timestamp ts = Timestamp.valueOf("2025-06-29 12:34:56");

        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_date, c_ts) values (?, ?, ?)")) {
            ps.setInt(1, 1);
            ps.setDate(2, date);
            ps.setTimestamp(3, ts);
            assertEquals(1, ps.executeUpdate());
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_date, c_ts from " + TABLE + " where id = ?")) {
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(date, rs.getDate(1));
                assertEquals(ts, rs.getTimestamp(2));
            }
        }
    }

    @Test
    public void testSetNullForVariousTypes() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE
                        + "(id, c_bool, c_int, c_bigint, c_double, c_decimal, c_varchar, c_bytea, c_date, c_ts)"
                        + " values (?,?,?,?,?,?,?,?,?,?)")) {
            ps.setInt(1, 1);
            ps.setNull(2, Types.BOOLEAN);
            ps.setNull(3, Types.INTEGER);
            ps.setNull(4, Types.BIGINT);
            ps.setNull(5, Types.DOUBLE);
            ps.setNull(6, Types.DECIMAL);
            ps.setNull(7, Types.VARCHAR);
            ps.setNull(8, Types.BINARY);
            ps.setNull(9, Types.DATE);
            ps.setNull(10, Types.TIMESTAMP);
            assertEquals(1, ps.executeUpdate());
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "select c_bool, c_int, c_bigint, c_double, c_decimal, c_varchar, c_bytea, c_date, c_ts"
                        + " from " + TABLE + " where id = ?")) {
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                for (int i = 1; i <= 9; i++) {
                    assertNull(rs.getObject(i));
                    assertTrue(rs.wasNull());
                }
            }
        }
    }

    @Test
    public void testSetObjectForVariousTypes() throws SQLException {
        BigDecimal decimal = new BigDecimal("98.7600");
        Date date = Date.valueOf("2024-01-15");
        Timestamp ts = Timestamp.valueOf("2024-01-15 08:09:10");
        byte[] bytes = new byte[]{9, 8, 7};

        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE
                        + "(id, c_bool, c_int, c_bigint, c_double, c_decimal, c_varchar, c_bytea, c_date, c_ts)"
                        + " values (?,?,?,?,?,?,?,?,?,?)")) {
            ps.setObject(1, 1);
            ps.setObject(2, Boolean.TRUE);
            ps.setObject(3, 100);
            ps.setObject(4, 200L);
            ps.setObject(5, 2.5d);
            ps.setObject(6, decimal);
            ps.setObject(7, "setObject");
            ps.setObject(8, bytes);
            ps.setObject(9, date);
            ps.setObject(10, ts);
            assertEquals(1, ps.executeUpdate());
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "select c_bool, c_int, c_bigint, c_double, c_decimal, c_varchar, c_bytea, c_date, c_ts"
                        + " from " + TABLE + " where id = ?")) {
            ps.setObject(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(true, rs.getBoolean(1));
                assertEquals(100, rs.getInt(2));
                assertEquals(200L, rs.getLong(3));
                assertEquals(2.5d, rs.getDouble(4), 0.0001d);
                assertEquals(0, decimal.compareTo(rs.getBigDecimal(5)));
                assertEquals("setObject", rs.getString(6));
                assertArrayEquals(bytes, rs.getBytes(7));
                assertEquals(date, rs.getDate(8));
                assertEquals(ts, rs.getTimestamp(9));
            }
        }
    }

    @Test
    public void testSetBinaryStream() throws SQLException {
        byte[] value = new byte[256];
        for (int i = 0; i < value.length; i++) {
            value[i] = (byte) i;
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_bytea) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setBinaryStream(2, new ByteArrayInputStream(value), value.length);
            assertEquals(1, ps.executeUpdate());
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_bytea from " + TABLE + " where id = ?")) {
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertArrayEquals(value, rs.getBytes(1));
            }
        }
    }

    @Test
    public void testSetCharacterStream() throws SQLException {
        String value = "stream-text-" + Arrays.toString(new int[]{1, 2, 3});
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_text) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setCharacterStream(2, new StringReader(value), value.length());
            assertEquals(1, ps.executeUpdate());
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_text from " + TABLE + " where id = ?")) {
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(value, rs.getString(1));
            }
        }
    }

    @Test
    public void testSetXxxThenQueryByBoundParameter() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_varchar, c_int) values (?, ?, ?)")) {
            ps.setInt(1, 10);
            ps.setString(2, "find-me");
            ps.setInt(3, 55);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "select id, c_int from " + TABLE + " where c_varchar = ?")) {
            ps.setString(1, "find-me");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(10, rs.getInt(1));
                assertEquals(55, rs.getInt(2));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testCreateArrayOfIntegerTypeName() throws SQLException {
        Array array = conn.createArrayOf("integer", new Integer[]{1, 2, 3});
        assertNotNull(array);
        Object raw = array.getArray();
        assertTrue(raw instanceof Object[]);
        assertEquals(3, ((Object[]) raw).length);
    }

    @Test
    public void testCreateArrayOfRejectsBinaryInteger() {
        try {
            conn.createArrayOf("binary_integer", new Integer[]{1, 2, 3});
            fail("expected SQLException for typeName binary_integer");
        } catch (SQLException expected) {
            assertTrue(expected.getMessage().toLowerCase().contains("invalid")
                    || expected.getMessage().toLowerCase().contains("binary_integer"));
        }
    }

    @Test
    public void testSetArrayIntegerElementsRejected() throws SQLException {
        Array array = conn.createArrayOf("integer", new Integer[]{1, 2, 3});
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_int_arr) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setArray(2, array);
            ps.executeUpdate();
            fail("expected Invalid array format from ORArray.toString bind");
        } catch (SQLException expected) {
            assertTrue(expected.getMessage().toLowerCase().contains("array")
                    || expected.getMessage().toLowerCase().contains("invalid"));
        }
    }

    @Test
    public void testSetArrayVarcharElementsRejected() throws SQLException {
        Array array = conn.createArrayOf("varchar", new String[]{"a", "b", "ograc"});
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_varchar_arr) values (?, ?)")) {
            ps.setInt(1, 2);
            ps.setArray(2, array);
            ps.executeUpdate();
            fail("expected Invalid array format from ORArray.toString bind");
        } catch (SQLException expected) {
            assertTrue(expected.getMessage().toLowerCase().contains("array")
                    || expected.getMessage().toLowerCase().contains("invalid"));
        }
    }

    @Test
    public void testSetArrayNull() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_int_arr) values (?, ?)")) {
            ps.setInt(1, 3);
            ps.setArray(2, null);
            assertEquals(1, ps.executeUpdate());
        }

        try (PreparedStatement ps = conn.prepareStatement("select c_int_arr from " + TABLE + " where id = ?")) {
            ps.setInt(1, 3);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                rs.getArray(1);
                assertTrue(rs.wasNull());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
            }
        }
    }

    @Test
    public void testSetNullArrayType() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_int_arr) values (?, ?)")) {
            ps.setInt(1, 4);
            ps.setNull(2, Types.ARRAY);
            assertEquals(1, ps.executeUpdate());
        }

        try (PreparedStatement ps = conn.prepareStatement("select c_int_arr from " + TABLE + " where id = ?")) {
            ps.setInt(1, 4);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
            }
        }
    }

    @Test
    public void testSetObjectWithArrayRejected() throws SQLException {
        Array array = conn.createArrayOf("integer", new Integer[]{10, 20});
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_int_arr) values (?, ?)")) {
            ps.setInt(1, 5);
            ps.setObject(2, array);
            ps.executeUpdate();
            fail("expected Invalid array format");
        } catch (SQLException expected) {
            assertTrue(expected.getMessage().toLowerCase().contains("array")
                    || expected.getMessage().toLowerCase().contains("invalid"));
        }
    }

    @Test
    public void testSetArrayEmptyElementsRejected() throws SQLException {
        Array array = conn.createArrayOf("integer", new Integer[0]);
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_int_arr) values (?, ?)")) {
            ps.setInt(1, 6);
            ps.setArray(2, array);
            ps.executeUpdate();
            fail("expected Invalid array format");
        } catch (SQLException expected) {
            assertTrue(expected.getMessage().toLowerCase().contains("array")
                    || expected.getMessage().toLowerCase().contains("invalid"));
        }
    }

    @Test
    public void testLiteralArrayInsertReadableViaObject() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("insert into " + TABLE + "(id, c_int_arr) values (7, array[7, 8, 9])");
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_int_arr from " + TABLE + " where id = ?")) {
            ps.setInt(1, 7);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertNotNull(rs.getObject(1));
                assertFalse(rs.wasNull());
                assertNotNull(rs.getArray(1));
            }
        }
    }

    @Test
    public void testInsertWithSetIntLong() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_int, c_bigint, c_varchar) values (?, ?, ?, ?)")) {
            ps.setInt(1, 1);
            ps.setInt(2, 1);
            ps.setLong(3, 2L);
            ps.setString(4, "abc");
            assertTrue(ps.executeUpdate() >= 0);
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "select c_int, c_bigint, c_varchar from " + TABLE + " where id = ?")) {
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals(2L, rs.getLong(2));
                assertEquals("abc", rs.getString(3));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testSetObjectBasic() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_int, c_bigint, c_varchar) values (?, ?, ?, ?)")) {
            ps.setObject(1, 7);
            ps.setObject(2, 7);
            ps.setObject(3, 8L);
            ps.setObject(4, "obj");
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_varchar from " + TABLE + " where id = ?")) {
            ps.setObject(1, 7);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("obj", rs.getObject(1));
            }
        }
    }

    @Test
    public void testSetNullBasic() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_bigint, c_varchar) values (?, ?, ?)")) {
            ps.setInt(1, 9);
            ps.setNull(2, Types.BIGINT);
            ps.setNull(3, Types.VARCHAR);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "select c_bigint, c_varchar from " + TABLE + " where id = ?")) {
            ps.setInt(1, 9);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertNull(rs.getString(2));
            }
        }
    }

    @Test
    public void testExecute() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_int, c_bigint, c_varchar) values (1, 2, 2, 'x')")) {
            assertFalse(ps.execute());
        }
    }

    @Test
    public void testReusePreparedStatement() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_int, c_bigint, c_varchar) values (?, ?, ?, ?)")) {
            for (int i = 0; i < 3; i++) {
                ps.setInt(1, i);
                ps.setInt(2, i);
                ps.setLong(3, i * 10L);
                ps.setString(4, "v" + i);
                ps.executeUpdate();
            }
        }
        try (PreparedStatement ps = conn.prepareStatement("select count(*) from " + TABLE)) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
            }
        }
    }

    @Test
    public void testUpdateViaPreparedStatement() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_int, c_bigint, c_varchar) values (1, 1, 1, 'a')")) {
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement("update " + TABLE + " set c_varchar = ? where id = ?")) {
            ps.setString(1, "b");
            ps.setInt(2, 1);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_varchar from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("b", rs.getString(1));
            }
        }
    }

    private boolean getBoolean(int id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("select c_bool from " + TABLE + " where id = ?")) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return rs.getBoolean(1);
            }
        }
    }
}
