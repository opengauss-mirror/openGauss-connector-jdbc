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
import org.postgresql.jdbc.ORArray;

import java.sql.Array;
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
import static org.junit.Assert.fail;

/**
 * test array
 *
 * @author zhangting
 * @since  2026-08-22
 */
public class ArrayTest {
    private static final String TABLE = "ograc_array_t";

    private Connection conn;

    @Before
    public void setUp() throws Exception {
        conn = TestUtil.openDB();
        assertTrue("URL must use jdbc:oGRAC://", TestUtil.getUrl().startsWith(TestUtil.URL_PREFIX));
        TestUtil.dropTable(conn, TABLE);
        try (Statement st = conn.createStatement()) {
            st.execute("create table " + TABLE + "("
                    + "id int primary key, "
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
        conn = null;
    }

    @Test
    public void testConnectionUsesOgracUrl() throws Exception {
        assertTrue(conn.getMetaData().getURL().startsWith(TestUtil.URL_PREFIX)
                || TestUtil.getUrl().startsWith(TestUtil.URL_PREFIX));
    }

    @Test
    public void testCreateArrayOfInteger() throws SQLException {
        Array array = conn.createArrayOf("integer", new Integer[]{1, 2, 3});
        assertNotNull(array);
        assertTrue(array instanceof ORArray);
        Object raw = array.getArray();
        assertTrue(raw instanceof Object[]);
        Object[] elems = (Object[]) raw;
        assertEquals(3, elems.length);
        assertEquals(1, elems[0]);
        assertEquals(2, elems[1]);
        assertEquals(3, elems[2]);
    }

    @Test
    public void testCreateArrayOfIntAlias() throws SQLException {
        Array array = conn.createArrayOf("int", new Integer[]{10, 20});
        assertNotNull(array);
        Object[] elems = (Object[]) array.getArray();
        assertEquals(2, elems.length);
        assertEquals(10, elems[0]);
        assertEquals(20, elems[1]);
    }

    @Test
    public void testCreateArrayOfVarchar() throws SQLException {
        Array array = conn.createArrayOf("varchar", new String[]{"a", "b", "ograc"});
        assertNotNull(array);
        Object[] elems = (Object[]) array.getArray();
        assertEquals(3, elems.length);
        assertEquals("a", elems[0]);
        assertEquals("ograc", elems[2]);
    }

    @Test
    public void testCreateArrayOfEmptyElements() throws SQLException {
        Array array = conn.createArrayOf("integer", new Integer[0]);
        assertNotNull(array);
        Object[] elems = (Object[]) array.getArray();
        assertEquals(0, elems.length);
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
    public void testCreateArrayOfRejectsUnknownType() {
        try {
            conn.createArrayOf("not_a_real_type", new Object[]{1});
            fail("expected SQLException for unknown typeName");
        } catch (SQLException expected) {
            assertNotNull(expected.getMessage());
        }
    }

    @Test
    public void testOrArrayUnimplementedApis() throws SQLException {
        Array array = conn.createArrayOf("integer", new Integer[]{1, 2});
        try {
            array.getArray(1L, 1);
            fail("expected getArray(long,int) not implemented");
        } catch (SQLException expected) {
            assertTrue(expected.getMessage().toLowerCase().contains("not implemented"));
        }
        try {
            array.getResultSet();
            fail("expected getResultSet() not implemented");
        } catch (SQLException expected) {
            assertTrue(expected.getMessage().toLowerCase().contains("not implemented"));
        }
        try {
            array.free();
            fail("expected free() not implemented");
        } catch (SQLException expected) {
            assertTrue(expected.getMessage().toLowerCase().contains("not implemented"));
        }
    }

    @Test
    public void testSetArrayIntegerRejected() throws SQLException {
        Array array = conn.createArrayOf("integer", new Integer[]{1, 2, 3});
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_int_arr) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setArray(2, array);
            ps.executeUpdate();
            fail("expected Invalid array format from ORArray bind");
        } catch (SQLException expected) {
            assertTrue(expected.getMessage().toLowerCase().contains("array")
                    || expected.getMessage().toLowerCase().contains("invalid"));
        }
    }

    @Test
    public void testSetArrayVarcharRejected() throws SQLException {
        Array array = conn.createArrayOf("varchar", new String[]{"x", "y"});
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_varchar_arr) values (?, ?)")) {
            ps.setInt(1, 2);
            ps.setArray(2, array);
            ps.executeUpdate();
            fail("expected Invalid array format from ORArray bind");
        } catch (SQLException expected) {
            assertTrue(expected.getMessage().toLowerCase().contains("array")
                    || expected.getMessage().toLowerCase().contains("invalid"));
        }
    }

    @Test
    public void testSetObjectWithArrayRejected() throws SQLException {
        Array array = conn.createArrayOf("integer", new Integer[]{10, 20});
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_int_arr) values (?, ?)")) {
            ps.setInt(1, 3);
            ps.setObject(2, array);
            ps.executeUpdate();
            fail("expected Invalid array format");
        } catch (SQLException expected) {
            assertTrue(expected.getMessage().toLowerCase().contains("array")
                    || expected.getMessage().toLowerCase().contains("invalid"));
        }
    }

    @Test
    public void testSetArrayEmptyRejected() throws SQLException {
        Array array = conn.createArrayOf("integer", new Integer[0]);
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_int_arr) values (?, ?)")) {
            ps.setInt(1, 4);
            ps.setArray(2, array);
            ps.executeUpdate();
            fail("expected Invalid array format");
        } catch (SQLException expected) {
            assertTrue(expected.getMessage().toLowerCase().contains("array")
                    || expected.getMessage().toLowerCase().contains("invalid"));
        }
    }

    @Test
    public void testSetArrayNull() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_int_arr) values (?, ?)")) {
            ps.setInt(1, 10);
            ps.setArray(2, null);
            assertEquals(1, ps.executeUpdate());
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_int_arr from " + TABLE + " where id = ?")) {
            ps.setInt(1, 10);
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
            ps.setInt(1, 11);
            ps.setNull(2, Types.ARRAY);
            assertEquals(1, ps.executeUpdate());
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_int_arr from " + TABLE + " where id = ?")) {
            ps.setInt(1, 11);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
            }
        }
    }

    @Test
    public void testSqlLiteralArrayInsertAndRead() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("insert into " + TABLE + "(id, c_int_arr) values (20, array[7, 8, 9])");
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_int_arr from " + TABLE + " where id = ?")) {
            ps.setInt(1, 20);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                Object obj = rs.getObject(1);
                assertNotNull(obj);
                assertFalse(rs.wasNull());
                Array arr = rs.getArray(1);
                assertNotNull(arr);
                Object raw = arr.getArray();
                assertNotNull(raw);
            }
        }
    }

    @Test
    public void testSqlLiteralVarcharArrayInsertAndRead() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("insert into " + TABLE + "(id, c_varchar_arr) values (21, array['a','b','c'])");
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "select c_varchar_arr from " + TABLE + " where id = ?")) {
            ps.setInt(1, 21);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertNotNull(rs.getObject(1));
                assertNotNull(rs.getArray(1));
            }
        }
    }

    @Test
    public void testBatchSetNullArray() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_int_arr) values (?, ?)")) {
            for (int i = 30; i < 33; i++) {
                ps.setInt(1, i);
                ps.setNull(2, Types.ARRAY);
                ps.addBatch();
            }
            int[] counts = ps.executeBatch();
            assertEquals(3, counts.length);
        }
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("select count(*) from " + TABLE + " where id between 30 and 32")) {
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
        }
    }

    @Test
    public void testBinaryIntegerArrayColumnSetArrayAndNull() throws SQLException {
        final String table = "ograc_binary_integer_array_t";
        try (Statement st = conn.createStatement()) {
            st.execute("drop table if exists " + table);
            st.execute("create table " + table + " (c1 binary_integer[])");
        }
        try {
            Array array = conn.createArrayOf("integer", new Integer[]{1, 2, 3});
            try (PreparedStatement ps = conn.prepareStatement("insert into " + table + " (c1) values (?)")) {
                ps.setArray(1, array);
                ps.executeUpdate();
                fail("expected Invalid array format from ORArray bind");
            } catch (SQLException expected) {
                assertTrue(expected.getMessage().toLowerCase().contains("array")
                        || expected.getMessage().toLowerCase().contains("invalid"));
            }

            try (PreparedStatement ps = conn.prepareStatement("insert into " + table + " (c1) values (?)")) {
                ps.setNull(1, Types.ARRAY);
                assertTrue(ps.executeUpdate() >= 0);
            }
            try (PreparedStatement ps = conn.prepareStatement("select c1 from " + table);
                 ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
            }
        } finally {
            TestUtil.dropTable(conn, table);
        }
    }
}
