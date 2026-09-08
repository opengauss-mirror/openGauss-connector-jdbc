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
 * test bool type
 *
 * @author zhangting
 * @since  2026-09-05
 */
public class BoolTest {
    private static final String TABLE = "ograc_bit_t";

    private Connection conn;

    @Before
    public void setUp() throws Exception {
        conn = TestUtil.openDB();
        TestUtil.dropTable(conn, TABLE);
        try (Statement st = conn.createStatement()) {
            st.execute("create table " + TABLE + "("
                    + "id int primary key, "
                    + "c_bit1 boolean, "
                    + "c_bit2 boolean)");
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
    public void testBit1WithBoolean() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_bit1) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setBoolean(2, true);
            ps.executeUpdate();
            ps.setInt(1, 2);
            ps.setBoolean(2, false);
            ps.executeUpdate();
        }
        assertTrue(getBit1AsBoolean(1));
        assertFalse(getBit1AsBoolean(2));
    }

    @Test
    public void testBit1WithStringAndObject() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_bit1) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setString(2, "1");
            ps.executeUpdate();
            ps.setInt(1, 2);
            ps.setObject(2, false, Types.BIT);
            ps.executeUpdate();
        }
        assertTrue(getBit1AsBoolean(1));
        assertFalse(getBit1AsBoolean(2));
    }

    @Test
    public void testBitViaTypesBoolean() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_bit1) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setObject(2, true, Types.BOOLEAN);
            assertEquals(1, ps.executeUpdate());
        }
        assertTrue(getBit1AsBoolean(1));
    }

    @Test
    public void testBitViaTypesBit() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_bit1) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setObject(2, true, Types.BIT);
            assertEquals(1, ps.executeUpdate());
        }
        assertTrue(getBit1AsBoolean(1));
    }

    @Test
    public void testBothBooleanColumnsTogether() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into "
                + TABLE + "(id, c_bit1, c_bit2) values (?,?,?)")) {
            ps.setInt(1, 1);
            ps.setBoolean(2, true);
            ps.setObject(3, false, Types.BIT);
            assertEquals(1, ps.executeUpdate());
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_bit1, c_bit2 from " + TABLE + " where id = 1");
             ResultSet rs = ps.executeQuery()) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
            assertFalse(rs.getBoolean(2));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testBitNull() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into "
                + TABLE + "(id, c_bit1, c_bit2) values (?,?,?)")) {
            ps.setInt(1, 1);
            ps.setNull(2, Types.BIT);
            ps.setNull(3, Types.BOOLEAN);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_bit1, c_bit2 from " + TABLE + " where id = 1");
             ResultSet rs = ps.executeQuery()) {
            assertTrue(rs.next());
            assertNull(rs.getObject(1));
            assertTrue(rs.wasNull());
            assertNull(rs.getObject(2));
            assertTrue(rs.wasNull());
        }
    }

    @Test
    public void testBitGetObject() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into "
                + TABLE + "(id, c_bit1, c_bit2) values (?,?,?)")) {
            ps.setInt(1, 1);
            ps.setBoolean(2, true);
            ps.setBoolean(3, false);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_bit1, c_bit2 from " + TABLE + " where id = 1");
             ResultSet rs = ps.executeQuery()) {
            assertTrue(rs.next());
            Object o1 = rs.getObject(1);
            Object o2 = rs.getObject(2);
            assertNotNull(o1);
            assertNotNull(o2);
            assertTrue(o1 instanceof Boolean);
            assertTrue(o2 instanceof Boolean);
            assertEquals(Boolean.TRUE, o1);
            assertEquals(Boolean.FALSE, o2);
        }
    }

    @Test
    public void testUpdateBit() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_bit1) values (1, ?)")) {
            ps.setBoolean(1, false);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement("update " + TABLE + " set c_bit1 = ? where id = 1")) {
            ps.setBoolean(1, true);
            assertEquals(1, ps.executeUpdate());
        }
        assertTrue(getBit1AsBoolean(1));
    }

    @Test
    public void testBitBatchInsert() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_bit1) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setBoolean(2, true);
            ps.addBatch();
            ps.setInt(1, 2);
            ps.setBoolean(2, false);
            ps.addBatch();
            assertEquals(2, ps.executeBatch().length);
        }
        assertTrue(getBit1AsBoolean(1));
        assertFalse(getBit1AsBoolean(2));
    }

    @Test
    public void testBitCompareInWhere() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_bit1) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setBoolean(2, true);
            ps.executeUpdate();
            ps.setInt(1, 2);
            ps.setBoolean(2, false);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement("select id from "
                + TABLE + " where c_bit1 = ? order by id")) {
            ps.setBoolean(1, true);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    private boolean getBit1AsBoolean(int id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("select c_bit1 from " + TABLE + " where id = ?")) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return rs.getBoolean(1);
            }
        }
    }
}
