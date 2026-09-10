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
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * test batch execute
 *
 * @author zhangting
 * @since  2026-08-22
 */
public class BatchTest {
    private static final String TABLE = "ograc_addbatch_t";

    private Connection conn;

    @Before
    public void setUp() throws Exception {
        conn = TestUtil.openDB();
        TestUtil.dropTable(conn, TABLE);
        try (Statement st = conn.createStatement()) {
            st.execute("create table " + TABLE + "(c1 int, c2 varchar(64))");
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
    public void testStatementAddBatchInsert() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.addBatch("insert into " + TABLE + " values(1, 'a')");
            st.addBatch("insert into " + TABLE + " values(2, 'b')");
            st.addBatch("insert into " + TABLE + " values(3, 'c')");
            int[] counts = st.executeBatch();
            assertEquals(3, counts.length);
        }
        assertEquals(3, countRows());
        assertEquals("a", getC2(1));
        assertEquals("b", getC2(2));
        assertEquals("c", getC2(3));
    }

    @Test
    public void testStatementAddBatchMixedDml() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("insert into " + TABLE + " values(1, 'old')");
            st.addBatch("insert into " + TABLE + " values(2, 'new')");
            st.addBatch("update " + TABLE + " set c2 = 'upd' where c1 = 1");
            st.addBatch("delete from " + TABLE + " where c1 = 2");
            int[] counts = st.executeBatch();
            assertEquals(3, counts.length);
        }
        assertEquals(1, countRows());
        assertEquals("upd", getC2(1));
    }

    @Test
    public void testStatementClearBatch() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.addBatch("insert into " + TABLE + " values(1, 'x')");
            st.addBatch("insert into " + TABLE + " values(2, 'y')");
            st.clearBatch();
            st.addBatch("insert into " + TABLE + " values(3, 'z')");
            int[] counts = st.executeBatch();
            assertEquals(1, counts.length);
        }
        assertEquals(1, countRows());
        assertEquals("z", getC2(3));
    }

    @Test
    public void testStatementEmptyBatch() throws SQLException {
        try (Statement st = conn.createStatement()) {
            int[] counts = st.executeBatch();
            assertEquals(0, counts.length);
        }
        assertEquals(0, countRows());
    }

    @Test
    public void testPreparedStatementAddBatchInsert() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + " values(?, ?)")) {
            for (int i = 1; i <= 5; i++) {
                ps.setInt(1, i);
                ps.setString(2, "v" + i);
                ps.addBatch();
            }
            int[] counts = ps.executeBatch();
            assertEquals(5, counts.length);
        }
        assertEquals(5, countRows());
        for (int i = 1; i <= 5; i++) {
            assertEquals("v" + i, getC2(i));
        }
    }

    @Test
    public void testPreparedStatementAddBatchUpdate() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("insert into " + TABLE + " values(1, 'a')");
            st.executeUpdate("insert into " + TABLE + " values(2, 'b')");
            st.executeUpdate("insert into " + TABLE + " values(3, 'c')");
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "update " + TABLE + " set c2 = ? where c1 = ?")) {
            ps.setString(1, "u1");
            ps.setInt(2, 1);
            ps.addBatch();
            ps.setString(1, "u3");
            ps.setInt(2, 3);
            ps.addBatch();
            int[] counts = ps.executeBatch();
            assertEquals(2, counts.length);
        }

        assertEquals("u1", getC2(1));
        assertEquals("b", getC2(2));
        assertEquals("u3", getC2(3));
    }

    @Test
    public void testPreparedStatementClearBatch() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + " values(?, ?)")) {
            ps.setInt(1, 1);
            ps.setString(2, "skip");
            ps.addBatch();
            ps.clearBatch();
            ps.setInt(1, 2);
            ps.setString(2, "keep");
            ps.addBatch();
            int[] counts = ps.executeBatch();
            assertEquals(1, counts.length);
        }
        assertEquals(1, countRows());
        assertEquals("keep", getC2(2));
    }

    @Test
    public void testPreparedStatementReuseAfterExecuteBatch() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + " values(?, ?)")) {
            ps.setInt(1, 1);
            ps.setString(2, "first");
            ps.addBatch();
            ps.executeBatch();

            ps.setInt(1, 2);
            ps.setString(2, "second");
            ps.addBatch();
            int[] counts = ps.executeBatch();
            assertEquals(1, counts.length);
        }
        assertEquals(2, countRows());
        assertEquals("first", getC2(1));
        assertEquals("second", getC2(2));
    }

    @Test
    public void testStatementAndPreparedStatementBatchTogether() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.addBatch("insert into " + TABLE + " values(1, 's1')");
            st.addBatch("insert into " + TABLE + " values(2, 's2')");
            assertEquals(2, st.executeBatch().length);
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + " values(?, ?)")) {
            ps.setInt(1, 3);
            ps.setString(2, "p1");
            ps.addBatch();
            ps.setInt(1, 4);
            ps.setString(2, "p2");
            ps.addBatch();
            assertEquals(2, ps.executeBatch().length);
        }

        assertEquals(4, countRows());
        assertEquals(Arrays.asList("s1", "s2", "p1", "p2"),
                Arrays.asList(getC2(1), getC2(2), getC2(3), getC2(4)));
    }

    private int countRows() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("select count(*) from " + TABLE)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private String getC2(int c1) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "select c2 from " + TABLE + " where c1 = ?")) {
            ps.setInt(1, c1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return rs.getString(1);
            }
        }
    }
}
