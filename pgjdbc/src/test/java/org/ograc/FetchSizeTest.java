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
import org.postgresql.jdbc.ORConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * test fetchSize
 *
 * @author zhangting
 * @since  2026-09-23
 */
public class FetchSizeTest {
    private static final String TABLE = "ograc_fetchsize_t";
    private static final int ROW_COUNT = 30;

    private Connection conn;

    @Before
    public void setUp() throws Exception {
        conn = TestUtil.openDB();
        TestUtil.dropTable(conn, TABLE);
        try (Statement st = conn.createStatement()) {
            st.execute("create table " + TABLE + "(id int primary key, val int)");
        }
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, val) values (?, ?)")) {
            for (int i = 1; i <= ROW_COUNT; i++) {
                ps.setInt(1, i);
                ps.setInt(2, i * 10);
                ps.addBatch();
            }
            ps.executeBatch();
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
    public void testStatementSetAndGetFetchSize() throws SQLException {
        try (Statement st = conn.createStatement()) {
            assertEquals(0, st.getFetchSize());
            st.setFetchSize(5);
            assertEquals(5, st.getFetchSize());
            st.setFetchSize(0);
            assertEquals(0, st.getFetchSize());
        }
    }

    @Test
    public void testNegativeFetchSizeThrows() throws SQLException {
        try (Statement st = conn.createStatement()) {
            try {
                st.setFetchSize(-1);
                fail("expected SQLException for negative fetchSize");
            } catch (SQLException expected) {
                assertTrue(expected.getMessage().toLowerCase().contains("fetch size"));
            }
        }
    }

    @Test
    public void testConnectionFetchSizeFromUrl() throws Exception {
        try (Connection c = TestUtil.openDB("fetchsize=10")) {
            assertTrue(c instanceof ORConnection);
            assertEquals(10, ((ORConnection) c).getFetchSize());
            try (Statement st = c.createStatement()) {
                assertEquals(10, st.getFetchSize());
            }
            try (PreparedStatement ps = c.prepareStatement("select 1")) {
                assertEquals(10, ps.getFetchSize());
            }
        }
    }

    @Test
    public void testORConnectionSetFetchSizeAppliesToNewStatement() throws SQLException {
        assertTrue(conn instanceof ORConnection);
        ORConnection orConn = (ORConnection) conn;
        int original = orConn.getFetchSize();
        orConn.setFetchSize(8);
        try (Statement st = orConn.createStatement()) {
            assertEquals(8, st.getFetchSize());
        } finally {
            orConn.setFetchSize(original);
        }
    }

    @Test
    public void testFetchAllRowsWithSmallFetchSizeOnStatement() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.setFetchSize(5);
            try (ResultSet rs = st.executeQuery("select id, val from " + TABLE + " order by id")) {
                int count = 0;
                int sum = 0;
                while (rs.next()) {
                    count++;
                    assertEquals(count, rs.getInt(1));
                    assertEquals(count * 10, rs.getInt(2));
                    sum += rs.getInt(1);
                }
                assertEquals(ROW_COUNT, count);
                assertEquals(ROW_COUNT * (ROW_COUNT + 1) / 2, sum);
            }
        }
    }

    @Test
    public void testFetchAllRowsWithSmallFetchSizeOnPreparedStatement() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "select id, val from " + TABLE + " where id >= ? order by id")) {
            ps.setFetchSize(7);
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                int count = 0;
                while (rs.next()) {
                    count++;
                }
                assertEquals(ROW_COUNT, count);
            }
        }
    }

    @Test
    public void testFetchSizeZeroReadsAllRows() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.setFetchSize(0);
            try (ResultSet rs = st.executeQuery("select count(*) from " + TABLE)) {
                assertTrue(rs.next());
                assertEquals(ROW_COUNT, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testFetchSizeLargerThanRowCount() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.setFetchSize(ROW_COUNT + 100);
            try (ResultSet rs = st.executeQuery("select id from " + TABLE + " order by id")) {
                int lastId = 0;
                int count = 0;
                while (rs.next()) {
                    count++;
                    int id = rs.getInt(1);
                    assertTrue(id > lastId);
                    lastId = id;
                }
                assertEquals(ROW_COUNT, count);
                assertEquals(ROW_COUNT, lastId);
            }
        }
    }

    @Test
    public void testFetchSizeOneIteratesRowByRow() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.setFetchSize(1);
            try (ResultSet rs = st.executeQuery("select id from " + TABLE + " order by id")) {
                for (int i = 1; i <= ROW_COUNT; i++) {
                    assertTrue("missing row " + i, rs.next());
                    assertEquals(i, rs.getInt(1));
                }
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testFetchSizeWithFilteredQuery() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "select id, val from " + TABLE + " where id between ? and ? order by id")) {
            ps.setFetchSize(3);
            ps.setInt(1, 10);
            ps.setInt(2, 20);
            try (ResultSet rs = ps.executeQuery()) {
                int count = 0;
                int expectedId = 10;
                while (rs.next()) {
                    assertEquals(expectedId, rs.getInt(1));
                    assertEquals(expectedId * 10, rs.getInt(2));
                    expectedId++;
                    count++;
                }
                assertEquals(11, count);
            }
        }
    }

    @Test
    public void testStatementFetchSizeOverridesConnectionDefault() throws Exception {
        try (Connection c = TestUtil.openDB("fetchsize=20")) {
            try (Statement st = c.createStatement()) {
                assertEquals(20, st.getFetchSize());
                st.setFetchSize(4);
                assertEquals(4, st.getFetchSize());
                try (ResultSet rs = st.executeQuery("select id from " + TABLE + " order by id")) {
                    int count = 0;
                    while (rs.next()) {
                        count++;
                    }
                    assertEquals(ROW_COUNT, count);
                }
            }
        }
    }
}
