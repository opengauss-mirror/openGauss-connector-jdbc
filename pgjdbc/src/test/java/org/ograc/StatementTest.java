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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * test Statement
 *
 * @author zhangting
 * @since  2026-09-15
 */
public class StatementTest {
    private static final String TABLE = "ograc_stmt_t";

    private Connection conn;
    private Statement stmt;

    @Before
    public void setUp() throws Exception {
        conn = TestUtil.openDB();
        stmt = conn.createStatement();
        TestUtil.dropTable(conn, TABLE);
        stmt.execute("create table " + TABLE + "(c1 int, c2 bigint)");
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
        TestUtil.closeQuietly(stmt);
        TestUtil.closeQuietly(conn);
    }

    @Test
    public void testExecuteUpdateInsert() throws SQLException {
        int updated = stmt.executeUpdate("insert into " + TABLE + " values(1, 2)");
        assertTrue(updated >= 0);
    }

    @Test
    public void testExecuteQuery() throws SQLException {
        stmt.executeUpdate("insert into " + TABLE + " values(10, 20)");
        try (ResultSet rs = stmt.executeQuery("select c1, c2 from " + TABLE + " where c1 = 10")) {
            assertTrue(rs.next());
            assertEquals(10, rs.getInt(1));
            assertEquals(20L, rs.getLong(2));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testExecuteReturnsFalseForUpdate() throws SQLException {
        boolean hasResultSet = stmt.execute("insert into " + TABLE + " values(3, 4)");
        assertFalse(hasResultSet);
    }

    @Test
    public void testExecuteReturnsTrueForQuery() throws SQLException {
        stmt.executeUpdate("insert into " + TABLE + " values(5, 6)");
        boolean hasResultSet = stmt.execute("select * from " + TABLE);
        assertTrue(hasResultSet);
        try (ResultSet rs = stmt.getResultSet()) {
            assertNotNull(rs);
            assertTrue(rs.next());
        }
    }

    @Test
    public void testMultipleInsertsAndCount() throws SQLException {
        for (int i = 0; i < 5; i++) {
            stmt.executeUpdate("insert into " + TABLE + " values(" + i + ", " + (i * 10) + ")");
        }
        try (ResultSet rs = stmt.executeQuery("select count(*) from " + TABLE)) {
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));
        }
    }

    @Test
    public void testDropAndRecreate() throws SQLException {
        TestUtil.dropTable(conn, TABLE);
        stmt.execute("create table " + TABLE + "(id int)");
        stmt.executeUpdate("insert into " + TABLE + " values(1)");
        try (ResultSet rs = stmt.executeQuery("select id from " + TABLE)) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
    }

    @Test
    public void testSetFetchSize() throws SQLException {
        stmt.setFetchSize(10);
        assertEquals(10, stmt.getFetchSize());
    }

    @Test
    public void testUpdateExistingRow() throws SQLException {
        stmt.executeUpdate("insert into " + TABLE + " values(1, 100)");
        stmt.executeUpdate("update " + TABLE + " set c2 = 200 where c1 = 1");
        try (ResultSet rs = stmt.executeQuery("select c2 from " + TABLE + " where c1 = 1")) {
            assertTrue(rs.next());
            assertEquals(200L, rs.getLong(1));
        }
    }

    @Test
    public void testDeleteRow() throws SQLException {
        stmt.executeUpdate("insert into " + TABLE + " values(1, 1)");
        stmt.executeUpdate("insert into " + TABLE + " values(2, 2)");
        stmt.executeUpdate("delete from " + TABLE + " where c1 = 1");
        try (ResultSet rs = stmt.executeQuery("select count(*) from " + TABLE)) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
    }
}
