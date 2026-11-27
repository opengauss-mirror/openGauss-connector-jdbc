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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * test auto commit
 *
 * @author zhangting
 * @since  2026-08-22
 */
public class AutoCommitTest {
    private static final String TABLE = "ograc_autocommit_t";

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
                if (!conn.getAutoCommit()) {
                    conn.setAutoCommit(true);
                }
                TestUtil.dropTable(conn, TABLE);
            }
        } catch (SQLException ignore) {
            // ignore
        }
        TestUtil.closeQuietly(conn);
    }

    @Test
    public void testDefaultAutoCommitIsTrue() throws SQLException {
        assertTrue(conn.getAutoCommit());
    }

    @Test
    public void testSetAutoCommitToggle() throws SQLException {
        conn.setAutoCommit(false);
        assertFalse(conn.getAutoCommit());
        conn.setAutoCommit(true);
        assertTrue(conn.getAutoCommit());
    }

    @Test
    public void testAutoCommitTrueInsertVisibleImmediately() throws SQLException {
        conn.setAutoCommit(true);
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("insert into " + TABLE + " values(1, 'a')");
        }
        assertEquals(1, countRows());
        assertEquals("a", getC2(1));
    }

    @Test
    public void testAutoCommitFalseRequiresCommit() throws SQLException {
        conn.setAutoCommit(false);
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("insert into " + TABLE + " values(1, 'b')");
        }
        conn.commit();
        assertEquals(1, countRows());
        assertEquals("b", getC2(1));
    }

    @Test
    public void testUncommittedInsertNotVisibleOnOtherConnection() throws Exception {
        conn.setAutoCommit(false);
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("insert into " + TABLE + " values(1, 'hidden')");
        }

        try (Connection other = TestUtil.openDB();
             Statement st = other.createStatement();
             ResultSet rs = st.executeQuery("select count(*) from " + TABLE)) {
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
        }

        conn.commit();
        try (Connection other = TestUtil.openDB();
             Statement st = other.createStatement();
             ResultSet rs = st.executeQuery("select count(*) from " + TABLE)) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
    }

    @Test
    public void testCommitThenNewWorkNeedsAnotherCommit() throws SQLException {
        conn.setAutoCommit(false);
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("insert into " + TABLE + " values(1, 'first')");
        }
        conn.commit();
        assertEquals(1, countRows());

        try (Statement st = conn.createStatement()) {
            st.executeUpdate("insert into " + TABLE + " values(2, 'second')");
        }
        conn.commit();
        assertEquals(2, countRows());
        assertEquals("first", getC2(1));
        assertEquals("second", getC2(2));
    }

    @Test
    public void testRollbackDiscardsUncommittedWork() throws SQLException {
        conn.setAutoCommit(false);
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("insert into " + TABLE + " values(1, 'x')");
        }
        conn.rollback();
        assertEquals(0, countRows());
    }

    private int countRows() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("select count(*) from " + TABLE)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private String getC2(int c1) throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("select c2 from " + TABLE + " where c1 = " + c1)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }
}
