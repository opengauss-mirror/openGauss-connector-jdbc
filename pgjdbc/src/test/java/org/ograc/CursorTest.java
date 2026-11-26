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
import org.postgresql.jdbc.ORCursorResultSet;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * test cursor
 *
 * @author zhangting
 * @since  2026-09-15
 */
public class CursorTest {
    private static final String TABLE = "ograc_curfetch_t";
    private static final String PROC_OPEN = "ograc_curfetch_open";
    private static final String PROC_LIMIT = "ograc_curfetch_limit";
    private static final String PROC_EMPTY = "ograc_curfetch_empty";
    private static final int ROW_COUNT = 25;

    private Connection conn;
    private boolean isProceduresCreated;

    @Before
    public void setUp() throws Exception {
        conn = TestUtil.openDB();
        dropProceduresQuietly();
        TestUtil.dropTable(conn, TABLE);
        try (Statement st = conn.createStatement()) {
            st.execute("create table " + TABLE + "(id int primary key, val int)");
            createProcedures(st);
            isProceduresCreated = true;
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
        if (isProceduresCreated) {
            dropProceduresQuietly();
        }
        TestUtil.closeQuietly(conn);
    }

    private void createProcedures(Statement st) throws SQLException {
        st.execute("create or replace procedure " + PROC_OPEN + "(p_out out sys_refcursor) as "
                + "begin open p_out for select id, val from " + TABLE + " order by id; end;");
        st.execute("create or replace procedure " + PROC_LIMIT + "(p_max in int, p_out out sys_refcursor) as "
                + "begin open p_out for select id, val from " + TABLE
                + " where id <= p_max order by id; end;");
        st.execute("create or replace procedure " + PROC_EMPTY + "(p_out out sys_refcursor) as "
                + "begin open p_out for select id, val from " + TABLE + " where id < 0 order by id; end;");
    }

    private void dropProceduresQuietly() {
        if (conn == null) {
            return;
        }
        try (Statement st = conn.createStatement()) {
            execIgnore(st, "drop procedure if exists " + PROC_EMPTY);
            execIgnore(st, "drop procedure if exists " + PROC_LIMIT);
            execIgnore(st, "drop procedure if exists " + PROC_OPEN);
            execIgnore(st, "drop procedure " + PROC_EMPTY);
            execIgnore(st, "drop procedure " + PROC_LIMIT);
            execIgnore(st, "drop procedure " + PROC_OPEN);
        } catch (SQLException ignore) {
            // ignore
        }
    }

    private static void execIgnore(Statement st, String sql) {
        try {
            st.execute(sql);
        } catch (SQLException ignore) {
            // ignore
        }
    }

    @Test
    public void testRefCursorIsORCursorResultSet() throws SQLException {
        try (CallableStatement cs = conn.prepareCall("call " + PROC_OPEN + "(?)")) {
            cs.registerOutParameter(1, Types.REF_CURSOR);
            cs.execute();
            Object obj = cs.getObject(1);
            assertNotNull(obj);
            assertTrue(obj instanceof ResultSet);
            assertTrue(obj instanceof ORCursorResultSet);
            try (ResultSet rs = (ResultSet) obj) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
    }

    @Test
    public void testCursorFetchAllRows() throws SQLException {
        List<Integer> ids = fetchIdsFromOpenCursor(0);
        assertEquals(ROW_COUNT, ids.size());
        for (int i = 1; i <= ROW_COUNT; i++) {
            assertEquals(i, (int) ids.get(i - 1));
        }
    }

    @Test
    public void testCursorFetchWithStatementFetchSize() throws SQLException {
        try (CallableStatement cs = conn.prepareCall("call " + PROC_OPEN + "(?)")) {
            cs.setFetchSize(5);
            assertEquals(5, cs.getFetchSize());
            cs.registerOutParameter(1, Types.REF_CURSOR);
            cs.execute();
            Object obj = cs.getObject(1);
            assertTrue(obj instanceof ResultSet);
            try (ResultSet rs = (ResultSet) obj) {
                assertTrue(rs instanceof ORCursorResultSet);
                int count = 0;
                int expectedId = 1;
                while (rs.next()) {
                    assertEquals(expectedId, rs.getInt(1));
                    assertEquals(expectedId * 10, rs.getInt(2));
                    expectedId++;
                    count++;
                }
                assertEquals(ROW_COUNT, count);
            }
        }
    }

    @Test
    public void testCursorFetchWithConnectionFetchSize() throws Exception {
        try (Connection c = TestUtil.openDB("fetchsize=8")) {
            try (CallableStatement cs = c.prepareCall("call " + PROC_OPEN + "(?)")) {
                assertEquals(8, cs.getFetchSize());
                cs.registerOutParameter(1, Types.REF_CURSOR);
                cs.execute();
                Object obj = cs.getObject(1);
                assertTrue(obj instanceof ResultSet);
                try (ResultSet rs = (ResultSet) obj) {
                    int count = 0;
                    while (rs.next()) {
                        count++;
                    }
                    assertEquals(ROW_COUNT, count);
                }
            }
        }
    }

    @Test
    public void testCursorFetchLimitedRows() throws SQLException {
        try (CallableStatement cs = conn.prepareCall("call " + PROC_LIMIT + "(?, ?)")) {
            cs.setInt(1, 10);
            cs.setFetchSize(3);
            cs.registerOutParameter(2, Types.REF_CURSOR);
            cs.execute();
            Object obj = cs.getObject(2);
            assertTrue(obj instanceof ResultSet);
            try (ResultSet rs = (ResultSet) obj) {
                int count = 0;
                while (rs.next()) {
                    count++;
                    int val = rs.getInt(2);
                    assertTrue("val=" + val, val <= 100);
                    assertEquals(0, val % 10);
                }
                assertEquals(10, count);
            }
        }
    }

    @Test
    public void testEmptyRefCursor() throws SQLException {
        try (CallableStatement cs = conn.prepareCall("call " + PROC_EMPTY + "(?)")) {
            cs.registerOutParameter(1, Types.REF_CURSOR);
            cs.execute();
            Object obj = cs.getObject(1);
            assertTrue(obj instanceof ResultSet);
            try (ResultSet rs = (ResultSet) obj) {
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testCursorFetchMetaData() throws SQLException {
        try (CallableStatement cs = conn.prepareCall("call " + PROC_OPEN + "(?)")) {
            cs.registerOutParameter(1, Types.REF_CURSOR);
            cs.execute();
            Object obj = cs.getObject(1);
            assertTrue(obj instanceof ResultSet);
            try (ResultSet rs = (ResultSet) obj) {
                ResultSetMetaData md = rs.getMetaData();
                assertNotNull(md);
                assertEquals(2, md.getColumnCount());
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals(10, rs.getInt(2));
            }
        }
    }

    @Test
    public void testCursorFetchRowByRowWithFetchSizeOne() throws SQLException {
        try (CallableStatement cs = conn.prepareCall("call " + PROC_OPEN + "(?)")) {
            cs.setFetchSize(1);
            cs.registerOutParameter(1, Types.REF_CURSOR);
            cs.execute();
            Object obj = cs.getObject(1);
            assertTrue(obj instanceof ResultSet);
            try (ResultSet rs = (ResultSet) obj) {
                for (int i = 1; i <= ROW_COUNT; i++) {
                    assertTrue("missing row " + i, rs.next());
                    assertEquals(i, rs.getInt(1));
                    assertEquals(i * 10, rs.getInt(2));
                }
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testCursorFetchSumValues() throws SQLException {
        try (CallableStatement cs = conn.prepareCall("call " + PROC_OPEN + "(?)")) {
            cs.setFetchSize(7);
            cs.registerOutParameter(1, Types.REF_CURSOR);
            cs.execute();
            int sum = 0;
            Object obj = cs.getObject(1);
            assertTrue(obj instanceof ResultSet);
            try (ResultSet rs = (ResultSet) obj) {
                while (rs.next()) {
                    sum += rs.getInt(2);
                }
            }
            assertEquals(ROW_COUNT * (ROW_COUNT + 1) / 2 * 10, sum);
        }
    }

    private List<Integer> fetchIdsFromOpenCursor(int fetchSize) throws SQLException {
        List<Integer> ids = new ArrayList<>();
        try (CallableStatement cs = conn.prepareCall("call " + PROC_OPEN + "(?)")) {
            if (fetchSize > 0) {
                cs.setFetchSize(fetchSize);
            }
            cs.registerOutParameter(1, Types.REF_CURSOR);
            cs.execute();
            Object obj = cs.getObject(1);
            assertTrue(obj instanceof ResultSet);
            try (ResultSet rs = (ResultSet) obj) {
                while (rs.next()) {
                    ids.add(rs.getInt(1));
                }
            }
        }
        return ids;
    }
}
