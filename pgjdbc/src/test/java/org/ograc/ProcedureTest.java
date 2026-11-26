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
import org.postgresql.jdbc.ORConnection;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * test procedure
 *
 * @author zhangting
 * @since  2026-09-17
 */
public class ProcedureTest {
    private static final String PROC_INSERT = "ograc_proc_insert";
    private static final String PROC_INOUT = "ograc_proc_inout";
    private static final String PROC_UPDATE = "ograc_proc_update";
    private static final String PROC_DELETE = "ograc_proc_delete";
    private static final String TABLE = "ograc_proc_t";

    private Connection conn;
    private boolean isProceduresCreated;

    @Before
    public void setUp() throws Exception {
        conn = TestUtil.openDB();
        assertTrue(conn instanceof ORConnection);
        dropProceduresQuietly();
        TestUtil.dropTable(conn, TABLE);
        try (Statement st = conn.createStatement()) {
            st.execute("create table " + TABLE + "(id int primary key, val int, note varchar(64))");
            createProcedures(st);
            isProceduresCreated = true;
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
        if (isProceduresCreated) {
            dropProceduresQuietly();
        }
        TestUtil.closeQuietly(conn);
    }

    private void createProcedures(Statement st) throws SQLException {
        st.execute("create or replace procedure " + PROC_INSERT + "(p_id in int, p_val in int) as "
                + "begin insert into " + TABLE + "(id, val) values (p_id, p_val); end;");
        st.execute("create or replace procedure " + PROC_INOUT + "("
                + "p_in in int, p_out out int, p_inout in out int, p_note out varchar2) as "
                + "begin "
                + "p_out := p_in * 3; "
                + "p_inout := p_inout + 100; "
                + "p_note := 'done-' || p_in; "
                + "insert into " + TABLE + "(id, val, note) values (p_in, p_out, p_note); "
                + "end;");
        st.execute("create or replace procedure " + PROC_UPDATE + "(p_id in int, p_val in int) as "
                + "begin update " + TABLE + " set val = p_val where id = p_id; end;");
        st.execute("create or replace procedure " + PROC_DELETE + "(p_id in int) as "
                + "begin delete from " + TABLE + " where id = p_id; end;");
    }

    private void dropProceduresQuietly() {
        if (conn == null) {
            return;
        }
        try (Statement st = conn.createStatement()) {
            execIgnore(st, "drop procedure if exists " + PROC_DELETE);
            execIgnore(st, "drop procedure if exists " + PROC_UPDATE);
            execIgnore(st, "drop procedure if exists " + PROC_INOUT);
            execIgnore(st, "drop procedure if exists " + PROC_INSERT);
            execIgnore(st, "drop procedure " + PROC_DELETE);
            execIgnore(st, "drop procedure " + PROC_UPDATE);
            execIgnore(st, "drop procedure " + PROC_INOUT);
            execIgnore(st, "drop procedure " + PROC_INSERT);
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
    public void testCallProcedureWithStatement() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("call " + PROC_INSERT + "(1, 10)");
            st.execute("call " + PROC_INSERT + "(2, 20)");
        }
        assertEquals(2, countRows());
        assertEquals(10, getVal(1));
        assertEquals(20, getVal(2));
    }

    @Test
    public void testCallableStatementInParameters() throws SQLException {
        try (CallableStatement cs = conn.prepareCall("call " + PROC_INSERT + "(?, ?)")) {
            cs.setInt(1, 3);
            cs.setInt(2, 30);
            cs.execute();
            cs.setInt(1, 4);
            cs.setInt(2, 40);
            cs.execute();
        }
        assertEquals(2, countRows());
        assertEquals(30, getVal(3));
        assertEquals(40, getVal(4));
    }

    @Test
    public void testCallableStatementInOutParameters() throws SQLException {
        try (CallableStatement cs = conn.prepareCall("call " + PROC_INOUT + "(?, ?, ?, ?)")) {
            cs.setInt(1, 5);
            cs.registerOutParameter(2, Types.INTEGER);
            cs.setInt(3, 7);
            cs.registerOutParameter(3, Types.INTEGER);
            cs.registerOutParameter(4, Types.VARCHAR);
            cs.execute();
            assertEquals(15, cs.getInt(2));
            assertEquals(107, cs.getInt(3));
            assertEquals("done-5", cs.getString(4));
        }
        assertEquals(1, countRows());
        assertEquals(15, getVal(5));
        assertEquals("done-5", getNote(5));
    }

    @Test
    public void testProcedureUpdate() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("call " + PROC_INSERT + "(1, 1)");
        }
        try (CallableStatement cs = conn.prepareCall("call " + PROC_UPDATE + "(?, ?)")) {
            cs.setInt(1, 1);
            cs.setInt(2, 99);
            assertEquals(false, cs.execute());
        }
        assertEquals(99, getVal(1));
    }

    @Test
    public void testProcedureDelete() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("call " + PROC_INSERT + "(1, 1)");
            st.execute("call " + PROC_INSERT + "(2, 2)");
        }
        try (CallableStatement cs = conn.prepareCall("call " + PROC_DELETE + "(?)")) {
            cs.setInt(1, 1);
            cs.execute();
        }
        assertEquals(1, countRows());
        assertEquals(2, getVal(2));
    }

    @Test
    public void testProcedureWithinTransactionCommit() throws SQLException {
        conn.setAutoCommit(false);
        try {
            try (CallableStatement cs = conn.prepareCall("call " + PROC_INSERT + "(?, ?)")) {
                cs.setInt(1, 10);
                cs.setInt(2, 100);
                cs.execute();
            }
            conn.commit();
        } finally {
            conn.setAutoCommit(true);
        }
        assertEquals(1, countRows());
        assertEquals(100, getVal(10));
    }

    @Test
    public void testProcedureWithinTransactionRollback() throws SQLException {
        conn.setAutoCommit(false);
        try {
            try (CallableStatement cs = conn.prepareCall("call " + PROC_INSERT + "(?, ?)")) {
                cs.setInt(1, 11);
                cs.setInt(2, 110);
                cs.execute();
            }
            conn.rollback();
        } finally {
            conn.setAutoCommit(true);
        }
        assertEquals(0, countRows());
    }

    @Test
    public void testRecreateProcedure() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("drop procedure " + PROC_INSERT);
            st.execute("create or replace procedure " + PROC_INSERT + "(p_id in int, p_val in int) as "
                    + "begin insert into " + TABLE + "(id, val) values (p_id, p_val + 1); end;");
            st.execute("call " + PROC_INSERT + "(20, 5)");
        }
        assertEquals(1, countRows());
        assertEquals(6, getVal(20));
    }

    @Test
    public void testMultipleProcedureCallsInOneConnection() throws SQLException {
        try (CallableStatement insert = conn.prepareCall("call " + PROC_INSERT + "(?, ?)");
             CallableStatement update = conn.prepareCall("call " + PROC_UPDATE + "(?, ?)")) {
            insert.setInt(1, 30);
            insert.setInt(2, 300);
            insert.execute();
            update.setInt(1, 30);
            update.setInt(2, 301);
            update.execute();
        }
        assertEquals(1, countRows());
        assertEquals(301, getVal(30));
    }

    private int countRows() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("select count(*) from " + TABLE)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private int getVal(int id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("select val from " + TABLE + " where id = ?")) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return rs.getInt(1);
            }
        }
    }

    private String getNote(int id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("select note from " + TABLE + " where id = ?")) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return rs.getString(1);
            }
        }
    }
}
