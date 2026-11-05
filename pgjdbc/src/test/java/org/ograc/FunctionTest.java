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
import org.postgresql.jdbc.ORCallableStatement;
import org.postgresql.jdbc.ORConnection;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * test function
 *
 * @author zhangting
 * @since  2026-09-23
 */
public class FunctionTest {
    private static final String FUNC_ADD = "ograc_fn_add";
    private static final String FUNC_MUL = "ograc_fn_mul";
    private static final String FUNC_ECHO = "ograc_fn_echo";
    private static final String FUNC_NULL = "ograc_fn_null";
    private static final String FUNC_CONST = "ograc_fn_const";
    private static final String PROC_ADD = "ograc_fn_add_p";

    private Connection conn;
    private ORConnection orConn;
    private boolean isFunctionsCreated;

    @Before
    public void setUp() throws Exception {
        conn = TestUtil.openDB();
        assertTrue(conn instanceof ORConnection);
        orConn = (ORConnection) conn;
        dropFunctionsQuietly();
        try (Statement st = conn.createStatement()) {
            createFunctions(st);
            isFunctionsCreated = true;
        }
    }

    @After
    public void tearDown() {
        if (isFunctionsCreated) {
            dropFunctionsQuietly();
        }
        TestUtil.closeQuietly(conn);
    }

    private void createFunctions(Statement st) throws SQLException {
        st.execute("create or replace function " + FUNC_ADD + "(p int) return number is "
                + "begin return p + 1; end;");
        st.execute("create or replace function " + FUNC_MUL + "(a int, b int) return number is "
                + "begin return a * b; end;");
        st.execute("create or replace function " + FUNC_ECHO + "(p varchar2) return varchar2 is "
                + "begin return p; end;");
        st.execute("create or replace function " + FUNC_NULL + "() return varchar2 is "
                + "begin return null; end;");
        st.execute("create or replace function " + FUNC_CONST + "() return number is "
                + "begin return 314; end;");
        st.execute("create or replace procedure " + PROC_ADD + "(p in number, r out number) as "
                + "begin r := " + FUNC_ADD + "(p); end;");
    }

    private void dropFunctionsQuietly() {
        if (conn == null) {
            return;
        }
        try (Statement st = conn.createStatement()) {
            execIgnore(st, "drop procedure if exists " + PROC_ADD);
            execIgnore(st, "drop procedure " + PROC_ADD);
            execIgnore(st, "drop function if exists " + FUNC_CONST);
            execIgnore(st, "drop function if exists " + FUNC_NULL);
            execIgnore(st, "drop function if exists " + FUNC_ECHO);
            execIgnore(st, "drop function if exists " + FUNC_MUL);
            execIgnore(st, "drop function if exists " + FUNC_ADD);
            execIgnore(st, "drop function " + FUNC_CONST);
            execIgnore(st, "drop function " + FUNC_NULL);
            execIgnore(st, "drop function " + FUNC_ECHO);
            execIgnore(st, "drop function " + FUNC_MUL);
            execIgnore(st, "drop function " + FUNC_ADD);
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
    public void testPrepareCallReturnsORCallableStatement() throws SQLException {
        try (CallableStatement cs = orConn.prepareCall("{ call " + PROC_ADD + "(?, ?) }")) {
            assertNotNull(cs);
            assertTrue(cs instanceof ORCallableStatement);
        }
    }

    @Test
    public void testFunctionReturnIntViaCallable() throws SQLException {
        try (CallableStatement cs = orConn.prepareCall("{ call " + PROC_ADD + "(?, ?) }")) {
            cs.setInt(1, 41);
            cs.registerOutParameter(2, Types.INTEGER);
            cs.execute();
            assertEquals(42, cs.getInt(2));
            assertEquals(42L, cs.getLong(2));
            assertEquals(42.0d, cs.getDouble(2), 0.0001d);
        }
    }

    @Test
    public void testFunctionReturnBigDecimal() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("select " + FUNC_MUL + "(?, ?) as result")) {
            ps.setInt(1, 6);
            ps.setInt(2, 7);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                BigDecimal value = rs.getBigDecimal(1);
                assertNotNull(value);
                assertEquals(0, new BigDecimal("42").compareTo(value));
            }
        }
    }

    @Test
    public void testFunctionWithTwoParameters() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("select " + FUNC_MUL + "(?, ?) as result")) {
            ps.setInt(1, 8);
            ps.setInt(2, 5);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(40, rs.getInt(1));
                Object obj = rs.getObject(1);
                assertTrue(obj instanceof Number);
                assertEquals(40, ((Number) obj).intValue());
            }
        }
    }

    @Test
    public void testFunctionReturnString() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("select " + FUNC_ECHO + "(?) as result")) {
            ps.setString(1, "ograc-function");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("ograc-function", rs.getString(1));
                assertEquals("ograc-function", String.valueOf(rs.getObject(1)));
            }
        }
    }

    @Test
    public void testFunctionReturnNull() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("select " + FUNC_NULL + "() as result")) {
            assertTrue(rs.next());
            String value = rs.getString(1);
            assertTrue(value == null || value.isEmpty());
            assertTrue(rs.wasNull());
        }
    }

    @Test
    public void testFunctionNoParameters() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("select " + FUNC_CONST + "() as result")) {
            assertTrue(rs.next());
            assertEquals(314, rs.getInt(1));
        }
    }

    @Test
    public void testFunctionViaSelectStatement() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("select " + FUNC_ADD + "(9) as result")) {
            assertTrue(rs.next());
            assertEquals(10, rs.getInt(1));
            assertEquals(10, rs.getInt("result"));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testFunctionViaPreparedSelect() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("select " + FUNC_MUL + "(?, ?) as result")) {
            ps.setInt(1, 3);
            ps.setInt(2, 14);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(42, rs.getInt(1));
            }
        }
    }

    @Test
    public void testFunctionExpressionInSelect() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "select " + FUNC_ADD + "(?) + " + FUNC_CONST + "() as result")) {
            ps.setInt(1, 5);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(320, rs.getInt(1));
            }
        }
    }

    @Test
    public void testFunctionClearParametersAndReExecute() throws SQLException {
        try (CallableStatement cs = orConn.prepareCall("{ call " + PROC_ADD + "(?, ?) }")) {
            cs.setInt(1, 10);
            cs.registerOutParameter(2, Types.INTEGER);
            cs.execute();
            assertEquals(11, cs.getInt(2));

            cs.clearParameters();
            cs.setInt(1, 20);
            cs.registerOutParameter(2, Types.INTEGER);
            cs.execute();
            assertEquals(21, cs.getInt(2));
        }
    }

    @Test
    public void testFunctionGetObjectTypes() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("select " + FUNC_ECHO + "(?) as result")) {
            ps.setString(1, "fn-object");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                Object obj = rs.getObject(1);
                assertNotNull(obj);
                assertEquals("fn-object", obj.toString());
            }
        }
    }

    @Test
    public void testFunctionExecuteReturnsFalse() throws SQLException {
        try (CallableStatement cs = orConn.prepareCall("{ call " + PROC_ADD + "(?, ?) }")) {
            cs.setInt(1, 0);
            cs.registerOutParameter(2, Types.INTEGER);
            assertFalse(cs.execute());
            assertEquals(1, cs.getInt(2));
        }
    }
}
