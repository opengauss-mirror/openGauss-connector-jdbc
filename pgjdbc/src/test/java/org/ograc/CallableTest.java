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
import org.postgresql.jdbc.ORCallableStatement;
import org.postgresql.jdbc.ORConnection;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * test callable
 *
 * @author zhangting
 * @since  2026-09-12
 */
public class CallableTest {
    private static final String FUNC_ADD = "ograc_or_cs_add";
    private static final String FUNC_ECHO = "ograc_or_cs_echo";
    private static final String PROC_ADD = "ograc_or_cs_add_p";
    private static final String PROC_ECHO = "ograc_or_cs_echo_p";
    private static final String PROC_INOUT = "ograc_or_cs_inout";
    private static final String PROC_NULL = "ograc_or_cs_nullout";
    private static final String TABLE = "ograc_or_cs_t";

    private Connection conn;
    private ORConnection orConn;
    private boolean isRoutinesCreated;

    @Before
    public void setUp() throws Exception {
        conn = TestUtil.openDB();
        assertTrue(conn instanceof ORConnection);
        orConn = (ORConnection) conn;
        dropRoutinesQuietly();
        TestUtil.dropTable(conn, TABLE);
        try (Statement st = conn.createStatement()) {
            st.execute("create table " + TABLE + "(id int primary key, val int)");
            createOracleStyleRoutines(st);
            isRoutinesCreated = true;
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
        if (isRoutinesCreated) {
            dropRoutinesQuietly();
        }
        TestUtil.closeQuietly(conn);
    }

    private void createOracleStyleRoutines(Statement st) throws SQLException {
        st.execute("create or replace function " + FUNC_ADD + "(p int) return number is "
                + "begin return p + 1; end;");
        st.execute("create or replace function " + FUNC_ECHO + "(p varchar2) return varchar2 is "
                + "begin return p; end;");
        st.execute("create or replace procedure " + PROC_ADD + "(p in number, r out number) as "
                + "begin r := " + FUNC_ADD + "(p); end;");
        st.execute("create or replace procedure " + PROC_ECHO + "(p in varchar2, r out varchar2) as "
                + "begin r := " + FUNC_ECHO + "(p); end;");
        st.execute("create or replace procedure " + PROC_INOUT + "("
                + "p_in in int, p_out out int, p_inout in out int) as "
                + "begin "
                + "p_out := p_in * 2; "
                + "p_inout := p_inout + 10; "
                + "insert into " + TABLE + " values (p_in, p_out); "
                + "end;");
        st.execute("create or replace procedure " + PROC_NULL + "(p_out out varchar2) as "
                + "begin p_out := null; end;");
    }

    private void dropRoutinesQuietly() {
        if (conn == null) {
            return;
        }
        try (Statement st = conn.createStatement()) {
            execIgnore(st, "drop procedure if exists " + PROC_NULL);
            execIgnore(st, "drop procedure if exists " + PROC_INOUT);
            execIgnore(st, "drop procedure if exists " + PROC_ECHO);
            execIgnore(st, "drop procedure if exists " + PROC_ADD);
            execIgnore(st, "drop function if exists " + FUNC_ECHO);
            execIgnore(st, "drop function if exists " + FUNC_ADD);
            execIgnore(st, "drop procedure " + PROC_NULL);
            execIgnore(st, "drop procedure " + PROC_INOUT);
            execIgnore(st, "drop procedure " + PROC_ECHO);
            execIgnore(st, "drop procedure " + PROC_ADD);
            execIgnore(st, "drop function " + FUNC_ECHO);
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
    public void testPrepareCall() throws SQLException {
        try (CallableStatement cs = conn.prepareCall("{ call " + PROC_ADD + "(?, ?) }")) {
            assertNotNull(cs);
            cs.setInt(1, 41);
            cs.registerOutParameter(2, Types.INTEGER);
            cs.execute();
            assertEquals(42, cs.getInt(2));
        }
    }

    @Test
    public void testPrepareCallReturnsORCallableStatement() throws SQLException {
        try (CallableStatement cs = orConn.prepareCall("{ call " + PROC_ADD + "(?, ?) }")) {
            assertNotNull(cs);
            assertTrue(cs instanceof ORCallableStatement);
            assertFalse(cs.isWrapperFor(ORCallableStatement.class));
            assertFalse(cs.isWrapperFor(CallableStatement.class));
        }
    }

    @Test
    public void testPrepareCallWithResultSetType() throws SQLException {
        try (CallableStatement cs = orConn.prepareCall("{ call " + PROC_ADD + "(?, ?) }",
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY)) {
            assertTrue(cs instanceof ORCallableStatement);
            cs.setInt(1, 1);
            cs.registerOutParameter(2, Types.INTEGER);
            cs.execute();
            assertEquals(2, cs.getInt(2));
        }
    }

    @Test
    public void testFunctionReturnIntOutParameter() throws SQLException {
        try (CallableStatement cs = orConn.prepareCall("{ call " + PROC_ADD + "(?, ?) }")) {
            assertTrue(cs instanceof ORCallableStatement);
            cs.setInt(1, 41);
            cs.registerOutParameter(2, Types.INTEGER);
            cs.execute();
            assertEquals(42, cs.getInt(2));
            assertEquals(42L, cs.getLong(2));
            assertEquals(42.0d, cs.getDouble(2), 0.0001d);
            Object obj = cs.getObject(2);
            assertNotNull(obj);
            assertTrue(obj instanceof Number);
            assertEquals(42, ((Number) obj).intValue());
        }
    }

    @Test
    public void testFunctionReturnWithScaleRegister() throws SQLException {
        try (CallableStatement cs = orConn.prepareCall("{ call " + PROC_ADD + "(?, ?) }")) {
            cs.setInt(1, 9);
            cs.registerOutParameter(2, Types.NUMERIC, 0);
            cs.execute();
            BigDecimal value = cs.getBigDecimal(2);
            assertNotNull(value);
            assertEquals(0, new BigDecimal("10").compareTo(value));
        }
    }

    @Test
    public void testFunctionReturnString() throws SQLException {
        try (CallableStatement cs = orConn.prepareCall("{ call " + PROC_ECHO + "(?, ?) }")) {
            assertTrue(cs instanceof ORCallableStatement);
            cs.setString(1, "hello-or-cs");
            cs.registerOutParameter(2, Types.VARCHAR);
            cs.execute();
            assertEquals("hello-or-cs", cs.getString(2));
            assertEquals("hello-or-cs", String.valueOf(cs.getObject(2)));
        }
    }

    @Test
    public void testProcedureInOutParameters() throws SQLException {
        try (CallableStatement cs = orConn.prepareCall("call " + PROC_INOUT + "(?, ?, ?)")) {
            assertTrue(cs instanceof ORCallableStatement);
            cs.setInt(1, 5);
            cs.registerOutParameter(2, Types.INTEGER);
            cs.setInt(3, 7);
            cs.registerOutParameter(3, Types.INTEGER);
            cs.execute();
            assertEquals(10, cs.getInt(2));
            assertEquals(17, cs.getInt(3));
            assertFalse(cs.wasNull());
        }
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("select val from " + TABLE + " where id = 5")) {
            assertTrue(rs.next());
            assertEquals(10, rs.getInt(1));
        }
    }

    @Test
    public void testWasNullForNullOutParameter() throws SQLException {
        try (CallableStatement cs = orConn.prepareCall("call " + PROC_NULL + "(?)")) {
            assertTrue(cs instanceof ORCallableStatement);
            cs.registerOutParameter(1, Types.VARCHAR);
            cs.execute();
            String value = cs.getString(1);
            assertTrue(value == null || value.isEmpty());
            assertTrue(cs.wasNull());
        }
    }

    @Test
    public void testWasNullBeforeGetThrows() throws SQLException {
        try (CallableStatement cs = orConn.prepareCall("{ call " + PROC_ADD + "(?, ?) }")) {
            assertTrue(cs instanceof ORCallableStatement);
            ORCallableStatement orCs = (ORCallableStatement) cs;
            cs.setInt(1, 1);
            cs.registerOutParameter(2, Types.INTEGER);
            cs.execute();
            try {
                orCs.wasNull();
                fail("expected SQLException before getXxx");
            } catch (SQLException expected) {
                assertNotNull(expected.getMessage());
            }
            assertEquals(2, cs.getInt(2));
            assertFalse(cs.wasNull());
        }
    }

    @Test
    public void testAddBatchOnProcedureNotSupported() throws SQLException {
        try (CallableStatement cs = orConn.prepareCall("call " + PROC_INOUT + "(?, ?, ?)")) {
            cs.setInt(1, 1);
            cs.registerOutParameter(2, Types.INTEGER);
            cs.setInt(3, 1);
            cs.registerOutParameter(3, Types.INTEGER);
            try {
                cs.addBatch();
                fail("expected SQLException for procedure batch");
            } catch (SQLException expected) {
                assertTrue(expected.getMessage().toLowerCase().contains("batch")
                        || expected.getMessage().toLowerCase().contains("procedure"));
            }
        }
    }

    @Test
    public void testNamedRegisterOutParameterNotImplemented() throws SQLException {
        try (CallableStatement cs = orConn.prepareCall("{ call " + PROC_ADD + "(?, ?) }")) {
            try {
                cs.registerOutParameter("p", Types.INTEGER);
                fail("expected SQLException");
            } catch (SQLException expected) {
                assertNotNull(expected.getMessage());
            }
        }
    }

    @Test
    public void testClearParametersAndReExecute() throws SQLException {
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
    public void testCloseCallableStatement() throws SQLException {
        try (CallableStatement cs = orConn.prepareCall("{ call " + PROC_ADD + "(?, ?) }")) {
            assertTrue(cs instanceof ORCallableStatement);
            assertFalse(cs.isClosed());
            cs.close();
            assertTrue(cs.isClosed());
        }
    }
}
