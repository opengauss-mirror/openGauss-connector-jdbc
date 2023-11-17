package org.postgresql.test.jdbc2;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.test.TestUtil;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Adaptation overload function use case set.
 */
public class ProcOutparamOverrideTest extends BaseTest4 {
    /**
     * The sql of the create proc.
     */
    private static final String PROC_SQL = "CREATE OR REPLACE PROCEDURE test_proc(param_1 out integer,param_2 out " +
            "integer," +
            "param_3 in integer) " +
            "AS " +
            "DECLARE " +
            "BEGIN " +
            "select (1 + param_3) into param_1; " +
            "select (2 + param_3) into param_2; " +
            "END;";

    /**
     * The sql of the create package.
     */
    private static final String PACKAGE_SQL = "CREATE OR REPLACE package pck1 is " +
            "procedure p1(a int,b out int); " +
            "procedure p1(a2 int,b2 out varchar2); " +
            "end pck1;";

    /**
     * The sql of the create package body.
     */
    private static final String PACKAGE_BODY_SQL = "CREATE OR REPLACE package body pck1 is " +
            "procedure p1(a int,b out int) is " +
            "begin " +
            "b = a + 2; " +
            "end; " +
            "procedure p1(a2 int,b2 out varchar2) is " +
            "begin " +
            "b2 = a2 || b2; " +
            "end; " +
            "end pck1;";

    /**
     * The sql of the create proc no out param.
     */
    private static final String PROC_NO_OUTPARAM_SQL = "CREATE OR REPLACE PROCEDURE test_proc_1(id in integer,name in" +
            " varchar) " +
            "AS " +
            "DECLARE " +
            "BEGIN " +
            "insert into test_1 values(id,name); " +
            "END;";

    /**
     * The sql of the create table.
     */
    private static final String CREATE_TABLE_SQL = "CREATE TABLE if not exists test_1(id int,name varchar(20))";

    /**
     * The sql of the set guc param behavior_compat_options
     */
    private static final String TURN_ON_OVERRIDE = "set behavior_compat_options='proc_outparam_override'";

    /**
     * The sql of the set guc param behavior_compat_options
     */
    private static final String TURN_OFF_OVERRIDE = "set behavior_compat_options=''";

    @Before
    public void setUp() throws Exception {
        super.setUp();
        String[] databases = {"ora_compatible_db", "mysql_compatible_db", "td_compatible_db", "pg_compatible_db"};
        for (String database: databases) {
            try {
                TestUtil.execute(String.format("DROP DATABASE %s", database), con);
            } catch (Exception exp) {
            
            }
        }
        TestUtil.execute("drop table if exists test_1", con);
        TestUtil.execute("CREATE DATABASE ora_compatible_db DBCOMPATIBILITY 'A'", con);
        TestUtil.execute("CREATE DATABASE mysql_compatible_db DBCOMPATIBILITY 'B';", con);
        TestUtil.execute("CREATE DATABASE td_compatible_db DBCOMPATIBILITY 'C';", con);
        TestUtil.execute("CREATE DATABASE pg_compatible_db DBCOMPATIBILITY 'PG';", con);
    }

    @After
    public void tearDown() throws SQLException {
        String[] databases = {"ora_compatible_db", "mysql_compatible_db", "td_compatible_db", "pg_compatible_db"};
        for (String database: databases) {
            try {
                TestUtil.execute(String.format("DROP DATABASE %s", database), con);
            } catch (Exception exp) {
            
            }
        };
    }

    /*****************************************************************
     * 描述：测试A兼容模式，重载开启与关闭下，存储过程的调用
     * 被测对象：PgCallableStatement
     * 输入：存储过程名
     * 测试场景：开启重载，调用储存过程；关闭重载，调用存储过程
     * 期望输出：正确返回结果
     ******************************************************************/
    @Test
    public void testOraCompatible() throws Exception {
        Properties props = new Properties();
        props.setProperty("PGDBNAME", "ora_compatible_db");
        con = TestUtil.openDB(props);
        verifyOutparamOverride(con);
        verifyPackageReloadProc(con);
        verifyPrepareStatement(con);
    }

    /*****************************************************************
     * 描述：测试PostgreSQL兼容模式，重载开启与关闭下，存储过程的调用
     * 被测对象：PgCallableStatement
     * 输入：存储过程名
     * 测试场景：开启重载，调用储存过程；关闭重载，调用存储过程
     * 期望输出：正确返回结果
     ******************************************************************/
    @Test
    public void testPgCompatible() throws Exception {
        Properties props = new Properties();
        props.setProperty("PGDBNAME", "pg_compatible_db");
        con = TestUtil.openDB(props);
        verifyOutparamOverride(con);
    }

    /**
     * Verify the result of reload open and close the stored procedure call.
     *
     * @param conn Database connection of different compatibility modes.
     * @throws Exception if a JDBC or database problem occurs.
     */
    private void verifyOutparamOverride(Connection conn) throws Exception {
        Statement stmt = null;
        CallableStatement cmt = null;
        String callProc = "{call test_proc(?,?,?)}";
        try {
            stmt = conn.createStatement();
            stmt.execute(TURN_ON_OVERRIDE);
            stmt.execute(PROC_SQL);
            cmt = conn.prepareCall(callProc);
            cmt.registerOutParameter(1, Types.INTEGER);
            cmt.registerOutParameter(2, Types.INTEGER);
            cmt.setInt(3, 1);
            cmt.execute();
            assertEquals(2, cmt.getInt(1));
            assertEquals(3, cmt.getInt(2));

            stmt.execute(TURN_OFF_OVERRIDE);
            cmt = conn.prepareCall(callProc);
            cmt.registerOutParameter(1, Types.INTEGER);
            cmt.registerOutParameter(2, Types.INTEGER);
            cmt.setInt(3, 6);
            cmt.execute();
            assertEquals(7, cmt.getInt(1));
            assertEquals(8, cmt.getInt(2));
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /**
     * package contains a stored procedure with the same name.
     *
     * @param conn Database connection of different compatibility modes.
     * @throws Exception if a JDBC or database problem occurs.
     */
    private void verifyPackageReloadProc(Connection conn) throws Exception {
        Statement stmt = null;
        CallableStatement cmt = null;
        String callProc = "{call pck1.p1(?,?)}";
        try {
            stmt = conn.createStatement();
            stmt.execute(TURN_ON_OVERRIDE);
            stmt.execute(PACKAGE_SQL);
            stmt.execute(PACKAGE_BODY_SQL);
            cmt = conn.prepareCall(callProc);
            cmt.setInt(1, 1);
            cmt.registerOutParameter(2, Types.INTEGER);
            cmt.execute();
            assertEquals(3, cmt.getInt(2));
            cmt = conn.prepareCall(callProc);
            cmt.setInt(1, 2);
            cmt.registerOutParameter(2, Types.VARCHAR);
            cmt.execute();
            assertEquals("2", cmt.getString(2));

            stmt.execute(TURN_OFF_OVERRIDE);
;
            cmt = conn.prepareCall(callProc);
            cmt.setInt(1, 4);
            cmt.registerOutParameter(2, Types.VARCHAR);
            cmt.execute();
            assertEquals("4", cmt.getString(2));
    
            try {
                cmt = conn.prepareCall(callProc);
                cmt.setInt(1, 6);
                cmt.registerOutParameter(2, Types.INTEGER);
                cmt.execute();
                assertEquals(8, cmt.getInt(2));
                fail("can't run here!");
            } catch (Exception exp) {
            }
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /**
     * PrepareStatement call proc.
     *
     * @param conn Database connection of different compatibility modes.
     * @throws Exception if a JDBC or database problem occurs.
     */
    private void verifyPrepareStatement(Connection conn) throws Exception {
        Statement stmt = null;
        PreparedStatement pstm = null;
        try {
            stmt = conn.createStatement();
            stmt.execute(CREATE_TABLE_SQL);
            stmt.execute(PROC_NO_OUTPARAM_SQL);
            stmt.execute(TURN_ON_OVERRIDE);
            pstm = conn.prepareStatement("select * from test_proc_1(?,?)");
            pstm.setInt(1, 1);
            pstm.setString(2, "Tom");
            pstm.execute();

            stmt.execute(TURN_OFF_OVERRIDE);
            pstm = conn.prepareStatement("select * from test_proc_1(?,?)");
            pstm.setInt(1, 2);
            pstm.setString(2, "Jone");
            pstm.execute();

        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(pstm);
        }
    }

}