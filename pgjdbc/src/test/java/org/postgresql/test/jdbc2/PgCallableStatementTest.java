package org.postgresql.test.jdbc2;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.test.TestUtil;
import org.postgresql.util.PGobject;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class PgCallableStatementTest extends BaseTest4 {

    @Before
    public void setUp() throws Exception {
        super.setUp();
        TestUtil.execute("set behavior_compat_options='proc_outparam_override'", con);
        TestUtil.createCompositeType(con, "compfoos", "f1 int, f2 varchar(20), f3 varchar(20)");
        TestUtil.createTable(con, "test_user_type", "a int, b compfoos, c compfoos, d varchar(20)");
        String insertSQL = TestUtil.insertSQL("test_user_type", "1, (1,'demo','demo'), (1,'demo1','demo1'), " +
                "'123456@email'");
        TestUtil.execute(insertSQL, con);
        TestUtil.createTable(con, "test_tbl_commonType", "" +
                "col_str        varchar(26),\n" +
                "col_bool       bool,\n" +
                "col_byte       TINYINT,\n" +
                "col_short      smallint,\n" +
                "col_int        int,\n" +
                "col_long       bigint,\n" +
                "col_float      REAL,\n" +
                "col_doule      DOUBLE PRECISION,\n" +
                "col_bigDecimal number,\n" +
                "col_bytes      bytea,\n" +
                "col_date       date,\n" +
                "col_time       time,\n" +
                "col_timestamp  timestamp,\n" +
                "col_object     varchar(52)");
        TestUtil.execute(TestUtil.insertSQL("test_tbl_commonType", "" +
                "'abcdefghijklmnopqrstuvwxyz',\n" +
                "true,\n" +
                "12,\n" +
                "123,\n" +
                "888888888,\n" +
                "999999999,\n" +
                "123456.789123,\n" +
                "123456.789123,\n" +
                "99999999999999999999.1234567890123456,\n" +
                "E'DEADBEEF',\n" +
                "'2010-12-12',\n" +
                "'21:21:21',\n" +
                "'2003-04-12 04:05:06',\n" +
                "'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'"), con);
    }

    @After
    public void tearDown() throws SQLException {
        TestUtil.dropType(con, "compfoos");
        TestUtil.dropTable(con, "test_user_type");
    }

    @Test
    public void testOneCompositeTypeOutParam() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            stmt = con.createStatement();
            stmt.execute("CREATE OR REPLACE PROCEDURE test_proc_user_type_out(id in int,name out compfoos,address in " +
                    "int)\n" +
                    "AS\n" +
                    "BEGIN\n" +
                    "select b into name from test_user_type where a = id or a = address;\n" +
                    "END;\n");
            String query_str = "{call test_proc_user_type_out(?,?,?)}";
            cmt = con.prepareCall(query_str);
            cmt.setInt(1, 1);
            cmt.registerOutParameter(2, Types.STRUCT, "wumk3.compfoos");
            cmt.setInt(3, 1);
            cmt.execute();
            PGobject object = (PGobject) cmt.getObject(2);
            assertEquals("(1,demo,demo)", object.getValue());
            assertEquals("[f1, f2, f3]", Arrays.toString(object.getStruct()));
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    @Test
    public void testMultipleCompositeTypeOutParam() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            stmt = con.createStatement();
            stmt.execute("CREATE OR REPLACE PROCEDURE test_proc_user_type_out(id in int,name out compfoos,address out" +
                    " compfoos,email out varchar(20))\n" +
                    "AS\n" +
                    "BEGIN\n" +
                    "select b into name from test_user_type where a = id;\n" +
                    "select c into address from test_user_type where a = id;\n" +
                    "select d into email from test_user_type where a = id;\n" +
                    "END;\n");
            String query_str = "{call test_proc_user_type_out(?,?,?,?)}";
            cmt = con.prepareCall(query_str);
            cmt.setInt(1, 1);
            cmt.registerOutParameter(2, Types.STRUCT, "wumk3.compfoos");
            cmt.registerOutParameter(3, Types.STRUCT, "wumk3.compfoos");
            cmt.registerOutParameter(4, Types.VARCHAR);
            cmt.execute();
            PGobject firstObject = (PGobject) cmt.getObject(2);
            assertEquals("[f1, f2, f3]", Arrays.toString(firstObject.getStruct()));
            assertEquals("(1,demo,demo)", firstObject.getValue());
            PGobject secondObject = (PGobject) cmt.getObject(3);
            assertEquals("[f1, f2, f3]", Arrays.toString(secondObject.getStruct()));
            assertEquals("(1,demo1,demo1)", secondObject.getValue());
            assertEquals("123456@email", cmt.getString(4));
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    @Test
    public void testCommonTypesOutParam() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            stmt = con.createStatement();
            String createProcedure = "CREATE OR REPLACE PROCEDURE procedure_test("
                    + "v_str OUT varchar(26),"
                    + "v_bool OUT bool,"
                    + "v_byte OUT TINYINT,"
                    + "v_short OUT smallint,"
                    + "v_int OUT int,"
                    + "v_long OUT bigint,"
                    + "v_float OUT REAL,"
                    + "v_doule OUT DOUBLE PRECISION,"
                    + "v_bigDecimal OUT number,"
                    + "v_bytes OUT bytea,"
                    + "v_date OUT date,"
                    + "v_time OUT time,"
                    + "v_timestamp OUT timestamp,"
                    + "v_object OUT varchar(52))"
                    + " AS "
                    + " BEGIN "
                    + "SELECT col_str INTO v_str FROM test_tbl_err_2;"
                    + "SELECT col_bool INTO v_bool FROM test_tbl_err_2;"
                    + "SELECT col_byte INTO v_byte FROM test_tbl_err_2;"
                    + "SELECT col_short INTO v_short FROM test_tbl_err_2;"
                    + "SELECT col_int INTO v_int FROM test_tbl_err_2;"
                    + "SELECT col_long INTO v_long FROM test_tbl_err_2;"
                    + "SELECT col_float INTO v_float FROM test_tbl_err_2;"
                    + "SELECT col_doule INTO v_doule FROM test_tbl_err_2;"
                    + "SELECT col_bigDecimal INTO v_bigDecimal FROM test_tbl_err_2;"
                    + "SELECT col_bytes INTO v_bytes FROM test_tbl_err_2;"
                    + "SELECT col_date INTO v_date FROM test_tbl_err_2;"
                    + "SELECT col_time INTO v_time FROM test_tbl_err_2;"
                    + "SELECT col_timestamp INTO v_timestamp FROM test_tbl_err_2;"
                    + "SELECT col_object INTO v_object FROM test_tbl_err_2;"
                    + " END;";
            stmt.execute(createProcedure);
            String sql = "{call procedure_test(?,?,?,?,?,?,?,?,?,?,?,?,?,?)}";
            cmt = con.prepareCall(sql);
            cmt.registerOutParameter(1, Types.VARCHAR);
            cmt.registerOutParameter(2, Types.BOOLEAN);
            cmt.registerOutParameter(3, Types.TINYINT);
            cmt.registerOutParameter(4, Types.SMALLINT);
            cmt.registerOutParameter(5, Types.INTEGER);
            cmt.registerOutParameter(6, Types.BIGINT);
            cmt.registerOutParameter(7, Types.REAL);
            cmt.registerOutParameter(8, Types.DOUBLE);
            cmt.registerOutParameter(9, Types.NUMERIC);
            cmt.registerOutParameter(10, Types.BINARY);
            cmt.registerOutParameter(11, Types.TIMESTAMP);
            cmt.registerOutParameter(12, Types.TIME);
            cmt.registerOutParameter(13, Types.TIMESTAMP);
            cmt.registerOutParameter(14, Types.VARCHAR);
            cmt.execute();
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试复合类型参数的取值
     * 被测对象：PGobject
     * 输入：复合类型值
     * 测试场景：构造不同复合类型参数的值，验证返回与输入一致
     * 期望输出：输入值与返回值一致
     ******************************************************************/
    @Test
    public void testCompositeTypeValue() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            stmt = con.createStatement();
            stmt.execute("CREATE OR REPLACE PROCEDURE test_proc_user_type_out2(id in int,name out compfoos) " +
                    "AS " +
                    "BEGIN " +
                    "select b into name from test_user_type where a = id;" +
                    "END;");

            // (1,'demo','demo')
            TestUtil.execute(TestUtil.insertSQL("test_user_type", "3, (1,'demo','demo'), null, null"), con);
            cmt = con.prepareCall("{call test_proc_user_type_out2(?,?)}");
            cmt.setInt(1, 3);
            cmt.registerOutParameter(2, Types.STRUCT, "wumk3.compfoos");
            cmt.execute();
            PGobject firstObject = (PGobject) cmt.getObject(2);
            assertEquals("[f1, f2, f3]", Arrays.toString(firstObject.getStruct()));
            assertEquals("(1,demo,demo)", firstObject.getValue());
            assertEquals("[1, demo, demo]", Arrays.toString(firstObject.getArrayValue()));
            // (1,'','demo')
            TestUtil.execute(TestUtil.insertSQL("test_user_type", "4, (1,'','demo'), null, null"), con);
            cmt = con.prepareCall("{call test_proc_user_type_out2(?,?)}");
            cmt.setInt(1, 4);
            cmt.registerOutParameter(2, Types.STRUCT, "wumk3.compfoos");
            cmt.execute();
            PGobject secondObject = (PGobject) cmt.getObject(2);
            assertEquals("[f1, f2, f3]", Arrays.toString(secondObject.getStruct()));
            assertEquals("(1,,demo)", secondObject.getValue());
            assertEquals("[1, null, demo]", Arrays.toString(secondObject.getArrayValue()));
            // (1,'"""demo","','(1,de",mo,demo)
            TestUtil.execute(TestUtil.insertSQL("test_user_type", "5, (1,'\"\"\"demo\",\"','(1,de\",mo,demo)'), null," +
                    "null"), con);
            cmt = con.prepareCall("{call test_proc_user_type_out2(?,?)}");
            cmt.setInt(1, 5);
            cmt.registerOutParameter(2, Types.STRUCT, "wumk3.compfoos");
            cmt.execute();
            PGobject thirdObject = (PGobject) cmt.getObject(2);
            assertEquals("[f1, f2, f3]", Arrays.toString(thirdObject.getStruct()));
            assertEquals("(1,\"\"\"\"\"\"\"demo\"\",\"\"\",\"(1,de\"\",mo,demo)\")", thirdObject.getValue());
            assertEquals("[1, \"\"\"\"\"\"\"demo\"\",\"\"\", \"(1,de\"\",mo,demo)\"]", Arrays.toString(thirdObject.getArrayValue()));
            // (1,'"\,| ''tt','')
            TestUtil.execute(TestUtil.insertSQL("test_user_type", "6, (1,'\"\\,| ''tt',''), null," +
                    "null"), con);
            cmt = con.prepareCall("{call test_proc_user_type_out2(?,?)}");
            cmt.setInt(1, 6);
            cmt.registerOutParameter(2, Types.STRUCT, "wumk3.compfoos");
            cmt.execute();
            PGobject fourthObject = (PGobject) cmt.getObject(2);
            assertEquals("[f1, f2, f3]", Arrays.toString(fourthObject.getStruct()));
            assertEquals("(1,\"\"\"\\\\,| 'tt\",)", fourthObject.getValue());
            assertEquals("[1, \"\"\"\\\\,| 'tt\", null]", Arrays.toString(fourthObject.getArrayValue()));
            // ('','','')
            TestUtil.execute(TestUtil.insertSQL("test_user_type", "7, ('','',''), null," +
                    "null"), con);
            cmt = con.prepareCall("{call test_proc_user_type_out2(?,?)}");
            cmt.setInt(1, 7);
            cmt.registerOutParameter(2, Types.STRUCT, "wumk3.compfoos");
            cmt.execute();
            PGobject fifthObject = (PGobject) cmt.getObject(2);
            assertEquals("[f1, f2, f3]", Arrays.toString(fifthObject.getStruct()));
            assertEquals("(,,)", fifthObject.getValue());
            assertEquals("[null, null, null]", Arrays.toString(fifthObject.getArrayValue()));
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    @Test
    public void testFuncOutNumeric() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            stmt = con.createStatement();
            stmt.execute("create or replace FUNCTION fn_ty_in_ty_out3(o_code out numeric) return numeric is "
                + "begin o_code := 0; return o_code; end;");
            String queryStr = "{? = call fn_ty_in_ty_out3(?)}";
            cmt = con.prepareCall(queryStr);
            {
                cmt.registerOutParameter(1, Types.NUMERIC);
                cmt.registerOutParameter(2, Types.NUMERIC);
            }
            cmt.execute();
            assertEquals(new BigDecimal(0), cmt.getObject(1));
            assertEquals(new BigDecimal(0), cmt.getObject(1));
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }
}