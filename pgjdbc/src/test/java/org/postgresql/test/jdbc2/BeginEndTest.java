package org.postgresql.test.jdbc2;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.test.TestUtil;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

public class BeginEndTest extends BaseTest4 {
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() throws SQLException {

    }

    /*****************************************************************
     * 描述：目前可用支持的使用场景
     * 被测对象：Parser
     * 输入：无
     * 测试场景：存储过程中有end if，后面语句中有空格加/组合
     * 期望输出：存储过程创建成功
     ******************************************************************/
    @Test
    public void testCase() throws SQLException {
        Statement stmt = con.createStatement();
        CallableStatement cmt = null;
        try {
            stmt.execute("create or replace procedure proc_test(p1 int,p2 int ,p3 VARCHAR2(5) ,p4 out int) as\n" +
                    "\n" +
                    "begin\n" +
                    "   p4 := 0;\n" +
                    "      if p3 = '+' then \n" +
                    "          p4 := p1 + p2;\n" +
                    "       end if;\n" +
                    "       \n" +
                    "       if p3 = '-' then \n" +
                    "          p4 := p1 - p2;\n" +
                    "       end if;\n" +
                    "       \n" +
                    "       if p3 = '*' then \n" +
                    "          p4 := p1 * p2;\n" +
                    "       end if;\n" +
                    "       if p3 = ' /' then \n" +
                    "          p4 := p1 / p2;\n" +
                    "       end if;    " + "\n" +
                    "       \n" +
                    "end;\n" +
                    "/");
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }
    /*****************************************************************
     * 描述：目前可用支持的使用场景
     * 被测对象：Parser
     * 输入：无
     * 测试场景：end if 中间有换行，结尾处end有注释的场景
     * 期望输出：存储过程创建成功
     ******************************************************************/
    @Test
    public void testCase2() throws SQLException {
        Statement stmt = con.createStatement();
        CallableStatement cmt = null;
        try {
            stmt.execute("create or replace procedure proc_test(num1 int,num2 int,p3 VARCHAR2(5),num3 out int ) as\n" +
                    "\n" +
                    "begin\n" +
                    "       if p3 = '*' then \n" +
                    "          num3 := num1 * num2;\n" +
                    "       end \n" + "" +
                    " if;\n" +
                    "       if p3 = '*' then \n" +
                    "          num3 := num1 * num2;\n" +
                    "       end if;\n" +
                    "       if p3 = '/' then \n" +
                    "          num3 :=   num1\n" +
                    "      / num2+1;" + "\n" +
                    "       end if;" + "\n" +
                    "end   /*ffff*/ ----- \n  " +
                    " /");
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }
    /*****************************************************************
     * 描述：目前可用支持的使用场景
     * 被测对象：Parser
     * 输入：无
     * 测试场景：end if 中间有换行，结尾处end有注释的场景
     * 期望输出：存储过程创建成功
     ******************************************************************/
    @Test
    public void testCase3() throws SQLException {
        Statement stmt = con.createStatement();
        CallableStatement cmt = null;
        try {
            stmt.execute("create or replace procedure proc_test(num1 int,num2 int,p3 VARCHAR2(5),num3 out int ) as\n" +
                    "\n" +
                    "begin\n" +
                    "       if p3 = '*' then \n" +
                    "          num3 := num1 * num2;\n" +
                    "       end \n" + "" +
                    " if;\n" +
                    "       if p3 = '*' then \n" +
                    "          num3 := num1 * num2;\n" +
                    "       end if;\n" +
                    "       if p3 = '/' then \n" +
                    "          num3 :=   num1" +
                    " / num2+1;" + "\n" +
                    "       end  if;" + "\n" +
                    "end /" +
                    " " + "create or replace procedure proc_test(num1 int,num2 int,p3 VARCHAR2(5),num3 out int ) as\n" +
                    "\n" +
                    "begin\n" +
                    "       if p3 = '*' then \n" +
                    "          num3 := num1 * num2;\n" +
                    "       end " + "" +
                    " if;\n" +
                    "       if p3 = '*' then \n" +
                    "          num3 := num1 * num2;\n" +
                    "       end if;\n" +
                    "       if p3 = '/' then \n" +
                    "          num3 :=   num1" +
                    "/ num2+1;" + "\n" +
                    "       end if;" + "\n" +
                    "end\t   /*ffff*/ \n  " +
                    " ");
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }
    /*****************************************************************
     * 描述：目前可用支持的使用场景
     * 被测对象：Parser
     * 输入：无
     * 测试场景：执行两个带$$language的语句
     * 期望输出：存储过程创建成功
     ******************************************************************/
    @Test
    public void testCase4() throws SQLException {
        Statement stmt = con.createStatement();
        CallableStatement cmt = null;
        try {
            stmt.execute("CREATE OR REPLACE FUNCTION compute(i int, out result_1 bigint)\n" +
                    "returns SETOF RECORD\n" +
                    "as $$\n" +
                    "begin\n" +
                    "    result_1 = i + 1;\n" +
                    "return next;\n" +
                    " end " +
                    "$$language plpgsql;\n" +
                    " /" + "CREATE OR REPLACE FUNCTION compute(i int, out result_1 bigint)\n" +
                    "returns SETOF RECORD\n" +
                    "as $$\n" +
                    "begin\n" +
                    "    result_1 = i + 1;\n" +
                    "return next;\n" +
                    " end " +
                    "$$ language plpgsql;\n");
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }
    /*****************************************************************
     * 描述：目前可用支持的使用场景
     * 被测对象：Parser
     * 输入：无
     * 测试场景：case和end的场景
     * 期望输出：存储过程创建成功
     ******************************************************************/
    @Test
    public void testCase5() throws SQLException {
        Statement stmt = con.createStatement();
        CallableStatement cmt = null;
        try {
            stmt.execute("select case when id =0 then '结果为0' when id=1 then '结果为1' end from company");
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }
    /*****************************************************************
     * 描述：目前可用支持的使用场景
     * 被测对象：Parser
     * 输入：无
     * 测试场景：创建匿名块
     * 期望输出：存储过程创建成功
     ******************************************************************/
    @Test
    public void testCase6() throws SQLException {
        Statement stmt = con.createStatement();
        CallableStatement cmt = null;
        try {
            stmt.execute("DECLARE      \n" +
                    "     my_var VARCHAR2(30);  \n" +
                    "BEGIN      \n" +
                    "     my_var :='world';     \n" +
                    "     dbe_output.print_line('hello'||my_var); \n" +
                    "END \n" +
                    " /");
            stmt.execute("BEGIN\n" +
                    "     dbe_output.print_line('hello world!'); \n" +
                    "END; \n" +
                    "/");
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }
    /*****************************************************************
     * 描述：目前可用支持的使用场景
     * 被测对象：Parser
     * 输入：无
     * 测试场景：end $$结尾
     * 期望输出：存储过程创建成功
     ******************************************************************/
    @Test
    public void testCase7() throws SQLException {
        Statement stmt = con.createStatement();
        CallableStatement cmt = null;
        try {
            stmt.execute("CREATE OR REPLACE FUNCTION func_return returns void\n" +
                    "language plpgsql\n" +
                    "AS $$\n" +
                    "DECLARE\n" +
                    "v_num INTEGER := 1;\n" +
                    "BEGIN\n" +
                    "dbe_output.print_line(v_num);\n" +
                    "RETURN;  --返回语句\n" +
                    "END $$ /\n");
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }
    /*****************************************************************
     * 描述：目前可用支持的使用场景
     * 被测对象：Parser
     * 输入：无
     * 测试场景：创建函数
     * 期望输出：存储过程创建成功
     ******************************************************************/
    @Test
    public void testCase8() throws SQLException {
        Statement stmt = con.createStatement();
        CallableStatement cmt = null;
        try {
            stmt.execute("create or replace function testVarchar()\n" +
                    "return nvarchar2\n" +
                    "as\n" +
                    "declare\n" +
                    "v_pare nvarchar2;\n" +
                    "begin\n" +
                    "select 'nvarchar2' into v_pare;\n" +
                    "return v_pare;\n" +
                    "end;");
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }
    /*****************************************************************
     * 描述：目前可用支持的使用场景
     * 被测对象：Parser
     * 输入：无
     * 测试场景：存储过程中间包含end，但是语句中没有跟着 /
     * 期望输出：存储过程创建成功
     ******************************************************************/
    @Test
    public void testCase9() throws SQLException {
        Statement stmt = con.createStatement();
        CallableStatement cmt = null;
        try {
            stmt.execute("create or replace procedure proc_test1() as\n" +
                    "\n" +
                    "begin\n" +
                    "select 2 as end;\n" +
                    "end   \n  " +
                    " /" + "create or replace procedure proc_test1() as\n" +
                    "\n" +
                    "begin\n" +
                    "select 1 - 2;" +
                    "end   \n  " +
                    " /");
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }
    /*****************************************************************
     * 描述：目前可用支持的使用场景
     * 被测对象：Parser
     * 输入：无
     * 测试场景：存储过程中间包含end，但是语句中的/，前面没有空格和回车
     * 期望输出：存储过程创建成功
     ******************************************************************/
    @Test
    public void testCase10() throws SQLException {
        Statement stmt = con.createStatement();
        CallableStatement cmt = null;
        try {
            stmt.execute("DECLARE      \n" +
                    "     my_var VARCHAR2(30);  \n" +
                    "BEGIN      \n" +
                    "     my_var :='world';     \n" +
                    "     dbe_output.print_line('hello'||my_var); \n" +
                    "END \n" +
                    " /" +
                    "");
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：目前可用支持的使用场景
     * 被测对象：Parser
     * 输入：无
     * 测试场景：创建空包场景
     * 期望输出：存储过程创建成功
     ******************************************************************/
    @Test
    public void testCase10() throws SQLException {
        Statement stmt = con.createStatement();
        CallableStatement cmt = null;
        try {
            stmt.execute("create or replace package cnt6\n" +
                    "as\n" +
                    "count_sum constant number := 1;\n" +
                    "end cnt6;\n" +
                    "/");
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：目前可用支持的使用场景
     * 被测对象：Parser
     * 输入：无
     * 测试场景：存储过程中间包含end加括号的组合
     * 期望输出：存储过程创建成功
     ******************************************************************/
    @Test
    public void testCase11() throws SQLException {
        Statement stmt = con.createStatement();
        CallableStatement cmt = null;
        try {
            stmt.execute("CREATE OR REPLACE PROCEDURE array_proc\n" +
                    "AS\n" +
                    "TYPE ARRAY_INTEGER IS VARRAY(1024) OF INTEGER;--定义数组类型\n" +
                    "ARRINT ARRAY_INTEGER := ARRAY_INTEGER(); --声明数组类型的变量\n" +
                    "BEGIN \n" +
                    "ARRINT.extend(10);\n" +
                    "FOR I IN 1..10 LOOP\n" +
                    "ARRINT(I) := I;\n" +
                    "END LOOP;\n" +
                    "DBE_OUTPUT.PRINT_LINE(ARRINT.COUNT);\n" +
                    "DBE_OUTPUT.PRINT_LINE(ARRINT(1));\n" +
                    "DBE_OUTPUT.PRINT_LINE(ARRINT(10));\n" +
                    "DBE_OUTPUT.PRINT_LINE(ARRINT(ARRINT.FIRST));\n" +
                    "DBE_OUTPUT.PRINT_LINE(ARRINT(ARRINT.last));\n" +
                    "END;\n" +
                    "/");
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：目前不可用场景，在存储过程内部构建出了end关键字。
     * 被测对象：Parser
     * 输入：无
     * 测试场景：存储过程中间包含end，但是语句中的/，前面有空格或回车
     * 期望输出：存储过程创建失败
     ******************************************************************/
    @Test
    public void testFailCase() throws SQLException {
        Statement stmt = con.createStatement();
        CallableStatement cmt = null;
        try {
            try {
                stmt.execute("create or replace procedure proc_test(num1 int,num2 int,p3 VARCHAR2(5),num3 out int ) as\n" +
                        "\n" +
                        "begin\n" +
                        "select 1 as " +
                        "end;\n" +
                        "select 1 /1;\n" +
                        "end\n  " +
                        "/");
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
            try {
                stmt.execute("create or replace procedure proc_test(num1 int,num2 int,p3 VARCHAR2(5),num3 out int ) as\n" +
                        "\n" +
                        "begin\n" +
                        "select 1 as " +
                        "end;\n" +
                        "select 1\n/1;\n" +
                        "end\n  " +
                        "/");
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }
}
