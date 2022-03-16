package org.postgresql.test.jdbc2;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.test.TestUtil;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

public class HintNodeNameTest extends BaseTest4 {
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() throws SQLException {

    }

    /*****************************************************************
     * 描述：先设置nodeName的值，测试prepareStatement(String sql)方法
     * 被测对象：PgConnection
     * 输入：无
     * 测试场景：运行方法看是否可以正常运行
     * 期望输出：可以正常运行无报错
     ******************************************************************/
    @Test
    public void testPrepareStatementScene1() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            con.setClientInfo("nodeName","datanode1");
            ResultSet rs;
            PreparedStatement cs;
            cs = con.prepareStatement("/*+  ddd*/select name as selectp from test");
            rs = cs.executeQuery();
            rs.next();
            assertEquals("lisi", rs.getString(1));
            cs = con.prepareStatement("select /*+   */ name as selectp from test");
            rs = cs.executeQuery();
            rs.next();
            assertEquals("lisi", rs.getString(1));
            cs = con.prepareStatement("------select" + "\n" +
                    "/**/select " + "\n" +
                    " name as selectp from test;" + "\n" +
                    "/*===select==*/" + "\n" +
                    "/*delete*/");
            cs.execute();

            cs = con.prepareStatement("------select" + "\n" +
                    "/**/select " + "\n" +
                    " name as selectp from test where id in (select id from test)" + "\n" +
                    "/*===select==*/" + "\n" +
                    "/*delete*/");
            rs = cs.executeQuery();
            rs.next();
            assertEquals("lisi", rs.getString(1));
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：先调用prepareStatement(String sql)方法，在设置nodeName的值，执行SQL语句
     * 被测对象：PgConnection
     * 输入：无
     * 测试场景：运行方法看是否可以正常运行
     * 期望输出：可以正常运行无报错
     ******************************************************************/
    @Test
    public void testPrepareStatementScene2() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            ResultSet rs;
            PreparedStatement cs;
            cs = con.prepareStatement("select name as selectp from test");
            con.setClientInfo("nodeName","datanode1");
            rs = cs.executeQuery();
            rs.next();
            assertEquals("lisi", rs.getString(1));
            con.setClientInfo("nodeName","");
            cs = con.prepareStatement("select /*+   */ name as selectp from test");
            con.setClientInfo("nodeName","datanode1");
            rs = cs.executeQuery();
            rs.next();
            assertEquals("lisi", rs.getString(1));
            con.setClientInfo("nodeName","");
            cs = con.prepareStatement("------select" + "\n" +
                    "/**/select " + "\n" +
                    " name as selectp from test where id in (select id from test)" + "\n" +
                    "/*===select==*/" + "\n" +
                    "/*delete*/");
            con.setClientInfo("nodeName","datanode1");
            rs = cs.executeQuery();
            rs.next();
            assertEquals("lisi", rs.getString(1));
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：设置nodeName测试prepareStatement(String sql)方法，多次执行后，修改nodeName值，
     * 被测对象：PgConnection
     * 输入：无
     * 测试场景：运行方法看是否可以正常运行
     * 期望输出：可以正常运行无报错
     ******************************************************************/
    @Test
    public void testManyPrepareStatementScene1() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            ResultSet rs;
            PreparedStatement cs;
            con.setClientInfo("nodeName","datanode1");
            cs = con.prepareStatement("with w1 as  (select id /*A  ) select */ from test) select  id from w1");
            cs.executeQuery();
            cs.executeQuery();
            cs.executeQuery();
            cs.executeQuery();
            cs.executeQuery();
            rs = cs.executeQuery();
            rs.next();
            assertEquals("1", rs.getString(1));
            con.setClientInfo("nodeName","datanode2");
            rs = cs.executeQuery();
            rs.next();
            assertEquals("ll", rs.getString(1));
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试prepareStatement(String sql)方法，多次执行触发缓存后，设置nodeName
     * 被测对象：PgConnection
     * 输入：无
     * 测试场景：运行方法看是否可以正常运行
     * 期望输出：可以正常运行无报错
     ******************************************************************/
    @Test
    public void testManyPrepareStatementScene2() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            ResultSet rs;
            PreparedStatement cs;
            cs = con.prepareStatement("select name as selectp from test");
            cs.executeQuery();
            cs.executeQuery();
            cs.executeQuery();
            cs.executeQuery();
            cs.executeQuery();
            con.setClientInfo("nodeName","datanode1");
            rs = cs.executeQuery();
            rs.next();
            assertEquals("lisi", rs.getString(1));
            cs = con.prepareStatement("select name as selectp from test");
            con.setClientInfo("nodeName","datanode2");
            rs = cs.executeQuery();
            rs.next();
            assertEquals("ll", rs.getString(1));
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试executeQuery(String sql)方法
     * 被测对象：PgConnection
     * 输入：无
     * 测试场景：运行方法看是否可以正常运行
     * 期望输出：可以正常运行无报错
     ******************************************************************/
    @Test
    public void testExecuteQuery() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            con.setClientInfo("nodeName","datanode1");
            ResultSet rs;
            Statement statement = con.createStatement();
            rs = statement.executeQuery(" with w1 as (select id as with_ABC from test) select/**/ id from w1 ");
            rs.next();
            assertEquals("1", rs.getString(1));
            rs = statement.executeQuery("select /*+*/ name from test ");
            rs.next();
            assertEquals("lisi", rs.getString(1));
            rs = statement.executeQuery("--select * from " +
                    "\n/*select * from*/------ \n select " +
                    " name from test ");
            rs.next();
            assertEquals("lisi", rs.getString(1));
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试execute(String sql)方法
     * 被测对象：PgConnection
     * 输入：无
     * 测试场景：运行方法看是否可以正常运行
     * 期望输出：可以正常运行无报错
     ******************************************************************/
    @Test
    public void testExecute() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            con.setClientInfo("nodeName","datanode1");
            ResultSet rs;
            Statement statement = con.createStatement();
            statement.execute("with /*+'''*/ temp as (select id from test) select id from temp ");
            rs=statement.getResultSet();
            rs.next();
            assertEquals("1", rs.getString(1));
            statement.execute("select /*+*/ name from test ");
            rs=statement.getResultSet();
            rs.next();
            assertEquals("lisi", rs.getString(1));
            statement.execute("--select * from " +
                    "\n/*select * from*/------ \n select " +
                    " name from test ");
            rs=statement.getResultSet();
            rs.next();
            assertEquals("lisi", rs.getString(1));
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }
    /*----------------负向用例----------------*/
    /*****************************************************************
     * 描述：测试设置节点错误方法
     * 被测对象：PgConnection
     * 输入：无
     * 测试场景：运行方法看是否可以正常运行
     * 期望输出：可以正常运行无报错
     ******************************************************************/
    @Test
    public void testWrongNode() {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            try {
                con.setClientInfo("nodeName","datanode12");
                ResultSet rs;
                Statement statement = con.createStatement();
                statement.execute(" select name from test ");
                rs=statement.getResultSet();
                rs.next();
            }catch (SQLException e){
                assertEquals("Node name does not exist.",e.getMessage());
            }
            try {
                con.setClientInfo("nodeName","datanode1/*");
                ResultSet rs;
                Statement statement = con.createStatement();
                statement.execute(" select name from test ");
                rs=statement.getResultSet();
                rs.next();
            }catch (SQLException e){
                assertEquals("Illegal node name.",e.getMessage());
            }

        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }
}

