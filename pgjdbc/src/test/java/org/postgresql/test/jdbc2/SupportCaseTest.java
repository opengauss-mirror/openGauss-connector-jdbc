package org.postgresql.test.jdbc2;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.test.TestUtil;

import java.sql.CallableStatement;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

public class SupportCaseTest extends BaseTest4 {
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() throws SQLException {
        TestUtil.dropType(con, "compfoos");
        TestUtil.dropTable(con, "test_user_type");
    }

    /*****************************************************************
     * 描述：测试getTables方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：运行方法看是否可以正常运行
     * 期望输出：可以正常运行无报错
     ******************************************************************/
    @Test
    public void testGetTables() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            con.setClientInfo("uppercaseAttributeName", "true");
            DatabaseMetaData metaData = con.getMetaData();
            ResultSet tables = metaData.getTables(con.getCatalog(), "test", "COMPANY", new String[]{"TABLE"});
            tables.next();
            assertEquals("COMPANY", tables.getString("TABLE_NAME"));
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试getColumns方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：运行方法看是否可以正常运行
     * 期望输出：可以正常运行无报错
     ******************************************************************/
    @Test
    public void testGetColumns() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            DatabaseMetaData metaData = con.getMetaData();
            ResultSet tables = metaData.getColumns(con.getCatalog(), "test", "COMPANY", "ID");
            tables.next();
            assertEquals("ID", tables.getString("COLUMN_NAME"));
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试getTablePrivileges方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：运行方法看是否可以正常运行
     * 期望输出：可以正常运行无报错
     ******************************************************************/
    @Test
    public void testGetTablePrivileges() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            DatabaseMetaData metaData = con.getMetaData();
            ResultSet tables = metaData.getTablePrivileges(con.getCatalog(), "test", "COMPANY");
            tables.next();
            assertEquals("YES", tables.getString("IS_GRANTABLE"));
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试getIndexInfo方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：运行方法看是否可以正常运行
     * 期望输出：可以正常运行无报错
     ******************************************************************/
    @Test
    public void testGetIndexInfo() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            DatabaseMetaData metaData = con.getMetaData();
            ResultSet tables = metaData.getIndexInfo(con.getCatalog(), "test", "COMPANY", true, true);
            tables.next();
            assertEquals("TEST", tables.getString("TABLE_SCHEM"));
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试getColumnPrivileges方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：运行方法看是否可以正常运行
     * 期望输出：可以正常运行无报错
     ******************************************************************/
    @Test
    public void testGetColumnPrivileges() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            DatabaseMetaData metaData = con.getMetaData();
            ResultSet tables = metaData.getColumnPrivileges(con.getCatalog(), "test", "COMPANY", "id");
            tables.next();
            assertEquals("TEST", tables.getString("GRANTOR"));
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试getBestRowIdentifier方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：运行方法看是否可以正常运行
     * 期望输出：可以正常运行无报错
     ******************************************************************/
    @Test
    public void testGetBestRowIdentifier() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            DatabaseMetaData metaData = con.getMetaData();
            ResultSet tables = metaData.getBestRowIdentifier(con.getCatalog(), "test", "COMPANY", 1, true);
            tables.next();
            assertEquals("INT4", tables.getString("TYPE_NAME"));
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试getPrimaryKeys方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：运行方法看是否可以正常运行
     * 期望输出：可以正常运行无报错
     ******************************************************************/
    @Test
    public void testGetPrimaryKeys() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            DatabaseMetaData metaData = con.getMetaData();
            ResultSet tables = metaData.getPrimaryKeys(con.getCatalog(), "test", "COMPANY");
            tables.next();
            assertEquals("COMPANY_PKEY", tables.getString("PK_NAME"));
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试getUDTs方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：运行方法看是否可以正常运行
     * 期望输出：可以正常运行无报错
     ******************************************************************/
    @Test
    public void testGetUDTs() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            DatabaseMetaData metaData = con.getMetaData();
            ResultSet tables = metaData.getUDTs(con.getCatalog(), "test", "COMPANY", new int[]{2002});
            tables.next();
            assertEquals("TEST", tables.getString("type_schem"));
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试getProcedureColumns方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：运行方法看是否可以正常运行
     * 期望输出：可以正常运行无报错
     ******************************************************************/
    @Test
    public void testGetProcedureColumns() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            DatabaseMetaData metaData = con.getMetaData();
            ResultSet tables = metaData.getProcedureColumns(con.getCatalog(), "test", "test_proc_user_type_out", "id");
            tables.next();
            assertEquals("TEST_PROC_USER_TYPE_OUT", tables.getString("PROCEDURE_NAME"));
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试getFunctions方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：运行方法看是否可以正常运行
     * 期望输出：可以正常运行无报错
     ******************************************************************/
    @Test
    public void testGetFunctions() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            DatabaseMetaData metaData = con.getMetaData();
            ResultSet tables = metaData.getFunctions(con.getCatalog(), "test", null);
            tables.next();
            assertEquals("TEST", tables.getString("FUNCTION_SCHEM"));
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试getProcedures方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：运行方法看是否可以正常运行
     * 期望输出：可以正常运行无报错
     ******************************************************************/
    @Test
    public void testGetProcedures() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            DatabaseMetaData metaData = con.getMetaData();
            ResultSet tables = metaData.getProcedures(con.getCatalog(), "test", "test_proc_user_type_out");
            tables.next();
            assertEquals("TEST_PROC_USER_TYPE_OUT_16467", tables.getString("SPECIFIC_NAME"));
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试getSchemas方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：运行方法看是否可以正常运行
     * 期望输出：可以正常运行无报错
     ******************************************************************/
    @Test
    public void testGetSchemas() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            DatabaseMetaData metaData = con.getMetaData();
            ResultSet tables = metaData.getSchemas(con.getCatalog(), "test");
            tables.next();
            assertEquals("TEST", tables.getString("TABLE_SCHEM"));
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试getTables方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：负向用例场景,大小写参数关闭时用户执行大写字段输入
     * 期望输出：获取结果为空
     ******************************************************************/
    @Test
    public void testFailGetTables() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            try {
                DatabaseMetaData metaData = con.getMetaData();
                ResultSet tables = metaData.getSchemas(con.getCatalog(), "test");
                tables.next();
            } catch (SQLException e) {
                assertEquals(e.getMessage(), "查询结果指标位置不正确，您也许需要呼叫 ResultSet 的 next() 方法。");
            }
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试getColumns方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：负向用例场景,大小写参数关闭时用户执行大写字段输入
     * 期望输出：获取结果为空
     ******************************************************************/
    @Test
    public void testFailGetColumns() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            try {
                DatabaseMetaData metaData = con.getMetaData();
                ResultSet tables = metaData.getColumns(con.getCatalog(), "test", "COMPANY", "ID");
                tables.next();
            } catch (SQLException e) {
                assertEquals(e.getMessage(), "查询结果指标位置不正确，您也许需要呼叫 ResultSet 的 next() 方法。");
            }

        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试getTablePrivileges方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：负向用例场景,大小写参数关闭时用户执行大写字段输入
     * 期望输出：获取结果为空
     ******************************************************************/
    @Test
    public void testFailGetTablePrivileges() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            try {
                DatabaseMetaData metaData = con.getMetaData();
                ResultSet tables = metaData.getTablePrivileges(con.getCatalog(), "test", "COMPANY");
                tables.next();
            } catch (SQLException e) {
                assertEquals(e.getMessage(), "查询结果指标位置不正确，您也许需要呼叫 ResultSet 的 next() 方法。");
            }

        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试getIndexInfo方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：负向用例场景,大小写参数关闭时用户执行大写字段输入
     * 期望输出：获取结果为空
     ******************************************************************/
    @Test
    public void testFailGetIndexInfo() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            try {
                DatabaseMetaData metaData = con.getMetaData();
                ResultSet tables = metaData.getIndexInfo(con.getCatalog(), "test", "COMPANY", true, true);
                tables.next();
            } catch (SQLException e) {
                assertEquals(e.getMessage(), "查询结果指标位置不正确，您也许需要呼叫 ResultSet 的 next() 方法。");
            }

        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试getColumnPrivileges方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：负向用例场景,大小写参数关闭时用户执行大写字段输入
     * 期望输出：获取结果为空
     ******************************************************************/
    @Test
    public void testFailGetColumnPrivileges() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            try {
                DatabaseMetaData metaData = con.getMetaData();
                ResultSet tables = metaData.getColumnPrivileges(con.getCatalog(), "test", "COMPANY", "id");
                tables.next();
            } catch (SQLException e) {
                assertEquals(e.getMessage(), "查询结果指标位置不正确，您也许需要呼叫 ResultSet 的 next() 方法。");
            }

        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试getBestRowIdentifier方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：负向用例场景,大小写参数关闭时用户执行大写字段输入
     * 期望输出：获取结果为空
     ******************************************************************/
    @Test
    public void testFailGetBestRowIdentifier() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            try {
                DatabaseMetaData metaData = con.getMetaData();
                ResultSet tables = metaData.getBestRowIdentifier(con.getCatalog(), "test", "COMPANY", 1, true);
                tables.next();
            } catch (SQLException e) {
                assertEquals(e.getMessage(), "查询结果指标位置不正确，您也许需要呼叫 ResultSet 的 next() 方法。");
            }
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试getPrimaryKeys方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：负向用例场景,大小写参数关闭时用户执行大写字段输入
     * 期望输出：获取结果为空
     ******************************************************************/
    @Test
    public void testFailGetPrimaryKeys() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            try {
                DatabaseMetaData metaData = con.getMetaData();
                ResultSet tables = metaData.getPrimaryKeys(con.getCatalog(), "test", "COMPANY");
                tables.next();
            } catch (SQLException e) {
                assertEquals(e.getMessage(), "查询结果指标位置不正确，您也许需要呼叫 ResultSet 的 next() 方法。");
            }

        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试getUDTs方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：负向用例场景,大小写参数关闭时用户执行大写字段输入
     * 期望输出：获取结果为空
     ******************************************************************/
    @Test
    public void testFailGetUDTs() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            try {
                DatabaseMetaData metaData = con.getMetaData();
                ResultSet tables = metaData.getUDTs(con.getCatalog(), "test", "COMPANY", new int[]{2002});
                tables.next();
            } catch (SQLException e) {
                assertEquals(e.getMessage(), "查询结果指标位置不正确，您也许需要呼叫 ResultSet 的 next() 方法。");
            }
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试getProcedureColumns方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：负向用例场景,大小写参数关闭时用户执行大写字段输入
     * 期望输出：获取结果为空
     ******************************************************************/
    @Test
    public void testFailGetProcedureColumns() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            try {
                DatabaseMetaData metaData = con.getMetaData();
                ResultSet tables = metaData.getProcedureColumns(con.getCatalog(), "test", "test_proc_user_type_out", "id");
                tables.next();
            } catch (SQLException e) {
                assertEquals(e.getMessage(), "查询结果指标位置不正确，您也许需要呼叫 ResultSet 的 next() 方法。");
            }
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试getFunctions方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：负向用例场景,大小写参数关闭时用户执行大写字段输入
     * 期望输出：获取结果为空
     ******************************************************************/
    @Test
    public void testFailGetFunctions() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            try {
                DatabaseMetaData metaData = con.getMetaData();
                ResultSet tables = metaData.getFunctions(con.getCatalog(), "test", null);
                tables.next();
            } catch (SQLException e) {
                assertEquals(e.getMessage(), "查询结果指标位置不正确，您也许需要呼叫 ResultSet 的 next() 方法。");
            }
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试getProcedures方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：负向用例场景,大小写参数关闭时用户执行大写字段输入
     * 期望输出：获取结果为空
     ******************************************************************/
    @Test
    public void testFailGetProcedures() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            try {
                DatabaseMetaData metaData = con.getMetaData();
                ResultSet tables = metaData.getProcedures(con.getCatalog(), "test", "test_proc_user_type_out");
                tables.next();
            } catch (SQLException e) {
                assertEquals(e.getMessage(), "查询结果指标位置不正确，您也许需要呼叫 ResultSet 的 next() 方法。");
            }
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试getSchemas方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：负向用例场景,大小写参数关闭时用户执行大写字段输入
     * 期望输出：获取结果为空
     ******************************************************************/
    @Test
    public void testFailGetSchemas() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            try {
                DatabaseMetaData metaData = con.getMetaData();
                ResultSet tables = metaData.getSchemas(con.getCatalog(), "test");
                tables.next();
            } catch (SQLException e) {
                assertEquals(e.getMessage(), "查询结果指标位置不正确，您也许需要呼叫 ResultSet 的 next() 方法。");
            }
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试getTables方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：黑盒测试,开启参数,模拟用户输入场景，测试输入为大小写混合
     * 期望输出：可以正常运行无报错
     ******************************************************************/
    @Test
    public void testBlackBoxGetTables() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            DatabaseMetaData metaData = con.getMetaData();
            ResultSet tables = metaData.getTables(con.getCatalog(), "Test", "Company", new String[]{"TABLE"});
            tables.next();
            assertEquals("COMPANY", tables.getString("TABLE_NAME"));
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试getColumns方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：黑盒测试,开启参数,模拟用户输入场景，测试输入为大小写混合
     * 期望输出：可以正常运行无报错
     ******************************************************************/
    @Test
    public void testBlackBoxGetColumns() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            DatabaseMetaData metaData = con.getMetaData();
            ResultSet tables = metaData.getColumns(con.getCatalog(), "Test", "Company", "ID");
            tables.next();
            assertEquals("ID", tables.getString("COLUMN_NAME"));
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试getTablePrivileges方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：黑盒测试,开启参数,模拟用户输入场景，测试输入为大小写混合
     * 期望输出：可以正常运行无报错
     ******************************************************************/
    @Test
    public void testBlackBoxGetTablePrivileges() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            DatabaseMetaData metaData = con.getMetaData();
            ResultSet tables = metaData.getTablePrivileges(con.getCatalog(), "Test", "Company");
            tables.next();
            assertEquals("YES", tables.getString("IS_GRANTABLE"));
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试getIndexInfo方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：黑盒测试,开启参数,模拟用户输入场景，测试输入为大小写混合
     * 期望输出：可以正常运行无报错
     ******************************************************************/
    @Test
    public void testBlackBoxGetIndexInfo() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            DatabaseMetaData metaData = con.getMetaData();
            ResultSet tables = metaData.getIndexInfo(con.getCatalog(), "Test", "Company", true, true);
            tables.next();
            assertEquals("TEST", tables.getString("TABLE_SCHEM"));
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试getColumnPrivileges方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：黑盒测试,开启参数,模拟用户输入场景，测试输入为大小写混合
     * 期望输出：可以正常运行无报错
     ******************************************************************/
    @Test
    public void testBlackBoxGetColumnPrivileges() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            DatabaseMetaData metaData = con.getMetaData();
            ResultSet tables = metaData.getColumnPrivileges(con.getCatalog(), "Test", "Company", "id");
            tables.next();
            assertEquals("TEST", tables.getString("GRANTOR"));
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试getBestRowIdentifier方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：黑盒测试,开启参数,模拟用户输入场景，测试输入为大小写混合
     * 期望输出：可以正常运行无报错
     ******************************************************************/
    @Test
    public void testBlackBoxGetBestRowIdentifier() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            DatabaseMetaData metaData = con.getMetaData();
            ResultSet tables = metaData.getBestRowIdentifier(con.getCatalog(), "Test", "Company", 1, true);
            tables.next();
            assertEquals("INT4", tables.getString("TYPE_NAME"));
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试getPrimaryKeys方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：黑盒测试,开启参数,模拟用户输入场景，测试输入为大小写混合
     * 期望输出：可以正常运行无报错
     ******************************************************************/
    @Test
    public void testBlackBoxGetPrimaryKeys() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            DatabaseMetaData metaData = con.getMetaData();
            ResultSet tables = metaData.getPrimaryKeys(con.getCatalog(), "Test", "Company");
            tables.next();
            assertEquals("COMPANY_PKEY", tables.getString("PK_NAME"));
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试getUDTs方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：黑盒测试,开启参数,模拟用户输入场景，测试输入为大小写混合
     * 期望输出：可以正常运行无报错
     ******************************************************************/
    @Test
    public void testBlackBoxGetUDTs() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            DatabaseMetaData metaData = con.getMetaData();
            ResultSet tables = metaData.getUDTs(con.getCatalog(), "Test", "Company", new int[]{2002});
            tables.next();
            assertEquals("TEST", tables.getString("type_schem"));
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试getProcedureColumns方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：黑盒测试,开启参数,模拟用户输入场景，测试输入为大小写混合
     * 期望输出：可以正常运行无报错
     ******************************************************************/
    @Test
    public void testBlackBoxGetProcedureColumns() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            DatabaseMetaData metaData = con.getMetaData();
            ResultSet tables = metaData.getProcedureColumns(con.getCatalog(), "Test", "test_proc_user_type_out", "id");
            tables.next();
            assertEquals("TEST_PROC_USER_TYPE_OUT", tables.getString("PROCEDURE_NAME"));
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试getFunctions方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：黑盒测试,开启参数,模拟用户输入场景，测试输入为大小写混合
     * 期望输出：可以正常运行无报错
     ******************************************************************/
    @Test
    public void testBlackBoxGetFunctions() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            DatabaseMetaData metaData = con.getMetaData();
            ResultSet tables = metaData.getFunctions(con.getCatalog(), "Test", null);
            tables.next();
            assertEquals("TEST", tables.getString("FUNCTION_SCHEM"));
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试getProcedures方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：黑盒测试,开启参数,模拟用户输入场景，测试输入为大小写混合
     * 期望输出：可以正常运行无报错
     ******************************************************************/
    @Test
    public void testBlackBoxGetProcedures() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            DatabaseMetaData metaData = con.getMetaData();
            ResultSet tables = metaData.getProcedures(con.getCatalog(), "Test", "test_proc_user_type_out");
            tables.next();
            assertEquals("TEST_PROC_USER_TYPE_OUT_16467", tables.getString("SPECIFIC_NAME"));
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    /*****************************************************************
     * 描述：测试getSchemas方法
     * 被测对象：PgDatabaseMetaData
     * 输入：无
     * 测试场景：黑盒测试,开启参数,模拟用户输入场景，测试输入为大小写混合
     * 期望输出：可以正常运行无报错
     ******************************************************************/
    @Test
    public void testBlackBoxGetSchemas() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            DatabaseMetaData metaData = con.getMetaData();
            ResultSet tables = metaData.getSchemas(con.getCatalog(), "Test");
            tables.next();
            assertEquals("TEST", tables.getString("TABLE_SCHEM"));
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }
}
