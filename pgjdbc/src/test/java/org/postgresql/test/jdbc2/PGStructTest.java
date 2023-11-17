/*
 * Copyright (c) 2023, openGauss Global Development Group
 * See the LICENSE file in the project root for more information.
 */
package org.postgresql.test.jdbc2;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.test.TestUtil;

import java.sql.*;

import static org.junit.Assert.assertEquals;

public class PGStructTest extends BaseTest4 {
    private String schemaName = getSchemaByRandom();
    private String setPathSql = "SET search_path TO ";

    @Before
    public void setUp() throws Exception {
        super.setUp();
        TestUtil.createSchema(con, schemaName);
        TestUtil.execute(setPathSql + schemaName, con);

        TestUtil.execute("set behavior_compat_options='proc_outparam_override'", con);
        // by 3 level
        TestUtil.createCompositeType(con, "name_object_type_v1", "id int,name VARCHAR(20)");
        TestUtil.createCompositeType(con, "addr_object_type_v1", "street VARCHAR(30),city VARCHAR(20), "
                + "state CHAR(2),  zip int,name name_object_type_v1");
        TestUtil.createCompositeType(con, "emp_obj_typ_v1", "empno int, ename VARCHAR(20),"
                + "addr addr_object_type_v1");
        // by 2 level
        TestUtil.createCompositeType(con, "addr_object_type_v2", "street VARCHAR(30),city VARCHAR(20), "
                + "state CHAR(2),  zip int");
        TestUtil.createCompositeType(con, "emp_obj_typ_v2", "empno int, ename VARCHAR(20),addr addr_object_type_v2");
        // by 4 level
        TestUtil.createCompositeType(con, "ch_name_object_type_v3", "ch1 VARCHAR(20),ch2 VARCHAR(20)");
        TestUtil.createCompositeType(con, "name_object_type_v3", "id int,name VARCHAR(20),ch_name ch_name_object_type_v3");
        TestUtil.createCompositeType(con, "addr_object_type_v3", "street VARCHAR(30),city VARCHAR(20), "
                + "state CHAR(2),  zip int,name name_object_type_v3");
        TestUtil.createCompositeType(con, "emp_obj_typ_v3", "empno int, ename VARCHAR(20),addr addr_object_type_v3");
        // create test cursor table
        TestUtil.createTable(con, "test_cursor", "c1 int, c2 float, c3 float, c4 text, c5 bigint");
        String insertSQL = TestUtil.insertSQL("test_cursor", "1,2.2,3.3,'test',5");
        TestUtil.execute(insertSQL, con);
    }

    @After
    public void tearDown() throws SQLException {
        // by 3 level
        TestUtil.dropType(con, "name_object_type_v1");
        TestUtil.dropType(con, "addr_object_type_v1");
        TestUtil.dropType(con, "emp_obj_typ_v1");
        // by 2 level
        TestUtil.dropType(con, "addr_object_type_v2");
        TestUtil.dropType(con, "emp_obj_typ_v2");
        // by 4 level
        TestUtil.dropType(con, "ch_name_object_type_v3");
        TestUtil.dropType(con, "name_object_type_v3");
        TestUtil.dropType(con, "addr_object_type_v3");
        TestUtil.dropType(con, "emp_obj_typ_v3");

        // drop test cursor table
        TestUtil.dropTable(con, "test_cursor");
        // drop schema
        TestUtil.dropSchema(con, schemaName);
    }

    @Test
    public void testCreateStructByTwoLevel() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            // Create display_emp and save it.
            // The input and output parameters are both emp_obj_typ_v2 custom types.
            String procedureSql = "create or replace PROCEDURE display_emp (a INOUT emp_obj_typ_v2)\n"
                    + "  IS\n"
                    + "  BEGIN\n"
                    + "    a.empno:=1;\n"
                    + "  END;";
            stmt = con.createStatement();
            stmt.execute(procedureSql);

            // Create addr_object_type_v2 Struct object using jdbc connection
            Struct address = con.createStruct("addr_object_type_v2",
                    new Object[]{"123 MAIN STREET", "EDISON", "NJ", 8817});
            // Use jdbc connection to create the emp_obj_typ_v2 Struct object
            // and put addr_object_type_v2 into emp_obj_typ_v2.
            Struct emp = con.createStruct("emp_obj_typ_v2", new Object[]{9001, "JONES", address});

            // set emp_obj_typ_v2 type param
            String commandText = "{call display_emp(?)}";
            cmt = con.prepareCall(commandText);
            cmt.registerOutParameter(1, Types.STRUCT, schemaName + ".emp_obj_typ_v2");
            cmt.setObject(1, emp);
            cmt.execute();

            emp = (Struct) cmt.getObject(1);
            Object[] attrEmp = emp.getAttributes();
            assertEquals(1, attrEmp[0]);
            assertEquals("JONES", attrEmp[1]);
            address = (Struct) attrEmp[2];
            Object[] attrAddress = address.getAttributes();
            assertEquals("123 MAIN STREET", attrAddress[0]);
            assertEquals("EDISON", attrAddress[1]);
            assertEquals("NJ", attrAddress[2]);
            assertEquals(8817, attrAddress[3]);
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    @Test
    public void testCreateStructByThreeLevel() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            // Create display_emp and save it. The input and output parameters are both emp_obj_typ_v1 custom types.
            String procedureSql = "create or replace PROCEDURE display_emp (a INOUT emp_obj_typ_v1)\n"
                    + "  IS\n"
                    + "  BEGIN\n"
                    + "    a.empno:=1;\n"
                    + "  END;";
            stmt = con.createStatement();
            stmt.execute(procedureSql);

            Struct fullName = con.createStruct("name_object_type_v1", new Object[]{101, "zp1001"});
            // Create addr_object_type_v1 Struct object using jdbc connection
            Struct address = con.createStruct("addr_object_type_v1",
                    new Object[]{"123 MAIN STREET", "EDISON", "NJ", 8817, fullName});
            // Use jdbc connection to create the emp_obj_typ_v1 Struct object
            // and put addr_object_type_v1 into emp_obj_typ_v1.
            Struct emp = con.createStruct("emp_obj_typ_v1", new Object[]{9001, "JONES", address});

            // set emp_obj_typ_v1 type param
            String commandText = "{call display_emp(?)}";
            cmt = con.prepareCall(commandText);
            cmt.registerOutParameter(1, Types.STRUCT, schemaName + ".emp_obj_typ_v1");
            cmt.setObject(1, emp);
            cmt.execute();

            emp = (Struct) cmt.getObject(1);
            Object[] attrEmp = emp.getAttributes();
            assertEquals(1, attrEmp[0]);
            assertEquals("JONES", attrEmp[1]);
            address = (Struct) attrEmp[2];
            Object[] attrAddress = address.getAttributes();
            assertEquals("123 MAIN STREET", attrAddress[0]);
            assertEquals("EDISON", attrAddress[1]);
            assertEquals("NJ", attrAddress[2]);
            assertEquals(8817, attrAddress[3]);
            fullName = (Struct) attrAddress[4];
            Object[] fullNameAttrs = fullName.getAttributes();
            assertEquals(101, fullNameAttrs[0]);
            assertEquals("zp1001", fullNameAttrs[1]);
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    @Test
    public void testCreateStructByFourLevel() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            // Create display_emp and save it. The input and output parameters are both emp_obj_typ_v1 custom types.
            String procedureSql = "create or replace PROCEDURE display_emp (a INOUT emp_obj_typ_v3)\n"
                    + "  IS\n"
                    + "  BEGIN\n"
                    + "    a.empno:=1;\n"
                    + "  END;";
            stmt = con.createStatement();
            stmt.execute(procedureSql);

            Struct chName = con.createStruct("ch_name_object_type_v3", new Object[]{"zz", "pp"});
            Struct fullName = con.createStruct("name_object_type_v3", new Object[]{101, "zp1001", chName});
            // 使用 jdbc connection 创建 addr_object_type Struct 对象
            Struct address = con.createStruct("addr_object_type_v3",
                    new Object[]{"123 MAIN STREET", "EDISON", "NJ", 8817, fullName});
            // 使用 jdbc connection 创建 emp_obj_typ Struct 对象，并把 addr_object_type  放入 emp_obj_typ 中。
            Struct emp = con.createStruct("emp_obj_typ_v3", new Object[]{9001, "JONES", address});

            // set emp_obj_typ_v1 type param
            String commandText = "{call display_emp(?)}";
            cmt = con.prepareCall(commandText);
            cmt.registerOutParameter(1, Types.STRUCT, schemaName + ".emp_obj_typ_v3");
            cmt.setObject(1, emp);
            cmt.execute();

            emp = (Struct) cmt.getObject(1);
            Object[] attrEmp = emp.getAttributes();
            assertEquals(1, attrEmp[0]);
            assertEquals("JONES", attrEmp[1]);
            address = (Struct) attrEmp[2];
            Object[] attrAddress = address.getAttributes();
            assertEquals("123 MAIN STREET", attrAddress[0]);
            assertEquals("EDISON", attrAddress[1]);
            assertEquals("NJ", attrAddress[2]);
            assertEquals(8817, attrAddress[3]);
            fullName = (Struct) attrAddress[4];
            Object[] fullNameAttrs = fullName.getAttributes();
            assertEquals(101, fullNameAttrs[0]);
            assertEquals("zp1001", fullNameAttrs[1]);
            chName = (Struct) fullNameAttrs[2];
            Object[] chNameAttrs = chName.getAttributes();
            assertEquals("zz", chNameAttrs[0]);
            assertEquals("pp", chNameAttrs[1]);
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    @Test
    public void testCreateStructContainsNullAttribute() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            // Create display_emp and save it.
            // The input and output parameters are both emp_obj_typ_v2 custom types.
            String procedureSql = "create or replace PROCEDURE display_emp (a INOUT emp_obj_typ_v2)\n"
                    + "  IS\n"
                    + "  BEGIN\n"
                    + "    a.empno:=1;\n"
                    + "  END;";
            stmt = con.createStatement();
            stmt.execute(procedureSql);

            // Create addr_object_type_v2 Struct object using jdbc connection
            Struct address = con.createStruct("addr_object_type_v2",
                    new Object[]{"123 MAIN STREET", null, "NJ", null});
            // Use jdbc connection to create the emp_obj_typ_v2 Struct object
            // and put addr_object_type_v2 into emp_obj_typ_v2.
            Struct emp = con.createStruct("emp_obj_typ_v2", new Object[]{9001, null, address});

            // set emp_obj_typ_v2 type param
            String commandText = "{call display_emp(?)}";
            cmt = con.prepareCall(commandText);
            cmt.registerOutParameter(1, Types.STRUCT, schemaName + ".emp_obj_typ_v2");
            cmt.setObject(1, emp);
            cmt.execute();

            emp = (Struct) cmt.getObject(1);
            Object[] attrEmp = emp.getAttributes();
            assertEquals(1, attrEmp[0]);
            assertEquals(null, attrEmp[1]);
            address = (Struct) attrEmp[2];
            Object[] attrAddress = address.getAttributes();
            assertEquals("123 MAIN STREET", attrAddress[0]);
            assertEquals(null, attrAddress[1]);
            assertEquals("NJ", attrAddress[2]);
            assertEquals(null, attrAddress[3]);
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    @Test
    public void testCreateStructContainsNullStruct() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            // Create display_emp and save it.
            // The input and output parameters are both emp_obj_typ_v2 custom types.
            String procedureSql = "create or replace PROCEDURE display_emp (a INOUT emp_obj_typ_v2)\n"
                    + "  IS\n"
                    + "  BEGIN\n"
                    + "    a.empno:=1;\n"
                    + "  END;";
            stmt = con.createStatement();
            stmt.execute(procedureSql);

            // Create addr_object_type_v2 Struct object using jdbc connection
            Struct address = con.createStruct("addr_object_type_v2",
                    new Object[]{"123 MAIN STREET", null, "NJ", 8817});
            // Use jdbc connection to create the emp_obj_typ_v2 Struct object
            // and put addr_object_type_v2 into emp_obj_typ_v2.
            Struct emp = con.createStruct("emp_obj_typ_v2", new Object[]{9001, "JONES", null});

            // set emp_obj_typ_v2 type param
            String commandText = "{call display_emp(?)}";
            cmt = con.prepareCall(commandText);
            cmt.registerOutParameter(1, Types.STRUCT, schemaName + ".emp_obj_typ_v2");
            cmt.setObject(1, emp);
            cmt.execute();

            emp = (Struct) cmt.getObject(1);
            Object[] attrEmp = emp.getAttributes();
            assertEquals(1, attrEmp[0]);
            assertEquals("JONES", attrEmp[1]);
            address = (Struct) attrEmp[2];
            Object[] attrAddress = address.getAttributes();
            assertEquals(null, attrAddress[0]);
            assertEquals(null, attrAddress[1]);
            assertEquals(null, attrAddress[2]);
            assertEquals(null, attrAddress[3]);
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }

    @Test
    public void testSpOutDouble() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            con.setAutoCommit(false);
            // create sp
            String procedureSql = "CREATE OR REPLACE FUNCTION test_refcursor () RETURNS refcursor AS \n" +
                    "\t'declare v_resset refcursor; begin open v_resset for select c1,c2 from test_cursor;\n" +
                    "\treturn v_resset; end;' \n" +
                    "LANGUAGE plpgsql;";
            stmt = con.createStatement();
            stmt.execute(procedureSql);

            // execute sp
            String commandText = "{? = call test_refcursor()}";
            cmt = con.prepareCall(commandText);
            int OracleTypes_CURSOR = -10;
            cmt.registerOutParameter(1, OracleTypes_CURSOR);
            cmt.execute();

            // get result set
            ResultSet rs = (ResultSet) cmt.getObject(1);
            rs.next();
            assertEquals(1, rs.getInt(1));
            assertEquals(2.2d, rs.getDouble(2), 0.0001);
            con.setAutoCommit(true);
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }
}