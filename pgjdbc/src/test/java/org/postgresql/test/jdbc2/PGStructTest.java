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
            // create sp
            String procedureSql = "create or replace procedure test_sp_out_double(\n" +
                    "\t pa in int4,\n" +
                    "\t pb out numeric,\n" +
                    "\t pc out int8)\n" +
                    "IS \n" +
                    "begin \n" +
                    "pb := 2.22; \n" +
                    "pc := pa + 1024; \n" +
                    "end;";
            stmt = con.createStatement();
            stmt.execute(procedureSql);

            // execute sp
            String commandText = "{call test_sp_out_double(?, ?, ?)}";
            cmt = con.prepareCall(commandText);
            cmt.setInt(1, 1024);
            cmt.registerOutParameter(2, Types.DOUBLE);
            cmt.registerOutParameter(3, Types.BIGINT);
            cmt.execute();

            // get result set
            Object pa = cmt.getObject(1);
            double pb = cmt.getDouble(2);
            long pc = cmt.getLong(3);
            assertEquals(null, pa);
            assertEquals(2.22d, pb, 0.0001d);
            assertEquals(2048, pc);
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }
}