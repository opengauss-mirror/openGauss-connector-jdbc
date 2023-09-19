/*
 * Copyright (c) 2023, openGauss Global Development Group
 * See the LICENSE file in the project root for more information.
 */
package org.postgresql.test.jdbc2;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.postgresql.test.TestUtil;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class PGStructSpecialSymbolsTest extends BaseTest4 {
    private String schemaName = getSchemaByRandom();
    private String setPathSql = "SET search_path TO ";
    private static List<String> specialList = Arrays.asList("\"", ",", "123,abc",
            "\\", "123\\abc", "\\\\", "123\\\\abc", "\\\\\\'", "123\\\\\\'abc",
            "'", "123'abc", "\")", "(,)", "(        ,   )");

    @Parameterized.Parameter
    public String addressStr;

    @Parameterized.Parameters
    public static Collection data() {
        List<Object[]> data = new ArrayList<>();
        for (String special : specialList) {
            Object[] objects = new Object[]{special};
            data.add(objects);
        }
        return data;
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        TestUtil.createSchema(con, schemaName);
        TestUtil.execute(setPathSql + schemaName, con);

        TestUtil.execute("set behavior_compat_options='proc_outparam_override'", con);
        // by 2 level
        TestUtil.createCompositeType(con, "addr_object_type", "street VARCHAR(30),city VARCHAR(20), "
                + "state CHAR(2),  zip int");
        TestUtil.createCompositeType(con, "emp_obj_typ", "empno int, ename VARCHAR(20),addr addr_object_type");
    }

    @After
    public void tearDown() throws SQLException {
        // by 2 level
        TestUtil.dropType(con, "addr_object_type");
        TestUtil.dropType(con, "emp_obj_typ");

        // drop schema
        TestUtil.dropSchema(con, schemaName);
    }


    @Test
    public void testSpecialSymbols() throws SQLException {
        Statement stmt = null;
        CallableStatement cmt = null;
        try {
            String procedureSql = "create or replace PROCEDURE display_emp (a INOUT emp_obj_typ)\n"
                    + "  IS\n"
                    + "  BEGIN\n"
                    + "    a.empno:=1010;\n"
                    + "  END;";
            stmt = con.createStatement();
            stmt.execute(procedureSql);

            Struct address = con.createStruct("addr_object_type", new Object[]{addressStr, "EDISON", "NJ", 8817});
            Struct emp = con.createStruct("emp_obj_typ", new Object[]{9001, "JONES", address});

            String commandText = "{call display_emp(?)}";
            cmt = con.prepareCall(commandText);
            cmt.registerOutParameter(1, Types.STRUCT, schemaName + ".emp_obj_typ");
            cmt.setObject(1, emp);
            cmt.execute();

            emp = (Struct) cmt.getObject(1);
            Object[] attrEmp = emp.getAttributes();
            assertEquals(1010, attrEmp[0]);
            assertEquals("JONES", attrEmp[1]);
            address = (Struct) attrEmp[2];
            Object[] attrAddress = address.getAttributes();
            assertEquals(addressStr, attrAddress[0]);
            assertEquals("EDISON", attrAddress[1]);
            assertEquals("NJ", attrAddress[2]);
            assertEquals(8817, attrAddress[3]);
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cmt);
        }
    }
}