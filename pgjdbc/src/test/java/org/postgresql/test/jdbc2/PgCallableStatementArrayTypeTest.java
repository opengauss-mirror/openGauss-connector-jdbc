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
import org.postgresql.jdbc.PGStruct;
import org.postgresql.test.TestUtil;
import org.postgresql.util.PGobject;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class PgCallableStatementArrayTypeTest extends BaseTest4 {
    private String schemaName = "arraytype";
    private String setPathSql = "SET search_path TO ";

    private final String structName;
    private final String inArrayName;
    private final String outArrayName;

    public PgCallableStatementArrayTypeTest(String structName, String inArrayName, String outArrayName) {
        this.structName = structName;
        this.inArrayName = inArrayName;
        this.outArrayName = outArrayName;
    }

    @Parameterized.Parameters(name = "in_struct = {0}, in_array = {1}, out_array = {2}")
    public static Iterable<Object[]> data() {
        Collection<Object[]> ids = new ArrayList<>();
        ids.add(new String[]{"ty_test", "ty_test", "ty_test"});
        ids.add(new String[]{"ty_test", "tyt_test", "tyt_test"});
        ids.add(new String[]{"ty_test", "ty_test", "arraytype.ty_test"});
        ids.add(new String[]{"ty_test", "tyt_test", "arraytype.tyt_test"});
        ids.add(new String[]{"ty_test", "arraytype.ty_test", "arraytype.ty_test"});
        ids.add(new String[]{"arraytype.ty_test", "arraytype.ty_test", "arraytype.ty_test"});
        ids.add(new String[]{"arraytype.ty_test", "arraytype.tyt_test", "arraytype.tyt_test"});
        ids.add(new String[]{"arraytype.ty_test", "\"arraytype\".\"tyt_test\"", "arraytype.tyt_test"});
        return ids;
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        TestUtil.createSchema(con, schemaName);
        TestUtil.execute(setPathSql + schemaName, con);
        TestUtil.createCompositeType(con, "ty_test", "col1 int,col2 char(10),col3 varchar2(10)");
        TestUtil.execute("create type tyt_test is table of ty_test", con);
        TestUtil.execute("CREATE OR REPLACE PROCEDURE sp_tyt(i_tyt in tyt_test, o_tyt out tyt_test)\n" +
                "AS  DECLARE \n" +
                "begin \n" +
                "   o_tyt := i_tyt;\n" +
                "end;", con);
    }

    @After
    public void tearDown() throws SQLException {
        TestUtil.dropType(con, "tyt_app");
        TestUtil.dropType(con, "ty_app");
        TestUtil.dropSchema(con, schemaName);
    }

    /*****************************************************************
     * 描述：测试array type in/out
     ******************************************************************/
    @Test
    public void testOutListType() throws SQLException {
        CallableStatement cmt = null;
        try {
            Struct tyApp1 = con.createStruct(this.structName, new Object[]{"1", "MAIN", "8817"});
            Array array = con.createArrayOf(this.inArrayName, new Object[]{tyApp1});
            String commandText = "{call sp_tyt(?,?)}";
            cmt = con.prepareCall(commandText);
            cmt.setArray(1, array);
            cmt.registerOutParameter(2, Types.ARRAY, this.outArrayName);
            cmt.execute();
            Array array1 = cmt.getArray(2);
            ResultSet rs = array1.getResultSet();
            while (rs.next()) {
                Object o = rs.getObject(2);
                PGStruct s = (PGStruct) o;
                Object[] attrEmp = s.getAttributes();
                assertEquals(1, attrEmp[0]);
                assertEquals("MAIN      ", attrEmp[1]);
                assertEquals("8817", attrEmp[2]);
                PGobject p = (PGobject) o;
                assertEquals("(1,\"MAIN      \",8817)", p.getValue());
                assertEquals("[col1, col2, col3]", Arrays.toString(p.getStruct()));
            }
            array1.free();
            TestUtil.execute("set behavior_compat_options=''", con);
            commandText = "{call sp_tyt(?,?)}";
            cmt = con.prepareCall(commandText);
            cmt.setArray(1, array);
            cmt.registerOutParameter(2, Types.ARRAY);
            cmt.execute();
            array1 = cmt.getArray(2);
            Object[] retObj = (Object[]) array1.getArray();
            for (Object o : retObj) {
                Struct pgo = (Struct) o;
                Object[] attrEmp = pgo.getAttributes();
                assertEquals(1, attrEmp[0]);
                assertEquals("MAIN      ", attrEmp[1]);
                assertEquals("8817", attrEmp[2]);
            }
            TestUtil.execute("set behavior_compat_options='proc_outparam_override'", con);
        } catch (SQLException e) {
            fail(e.getMessage());
        } finally {
            TestUtil.closeQuietly(cmt);
        }
    }
}
