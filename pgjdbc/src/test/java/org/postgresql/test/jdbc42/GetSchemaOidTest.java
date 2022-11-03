/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc42;

import org.junit.Assert;
import org.junit.Test;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.test.TestUtil;
import org.postgresql.test.jdbc2.BaseTest4;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class GetSchemaOidTest extends BaseTest4 {
 private static String schema1 = "schema1";
 private static String schema2 = "schema2";
 private static String commonTypeName = "mytype";
 private static String myType1 = schema1 + "." + commonTypeName;
 private static String myType2 = schema2 + "." + commonTypeName;
 private static int createFlag = 0;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    createFlag = prepareSchemaAndType();
  }

  @Override
  public void tearDown() throws SQLException {
    clearSchemaAndType(createFlag);
    super.tearDown();
  }

  private int prepareSchemaAndType() {
    int createFlag = 0;
    try {
      TestUtil.createSchema(con, schema1);
      createFlag |= 0x1;
      TestUtil.createSchema(con, schema2);
      createFlag |= 0x2;
      TestUtil.createCompositeType(con, myType1, "f1 int, f2 varchar(100)");
      createFlag |= 0x4;
      TestUtil.createCompositeType(con, myType2, "f3 int, f4 varchar(100)");
      createFlag |= 0x8;
    } catch (SQLException sqlException) {
      createFlag |= 0x10;
    }
    return createFlag;
  }
  
  private void clearSchemaAndType(int createFlag) {
    Consumer<String> dropTypeFunc = arg -> {
      try {
        TestUtil.dropType(con, arg);
      } catch (SQLException sqlExp) {
        sqlExp.printStackTrace();
        System.out.println("clear mytype with exp:" + sqlExp);
      }
    };
  
    Consumer<String> dropSchemaFunc = arg -> {
      try {
        TestUtil.dropSchema(con, arg);
      } catch (SQLException sqlException) {
        sqlException.printStackTrace();
        System.out.println("clear schema with exp:" + sqlException);
      }
    };
    List<String> clearObjs = Arrays.stream(new String[]{myType2, myType1, schema2, schema1}).collect(Collectors.toList());
    int moveBit = clearObjs.size();
    Consumer<String> optFunc = dropTypeFunc;
    for (int i = 0; i < moveBit; i++) {
      if (i > 1) {
        optFunc = dropSchemaFunc;
      }
      if ((createFlag >> (moveBit - i -1) & 0x1) != 0) {
        optFunc.accept(clearObjs.get(i));
      }
    }
  }
  /**
   * Test schema change to find self type oid.
   * @throws SQLException if any database exception
   */
  @Test
  public void testType() throws SQLException {
      if ((createFlag & 0x10) != 0) {
        Assert.fail("create type or schema failed");
      }
      Map<String, Integer> schemaToOid = getPgTypes(commonTypeName);
      PgConnection curCon = con.unwrap(PgConnection.class);
      con.setSchema(schema1);
      int typeOid = curCon.getTypeInfo().getPGType(commonTypeName);
      Assert.assertEquals(schemaToOid.get(schema1).intValue(), typeOid);
      
      // because already cache commonTypeName, so change schema with no influence.
      con.setSchema(schema2);
      int typeOid2 = curCon.getTypeInfo().getPGType(commonTypeName);
      Assert.assertEquals(typeOid, typeOid2);
  }
    
    /**
     * Test schema in different connection
     * @throws Exception if any database exception
     */
  @Test
  public void testTypeNewConn() throws Exception {
    if ((createFlag & 0x10) != 0) {
      Assert.fail("create type or schema failed");
    }
    Map<String, Integer> schemaToOid = getPgTypes(commonTypeName);
    PgConnection curCon = con.unwrap(PgConnection.class);
    con.setSchema(schema1);
    int typeOid = curCon.getTypeInfo().getPGType(commonTypeName);
    Assert.assertEquals(schemaToOid.get(schema1).intValue(), typeOid);
    Properties props = new Properties();
    updateProperties(props);
    try (Connection newConnect = TestUtil.openDB(props)) {
      PgConnection connNew = newConnect.unwrap(PgConnection.class);
      connNew.setSchema(schema2);
      int typeOid2 = connNew.getTypeInfo().getPGType(commonTypeName);
      Assert.assertEquals(schemaToOid.get(schema2).intValue(), typeOid2);
    }
  }
    
    @Test
    public void testComplexArrayType() throws Exception {
        if ((createFlag & 0x10) != 0) {
            Assert.fail("create type or schema failed");
        }
        // because array type name changed from type[] to _type, so getOidStatementComplexArray in getOidStatement
        // can't work, there is no need to testcase.
    }
    
  private Map<String, Integer> getPgTypes(String commonTypeName) throws SQLException {
    String sql = "select t.oid,n.nspname from pg_type t, pg_namespace n where t.typnamespace=n.oid and t.typname = ?";
    Map<String, Integer> results = new HashMap<>();
    try (PreparedStatement ps = con.prepareStatement(sql)) {
      ps.setString(1, commonTypeName);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
            Integer oid = rs.getInt(1);
            String typNamespace = rs.getString(2);
            results.put(typNamespace, oid);
        }
      }
      return results;
    }
  }
}
