/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.
 */

package org.postgresql.test.jdbc2.optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import org.postgresql.ds.common.BaseDataSource;
import org.postgresql.jdbc2.optional.PoolingDataSource;

import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Minimal tests for pooling DataSource. Needs many more.
 *
 * @author Aaron Mulder (ammulder@chariotsolutions.com)
 */
public class PoolingDataSourceTest extends BaseDataSourceTest {
  private static final String DS_NAME = "JDBC 2 SE Test DataSource";

  @Override
  public void tearDown() throws Exception {
    if (bds instanceof PoolingDataSource) {
      ((PoolingDataSource) bds).close();
    }
    super.tearDown();
  }

  /**
   * Creates and configures a new SimpleDataSource.
   */
  @Override
  protected void initializeDataSource() {
    if (bds == null) {
      bds = new PoolingDataSource();
      setupDataSource(bds);
      ((PoolingDataSource) bds).setDataSourceName(DS_NAME);
      ((PoolingDataSource) bds).setInitialConnections(2);
      ((PoolingDataSource) bds).setMaxConnections(10);
    }
  }

  /**
   * In this case, we *do* want it to be pooled.
   */
  @Override
  public void testNotPooledConnection() throws SQLException {
    con = getDataSourceConnection();
    String name = con.toString();
    con.close();
    con = getDataSourceConnection();
    String name2 = con.toString();
    con.close();
    assertEquals("Pooled DS doesn't appear to be pooling connections!", name, name2);
  }

  /**
   * In this case, the desired behavior is dereferencing.
   */
  @Override
  protected void compareJndiDataSource(BaseDataSource oldbds, BaseDataSource bds) {
    assertSame("DataSource was serialized or recreated, should have been dereferenced",
        bds, oldbds);
  }

  /**
   * Check that 2 DS instances can't use the same name.
   */
  @Test
  public void testCantReuseName() {
    initializeDataSource();
    PoolingDataSource pds = new PoolingDataSource();
    try {
      pds.setDataSourceName(DS_NAME);
      fail("Should have denied 2nd DataSource with same name");
    } catch (IllegalArgumentException e) {
    }
  }

  /**
   * Closing a Connection twice is not an error.
   */
  @Test
  public void testDoubleConnectionClose() throws SQLException {
    con = getDataSourceConnection();
    con.close();
    con.close();
  }

  /**
   * Closing a Statement twice is not an error.
   */
  @Test
  public void testDoubleStatementClose() throws SQLException {
    con = getDataSourceConnection();
    Statement stmt = con.createStatement();
    stmt.close();
    stmt.close();
    con.close();
  }

  @Test
  public void testConnectionObjectMethods() throws SQLException {
    con = getDataSourceConnection();

    Connection conRef = con;
    assertEquals(con, conRef);

    int hc1 = con.hashCode();
    con.close();
    int hc2 = con.hashCode();

    assertEquals(con, conRef);
    assertEquals(hc1, hc2);
  }

  @Test
  public void testStatementObjectMethods() throws SQLException {
    con = getDataSourceConnection();

    Statement stmt = con.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT 1");
    Statement stmtRef = stmt;

    assertEquals(stmt, stmtRef);
    // Currently we aren't proxying ResultSet, so this doesn't
    // work, see Bug #1010542.
    // assertEquals(stmt, rs.getStatement());

    int hc1 = stmt.hashCode();
    stmt.close();
    int hc2 = stmt.hashCode();

    assertEquals(stmt, stmtRef);
    assertEquals(hc1, hc2);
  }

}
