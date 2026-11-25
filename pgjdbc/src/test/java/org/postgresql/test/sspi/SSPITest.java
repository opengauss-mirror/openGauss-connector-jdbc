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

package org.postgresql.test.sspi;

import org.junit.BeforeClass;
import org.junit.Test;
import org.postgresql.test.TestUtil;
import org.postgresql.util.PSQLException;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

/*
 * These tests require a working SSPI authentication setup
 * in the database server that allows the executing user
 * to authenticate as the "sspiusername" in the build
 * configuration.
 */
public class SSPITest {
    private static final String INVALID_AUTHORIZATION_SPECIFICATION_STATE = "28P01";

  /*
   * SSPI only exists on Windows.
   */
  @BeforeClass
  public static void checkPlatform() {
    assumeThat("SSPI not supported on this platform",
               System.getProperty("os.name").toLowerCase(),
               containsString("windows"));
  }

  /*
   * Tests that SSPI login succeeds and a query can be run.
   */
  @Test
  public void testAuthorized() throws Exception {
    Properties props = new Properties();
    props.setProperty("username", TestUtil.getSSPIUser());
    Connection con = TestUtil.openDB(props);

    Statement stmt = con.createStatement();
    stmt.executeQuery("SELECT 1");

    TestUtil.closeDB(con);
  }

  /*
   * Tests that SSPI login fails with an unknown/unauthorized
   * user name.
   */
  @Test
  public void testUnauthorized() throws Exception {
    Properties props = new Properties();
    props.setProperty("username", "invalid" + TestUtil.getSSPIUser());

    try {
      Connection con = TestUtil.openDB(props);
      TestUtil.closeDB(con);
      fail("Expected a PSQLException");
    } catch (PSQLException e) {
      assertThat(e.getSQLState(), is(INVALID_AUTHORIZATION_SPECIFICATION_STATE));
    }
  }

}
