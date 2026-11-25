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

package org.postgresql.test.jdbc2;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.postgresql.test.TestUtil;

import org.junit.Before;
import org.junit.Test;

import java.net.SocketTimeoutException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;


public class ConnectTimeoutTest {
  // The IP below is non-routable (see http://stackoverflow.com/a/904609/1261287)
  private static final String UNREACHABLE_HOST = "10.255.255.1";
  private static final String UNREACHABLE_URL = "jdbc:postgresql://" + UNREACHABLE_HOST + ":5432/test";
  private static final int CONNECT_TIMEOUT = 5;

  @Before
  public void setUp() throws Exception {
    TestUtil.initDriver();
  }

  @Test
  public void testTimeout() {
    final Properties props = new Properties();
    props.setProperty("user", "test");
    props.setProperty("password", "test");
    // with 0 (default value) it hangs for about 60 seconds (platform dependent)
    props.setProperty("connectTimeout", Integer.toString(CONNECT_TIMEOUT));

    final long startTime = System.currentTimeMillis();
    try {
      DriverManager.getConnection(UNREACHABLE_URL, props);
    } catch (SQLException e) {
      assertTrue("Unexpected " + e.toString(),
          e.getCause() instanceof SocketTimeoutException);
      final long interval = System.currentTimeMillis() - startTime;
      final long connectTimeoutMillis = CONNECT_TIMEOUT * 1000;
      final long maxDeviation = (connectTimeoutMillis + 2000) / 10;
      // check that it was not a default system timeout, an approximate value is used
      assertTrue(Math.abs(interval - connectTimeoutMillis) < maxDeviation);
      return;
    }
    fail("SQLException expected");
  }
}
