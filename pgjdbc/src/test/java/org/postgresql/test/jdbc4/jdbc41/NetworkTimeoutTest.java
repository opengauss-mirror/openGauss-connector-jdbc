/*
 * Copyright (c) 2007, PostgreSQL Global Development Group
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

package org.postgresql.test.jdbc4.jdbc41;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.postgresql.test.TestUtil;

import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

public class NetworkTimeoutTest {
  @Test
  public void testSetNetworkTimeout() throws Exception {
    Connection _conn = TestUtil.openDB();
    try {
      _conn.setNetworkTimeout(null, 0);
    } catch (SQLException e) {
      fail("Connection.setNetworkTimeout() throw exception");
    } finally {
      TestUtil.closeDB(_conn);
    }
  }

  @Test
  public void testSetNetworkTimeoutInvalid() throws Exception {
    Connection _conn = TestUtil.openDB();
    try {
      _conn.setNetworkTimeout(null, -1);
      fail("Connection.setNetworkTimeout() did not throw expected exception");
    } catch (SQLException e) {
      // Passed
    } finally {
      TestUtil.closeDB(_conn);
    }
  }

  @Test
  public void testSetNetworkTimeoutValid() throws Exception {
    Connection _conn = TestUtil.openDB();
    try {
      _conn.setNetworkTimeout(null, (int) TimeUnit.SECONDS.toMillis(5));
      assertEquals(TimeUnit.SECONDS.toMillis(5), _conn.getNetworkTimeout());
    } catch (SQLException e) {
      fail("Connection.setNetworkTimeout() throw exception");
    } finally {
      TestUtil.closeDB(_conn);
    }
  }

  @Test
  public void testSetNetworkTimeoutEnforcement() throws Exception {
    Connection _conn = TestUtil.openDB();
    Statement stmt = null;
    try {
      _conn.setNetworkTimeout(null, (int) TimeUnit.SECONDS.toMillis(1));
      stmt = _conn.createStatement();
      stmt.execute("SELECT pg_sleep(2)");
      fail("Connection.setNetworkTimeout() did not throw expected exception");
    } catch (SQLException e) {
      // assertTrue(stmt.isClosed());
      assertTrue(_conn.isClosed());
    } finally {
      TestUtil.closeQuietly(stmt);
      TestUtil.closeDB(_conn);
    }
  }
}
