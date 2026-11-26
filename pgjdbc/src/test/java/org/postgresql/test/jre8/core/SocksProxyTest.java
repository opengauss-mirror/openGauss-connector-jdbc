/*
 * Copyright (c) 2017, PostgreSQL Global Development Group
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * All rights reserved.
 *
 * Licensed under the 2-Clause BSD License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/license/bsd-2-clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.postgresql.test.jre8.core;

import static org.junit.Assert.assertNotNull;

import org.postgresql.test.TestUtil;

import org.junit.After;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * @author Joe Kutner on 10/9/17.
 *         Twitter: @codefinger
 */
public class SocksProxyTest {

  @After
  public void cleanup() {
    System.clearProperty("socksProxyHost");
    System.clearProperty("socksProxyPort");
    System.clearProperty("socksNonProxyHosts");
  }

  /**
   * Tests the connect method by connecting to the test database.
   */
  @Test
  public void testConnectWithSocksNonProxyHost() throws Exception {
    System.setProperty("socksProxyHost", "fake-socks-proxy");
    System.setProperty("socksProxyPort", "9999");
    System.setProperty("socksNonProxyHosts", TestUtil.getServer());

    TestUtil.initDriver(); // Set up log levels, etc.

    Connection con =
        DriverManager.getConnection(TestUtil.getURL(), TestUtil.getUser(), TestUtil.getPassword());

    assertNotNull(con);
    con.close();
  }
}
