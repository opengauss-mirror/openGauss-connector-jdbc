/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
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

package org.postgresql.test.socketfactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.postgresql.PGProperty;
import org.postgresql.test.TestUtil;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.util.Properties;

public class SocketFactoryTestSuite {

  private static final String STRING_ARGUMENT = "name of a socket";

  private Connection _conn;

  @Before
  public void setUp() throws Exception {
    Properties properties = new Properties();
    properties.put(PGProperty.SOCKET_FACTORY.getName(), CustomSocketFactory.class.getName());
    properties.put(PGProperty.SOCKET_FACTORY_ARG.getName(), STRING_ARGUMENT);
    _conn = TestUtil.openDB(properties);
  }

  @After
  public void tearDown() throws Exception {
    TestUtil.closeDB(_conn);
  }

  /**
   * Test custom socket factory.
   */
  @Test
  public void testDatabaseMetaData() throws Exception {
    assertNotNull("Custom socket factory not null", CustomSocketFactory.getInstance());
    assertEquals(STRING_ARGUMENT, CustomSocketFactory.getInstance().getArgument());
    assertEquals(1, CustomSocketFactory.getInstance().getSocketCreated());
  }

}
