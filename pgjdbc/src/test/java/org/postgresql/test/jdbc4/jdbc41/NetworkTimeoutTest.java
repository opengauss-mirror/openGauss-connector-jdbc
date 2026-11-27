/*
 * Copyright (c) 2007, PostgreSQL Global Development Group
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
