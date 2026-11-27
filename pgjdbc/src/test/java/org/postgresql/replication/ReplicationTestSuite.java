/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
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

package org.postgresql.replication;

import org.postgresql.core.ServerVersion;
import org.postgresql.test.TestUtil;
import org.postgresql.test.jdbc2.CopyBothResponseTest;

import org.junit.AssumptionViolatedException;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    CopyBothResponseTest.class,
    LogicalReplicationTest.class,
    LogSequenceNumberTest.class,
    PhysicalReplicationTest.class,
    LogicalReplicationStatusTest.class,
    ReplicationConnectionTest.class,
    ReplicationSlotTest.class})
public class ReplicationTestSuite {

  @BeforeClass
  public static void setUp() throws Exception {
    Connection connection = TestUtil.openDB();
    try {
      if (TestUtil.haveMinimumServerVersion(connection, ServerVersion.v9_0)) {
        assumeWalSenderEnabled(connection);
        assumeReplicationRole(connection);
      } else {
        throw new AssumptionViolatedException(
            "Skip replication test because current database version "
                + "too old and don't contain replication API"
        );
      }
    } finally {
      connection.close();
    }
  }

  private static void assumeWalSenderEnabled(Connection connection) throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("SHOW max_wal_senders");
    rs.next();
    int maxWalSenders = rs.getInt(1);
    rs.close();
    stmt.close();

    if (maxWalSenders == 0) {
      throw new AssumptionViolatedException(
          "Skip replication test because max_wal_senders = 0");
    }
  }

  private static void assumeReplicationRole(Connection connection) throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs =
        stmt.executeQuery("SELECT usename, userepl FROM pg_user WHERE usename = current_user");
    rs.next();
    String userName = rs.getString(1);
    boolean replicationGrant = rs.getBoolean(2);
    rs.close();
    stmt.close();

    if (!replicationGrant) {
      throw new AssumptionViolatedException(
          "Skip replication test because user '" + userName + "' doesn't have replication role");
    }
  }
}
