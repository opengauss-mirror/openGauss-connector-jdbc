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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.test.TestUtil;
import org.postgresql.test.util.rules.annotation.HaveMinimalServerVersion;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

@HaveMinimalServerVersion("9.4")
public class ReplicationConnectionTest {

  private Connection replConnection;

  @Before
  public void setUp() throws Exception {
    replConnection = openReplicationConnection();
    //DriverManager.setLogWriter(new PrintWriter(System.out));
  }

  @After
  public void tearDown() throws Exception {
    replConnection.close();
  }

  @Test
  public void testIsValid() throws Exception {
    boolean result = replConnection.isValid(3);

    PgConnection connection = (PgConnection) replConnection;
    connection.getQueryExecutor().getBackendPID();

    assertThat("Replication connection as Simple connection can be check on valid",
        result, equalTo(true)
    );
  }

  @Test
  public void testReplicationCommandResultSetAccessByIndex() throws Exception {
    Statement statement = replConnection.createStatement();
    ResultSet resultSet = statement.executeQuery("IDENTIFY_SYSTEM");

    String xlogpos = null;
    if (resultSet.next()) {
      xlogpos = resultSet.getString(3);
    }

    resultSet.close();
    statement.close();

    assertThat("Replication protocol supports a limited number of commands, "
            + "and it command can be execute via Statement(simple query protocol), "
            + "and result fetch via ResultSet",
        xlogpos, CoreMatchers.notNullValue()
    );
  }

  @Test
  public void testReplicationCommandResultSetAccessByName() throws Exception {
    Statement statement = replConnection.createStatement();
    ResultSet resultSet = statement.executeQuery("IDENTIFY_SYSTEM");

    String xlogpos = null;
    if (resultSet.next()) {
      xlogpos = resultSet.getString("xlogpos");
    }

    resultSet.close();
    statement.close();

    assertThat("Replication protocol supports a limited number of commands, "
            + "and it command can be execute via Statement(simple query protocol), "
            + "and result fetch via ResultSet",
        xlogpos, CoreMatchers.notNullValue()
    );
  }

  private Connection openReplicationConnection() throws Exception {
    Properties properties = new Properties();
    PGProperty.ASSUME_MIN_SERVER_VERSION.set(properties, "9.4");
    PGProperty.REPLICATION.set(properties, "database");
    //Only symple query protocol available for replication connection
    PGProperty.PREFER_QUERY_MODE.set(properties, "simple");
    return TestUtil.openDB(properties);
  }
}
