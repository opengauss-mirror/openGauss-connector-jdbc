/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
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
