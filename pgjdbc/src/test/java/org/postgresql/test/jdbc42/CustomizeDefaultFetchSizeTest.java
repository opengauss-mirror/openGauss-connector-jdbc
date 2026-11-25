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

package org.postgresql.test.jdbc42;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

import org.postgresql.PGProperty;
import org.postgresql.test.TestUtil;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Test;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class CustomizeDefaultFetchSizeTest {

  private Connection connection;

  @After
  public void tearDown() throws Exception {
    if (connection != null) {
      TestUtil.closeDB(connection);
    }
  }

  @Test
  public void testSetPredefineDefaultFetchSizeOnStatement() throws Exception {
    final int waitFetchSize = 13;
    Properties properties = new Properties();
    PGProperty.DEFAULT_ROW_FETCH_SIZE.set(properties, waitFetchSize);

    connection = TestUtil.openDB(properties);

    Statement statement = connection.createStatement();
    int resultFetchSize = statement.getFetchSize();

    statement.close();

    assertThat(
        "PGProperty.DEFAULT_ROW_FETCH_SIZE should be propagate to Statement that was create from connection "
            + "on that define it parameter",
        resultFetchSize, CoreMatchers.equalTo(waitFetchSize));
  }


  @Test
  public void testSetPredefineDefaultFetchSizeOnPreparedStatement() throws Exception {
    final int waitFetchSize = 14;

    Properties properties = new Properties();
    PGProperty.DEFAULT_ROW_FETCH_SIZE.set(properties, waitFetchSize);

    connection = TestUtil.openDB(properties);

    CallableStatement statement = connection.prepareCall("{ call unnest(array[1, 2, 3, 5])}");
    int resultFetchSize = statement.getFetchSize();

    assertThat(
        "PGProperty.DEFAULT_ROW_FETCH_SIZE should be propagate to CallableStatement that was create from connection "
            + "on that define it parameter",
        resultFetchSize, CoreMatchers.equalTo(waitFetchSize));
  }

  @Test(expected = SQLException.class)
  public void testNotAvailableSpecifyNegativeFetchSize() throws Exception {
    Properties properties = new Properties();
    PGProperty.DEFAULT_ROW_FETCH_SIZE.set(properties, Integer.MIN_VALUE);

    connection = TestUtil.openDB(properties);

    fail(
        "On step initialize connection we know about not valid parameter PGProperty.DEFAULT_ROW_FETCH_SIZE they can't be negative, "
            + "so we should throw correspond exception about it rather than fall with exception in runtime for example during create statement");
  }
}
